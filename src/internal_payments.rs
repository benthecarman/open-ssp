//! Reservations arbitrate local payments against external HTLC settlement.
use crate::{db::Db, lightning_store::LightningSend};
use rusqlite::{params, OptionalExtension};

pub fn migrate(c: &rusqlite::Connection) -> rusqlite::Result<()> {
    c.execute_batch("CREATE TABLE IF NOT EXISTS internal_payments(hash TEXT PRIMARY KEY REFERENCES lightning_receives(hash),send_id TEXT NOT NULL UNIQUE REFERENCES lightning_sends(request_id));")
}
impl Db {
    pub async fn internal_send(&self, hash: &str) -> Result<Option<String>, String> {
        self.with(|c| {
            c.query_row(
                "SELECT send_id FROM internal_payments WHERE hash=?1",
                [hash],
                |r| r.get(0),
            )
            .optional()
        })
        .await
    }
    /// Called under the receive lock. The SQL predicate also excludes a
    /// claimable HTLC recorded before this process acquired that lock.
    pub async fn reserve_internal_send(&self, send: &LightningSend) -> Result<(), String> {
        self.with(|c| {
            let tx=c.unchecked_transaction()?;
            let existing:Option<String>=tx.query_row("SELECT send_id FROM internal_payments WHERE hash=?1",[&send.expected_id],|r|r.get(0)).optional()?;
            if let Some(existing)=existing {
                if existing==send.request_id { return tx.commit(); }
                return Err(rusqlite::Error::ToSqlConversionFailure("local invoice already has a payer".into()));
            }
            let changed=tx.execute("INSERT INTO internal_payments(hash,send_id) SELECT l.hash,?1 FROM lightning_receives l LEFT JOIN receive_payments p ON p.hash=l.hash WHERE l.hash=?2 AND l.invoice=?3 AND l.amount_sats=?4 AND l.expires_at>?5 AND COALESCE(p.status,'INVOICE_CREATED')='INVOICE_CREATED' AND p.claimable_amount_msat IS NULL AND p.transfer_id IS NULL",params![send.request_id,send.expected_id,send.invoice,send.amount_sats,chrono::Utc::now().timestamp()])?;
            if changed!=1 { return Err(rusqlite::Error::ToSqlConversionFailure("local invoice is expired or already in progress".into())); }
            let intent=tx.execute("UPDATE lightning_sends SET status='SETTLING' WHERE request_id=?1",[&send.request_id])?;
            if intent!=1 { return Err(rusqlite::Error::ToSqlConversionFailure("send intent is missing".into())); }
            tx.commit()
        }).await
    }
    pub async fn finish_internal_send(&self, send: &LightningSend) -> Result<(), String> {
        self.with(|c| {
            let tx=c.unchecked_transaction()?;
            let intent=tx.execute("UPDATE lightning_sends SET status='SUCCEEDED',last_error=NULL WHERE request_id=?1",[&send.request_id])?;
            let payment=tx.execute("UPDATE receive_payments SET status='TRANSFER_COMPLETED' WHERE hash=?1",[&send.expected_id])?;
            let transfer=tx.execute("UPDATE transfers SET status='TRANSFER_COMPLETED' WHERE request_id=(SELECT request_id FROM lightning_receives WHERE hash=?1)",[&send.expected_id])?;
            // Every row is created by the checkpoints above, so a count other
            // than one means the settlement is incomplete and must not be
            // reported as SUCCEEDED. The transaction rolls back as a unit.
            if intent!=1||payment!=1||transfer!=1 {
                return Err(rusqlite::Error::ToSqlConversionFailure("internal settlement rows are incomplete".into()));
            }
            tx.commit()
        }).await
    }
}
