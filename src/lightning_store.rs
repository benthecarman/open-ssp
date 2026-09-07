//! Durable Lightning intents and explicit request/funding relationships.
use crate::db::Db;
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::str::FromStr;

macro_rules! sql_enum {
    ($name:ident { $($variant:ident => $text:literal),+ $(,)? }) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
        pub enum $name { $(#[serde(rename=$text)] $variant),+ }
        impl $name {
            pub fn as_str(self) -> &'static str { match self { $(Self::$variant => $text),+ } }
        }
        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str(self.as_str()) }
        }
        impl FromStr for $name {
            type Err = String;
            fn from_str(value: &str) -> Result<Self, String> {
                match value { $($text => Ok(Self::$variant)),+, _ => Err(format!("invalid {}: {value}", stringify!($name))) }
            }
        }
        impl rusqlite::types::FromSql for $name {
            fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
                value.as_str()?.parse().map_err(|e: String| rusqlite::types::FromSqlError::Other(e.into()))
            }
        }
        impl rusqlite::ToSql for $name {
            fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
                Ok(rusqlite::types::ToSqlOutput::Borrowed(rusqlite::types::ValueRef::Text(self.as_str().as_bytes())))
            }
        }
    }
}
sql_enum!(SendKind { Bolt11 => "BOLT11", Bolt12 => "BOLT12" });
sql_enum!(SendStatus {
    Prepared => "PREPARED", Submitting => "SUBMITTING", Pending => "PENDING",
    Settling => "SETTLING", Refunding => "REFUNDING", Succeeded => "SUCCEEDED", Failed => "FAILED"
});
sql_enum!(ReceiveStatus {
    InvoiceCreated => "INVOICE_CREATED", HtlcReceived => "HTLC_RECEIVED", HtlcFailed => "HTLC_FAILED",
    TransferCreated => "TRANSFER_CREATED", TransferCompleted => "TRANSFER_COMPLETED",
    TransferCreationFailed => "TRANSFER_CREATION_FAILED", PreimageRecovered => "PAYMENT_PREIMAGE_RECOVERED",
    PreimageRecoveryFailed => "PAYMENT_PREIMAGE_RECOVERING_FAILED", LightningReceived => "LIGHTNING_PAYMENT_RECEIVED"
});

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LightningSend {
    pub request_id: String,
    pub owner: String,
    pub outbound_transfer_id: String,
    pub invoice: String,
    pub amount_sats: u64,
    pub amount_override: Option<u64>,
    pub kind: SendKind,
    pub expected_id: String,
    pub payment_id: Option<String>,
    pub status: SendStatus,
}
impl LightningSend {
    pub fn payer_note(&self) -> String {
        format!("open-ssp:{}", self.request_id)
    }
}
fn send_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<LightningSend> {
    Ok(LightningSend {
        request_id: row.get(0)?,
        owner: row.get(1)?,
        outbound_transfer_id: row.get(2)?,
        invoice: row.get(3)?,
        amount_sats: row.get(4)?,
        amount_override: row.get(5)?,
        kind: row.get(6)?,
        expected_id: row.get(7)?,
        payment_id: row.get(8)?,
        status: row.get(9)?,
    })
}
const SEND_COLUMNS: &str = "request_id,owner,outbound_transfer_id,invoice,amount_sats,amount_override,kind,expected_id,payment_id,status";

pub fn migrate(conn: &rusqlite::Connection) -> Result<(), String> {
    // Removal must not silently strand an old SSP-owned receive or internal send.
    let internal: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM requests r LEFT JOIN payments p ON p.id=json_extract(r.payload,'$.payment_id') WHERE r.kind='LIGHTNING_SEND' AND json_extract(r.payload,'$.payment_kind')='INTERNAL_BOLT11' AND COALESCE(p.status,'PENDING') NOT IN ('SUCCEEDED','FAILED'))", [], |r| r.get(0)).map_err(|e| e.to_string())?;
    let old_secrets: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='preimages')",
            [],
            |r| r.get(0),
        )
        .map_err(|e| e.to_string())?;
    let pending_owned: bool = if old_secrets {
        conn.query_row("SELECT EXISTS(SELECT 1 FROM preimages s JOIN requests r ON r.kind='LIGHTNING_RECEIVE' AND json_extract(r.payload,'$.payment_hash')=s.hash LEFT JOIN receive_payments p ON p.hash=s.hash WHERE COALESCE(p.status,'INVOICE_CREATED') NOT IN ('TRANSFER_COMPLETED','HTLC_FAILED'))", [], |r| r.get(0)).map_err(|e| e.to_string())?
    } else {
        false
    };
    if internal || pending_owned {
        return Err("pending retired Lightning extension requests: run the previous release until SSP-owned receives and internal sends finish before upgrading".into());
    }
    let tx = conn.unchecked_transaction().map_err(|e| e.to_string())?;
    tx.execute_batch("CREATE TABLE IF NOT EXISTS lightning_sends(
        request_id TEXT PRIMARY KEY REFERENCES requests(id), owner TEXT NOT NULL,
        outbound_transfer_id TEXT NOT NULL UNIQUE, invoice TEXT NOT NULL, amount_sats INTEGER NOT NULL,
        amount_override INTEGER, kind TEXT NOT NULL CHECK(kind IN ('BOLT11','BOLT12')), expected_id TEXT NOT NULL,
        payment_id TEXT UNIQUE, status TEXT NOT NULL CHECK(status IN ('PREPARED','SUBMITTING','PENDING','SETTLING','REFUNDING','SUCCEEDED','FAILED')),
        last_error TEXT);
        CREATE UNIQUE INDEX IF NOT EXISTS lightning_send_hash ON lightning_sends(expected_id) WHERE kind='BOLT11' AND expected_id<>'';
        CREATE TABLE IF NOT EXISTS lightning_receives(
        hash TEXT PRIMARY KEY, request_id TEXT NOT NULL UNIQUE REFERENCES requests(id), owner TEXT NOT NULL,
        receiver TEXT NOT NULL, amount_sats INTEGER NOT NULL, invoice TEXT NOT NULL, expires_at INTEGER NOT NULL,
        kind TEXT NOT NULL CHECK(kind IN ('BOLT11','BOLT12')), settled_payment_hash TEXT);
        CREATE INDEX IF NOT EXISTS lightning_receive_invoice ON lightning_receives(invoice);").map_err(|e| e.to_string())?;
    {
        let mut statement = tx.prepare("SELECT id,kind,owner,created_at,payload FROM requests WHERE kind IN ('LIGHTNING_SEND','LIGHTNING_RECEIVE') AND id NOT IN (SELECT request_id FROM lightning_sends UNION ALL SELECT request_id FROM lightning_receives)").map_err(|e| e.to_string())?;
        let rows = statement
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                ))
            })
            .map_err(|e| e.to_string())?;
        for row in rows {
            let (id, kind, owner, created, payload) = row.map_err(|e| e.to_string())?;
            index_request(&tx, &id, &kind, &owner, &created, &payload)
                .map_err(|e| e.to_string())?;
        }
    }
    tx.execute_batch("DROP TABLE IF EXISTS preimages;")
        .map_err(|e| e.to_string())?;
    tx.commit().map_err(|e| e.to_string())
}

// Conversion at the legacy/API boundary; settlement never extracts JSON fields.
pub fn index_request(
    conn: &rusqlite::Connection,
    id: &str,
    kind: &str,
    owner: &str,
    created: &str,
    payload: &str,
) -> rusqlite::Result<()> {
    if !matches!(kind, "LIGHTNING_SEND" | "LIGHTNING_RECEIVE") {
        return Ok(());
    }
    let p: Value = serde_json::from_str(payload)
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
    let text = |name: &str| p[name].as_str().unwrap_or("");
    let payment_kind = p["payment_kind"].as_str().unwrap_or("BOLT11");
    if kind == "LIGHTNING_RECEIVE" {
        if let Some(quote_id) = p["quote_transfer_id"].as_str() {
            conn.execute(
                "INSERT INTO receive_quote_uses(quote_id,payment_hash) VALUES(?1,?2)",
                (quote_id, text("payment_hash")),
            )?;
        }
        let expiry = chrono::DateTime::parse_from_rfc3339(created)
            .map(|t| t.timestamp())
            .unwrap_or(i64::MAX)
            .saturating_add(
                p["expiry_secs"]
                    .as_u64()
                    .unwrap_or(86_400)
                    .min(i64::MAX as u64) as i64,
            );
        conn.execute("INSERT INTO lightning_receives(hash,request_id,owner,receiver,amount_sats,invoice,expires_at,kind,settled_payment_hash) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![text("payment_hash"),id,owner,p["receiver_identity_pubkey"].as_str().unwrap_or(owner),p["amount_sats"].as_u64().unwrap_or(0),text("invoice"),expiry,payment_kind,p["settled_payment_hash"].as_str()])?;
    } else if payment_kind != "INTERNAL_BOLT11" {
        let old_payment = text("payment_id");
        let decoded = lightning_invoice::Bolt11Invoice::from_str(text("encoded_invoice")).ok();
        let expected = decoded
            .as_ref()
            .map(|i| i.payment_hash().to_string())
            .unwrap_or_default();
        let status: String = conn
            .query_row(
                "SELECT status FROM payments WHERE id=?1",
                [old_payment],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or_else(|| "PENDING".into());
        let status = if old_payment.starts_with("init-failed:") {
            SendStatus::Submitting
        } else {
            status
                .parse()
                .map_err(|e: String| rusqlite::Error::ToSqlConversionFailure(e.into()))?
        };
        conn.execute("INSERT INTO lightning_sends(request_id,owner,outbound_transfer_id,invoice,amount_sats,amount_override,kind,expected_id,payment_id,status) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",params![id,owner,text("user_outbound_transfer_external_id"),text("encoded_invoice"),p["amount_sats"].as_u64().or_else(||decoded.as_ref().and_then(|i|i.amount_milli_satoshis().map(|a|a.div_ceil(1000)))).unwrap_or(0),p["amount_sats"].as_u64(),payment_kind,expected,if old_payment.starts_with("init-failed:"){None}else{Some(old_payment)},status])?;
    }
    Ok(())
}

impl Db {
    pub async fn prepare_lightning_send(
        &self,
        send: &LightningSend,
        key: &str,
        network: &str,
    ) -> Result<Value, String> {
        let created = chrono::Utc::now().to_rfc3339();
        let payload = json!({
            "encoded_invoice": send.invoice,
            "amount_sats": send.amount_override,
            "total_amount_sats": send.amount_sats,
            "idempotency_key": key,
            "payment_kind": send.kind,
            "payment_id": send.request_id,
            "network": network,
            "user_outbound_transfer_external_id": send.outbound_transfer_id,
        });
        self.with(|c| {
            let tx = c.unchecked_transaction()?;
            tx.execute(
                "INSERT INTO requests(id,kind,owner,created_at,payload,idempotency_key)
                 VALUES(?1,'LIGHTNING_SEND',?2,?3,?4,?5)",
                params![
                    send.request_id,
                    send.owner,
                    created,
                    payload.to_string(),
                    key
                ],
            )?;
            tx.execute(
                "INSERT INTO lightning_sends(request_id,owner,outbound_transfer_id,
                    invoice,amount_sats,amount_override,kind,expected_id,status)
                 VALUES(?1,?2,?3,?4,?5,?6,?7,?8,'PREPARED')",
                params![
                    send.request_id,
                    send.owner,
                    send.outbound_transfer_id,
                    send.invoice,
                    send.amount_sats,
                    send.amount_override,
                    send.kind,
                    send.expected_id
                ],
            )?;
            let funding_kind = match send.kind {
                SendKind::Bolt11 => "PREIMAGE_SWAP",
                SendKind::Bolt12 => "BOLT12_FUNDING",
            };
            tx.execute(
                "INSERT INTO transfers(spark_id,request_id,kind,status,owner)
                 VALUES(?1,?2,?3,'PENDING',?4)",
                params![
                    send.outbound_transfer_id,
                    send.request_id,
                    funding_kind,
                    send.owner
                ],
            )?;
            tx.commit()
        })
        .await?;
        Ok(json!({
            "id": send.request_id, "type": "LIGHTNING_SEND",
            "owner_identity_pubkey": send.owner, "created_at": created, "payload": payload,
        }))
    }

    pub async fn lightning_send_for_payment(
        &self,
        id: &str,
    ) -> Result<Option<LightningSend>, String> {
        self.with(|c| {
            c.query_row(
                &format!(
                    "SELECT {SEND_COLUMNS} FROM lightning_sends
                          WHERE request_id=?1 OR payment_id=?1"
                ),
                [id],
                send_row,
            )
            .optional()
        })
        .await
    }

    pub async fn unresolved_lightning_sends(&self) -> Result<Vec<LightningSend>, String> {
        self.with(|c| {
            let mut stmt = c.prepare(&format!(
                "SELECT {SEND_COLUMNS} FROM lightning_sends
                 WHERE status NOT IN ('SUCCEEDED','FAILED')"
            ))?;
            let rows = stmt.query_map([], send_row)?;
            rows.collect()
        })
        .await
    }

    pub async fn begin_lightning_submission(&self, id: &str) -> Result<bool, String> {
        self.with(|c| {
            c.execute(
                "UPDATE lightning_sends SET status='SUBMITTING'
                 WHERE request_id=?1 AND status='PREPARED'",
                [id],
            )
            .map(|n| n == 1)
        })
        .await
    }

    pub async fn bind_lightning_payment(&self, id: &str, payment_id: &str) -> Result<(), String> {
        self.with(|c| {
            let changed = c.execute(
                "UPDATE lightning_sends SET payment_id=?2,
                    status=CASE WHEN status IN ('SUBMITTING','PREPARED')
                                THEN 'PENDING' ELSE status END,
                    last_error=NULL
                 WHERE request_id=?1 AND (payment_id IS NULL OR payment_id=?2)",
                (id, payment_id),
            )?;
            if changed == 1 {
                Ok(())
            } else {
                Err(rusqlite::Error::InvalidQuery)
            }
        })
        .await
    }

    pub async fn lightning_submission_error(&self, id: &str, error: &str) -> Result<(), String> {
        self.with(|c| {
            c.execute(
                "UPDATE lightning_sends SET last_error=?2 WHERE request_id=?1",
                (id, error),
            )
            .map(|_| ())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn database() -> (Db, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("ssp-intents-{}", uuid::Uuid::new_v4()));
        (Db::open(dir.to_str().unwrap()).unwrap(), dir)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn funding_and_idempotency_conflicts_roll_back_the_whole_intent() {
        let (db, dir) = database();
        let first = LightningSend {
            request_id: "first".into(),
            owner: "owner".into(),
            outbound_transfer_id: "funding".into(),
            invoice: "invoice".into(),
            amount_sats: 1000,
            amount_override: None,
            kind: SendKind::Bolt11,
            expected_id: "hash".into(),
            payment_id: None,
            status: SendStatus::Prepared,
        };
        let record = db
            .prepare_lightning_send(&first, "key", "REGTEST")
            .await
            .unwrap();
        assert_eq!(record["payload"]["idempotency_key"], "key");
        for conflict in ["funding", "hash", "key"] {
            let mut second = first.clone();
            second.request_id = conflict.into();
            if conflict != "funding" {
                second.outbound_transfer_id = "other-funding".into();
            }
            if conflict != "hash" {
                second.expected_id = "other-hash".into();
            }
            let key = if conflict == "key" {
                "key"
            } else {
                "other-key"
            };
            assert!(db
                .prepare_lightning_send(&second, key, "REGTEST")
                .await
                .is_err());
            assert!(db.get_request(conflict, "owner").await.unwrap().is_none());
            assert!(db
                .lightning_send_for_payment(conflict)
                .await
                .unwrap()
                .is_none());
            assert!(db
                .transfer_for_request(conflict, "owner")
                .await
                .unwrap()
                .is_none());
        }
        db.bind_lightning_payment("first", "payment").await.unwrap();
        db.set_payment("payment", "SUCCEEDED").await.unwrap();
        db.set_payment("payment", "PENDING").await.unwrap();
        db.bind_lightning_payment("first", "payment").await.unwrap();
        assert_eq!(db.payment_status("first").await.unwrap(), "SUCCEEDED");
        assert!(db
            .bind_lightning_payment("first", "other-payment")
            .await
            .is_err());
        assert!(db.set_payment("first", "typo").await.is_err());
        drop(db);
        let db = Db::open(dir.to_str().unwrap()).unwrap();
        assert_eq!(db.payment_status("payment").await.unwrap(), "SUCCEEDED");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn upgrade_preserves_pending_retired_receive_secrets() {
        let (db, dir) = database();
        db.with(|c| c.execute_batch("CREATE TABLE preimages(hash TEXT PRIMARY KEY, preimage TEXT);
            INSERT INTO preimages VALUES('hash','secret');
            INSERT INTO requests(id,kind,owner,created_at,payload) VALUES('request','LIGHTNING_RECEIVE','owner','2026-01-01T00:00:00Z','{\"payment_hash\":\"hash\",\"invoice\":\"invoice\",\"amount_sats\":1000}');")).await.unwrap();
        drop(db);
        let error = Db::open(dir.to_str().unwrap()).err().unwrap();
        assert!(error.contains("previous release"));
        let conn = rusqlite::Connection::open(dir.join("ssp.sqlite")).unwrap();
        let secret: String = conn
            .query_row("SELECT preimage FROM preimages", [], |r| r.get(0))
            .unwrap();
        assert_eq!(secret, "secret");
        conn.execute(
            "INSERT INTO receive_payments(hash,status) VALUES('hash','TRANSFER_COMPLETED')",
            [],
        )
        .unwrap();
        drop(conn);
        let db = Db::open(dir.to_str().unwrap()).unwrap();
        assert_eq!(
            db.lightning_receive_for_hash("hash")
                .await
                .unwrap()
                .unwrap()
                .status,
            ReceiveStatus::TransferCompleted
        );
        let exists: bool = db
            .with(|c| {
                c.query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='preimages')",
                    [],
                    |r| r.get(0),
                )
            })
            .await
            .unwrap();
        assert!(!exists);
        drop(db);
        // Reopening the migrated database must not create duplicate receive rows.
        Db::open(dir.to_str().unwrap()).unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn upgrade_refuses_pending_internal_sends() {
        let (db, dir) = database();
        db.with(|c| c.execute_batch("INSERT INTO requests(id,kind,owner,created_at,payload) VALUES('request','LIGHTNING_SEND','owner','now','{\"payment_kind\":\"INTERNAL_BOLT11\",\"payment_id\":\"payment\"}');")).await.unwrap();
        drop(db);
        assert!(Db::open(dir.to_str().unwrap())
            .err()
            .unwrap()
            .contains("previous release"));
        let conn = rusqlite::Connection::open(dir.join("ssp.sqlite")).unwrap();
        conn.execute(
            "INSERT INTO payments(id,status) VALUES('payment','FAILED')",
            [],
        )
        .unwrap();
        drop(conn);
        let db = Db::open(dir.to_str().unwrap()).unwrap();
        assert!(db.get_request("request", "owner").await.unwrap().is_some());
        assert!(db.unresolved_lightning_sends().await.unwrap().is_empty());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
