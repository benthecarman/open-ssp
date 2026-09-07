//! Persist response timestamps when durable request data changes.
use crate::db::Db;
use rusqlite::Connection;

pub fn migrate(c: &Connection) -> rusqlite::Result<()> {
    c.execute_batch("CREATE TABLE IF NOT EXISTS request_updates(id TEXT PRIMARY KEY REFERENCES requests(id) ON DELETE CASCADE, updated_at TEXT NOT NULL);
        INSERT OR IGNORE INTO request_updates SELECT id,created_at FROM requests;
        CREATE TRIGGER IF NOT EXISTS request_created AFTER INSERT ON requests BEGIN
          INSERT INTO request_updates VALUES(NEW.id,NEW.created_at);
        END;
        CREATE TRIGGER IF NOT EXISTS request_deleted AFTER DELETE ON requests BEGIN
          DELETE FROM request_updates WHERE id=OLD.id;
        END;
        CREATE TRIGGER IF NOT EXISTS request_changed AFTER UPDATE OF payload ON requests WHEN OLD.payload IS NOT NEW.payload BEGIN
          UPDATE request_updates SET updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=NEW.id;
        END;")?;
    for (table, key, change) in [
        ("lightning_sends", "NEW.request_id", "OLD.status IS NOT NEW.status OR OLD.payment_id IS NOT NEW.payment_id"),
        ("receive_payments", "(SELECT request_id FROM lightning_receives WHERE hash=NEW.hash)", "OLD.status IS NOT NEW.status OR OLD.transfer_id IS NOT NEW.transfer_id OR OLD.preimage IS NOT NEW.preimage"),
        ("coop_exits", "NEW.id", "OLD.data IS NOT NEW.data"),
        ("transfers", "NEW.request_id", "OLD.status IS NOT NEW.status"),
    ] {
        c.execute_batch(&format!("CREATE TRIGGER IF NOT EXISTS {table}_changed AFTER UPDATE ON {table} WHEN {change} BEGIN
          UPDATE request_updates SET updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id={key};
        END;
        CREATE TRIGGER IF NOT EXISTS {table}_created AFTER INSERT ON {table} BEGIN
          UPDATE request_updates SET updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id={key};
        END;"))?;
    }
    Ok(())
}

impl Db {
    pub async fn request_updated_at(&self, id: &str) -> Result<String, String> {
        self.with(|c| {
            c.query_row(
                "SELECT updated_at FROM request_updates WHERE id=?1",
                [id],
                |r| r.get(0),
            )
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test(flavor = "multi_thread")]
    async fn timestamps_survive_reopen_and_ignore_noop_updates() {
        let dir = std::env::temp_dir().join(format!("ssp-updates-{}", uuid::Uuid::new_v4()));
        let path = dir.to_str().unwrap();
        let db = Db::open(path).unwrap();
        let created = "2020-01-01T00:00:00Z";
        db.insert_request(
            "deposit",
            "CLAIM_STATIC_DEPOSIT",
            "owner",
            created,
            &json!({"status":"IN_PROGRESS"}),
            None,
        )
        .await
        .unwrap();
        assert_eq!(db.request_updated_at("deposit").await.unwrap(), created);
        db.with(|c| c.execute("UPDATE requests SET payload=json_set(payload,'$.phase','SPEND_TX_BROADCAST') WHERE id='deposit'", [])).await.unwrap();
        let changed = db.request_updated_at("deposit").await.unwrap();
        assert_ne!(changed, created);
        // Set a known timestamp so a no-op update cannot pass by sharing a clock tick.
        db.with(|c| c.execute("UPDATE request_updates SET updated_at=?1", [created]))
            .await
            .unwrap();
        db.with(|c| c.execute("UPDATE requests SET payload=json_set(payload,'$.phase','SPEND_TX_BROADCAST') WHERE id='deposit'", [])).await.unwrap();
        assert_eq!(db.request_updated_at("deposit").await.unwrap(), created);
        db.with(|c| c.execute("INSERT INTO transfers(spark_id,request_id,kind,status,owner) VALUES('transfer','deposit','CLAIM_STATIC_DEPOSIT','COMPLETED','owner')", [])).await.unwrap();
        let transferred = db.request_updated_at("deposit").await.unwrap();
        assert_ne!(transferred, created);
        drop(db);
        let db = Db::open(path).unwrap();
        assert_eq!(db.request_updated_at("deposit").await.unwrap(), transferred);
        db.with(|c| c.execute("DELETE FROM requests WHERE id='deposit'", []))
            .await
            .unwrap();
        assert!(db.request_updated_at("deposit").await.is_err());
        drop(db);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
