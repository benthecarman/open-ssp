//! Owner-scoped, stable request pagination over durable settlement records.
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use rusqlite::{params, Connection};
use serde_json::{json, Value};

use crate::db::Db;

pub fn migrate(c: &Connection) -> rusqlite::Result<()> {
    c.execute_batch("CREATE TABLE IF NOT EXISTS coop_exit_quotes(id TEXT PRIMARY KEY, owner TEXT NOT NULL, expires_at INTEGER NOT NULL, data TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS coop_exits(id TEXT PRIMARY KEY, owner TEXT NOT NULL, transfer_id TEXT NOT NULL UNIQUE, idem TEXT NOT NULL, status TEXT NOT NULL, data TEXT NOT NULL, UNIQUE(owner,idem));
        CREATE INDEX IF NOT EXISTS requests_owner_order ON requests(owner,created_at DESC,id DESC);
        DROP VIEW IF EXISTS request_history;
        CREATE VIEW request_history AS SELECT r.*,
          CASE WHEN r.kind='COOP_EXIT_V2' THEN 'COOP_EXIT' WHEN r.kind='CLAIM_INSTANT_STATIC_DEPOSIT_V2' THEN 'CLAIM_STATIC_DEPOSIT' ELSE r.kind END AS request_type,
          COALESCE(json_extract(r.payload,'$.network'),'') AS network,
          CASE
            WHEN s.status='SUCCEEDED' OR p.status='TRANSFER_COMPLETED' OR x.status='SUCCEEDED' OR json_extract(r.payload,'$.status') IN ('SUCCEEDED','COMPLETED') THEN 'SUCCEEDED'
            WHEN s.status='FAILED' OR p.status='HTLC_FAILED' OR json_extract(r.payload,'$.status')='FAILED' THEN 'FAILED'
            WHEN x.status='EXPIRED' OR json_extract(r.payload,'$.status') IN ('EXPIRED','CANCELED') THEN 'CANCELED'
            WHEN s.status='PREPARED' OR p.status='INVOICE_CREATED' OR x.status='CREATED' OR json_extract(r.payload,'$.status')='CREATED' THEN 'CREATED'
            WHEN s.status IS NOT NULL OR p.status IS NOT NULL OR x.status IS NOT NULL OR json_extract(r.payload,'$.status') IN ('IN_PROGRESS','OUTBOUND_TRANSFER_SENT') THEN 'IN_PROGRESS'
            ELSE 'UNKNOWN' END AS request_status
        FROM requests r
        LEFT JOIN lightning_sends s ON s.request_id=r.id
        LEFT JOIN lightning_receives l ON l.request_id=r.id
        LEFT JOIN receive_payments p ON p.hash=l.hash
        LEFT JOIN coop_exits x ON x.id=r.id;")
}

pub struct Page {
    pub records: Vec<Value>,
    pub count: u64,
    pub has_next: bool,
    pub has_previous: bool,
    pub start: Option<String>,
    pub end: Option<String>,
}

fn filter(input: &Value, field: &str, allowed: &[&str]) -> Result<String, String> {
    let Some(value) = input.get(field).filter(|v| !v.is_null()) else {
        return Ok("[]".into());
    };
    let values = value
        .as_array()
        .ok_or_else(|| format!("{field} must be a list"))?;
    if values.len() > allowed.len()
        || values
            .iter()
            .any(|v| !v.as_str().is_some_and(|v| allowed.contains(&v)))
    {
        return Err(format!("invalid {field} filter"));
    }
    Ok(value.to_string())
}

fn cursor(owner: &str, created: &str, id: &str) -> String {
    URL_SAFE_NO_PAD.encode(json!([owner, created, id]).to_string())
}

impl Db {
    pub async fn request_history(
        &self,
        owner: &str,
        input: &Value,
        network: &str,
    ) -> Result<Page, String> {
        let first = match input.get("first").filter(|v| !v.is_null()) {
            None => 50,
            Some(v) => v
                .as_u64()
                .filter(|v| (1..=100).contains(v))
                .ok_or("first must be between 1 and 100")?,
        };
        let types = filter(
            input,
            "types",
            &[
                "LIGHTNING_SEND",
                "LIGHTNING_RECEIVE",
                "COOP_EXIT",
                "LEAVES_SWAP",
                "CLAIM_STATIC_DEPOSIT",
            ],
        )?;
        let statuses = filter(
            input,
            "statuses",
            &[
                "CREATED",
                "IN_PROGRESS",
                "SUCCEEDED",
                "FAILED",
                "CANCELED",
                "UNKNOWN",
            ],
        )?;
        let networks = filter(
            input,
            "networks",
            &["MAINNET", "TESTNET", "SIGNET", "REGTEST", "LOCAL"],
        )?;
        let after = input.get("after").filter(|v| !v.is_null());
        let (created, id) = if let Some(after) = after {
            let encoded = after
                .as_str()
                .filter(|v| v.len() <= 1024)
                .ok_or("invalid cursor")?;
            let bytes = URL_SAFE_NO_PAD
                .decode(encoded)
                .map_err(|_| "invalid cursor")?;
            let [cursor_owner, created, id]: [String; 3] =
                serde_json::from_slice(&bytes).map_err(|_| "invalid cursor")?;
            if cursor_owner != owner {
                return Err("invalid cursor owner".into());
            }
            (created, id)
        } else {
            (String::new(), String::new())
        };
        self.with(|c| {
            let scope = "owner=?1 AND (json_array_length(?2)=0 OR request_type IN (SELECT value FROM json_each(?2))) AND (json_array_length(?3)=0 OR request_status IN (SELECT value FROM json_each(?3))) AND (json_array_length(?4)=0 OR COALESCE(NULLIF(network,''),?5) IN (SELECT value FROM json_each(?4)))";
            let count = c.query_row(&format!("SELECT count(*) FROM request_history WHERE {scope}"),params![owner,types,statuses,networks,network], |r| r.get(0))?;
            let mut stmt = c.prepare(&format!("SELECT id,kind,owner,created_at,payload FROM request_history WHERE {scope} AND (?6='' OR (created_at,id)<(?6,?7)) ORDER BY created_at DESC,id DESC LIMIT ?8"))?;
            let mut records: Vec<Value> = stmt.query_map(params![owner,types,statuses,networks,network,created,id,first+1],|r| {
                let payload: String = r.get(4)?;
                let payload: Value = serde_json::from_str(&payload).map_err(|e| rusqlite::Error::FromSqlConversionFailure(4, rusqlite::types::Type::Text,Box::new(e)))?;
                Ok(json!({"id":r.get::<_,String>(0)?,"type":r.get::<_,String>(1)?,"owner_identity_pubkey":r.get::<_,String>(2)?,"created_at":r.get::<_,String>(3)?,"payload":payload}))
            })?.collect::<rusqlite::Result<_>>()?;
            let has_next = records.len() > first as usize;
            records.truncate(first as usize);
            let has_previous = if created.is_empty() { false } else {
                c.query_row(&format!("SELECT EXISTS(SELECT 1 FROM request_history WHERE {scope} AND (created_at,id)>=(?6,?7))"),params![owner,types,statuses,networks,network,created,id], |r| r.get(0))?
            };
            let make_cursor = |r: &Value| cursor(owner,r["created_at"].as_str().unwrap(),r["id"].as_str().unwrap());
            Ok(Page { start:records.first().map(make_cursor),end:records.last().map(make_cursor),records,count,has_next,has_previous })
        }).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test(flavor = "multi_thread")]
    async fn pages_are_stable_filtered_and_owner_scoped() {
        let dir = std::env::temp_dir().join(format!("ssp-history-{}", uuid::Uuid::new_v4()));
        let db = Db::open(dir.to_str().unwrap()).unwrap();
        for (id, owner) in [("a", "alice"), ("b", "alice"), ("c", "alice"), ("d", "bob")] {
            db.insert_request(
                id,
                "LEAVES_SWAP",
                owner,
                "2026-01-01T00:00:00Z",
                &json!({"status":"COMPLETED","network":"REGTEST"}),
                None,
            )
            .await
            .unwrap();
        }
        let first = db
            .request_history(
                "alice",
                &json!({"first":2,"statuses":["SUCCEEDED"]}),
                "REGTEST",
            )
            .await
            .unwrap();
        assert_eq!(first.count, 3);
        assert_eq!(first.records[0]["id"], "c");
        assert!(first.has_next);
        let next = db
            .request_history("alice", &json!({"first":2,"after":first.end}), "REGTEST")
            .await
            .unwrap();
        assert_eq!(next.records.len(), 1);
        assert_eq!(next.records[0]["id"], "a");
        assert!(!next.has_next);
        assert!(next.has_previous);
        assert!(db
            .request_history("bob", &json!({"after":first.end}), "REGTEST")
            .await
            .is_err());
        assert!(db
            .request_history("alice", &json!({"first":101}), "REGTEST")
            .await
            .is_err());
        assert_eq!(
            db.request_history("alice", &json!({"networks":["MAINNET"]}), "REGTEST")
                .await
                .unwrap()
                .count,
            0
        );
        drop(db);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
