use std::{path::Path, path::PathBuf, sync::Arc};

use serde_json::Value;
use tokio::sync::Mutex;

/// SQLite persistence for everything the SSP must survive restarts with:
/// auth challenges/sessions, user requests, and settlement checkpoints.
/// Single file at `<SSP_DATA_DIR>/ssp.sqlite` (volume-mount in compose).
#[derive(Clone)]
pub struct Db {
    inner: Arc<Mutex<rusqlite::Connection>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LightningReceive {
    pub request_id: String,
    pub owner: String,
    pub receiver: String,
    pub amount_sats: u64,
    pub invoice: String,
    pub status: crate::lightning_store::ReceiveStatus,
    pub transfer_id: Option<String>,
    pub preimage: Option<String>,
    pub claim_submitted: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SparkSplitOperation {
    pub operation_id: String,
    pub parent_node_id: String,
    pub parent_value_sats: u64,
    pub child_values_sats: Vec<u64>,
    /// Opaque, retryable split stage produced by spark-sdk. Child secrets are
    /// persisted separately before the first operator call.
    pub plan: Vec<u8>,
    pub status: String,
    pub child_node_ids: Vec<String>,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SparkLeafKeyOverride {
    pub node_id: String,
    pub key_material: Vec<u8>,
}

fn ensure_column(
    conn: &rusqlite::Connection,
    table: &str,
    column: &str,
    migration: &str,
) -> rusqlite::Result<()> {
    if conn
        .prepare(&format!("SELECT {column} FROM {table} LIMIT 0"))
        .is_err()
    {
        conn.execute(migration, [])?;
    }
    Ok(())
}

impl Db {
    pub fn open(data_dir: &str) -> Result<Self, String> {
        std::fs::create_dir_all(data_dir).map_err(|e| e.to_string())?;
        let path: PathBuf = [data_dir, "ssp.sqlite"].iter().collect();
        let conn = rusqlite::Connection::open(&path).map_err(|e| e.to_string())?;
        // The database holds session tokens and preimages. Secure it before
        // the WAL pragma: SQLite creates -wal/-journal/-shm sidecars with
        // exactly the database file's mode, and pre-existing sidecars from a
        // restored backup are tightened here too. Startup fails if a
        // group- or world-readable file cannot be secured.
        crate::fs::restrict_to_owner(&path)?;
        for suffix in ["-wal", "-shm", "-journal"] {
            let mut sidecar = path.clone().into_os_string();
            sidecar.push(suffix);
            crate::fs::restrict_to_owner(Path::new(&sidecar))?;
        }
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON;",
        )
        .map_err(|e| e.to_string())?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS challenges(identity TEXT PRIMARY KEY, protected TEXT NOT NULL, issued_at TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS sessions(token TEXT PRIMARY KEY, identity TEXT NOT NULL, valid_until TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS requests(id TEXT PRIMARY KEY, kind TEXT NOT NULL, owner TEXT NOT NULL, created_at TEXT NOT NULL, payload TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS transfers(spark_id TEXT PRIMARY KEY, request_id TEXT NOT NULL, kind TEXT NOT NULL, status TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS payments(id TEXT PRIMARY KEY, status TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS receive_payments(hash TEXT PRIMARY KEY, status TEXT NOT NULL, transfer_id TEXT, preimage TEXT, claimable_amount_msat INTEGER, claim_submitted INTEGER NOT NULL DEFAULT 0);
             CREATE TABLE IF NOT EXISTS receive_quote_uses(quote_id TEXT PRIMARY KEY, payment_hash TEXT NOT NULL UNIQUE);
             CREATE TABLE IF NOT EXISTS receive_quotes(id TEXT PRIMARY KEY,owner TEXT NOT NULL,manifest BLOB NOT NULL,expires_at INTEGER NOT NULL);
             CREATE TABLE IF NOT EXISTS static_quotes(txid TEXT NOT NULL, vout INTEGER NOT NULL, credit INTEGER NOT NULL, signature TEXT NOT NULL, created_at TEXT NOT NULL, claimed INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(txid, vout));
             CREATE TABLE IF NOT EXISTS spark_split_operations(
               operation_id TEXT PRIMARY KEY,
               parent_node_id TEXT NOT NULL UNIQUE,
               parent_value_sats INTEGER NOT NULL,
               child_values_sats TEXT NOT NULL,
               plan BLOB NOT NULL,
               status TEXT NOT NULL CHECK(status IN ('DRAFT','PREPARED','SUBMITTING','SUBMITTED','COMPLETED')),
               child_node_ids TEXT NOT NULL DEFAULT '[]',
               last_error TEXT,
               created_at TEXT NOT NULL,
               updated_at TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS spark_leaf_key_overrides(
               node_id TEXT PRIMARY KEY,
               operation_id TEXT NOT NULL REFERENCES spark_split_operations(operation_id),
               key_material BLOB NOT NULL,
               created_at TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS spark_pending_split_keys(
               operation_id TEXT PRIMARY KEY,
               parent_node_id TEXT NOT NULL,
               encrypted_keys BLOB NOT NULL,
               created_at TEXT NOT NULL
             );",
        )
        .map_err(|e| e.to_string())?;
        // Keep old databases compatible without a migration framework. Each
        // probe is idempotent and runs only at startup.
        for (table, column, migration) in [
            (
                "challenges",
                "issued_at",
                "ALTER TABLE challenges ADD COLUMN issued_at TEXT NOT NULL DEFAULT '1970-01-01T00:00:00+00:00'",
            ),
            (
                "transfers",
                "owner",
                "ALTER TABLE transfers ADD COLUMN owner TEXT NOT NULL DEFAULT ''",
            ),
            (
                "requests",
                "idempotency_key",
                "ALTER TABLE requests ADD COLUMN idempotency_key TEXT",
            ),
            (
                "static_quotes",
                "claimed",
                "ALTER TABLE static_quotes ADD COLUMN claimed INTEGER NOT NULL DEFAULT 0",
            ),
            (
                "receive_payments",
                "transfer_id",
                "ALTER TABLE receive_payments ADD COLUMN transfer_id TEXT",
            ),
            (
                "receive_payments",
                "preimage",
                "ALTER TABLE receive_payments ADD COLUMN preimage TEXT",
            ),
            (
                "receive_payments",
                "claimable_amount_msat",
                "ALTER TABLE receive_payments ADD COLUMN claimable_amount_msat INTEGER",
            ),
            (
                "receive_payments",
                "claim_submitted",
                "ALTER TABLE receive_payments ADD COLUMN claim_submitted INTEGER NOT NULL DEFAULT 0",
            ),
        ] {
            ensure_column(&conn, table, column, migration).map_err(|e| e.to_string())?;
        }
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_requests_owner_idem ON requests(owner, idempotency_key) WHERE idempotency_key IS NOT NULL AND idempotency_key <> ''",
            [],
        )
        .map_err(|e| e.to_string())?;
        crate::lightning_store::migrate(&conn)?;
        crate::static_deposits::migrate(&conn).map_err(|e| e.to_string())?;
        crate::internal_payments::migrate(&conn).map_err(|e| e.to_string())?;
        crate::history::migrate(&conn).map_err(|e| e.to_string())?;
        crate::webhooks::migrate(&conn).map_err(|e| e.to_string())?;
        Ok(Self {
            inner: Arc::new(Mutex::new(conn)),
        })
    }

    pub(crate) async fn with<T, F>(&self, f: F) -> Result<T, String>
    where
        F: FnOnce(&rusqlite::Connection) -> rusqlite::Result<T> + Send,
        T: Send + 'static,
    {
        // rusqlite is blocking I/O: keep it off async workers.
        tokio::task::block_in_place(|| {
            let conn = self.inner.blocking_lock();
            f(&conn).map_err(|e| e.to_string())
        })
    }

    // ---- challenges (single-use, 5-minute expiry) ----
    pub async fn save_challenge(
        &self,
        identity: &str,
        protected: &str,
        now: &str,
    ) -> Result<(), String> {
        self.with(|c| {
            c.execute(
                "INSERT INTO challenges(identity,protected,issued_at) VALUES(?1,?2,?3)
                 ON CONFLICT(identity) DO UPDATE SET protected=excluded.protected, issued_at=excluded.issued_at",
                (identity, protected, now),
            )
            .map(|_| ())
        })
        .await
    }

    /// Atomically consume a challenge: returns true only if the stored
    /// challenge matches and is younger than `max_age_secs`. Always deletes.
    pub async fn consume_challenge(
        &self,
        identity: &str,
        protected: &str,
        now_epoch_secs: i64,
        max_age_secs: i64,
    ) -> Result<bool, String> {
        let row: Option<(String, String)> = self
            .with(|conn| {
                let row = conn
                    .query_row(
                        "SELECT protected, issued_at FROM challenges WHERE identity=?1",
                        (identity,),
                        |r| Ok((r.get(0)?, r.get(1)?)),
                    )
                    .map(Some)
                    .or_else(|e| match e {
                        rusqlite::Error::QueryReturnedNoRows => Ok(None),
                        e => Err(e),
                    })?;
                conn.execute("DELETE FROM challenges WHERE identity=?1", (identity,))?;
                Ok(row)
            })
            .await?;
        let Some((stored, issued_at)) = row else {
            return Ok(false);
        };
        if stored != protected {
            return Ok(false);
        }
        let issued = chrono::DateTime::parse_from_rfc3339(&issued_at)
            .map(|d| d.timestamp())
            .unwrap_or(0);
        let age = now_epoch_secs.saturating_sub(issued);
        Ok(issued <= now_epoch_secs && age <= max_age_secs)
    }

    pub async fn prune_challenges(&self, older_than_rfc3339: &str) -> Result<(), String> {
        self.with(|c| {
            c.execute(
                "DELETE FROM challenges WHERE issued_at<?1",
                (older_than_rfc3339,),
            )
            .map(|_| ())
        })
        .await
    }

    // ---- sessions ----
    pub async fn save_session(
        &self,
        token: &str,
        identity: &str,
        valid_until: &str,
    ) -> Result<(), String> {
        self.with(|c| {
            c.execute(
                "INSERT INTO sessions(token,identity,valid_until) VALUES(?1,?2,?3)",
                (token, identity, valid_until),
            )
            .map(|_| ())
        })
        .await
    }

    pub async fn session_owner(
        &self,
        token: &str,
        now_rfc3339: &str,
    ) -> Result<Option<String>, String> {
        self.with(|c| {
            c.query_row(
                "SELECT identity FROM sessions WHERE token=?1 AND valid_until>?2",
                (token, now_rfc3339),
                |r| r.get(0),
            )
            .map(Some)
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                e => Err(e),
            })
        })
        .await
    }

    // ---- requests (owner-scoped) ----
    pub async fn insert_request(
        &self,
        id: &str,
        kind: &str,
        owner: &str,
        created_at: &str,
        payload: &Value,
        idempotency_key: Option<&str>,
    ) -> Result<(), String> {
        let payload = serde_json::to_string(payload).map_err(|e| e.to_string())?;
        self.with(|c| {
            let tx = c.unchecked_transaction()?;
            tx.execute(
                "INSERT INTO requests(id,kind,owner,created_at,payload,idempotency_key) VALUES(?1,?2,?3,?4,?5,?6)",
                (id, kind, owner, created_at, &payload, idempotency_key),
            )?;
            crate::lightning_store::index_request(&tx,id,kind,owner,created_at,&payload)?;
            tx.commit()
        })
        .await
    }

    /// Compatibility-only request kinds (stubbed operations with no
    /// settlement lifecycle). Their storage is bounded by a rolling per-owner
    /// quota and TTL pruning.
    pub const COMPAT_REQUEST_KINDS: [&str; 3] = [
        "COOP_EXIT",
        "CLAIM_STATIC_DEPOSIT",
        "CLAIM_INSTANT_STATIC_DEPOSIT",
    ];

    /// Per-owner compatibility request count inside the window.
    pub async fn compat_request_count(
        &self,
        owner: &str,
        since_rfc3339: &str,
    ) -> Result<i64, String> {
        let kinds = Self::COMPAT_REQUEST_KINDS.join("','");
        self.with(move |c| {
            c.query_row(
                &format!(
                    "SELECT COUNT(*) FROM requests
                     WHERE owner=?1 AND created_at>=?2 AND kind IN ('{kinds}')"
                ),
                (owner, since_rfc3339),
                |r| r.get(0),
            )
        })
        .await
    }

    /// Delete compatibility-only requests older than the cutoff, plus any
    /// orphaned compatibility transfer rows referencing them. These stubs
    /// carry no settlement audit value.
    pub async fn prune_compat_requests(&self, older_than_rfc3339: &str) -> Result<usize, String> {
        let kinds = Self::COMPAT_REQUEST_KINDS.join("','");
        self.with(move |c| {
            c.execute(
                "DELETE FROM transfers
                 WHERE kind='COOP_EXIT'
                   AND request_id NOT IN (SELECT id FROM requests)",
                [],
            )?;
            c.execute(
                &format!(
                    "DELETE FROM requests
                     WHERE created_at<?1 AND kind IN ('{kinds}') AND id NOT IN (SELECT id FROM deposit_claims)"
                ),
                (older_than_rfc3339,),
            )
        })
        .await
    }

    pub async fn get_request(&self, id: &str, owner: &str) -> Result<Option<Value>, String> {
        self.with(|c| {
            c.query_row(
                "SELECT id,kind,owner,created_at,payload FROM requests WHERE id=?1 AND owner=?2",
                (id, owner),
                |r| {
                    let payload: String = r.get(4)?;
                    Ok(serde_json::json!({
                        "id": r.get::<_, String>(0)?,
                        "type": r.get::<_, String>(1)?,
                        "owner_identity_pubkey": r.get::<_, String>(2)?,
                        "created_at": r.get::<_, String>(3)?,
                        "payload": serde_json::from_str::<Value>(&payload).unwrap_or(Value::Null),
                    }))
                },
            )
            .map(Some)
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                e => Err(e),
            })
        })
        .await
    }

    /// Idempotency: an in-flight/completed request with the same owner+key.
    pub async fn find_by_idempotency(
        &self,
        owner: &str,
        key: &str,
    ) -> Result<Option<Value>, String> {
        if key.is_empty() {
            return Ok(None);
        }
        self.with(|c| {
            c.query_row(
                "SELECT id,kind,owner,created_at,payload FROM requests WHERE owner=?1 AND idempotency_key=?2 ORDER BY created_at DESC LIMIT 1",
                (owner, key),
                |r| {
                    let payload: String = r.get(4)?;
                    Ok(serde_json::json!({
                        "id": r.get::<_, String>(0)?,
                        "type": r.get::<_, String>(1)?,
                        "owner_identity_pubkey": r.get::<_, String>(2)?,
                        "created_at": r.get::<_, String>(3)?,
                        "payload": serde_json::from_str::<Value>(&payload).unwrap_or(Value::Null),
                    }))
                },
            )
            .map(Some)
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                e => Err(e),
            })
        })
        .await
    }

    pub async fn commit_bolt12_receive(
        &self,
        offer_id: &str,
        payment_hash: &str,
        transfer_id: &str,
        request_id: &str,
        owner: &str,
    ) -> Result<(), String> {
        self.with(|c| {
            let tx = c.unchecked_transaction()?;
            tx.execute(
                "INSERT INTO transfers(spark_id,request_id,kind,status,owner)
                 VALUES(?1,?2,'LIGHTNING_RECEIVE','TRANSFER_COMPLETED',?3)
                 ON CONFLICT(spark_id) DO UPDATE SET status='TRANSFER_COMPLETED'",
                (transfer_id, request_id, owner),
            )?;
            tx.execute(
                "INSERT INTO receive_payments(hash,status,transfer_id,claim_submitted)
                 VALUES(?1,'TRANSFER_COMPLETED',?2,1)
                 ON CONFLICT(hash) DO UPDATE SET
                   status='TRANSFER_COMPLETED', transfer_id=excluded.transfer_id,
                   claim_submitted=1",
                (offer_id, transfer_id),
            )?;
            tx.execute(
                "UPDATE lightning_receives SET settled_payment_hash=?1 WHERE request_id=?2 AND owner=?3 AND hash=?4",
                (payment_hash, request_id, owner, offer_id),
            )?;
            tx.commit()
        })
        .await
    }

    pub async fn lightning_receive_for_hash(
        &self,
        payment_hash: &str,
    ) -> Result<Option<LightningReceive>, String> {
        self.with(|c| {
            c.query_row(
                "SELECT r.request_id, r.owner, r.receiver, r.amount_sats, r.invoice,
                        COALESCE(p.status, 'INVOICE_CREATED'), p.transfer_id,
                        p.preimage, COALESCE(p.claim_submitted, 0)
                 FROM lightning_receives r LEFT JOIN receive_payments p ON p.hash=r.hash WHERE r.hash=?1
                 LIMIT 1",
                (payment_hash,),
                |row| {
                    Ok(LightningReceive {
                        request_id: row.get(0)?,
                        owner: row.get(1)?,
                        receiver: row.get(2)?,
                        amount_sats: row.get(3)?,
                        invoice: row.get(4)?,
                        status: row.get(5)?,
                        transfer_id: row.get(6)?,
                        preimage: row.get(7)?,
                        claim_submitted: row.get::<_, i64>(8)? != 0,
                    })
                },
            )
            .map(Some)
            .or_else(|error| match error {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                error => Err(error),
            })
        })
        .await
    }

    /// Match the exact invoice issued by this SSP. Matching the encoded
    /// invoice, rather than only its payment hash, prevents an unrelated
    /// invoice that reuses a hash from being rejected as a local invoice.
    pub async fn lightning_receive_hash_for_invoice(
        &self,
        invoice: &str,
    ) -> Result<Option<String>, String> {
        self.with(|c| {
            c.query_row(
                "SELECT hash FROM lightning_receives WHERE invoice=?1 AND kind='BOLT11'
                 LIMIT 1",
                (invoice,),
                |row| row.get(0),
            )
            .map(Some)
            .or_else(|error| match error {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                error => Err(error),
            })
        })
        .await
    }

    pub async fn mark_receive_claimable(&self, hash: &str, amount_msat: u64) -> Result<(), String> {
        let amount_msat = i64::try_from(amount_msat)
            .map_err(|_| "claimable amount is too large for storage".to_string())?;
        self.with(|c| {
            c.execute(
                "INSERT INTO receive_payments(hash,status,claimable_amount_msat)
                 VALUES(?1,'HTLC_RECEIVED',?2)
                 ON CONFLICT(hash) DO UPDATE SET
                   status=CASE
                     WHEN receive_payments.status IN ('TRANSFER_COMPLETED','HTLC_FAILED')
                       OR receive_payments.transfer_id IS NOT NULL
                     THEN receive_payments.status
                     ELSE 'HTLC_RECEIVED'
                   END,
                   claimable_amount_msat=excluded.claimable_amount_msat",
                (hash, amount_msat),
            )
            .map(|_| ())
        })
        .await
    }

    /// Persist the operator result and its GraphQL transfer row together.
    pub async fn commit_lightning_receive_swap(
        &self,
        hash: &str,
        transfer_id: &str,
        preimage: &str,
        request_id: &str,
        owner: &str,
    ) -> Result<(), String> {
        self.with(|c| {
            let tx = c.unchecked_transaction()?;
            let changed = tx.execute(
                "INSERT INTO transfers(spark_id,request_id,kind,status,owner)
                 VALUES(?1,?2,'LIGHTNING_RECEIVE','TRANSFER_CREATED',?3)
                 ON CONFLICT(spark_id) DO UPDATE SET
                   status=excluded.status
                 WHERE transfers.owner=excluded.owner
                   AND transfers.request_id=excluded.request_id",
                (transfer_id, request_id, owner),
            )?;
            if changed != 1 {
                return Err(rusqlite::Error::InvalidQuery);
            }
            tx.execute(
                "INSERT INTO receive_payments(hash,status,transfer_id,preimage)
                 VALUES(?1,'PAYMENT_PREIMAGE_RECOVERED',?2,?3)
                 ON CONFLICT(hash) DO UPDATE SET
                   status='PAYMENT_PREIMAGE_RECOVERED',
                   transfer_id=excluded.transfer_id,
                   preimage=excluded.preimage",
                (hash, transfer_id, preimage),
            )?;
            tx.commit()
        })
        .await
    }

    pub async fn mark_receive_claim_submitted(&self, hash: &str) -> Result<(), String> {
        self.with(|c| {
            c.execute(
                "UPDATE receive_payments
                 SET claim_submitted=1, status='PAYMENT_PREIMAGE_RECOVERED'
                 WHERE hash=?1 AND transfer_id IS NOT NULL AND preimage IS NOT NULL",
                (hash,),
            )
            .and_then(|changed| {
                if changed == 1 {
                    Ok(())
                } else {
                    Err(rusqlite::Error::QueryReturnedNoRows)
                }
            })
        })
        .await
    }

    pub async fn transfer_for_request(
        &self,
        request_id: &str,
        owner: &str,
    ) -> Result<Option<String>, String> {
        self.with(|c| {
            c.query_row(
                "SELECT spark_id FROM transfers
                 WHERE request_id=?1 AND owner=?2
                 LIMIT 1",
                (request_id, owner),
                |row| row.get(0),
            )
            .map(Some)
            .or_else(|error| match error {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                error => Err(error),
            })
        })
        .await
    }

    pub async fn prune_expired_sessions(&self, now_rfc3339: &str) -> Result<(), String> {
        self.with(|c| {
            c.execute("DELETE FROM sessions WHERE valid_until<=?1", (now_rfc3339,))
                .map(|_| ())
        })
        .await
    }

    // ---- transfers (owner-scoped) ----
    pub async fn insert_transfer(
        &self,
        spark_id: &str,
        request_id: &str,
        kind: &str,
        status: &str,
        owner: &str,
    ) -> Result<(), String> {
        let changed = self
            .with(|c| {
                c.execute(
                "INSERT INTO transfers(spark_id,request_id,kind,status,owner) VALUES(?1,?2,?3,?4,?5)
                 ON CONFLICT(spark_id) DO UPDATE SET status=excluded.status WHERE transfers.owner=excluded.owner",
                (spark_id, request_id, kind, status, owner),
            )
            })
            .await?;
        if changed == 1 {
            Ok(())
        } else {
            Err("transfer id belongs to another owner".to_string())
        }
    }

    /// Single query, capped row count.
    pub async fn transfers_for(&self, ids: &[String], owner: &str) -> Result<Vec<Value>, String> {
        const MAX_IDS: usize = 500;
        if ids.is_empty() {
            return Ok(vec![]);
        }
        if ids.len() > MAX_IDS {
            return Err(format!("at most {MAX_IDS} transfer ids are allowed"));
        }
        let ids: Vec<&String> = ids.iter().collect();
        let placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        // rusqlite params must be homogeneous: build owned Vec<Value-ish> via params_from_iter.
        let sql = format!(
            "SELECT t.spark_id,t.request_id,t.kind,t.status,r.payload
             FROM transfers t
             LEFT JOIN requests r ON r.id=t.request_id AND r.owner=t.owner
             WHERE t.owner=? AND t.spark_id IN ({placeholders})"
        );
        self.with(move |c| {
            let mut stmt = c.prepare(&sql)?;
            let mut params: Vec<&dyn rusqlite::ToSql> = vec![&owner];
            for id in &ids {
                params.push(id);
            }
            let rows = stmt.query_map(rusqlite::params_from_iter(params), |r| {
                let payload = r
                    .get::<_, Option<String>>(4)?
                    .and_then(|value| serde_json::from_str::<Value>(&value).ok())
                    .unwrap_or(Value::Null);
                let total_amount_sats = payload
                    .get("total_amount_sats")
                    .or_else(|| payload.get("amount_sats"))
                    .or_else(|| payload.get("credit_amount_sats"))
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                Ok(serde_json::json!({
                    "spark_id": r.get::<_, String>(0)?,
                    "user_request_id": r.get::<_, String>(1)?,
                    "type": r.get::<_, String>(2)?,
                    "status": r.get::<_, String>(3)?,
                    "total_amount_sats": total_amount_sats,
                }))
            })?;
            rows.collect::<rusqlite::Result<Vec<Value>>>()
        })
        .await
    }

    /// Latest request of `kind` whose payload references `ext_id` as its
    /// `user_outbound_transfer_external_id`. Compatibility operations
    /// correlate through their stored request: an unverified client-supplied
    /// Spark id must never reserve a row in the global transfers namespace,
    /// where it could collide with SSP-derived receive transfer ids.
    pub async fn request_id_for_ext_id(
        &self,
        kind: &str,
        ext_id: &str,
        owner: &str,
    ) -> Result<Option<String>, String> {
        self.with(|c| {
            c.query_row(
                "SELECT id FROM requests
                 WHERE kind=?1 AND owner=?2
                   AND json_extract(payload, '$.user_outbound_transfer_external_id')=?3
                 ORDER BY created_at DESC LIMIT 1",
                (kind, owner, ext_id),
                |r| r.get(0),
            )
            .map(Some)
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                e => Err(e),
            })
        })
        .await
    }

    /// Record a terminal status inside a request payload. Compatibility
    /// requests keep their status in the payload (no status column).
    pub async fn set_request_status(
        &self,
        id: &str,
        owner: &str,
        status: &str,
    ) -> Result<(), String> {
        let changed = self
            .with(|c| {
                c.execute(
                    "UPDATE requests SET payload=json_set(payload, '$.status', ?3)
                     WHERE id=?1 AND owner=?2",
                    (id, owner, status),
                )
            })
            .await?;
        if changed == 1 {
            Ok(())
        } else {
            Err("request was not found".to_string())
        }
    }

    // ---- static deposit quotes (one row per UTXO) ----
    pub async fn record_static_quote(
        &self,
        txid: &str,
        vout: u32,
        credit: u64,
        signature: &str,
        created_at: &str,
    ) -> Result<Option<String>, String> {
        self.with(|c| {
            let existing = c
                .query_row(
                    "SELECT credit,signature,claimed FROM static_quotes WHERE txid=?1 AND vout=?2",
                    (txid, vout),
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, i64>(2)?,
                        ))
                    },
                )
                .map(Some)
                .or_else(|error| match error {
                    rusqlite::Error::QueryReturnedNoRows => Ok(None),
                    error => Err(error),
                })?;
            if let Some((stored_credit, stored_signature, claimed)) = existing {
                return Ok(
                    (stored_credit == credit as i64 && claimed == 0).then_some(stored_signature)
                );
            }
            c.execute(
                "INSERT INTO static_quotes(txid,vout,credit,signature,created_at)
                 VALUES(?1,?2,?3,?4,?5)",
                (txid, vout, credit as i64, signature, created_at),
            )?;
            Ok(Some(signature.to_string()))
        })
        .await
    }

    pub async fn consume_static_quote(
        &self,
        txid: &str,
        vout: u32,
        signature: &str,
    ) -> Result<bool, String> {
        self.with(|c| {
            c.execute(
                "UPDATE static_quotes SET claimed=1
                 WHERE txid=?1 AND vout=?2 AND signature=?3 AND claimed=0",
                (txid, vout, signature),
            )
            .map(|changed| changed == 1)
        })
        .await
    }
    // ---- Lightning receive expiry and status ----

    pub async fn expired_receive_hashes(&self, now_epoch: i64) -> Result<Vec<String>, String> {
        self.with(|c| {
            let mut stmt=c.prepare("SELECT r.hash FROM lightning_receives r LEFT JOIN receive_payments p ON p.hash=r.hash WHERE r.kind='BOLT11' AND r.expires_at<=?1 AND COALESCE(p.status,'INVOICE_CREATED') NOT IN ('TRANSFER_COMPLETED','HTLC_FAILED') AND p.transfer_id IS NULL AND NOT EXISTS(SELECT 1 FROM transfers t WHERE t.request_id=r.request_id AND t.kind='LIGHTNING_RECEIVE')")?;
            let rows=stmt.query_map([now_epoch],|r|r.get(0))?;
            rows.collect()
        }).await
    }

    pub async fn set_receive_status(&self, hash: &str, status: &str) -> Result<(), String> {
        let status: crate::lightning_store::ReceiveStatus = status.parse()?;
        self.with(|c| {
            c.execute(
                "INSERT INTO receive_payments(hash,status) VALUES(?1,?2)
                 ON CONFLICT(hash) DO UPDATE SET status=excluded.status",
                (hash, status),
            )
            .map(|_| ())
        })
        .await
    }

    pub async fn fail_external_receive(&self, hash: &str) -> Result<(), String> {
        self.with(|c| {
            c.execute(
                "UPDATE receive_payments SET status='HTLC_FAILED'
                 WHERE hash=?1 AND status != 'TRANSFER_COMPLETED' AND NOT EXISTS(SELECT 1 FROM internal_payments WHERE hash=?1)",
                (hash,),
            )
            .map(|_| ())
        })
        .await
    }

    pub async fn receive_status(&self, hash: &str) -> Result<String, String> {
        let found: Option<String> = self
            .with(|c| {
                c.query_row(
                    "SELECT status FROM receive_payments WHERE hash=?1",
                    (hash,),
                    |r| r.get(0),
                )
                .map(Some)
                .or_else(|e| match e {
                    rusqlite::Error::QueryReturnedNoRows => Ok(None),
                    e => Err(e),
                })
            })
            .await?;
        Ok(found.unwrap_or_else(|| "INVOICE_CREATED".to_string()))
    }

    // ---- payments ----
    pub async fn set_payment(&self, id: &str, status: &str) -> Result<(), String> {
        let status: crate::lightning_store::SendStatus = status.parse()?;
        self.with(|c| {
            c.execute("UPDATE lightning_sends SET status=?2 WHERE (request_id=?1 OR payment_id=?1) AND status NOT IN ('SUCCEEDED','FAILED')", (id,status))?;
            c.execute(
                "INSERT INTO payments(id,status) VALUES(?1,?2)
                 ON CONFLICT(id) DO UPDATE SET status=excluded.status",
                (id, status),
            )
            .map(|_| ())
        })
        .await
    }

    pub async fn payment_status(&self, id: &str) -> Result<String, String> {
        let found: Option<String> = self
            .with(|c| {
                c.query_row("SELECT status FROM lightning_sends WHERE request_id=?1 OR payment_id=?1 UNION ALL SELECT status FROM payments WHERE id=?1 LIMIT 1", (id,), |r| {
                    r.get(0)
                })
                .map(Some)
                .or_else(|e| match e {
                    rusqlite::Error::QueryReturnedNoRows => Ok(None),
                    e => Err(e),
                })
            })
            .await?;
        Ok(found.unwrap_or_else(|| "UNKNOWN".to_string()))
    }

    // ---- Spark leaf splitting ----

    /// Save a split plan before any operator call. A parent may only have one
    /// plan: retries get the original opaque plan and therefore reuse its
    /// child secrets instead of silently creating incompatible keys.
    pub async fn get_or_insert_spark_split(
        &self,
        candidate: &SparkSplitOperation,
    ) -> Result<SparkSplitOperation, String> {
        let candidate = candidate.clone();
        self.with(move |c| {
            let child_values = serde_json::to_string(&candidate.child_values_sats)
                .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
            let child_node_ids = serde_json::to_string(&candidate.child_node_ids)
                .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
            let now = chrono::Utc::now().to_rfc3339();
            c.execute(
                "INSERT OR IGNORE INTO spark_split_operations(
                   operation_id,parent_node_id,parent_value_sats,child_values_sats,plan,status,
                   child_node_ids,last_error,created_at,updated_at
                 ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?9)",
                rusqlite::params![
                    candidate.operation_id,
                    candidate.parent_node_id,
                    candidate.parent_value_sats,
                    child_values,
                    candidate.plan,
                    candidate.status,
                    child_node_ids,
                    candidate.last_error,
                    now,
                ],
            )?;
            read_spark_split(c, &candidate.parent_node_id)
        })
        .await
    }

    pub async fn spark_split_for_parent(
        &self,
        parent_node_id: &str,
    ) -> Result<Option<SparkSplitOperation>, String> {
        let parent_node_id = parent_node_id.to_string();
        self.with(move |c| optional_spark_split(c, &parent_node_id))
            .await
    }

    pub async fn incomplete_spark_splits(&self) -> Result<Vec<SparkSplitOperation>, String> {
        self.with(|c| {
            let mut statement = c.prepare(
                "SELECT operation_id,parent_node_id,parent_value_sats,child_values_sats,plan,
                        status,child_node_ids,last_error
                 FROM spark_split_operations WHERE status!='COMPLETED' ORDER BY created_at",
            )?;
            let rows = statement
                .query_map([], spark_split_from_row)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await
    }

    pub async fn mark_spark_split_submitting(&self, operation_id: &str) -> Result<(), String> {
        let operation_id = operation_id.to_string();
        self.with(move |c| {
            let changed = c.execute(
                "UPDATE spark_split_operations
                 SET status='SUBMITTING',last_error=NULL,updated_at=?2
                 WHERE operation_id=?1 AND status='PREPARED'",
                (&operation_id, chrono::Utc::now().to_rfc3339()),
            )?;
            if changed == 0 {
                let status: String = c.query_row(
                    "SELECT status FROM spark_split_operations WHERE operation_id=?1",
                    (&operation_id,),
                    |row| row.get(0),
                )?;
                if status != "SUBMITTING" && status != "SUBMITTED" && status != "COMPLETED" {
                    return Err(rusqlite::Error::InvalidQuery);
                }
            }
            Ok(())
        })
        .await
    }

    pub async fn save_prepared_spark_split(
        &self,
        operation_id: &str,
        plan: &[u8],
    ) -> Result<(), String> {
        let operation_id = operation_id.to_string();
        let plan = plan.to_vec();
        self.with(move |c| {
            let changed = c.execute(
                "UPDATE spark_split_operations
                 SET plan=?2,status='PREPARED',last_error=NULL,updated_at=?3
                 WHERE operation_id=?1 AND status IN ('DRAFT','PREPARED')",
                (&operation_id, &plan, chrono::Utc::now().to_rfc3339()),
            )?;
            if changed == 1 {
                Ok(())
            } else {
                Err(rusqlite::Error::InvalidQuery)
            }
        })
        .await
    }

    /// Replace the prepared plan with the serialized operator response before
    /// finalizing signatures. Retries can then resume without submitting a
    /// different tree request.
    pub async fn save_submitted_spark_split(
        &self,
        operation_id: &str,
        submitted: &[u8],
    ) -> Result<(), String> {
        let operation_id = operation_id.to_string();
        let submitted = submitted.to_vec();
        self.with(move |c| {
            let changed = c.execute(
                "UPDATE spark_split_operations
                 SET plan=?2,status='SUBMITTED',last_error=NULL,updated_at=?3
                 WHERE operation_id=?1 AND status IN ('SUBMITTING','SUBMITTED')",
                (&operation_id, &submitted, chrono::Utc::now().to_rfc3339()),
            )?;
            if changed == 1 {
                Ok(())
            } else {
                Err(rusqlite::Error::InvalidQuery)
            }
        })
        .await
    }

    /// Persist progress made inside the opaque SDK plan (for example prepared
    /// addresses) without changing its identity or child keys.
    pub async fn update_spark_split_plan(
        &self,
        operation_id: &str,
        plan: &[u8],
    ) -> Result<(), String> {
        let operation_id = operation_id.to_string();
        let plan = plan.to_vec();
        self.with(move |c| {
            let changed = c.execute(
                "UPDATE spark_split_operations SET plan=?2,updated_at=?3
                 WHERE operation_id=?1 AND status!='COMPLETED'",
                (&operation_id, &plan, chrono::Utc::now().to_rfc3339()),
            )?;
            if changed == 1 {
                Ok(())
            } else {
                Err(rusqlite::Error::InvalidQuery)
            }
        })
        .await
    }

    pub async fn record_spark_split_error(
        &self,
        operation_id: &str,
        error: &str,
    ) -> Result<(), String> {
        let operation_id = operation_id.to_string();
        let error = error.to_string();
        self.with(move |c| {
            c.execute(
                "UPDATE spark_split_operations SET last_error=?2,updated_at=?3
                 WHERE operation_id=?1 AND status!='COMPLETED'",
                (&operation_id, &error, chrono::Utc::now().to_rfc3339()),
            )
            .map(|_| ())
        })
        .await
    }

    /// Commit the operator-assigned child IDs and their private key material
    /// together. Once this returns, a restarted signer can resolve every new
    /// child before the completed operation is used for liquidity.
    pub async fn complete_spark_split(
        &self,
        operation_id: &str,
        overrides: &[SparkLeafKeyOverride],
    ) -> Result<(), String> {
        let operation_id = operation_id.to_string();
        let overrides = overrides.to_vec();
        self.with(move |c| {
            let tx = c.unchecked_transaction()?;
            let expected_children: String = tx.query_row(
                "SELECT child_values_sats FROM spark_split_operations WHERE operation_id=?1",
                (&operation_id,),
                |row| row.get(0),
            )?;
            let expected_children: Vec<u64> = serde_json::from_str(&expected_children)
                .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
            if overrides.len() != expected_children.len()
                || overrides.iter().any(|item| item.node_id.is_empty() || item.key_material.is_empty())
            {
                return Err(rusqlite::Error::InvalidQuery);
            }
            let child_node_ids = overrides
                .iter()
                .map(|item| item.node_id.clone())
                .collect::<Vec<_>>();
            let now = chrono::Utc::now().to_rfc3339();
            for item in &overrides {
                tx.execute(
                    "INSERT INTO spark_leaf_key_overrides(node_id,operation_id,key_material,created_at)
                     VALUES(?1,?2,?3,?4)
                     ON CONFLICT(node_id) DO UPDATE SET
                       operation_id=excluded.operation_id,key_material=excluded.key_material",
                    rusqlite::params![item.node_id, operation_id, item.key_material, now],
                )?;
            }
            tx.execute(
                "UPDATE spark_split_operations
                 SET status='COMPLETED',child_node_ids=?2,last_error=NULL,updated_at=?3
                 WHERE operation_id=?1",
                (
                    &operation_id,
                    serde_json::to_string(&child_node_ids)
                        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
                    &now,
                ),
            )?;
            tx.execute(
                "DELETE FROM spark_pending_split_keys WHERE operation_id=?1",
                (&operation_id,),
            )?;
            tx.commit()
        })
        .await
    }

    pub async fn mark_spark_split_completed(&self, operation_id: &str) -> Result<(), String> {
        let operation_id = operation_id.to_string();
        self.with(move |c| {
            let changed = c.execute(
                "UPDATE spark_split_operations
                 SET status='COMPLETED',last_error=NULL,updated_at=?2
                 WHERE operation_id=?1 AND status IN ('SUBMITTED','COMPLETED')
                   AND child_node_ids!='[]'",
                (&operation_id, chrono::Utc::now().to_rfc3339()),
            )?;
            if changed == 1 {
                Ok(())
            } else {
                Err(rusqlite::Error::InvalidQuery)
            }
        })
        .await
    }

    pub async fn spark_leaf_key_overrides(&self) -> Result<Vec<SparkLeafKeyOverride>, String> {
        self.with(|c| {
            let mut statement = c.prepare(
                "SELECT node_id,key_material FROM spark_leaf_key_overrides ORDER BY node_id",
            )?;
            let rows = statement
                .query_map([], |row| {
                    Ok(SparkLeafKeyOverride {
                        node_id: row.get(0)?,
                        key_material: row.get(1)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await
    }

    pub async fn spark_leaf_key_override(&self, node_id: &str) -> Result<Option<Vec<u8>>, String> {
        let node_id = node_id.to_string();
        self.with(move |c| {
            c.query_row(
                "SELECT key_material FROM spark_leaf_key_overrides WHERE node_id=?1",
                (&node_id,),
                |row| row.get(0),
            )
            .map(Some)
            .or_else(|error| match error {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                error => Err(error),
            })
        })
        .await
    }

    /// Retire overrides when an incoming transfer rotates those node IDs back
    /// to their normal derived keys. Pending split keys are intentionally kept:
    /// they represent a different, not-yet-bound creation operation.
    pub async fn retire_spark_leaf_keys(&self, node_ids: &[String]) -> Result<(), String> {
        let node_ids = node_ids.to_vec();
        self.with(move |c| {
            let tx = c.unchecked_transaction()?;
            for node_id in node_ids {
                tx.execute(
                    "DELETE FROM spark_leaf_key_overrides WHERE node_id=?1",
                    (&node_id,),
                )?;
            }
            tx.commit()
        })
        .await
    }

    pub async fn pending_spark_split_keys(
        &self,
        operation_id: &str,
        parent_node_id: &str,
    ) -> Result<Option<Vec<Vec<u8>>>, String> {
        let operation_id = operation_id.to_string();
        let parent_node_id = parent_node_id.to_string();
        self.with(move |c| {
            let encoded: Option<Vec<u8>> = c
                .query_row(
                    "SELECT encrypted_keys FROM spark_pending_split_keys
                     WHERE operation_id=?1 AND parent_node_id=?2",
                    (&operation_id, &parent_node_id),
                    |row| row.get(0),
                )
                .map(Some)
                .or_else(|error| match error {
                    rusqlite::Error::QueryReturnedNoRows => Ok(None),
                    error => Err(error),
                })?;
            encoded
                .map(|encoded| {
                    serde_json::from_slice(&encoded).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Blob,
                            Box::new(error),
                        )
                    })
                })
                .transpose()
        })
        .await
    }

    /// Persist once. A retry with different key material is rejected so an
    /// ambiguous split can never be retried using newly generated children.
    pub async fn put_pending_spark_split_keys(
        &self,
        operation_id: &str,
        parent_node_id: &str,
        encrypted_keys: &[Vec<u8>],
    ) -> Result<(), String> {
        if encrypted_keys.is_empty() || encrypted_keys.iter().any(Vec::is_empty) {
            return Err("pending Spark split keys must be non-empty".to_string());
        }
        let operation_id = operation_id.to_string();
        let parent_node_id = parent_node_id.to_string();
        let encoded = serde_json::to_vec(encrypted_keys).map_err(|e| e.to_string())?;
        self.with(move |c| {
            c.execute(
                "INSERT OR IGNORE INTO spark_pending_split_keys(operation_id,parent_node_id,encrypted_keys,created_at)
                 VALUES(?1,?2,?3,?4)",
                (
                    &operation_id,
                    &parent_node_id,
                    &encoded,
                    chrono::Utc::now().to_rfc3339(),
                ),
            )?;
            let stored: (String, Vec<u8>) = c.query_row(
                "SELECT parent_node_id,encrypted_keys FROM spark_pending_split_keys WHERE operation_id=?1",
                (&operation_id,),
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            if stored == (parent_node_id, encoded) {
                Ok(())
            } else {
                Err(rusqlite::Error::InvalidQuery)
            }
        })
        .await
    }

    pub async fn bind_pending_spark_split_keys(
        &self,
        operation_id: &str,
        node_ids: &[String],
    ) -> Result<(), String> {
        if node_ids.is_empty() || node_ids.iter().any(String::is_empty) {
            return Err("split child node IDs must be non-empty".to_string());
        }
        let operation_id = operation_id.to_string();
        let node_ids = node_ids.to_vec();
        self.with(move |c| {
            let tx = c.unchecked_transaction()?;
            let operation: (String, String) = tx.query_row(
                "SELECT child_values_sats,child_node_ids FROM spark_split_operations
                 WHERE operation_id=?1",
                (&operation_id,),
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            let expected_values: Vec<u64> = serde_json::from_str(&operation.0)
                .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
            if expected_values.len() != node_ids.len() {
                return Err(rusqlite::Error::InvalidQuery);
            }
            let pending: Option<Vec<u8>> = tx
                .query_row(
                    "SELECT encrypted_keys FROM spark_pending_split_keys WHERE operation_id=?1",
                    (&operation_id,),
                    |row| row.get(0),
                )
                .map(Some)
                .or_else(|error| match error {
                    rusqlite::Error::QueryReturnedNoRows => Ok(None),
                    error => Err(error),
                })?;
            let Some(pending) = pending else {
                let bound: Vec<String> = serde_json::from_str(&operation.1)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
                return if bound == node_ids {
                    tx.commit()
                } else {
                    Err(rusqlite::Error::InvalidQuery)
                };
            };
            let encrypted_keys: Vec<Vec<u8>> = serde_json::from_slice(&pending)
                .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
            if encrypted_keys.len() != node_ids.len() {
                return Err(rusqlite::Error::InvalidQuery);
            }
            let now = chrono::Utc::now().to_rfc3339();
            for (node_id, key_material) in node_ids.iter().zip(encrypted_keys) {
                tx.execute(
                    "INSERT INTO spark_leaf_key_overrides(node_id,operation_id,key_material,created_at)
                     VALUES(?1,?2,?3,?4)
                     ON CONFLICT(node_id) DO UPDATE SET
                       operation_id=excluded.operation_id,key_material=excluded.key_material",
                    rusqlite::params![node_id, operation_id, key_material, now],
                )?;
            }
            tx.execute(
                "UPDATE spark_split_operations
                 SET child_node_ids=?2,last_error=NULL,updated_at=?3
                 WHERE operation_id=?1",
                (
                    &operation_id,
                    serde_json::to_string(&node_ids)
                        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
                    &now,
                ),
            )?;
            tx.execute(
                "DELETE FROM spark_pending_split_keys WHERE operation_id=?1",
                (&operation_id,),
            )?;
            tx.commit()
        })
        .await
    }
}

fn spark_split_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SparkSplitOperation> {
    let parent_value_sats: u64 = row.get(2)?;
    let child_values: String = row.get(3)?;
    let child_node_ids: String = row.get(6)?;
    Ok(SparkSplitOperation {
        operation_id: row.get(0)?,
        parent_node_id: row.get(1)?,
        parent_value_sats,
        child_values_sats: serde_json::from_str(&child_values).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(3, rusqlite::types::Type::Text, Box::new(e))
        })?,
        plan: row.get(4)?,
        status: row.get(5)?,
        child_node_ids: serde_json::from_str(&child_node_ids).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(6, rusqlite::types::Type::Text, Box::new(e))
        })?,
        last_error: row.get(7)?,
    })
}

fn optional_spark_split(
    conn: &rusqlite::Connection,
    parent_node_id: &str,
) -> rusqlite::Result<Option<SparkSplitOperation>> {
    conn.query_row(
        "SELECT operation_id,parent_node_id,parent_value_sats,child_values_sats,plan,
                status,child_node_ids,last_error
         FROM spark_split_operations WHERE parent_node_id=?1",
        (parent_node_id,),
        spark_split_from_row,
    )
    .map(Some)
    .or_else(|error| match error {
        rusqlite::Error::QueryReturnedNoRows => Ok(None),
        error => Err(error),
    })
}

fn read_spark_split(
    conn: &rusqlite::Connection,
    parent_node_id: &str,
) -> rusqlite::Result<SparkSplitOperation> {
    optional_spark_split(conn, parent_node_id)?.ok_or(rusqlite::Error::QueryReturnedNoRows)
}

#[async_trait::async_trait]
impl ::spark::signer::LeafKeyOverrideStore for Db {
    async fn get_leaf_key(
        &self,
        node_id: &::spark::tree::TreeNodeId,
    ) -> Result<Option<Vec<u8>>, ::spark::signer::LeafKeyOverrideStoreError> {
        self.spark_leaf_key_override(&node_id.to_string())
            .await
            .map_err(::spark::signer::LeafKeyOverrideStoreError::Generic)
    }

    async fn get_pending_split_keys(
        &self,
        operation_id: &str,
        parent_node_id: &::spark::tree::TreeNodeId,
    ) -> Result<Option<Vec<Vec<u8>>>, ::spark::signer::LeafKeyOverrideStoreError> {
        self.pending_spark_split_keys(operation_id, &parent_node_id.to_string())
            .await
            .map_err(::spark::signer::LeafKeyOverrideStoreError::Generic)
    }

    async fn put_pending_split_keys(
        &self,
        operation_id: &str,
        parent_node_id: &::spark::tree::TreeNodeId,
        encrypted_keys: &[Vec<u8>],
    ) -> Result<(), ::spark::signer::LeafKeyOverrideStoreError> {
        self.put_pending_spark_split_keys(operation_id, &parent_node_id.to_string(), encrypted_keys)
            .await
            .map_err(::spark::signer::LeafKeyOverrideStoreError::Generic)
    }

    async fn bind_pending_split_keys(
        &self,
        operation_id: &str,
        node_ids: &[::spark::tree::TreeNodeId],
    ) -> Result<(), ::spark::signer::LeafKeyOverrideStoreError> {
        let node_ids = node_ids.iter().map(ToString::to_string).collect::<Vec<_>>();
        self.bind_pending_spark_split_keys(operation_id, &node_ids)
            .await
            .map_err(::spark::signer::LeafKeyOverrideStoreError::Generic)
    }

    async fn retire_leaf_keys(
        &self,
        node_ids: &[::spark::tree::TreeNodeId],
    ) -> Result<(), ::spark::signer::LeafKeyOverrideStoreError> {
        let node_ids = node_ids.iter().map(ToString::to_string).collect::<Vec<_>>();
        self.retire_spark_leaf_keys(&node_ids)
            .await
            .map_err(::spark::signer::LeafKeyOverrideStoreError::Generic)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_db() -> (Db, PathBuf) {
        let dir = std::env::temp_dir().join(format!("open-ssp-{}", uuid::Uuid::new_v4()));
        (Db::open(dir.to_str().unwrap()).unwrap(), dir)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn challenge_is_single_use() {
        let (db, dir) = test_db();
        let now = chrono::Utc::now();
        db.save_challenge("owner", "challenge", &now.to_rfc3339())
            .await
            .unwrap();

        assert!(db
            .consume_challenge("owner", "challenge", now.timestamp(), 300)
            .await
            .unwrap());
        assert!(!db
            .consume_challenge("owner", "challenge", now.timestamp(), 300)
            .await
            .unwrap());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn challenge_rejects_unknown_expired_and_future_values() {
        let (db, dir) = test_db();
        let now = chrono::Utc::now();

        assert!(!db
            .consume_challenge("owner", "never-issued", now.timestamp(), 300)
            .await
            .unwrap());

        db.save_challenge(
            "owner",
            "expired",
            &(now - chrono::Duration::seconds(301)).to_rfc3339(),
        )
        .await
        .unwrap();
        assert!(!db
            .consume_challenge("owner", "expired", now.timestamp(), 300)
            .await
            .unwrap());

        db.save_challenge(
            "owner",
            "future",
            &(now + chrono::Duration::seconds(1)).to_rfc3339(),
        )
        .await
        .unwrap();
        assert!(!db
            .consume_challenge("owner", "future", now.timestamp(), 300)
            .await
            .unwrap());

        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lightning_receive_links_request_and_transfer() {
        let (db, dir) = test_db();
        db.insert_request(
            "request",
            "LIGHTNING_RECEIVE",
            "owner",
            "now",
            &serde_json::json!({
                "payment_hash": "hash",
                "amount_sats": 1234,
                "invoice": "ln-invoice",
            }),
            None,
        )
        .await
        .unwrap();

        assert_eq!(
            db.lightning_receive_for_hash("hash").await.unwrap(),
            Some(LightningReceive {
                request_id: "request".to_string(),
                owner: "owner".to_string(),
                receiver: "owner".to_string(),
                amount_sats: 1234,
                invoice: "ln-invoice".to_string(),
                status: crate::lightning_store::ReceiveStatus::InvoiceCreated,
                transfer_id: None,
                preimage: None,
                claim_submitted: false,
            })
        );
        assert_eq!(
            db.lightning_receive_hash_for_invoice("ln-invoice")
                .await
                .unwrap(),
            Some("hash".to_string())
        );
        assert_eq!(
            db.lightning_receive_hash_for_invoice("other-invoice")
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            db.transfer_for_request("request", "owner").await.unwrap(),
            None
        );

        db.insert_transfer(
            "transfer",
            "request",
            "LIGHTNING_RECEIVE",
            "TRANSFER_COMPLETED",
            "owner",
        )
        .await
        .unwrap();
        assert_eq!(
            db.transfer_for_request("request", "owner").await.unwrap(),
            Some("transfer".to_string())
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn bolt12_send_restores_funding_metadata() {
        let (db, dir) = test_db();
        db.insert_request(
            "request",
            "LIGHTNING_SEND",
            "owner",
            "now",
            &serde_json::json!({
                "payment_id": "payment",
                "payment_kind": "BOLT12",
                "amount_sats": 1234,
                "user_outbound_transfer_external_id": "funding",
            }),
            None,
        )
        .await
        .unwrap();

        let send = db
            .lightning_send_for_payment("payment")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(send.owner, "owner");
        assert_eq!(send.outbound_transfer_id, "funding");
        assert_eq!(send.kind, crate::lightning_store::SendKind::Bolt12);
        assert_eq!(send.amount_sats, 1234);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn bolt12_receive_commit_is_idempotent() {
        let (db, dir) = test_db();
        db.insert_request(
            "request",
            "LIGHTNING_RECEIVE",
            "owner",
            "now",
            &serde_json::json!({
                "payment_hash": "offer",
                "offer_id": "offer",
                "payment_kind": "BOLT12",
                "amount_sats": 1234,
            }),
            None,
        )
        .await
        .unwrap();
        db.set_receive_status("offer", "INVOICE_CREATED")
            .await
            .unwrap();

        for _ in 0..2 {
            db.commit_bolt12_receive("offer", "hash", "transfer", "request", "owner")
                .await
                .unwrap();
        }

        assert_eq!(
            db.receive_status("offer").await.unwrap(),
            "TRANSFER_COMPLETED"
        );
        assert_eq!(
            db.transfer_for_request("request", "owner").await.unwrap(),
            Some("transfer".to_string())
        );
        let request = db.get_request("request", "owner").await.unwrap().unwrap();
        assert_eq!(request["payload"]["payment_hash"], "offer");
        let settled: String = db
            .with(|c| {
                c.query_row(
                    "SELECT settled_payment_hash FROM lightning_receives WHERE hash='offer'",
                    [],
                    |r| r.get(0),
                )
            })
            .await
            .unwrap();
        assert_eq!(settled, "hash");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn static_quote_is_retryable_but_claims_once() {
        let (db, dir) = test_db();
        let txid = "00".repeat(32);
        assert_eq!(
            db.record_static_quote(&txid, 0, 100_000, "first", "now")
                .await
                .unwrap(),
            Some("first".to_string())
        );
        assert_eq!(
            db.record_static_quote(&txid, 0, 100_000, "retry", "later")
                .await
                .unwrap(),
            Some("first".to_string())
        );
        assert!(db.consume_static_quote(&txid, 0, "first").await.unwrap());
        assert!(!db.consume_static_quote(&txid, 0, "first").await.unwrap());
        assert_eq!(
            db.record_static_quote(&txid, 0, 100_000, "after", "later")
                .await
                .unwrap(),
            None
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn coop_exit_request_cannot_poison_receive_transfer_ids() {
        let (db, dir) = test_db();
        let owner = "owner";
        // Deterministic receive transfer id a wallet can derive in advance
        // from its own payment hash.
        let spark_id = "00000000-0000-4000-8000-000000000001";
        db.insert_request(
            "coop-request",
            "COOP_EXIT",
            owner,
            "now",
            &serde_json::json!({
                "user_outbound_transfer_external_id": spark_id,
                "exit_speed": "MEDIUM",
            }),
            None,
        )
        .await
        .unwrap();
        db.insert_request(
            "receive-request",
            "LIGHTNING_RECEIVE",
            owner,
            "now",
            &serde_json::json!({
                "payment_hash": "hash",
                "amount_sats": 1000,
            }),
            None,
        )
        .await
        .unwrap();

        // CompleteCoopExit still correlates through the request payload...
        assert_eq!(
            db.request_id_for_ext_id("COOP_EXIT", spark_id, owner)
                .await
                .unwrap(),
            Some("coop-request".to_string())
        );
        db.set_request_status("coop-request", owner, "COMPLETED")
            .await
            .unwrap();
        let request = db
            .get_request("coop-request", owner)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(request["payload"]["status"], "COMPLETED");

        // ...and the receive checkpoint for the same Spark id commits cleanly
        // because the compatibility request reserved no transfers row.
        db.commit_lightning_receive_swap(
            "hash",
            spark_id,
            &"01".repeat(32),
            "receive-request",
            owner,
        )
        .await
        .unwrap();
        assert_eq!(
            db.transfer_for_request("receive-request", owner)
                .await
                .unwrap(),
            Some(spark_id.to_string())
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn spark_split_plan_and_keys_survive_restart() {
        let (db, dir) = test_db();
        db.put_pending_spark_split_keys(
            "split-1",
            "parent",
            &[b"encrypted-a".to_vec(), b"encrypted-b".to_vec()],
        )
        .await
        .unwrap();
        let candidate = SparkSplitOperation {
            operation_id: "split-1".to_string(),
            parent_node_id: "parent".to_string(),
            parent_value_sats: 10_000,
            child_values_sats: vec![7_000, 3_000],
            plan: b"opaque-encrypted-plan".to_vec(),
            status: "PREPARED".to_string(),
            child_node_ids: Vec::new(),
            last_error: None,
        };
        assert_eq!(
            db.get_or_insert_spark_split(&candidate).await.unwrap(),
            candidate
        );

        // Retrying preparation for the same parent must recover the first
        // plan, not replace its already-persisted child secrets.
        let mut conflicting = candidate.clone();
        conflicting.operation_id = "split-2".to_string();
        conflicting.plan = b"fresh-and-unsafe".to_vec();
        assert_eq!(
            db.get_or_insert_spark_split(&conflicting).await.unwrap(),
            candidate
        );
        db.mark_spark_split_submitting("split-1").await.unwrap();
        drop(db);

        let db = Db::open(dir.to_str().unwrap()).unwrap();
        assert_eq!(
            db.pending_spark_split_keys("split-1", "parent")
                .await
                .unwrap(),
            Some(vec![b"encrypted-a".to_vec(), b"encrypted-b".to_vec()])
        );
        let pending = db.incomplete_spark_splits().await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].plan, b"opaque-encrypted-plan");
        assert_eq!(pending[0].status, "SUBMITTING");
        db.save_submitted_spark_split("split-1", b"submitted-response")
            .await
            .unwrap();
        assert_eq!(
            db.spark_split_for_parent("parent")
                .await
                .unwrap()
                .unwrap()
                .plan,
            b"submitted-response"
        );

        let overrides = vec![
            SparkLeafKeyOverride {
                node_id: "child-a".to_string(),
                key_material: b"encrypted-a".to_vec(),
            },
            SparkLeafKeyOverride {
                node_id: "child-b".to_string(),
                key_material: b"encrypted-b".to_vec(),
            },
        ];
        db.bind_pending_spark_split_keys(
            "split-1",
            &["child-a".to_string(), "child-b".to_string()],
        )
        .await
        .unwrap();
        // A retry after an ambiguous response is an idempotent no-op.
        db.bind_pending_spark_split_keys(
            "split-1",
            &["child-a".to_string(), "child-b".to_string()],
        )
        .await
        .unwrap();
        db.mark_spark_split_completed("split-1").await.unwrap();
        drop(db);

        let db = Db::open(dir.to_str().unwrap()).unwrap();
        assert!(db.incomplete_spark_splits().await.unwrap().is_empty());
        assert_eq!(
            db.pending_spark_split_keys("split-1", "parent")
                .await
                .unwrap(),
            None
        );
        assert_eq!(db.spark_leaf_key_overrides().await.unwrap(), overrides);
        let completed = db.spark_split_for_parent("parent").await.unwrap().unwrap();
        assert_eq!(completed.status, "COMPLETED");
        assert_eq!(completed.child_node_ids, ["child-a", "child-b"]);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn spark_split_completion_is_atomic() {
        let (db, dir) = test_db();
        db.get_or_insert_spark_split(&SparkSplitOperation {
            operation_id: "split".to_string(),
            parent_node_id: "parent".to_string(),
            parent_value_sats: 2_000,
            child_values_sats: vec![1_000, 1_000],
            plan: vec![1],
            status: "PREPARED".to_string(),
            child_node_ids: Vec::new(),
            last_error: None,
        })
        .await
        .unwrap();

        let error = db
            .complete_spark_split(
                "split",
                &[SparkLeafKeyOverride {
                    node_id: "only-one".to_string(),
                    key_material: vec![2],
                }],
            )
            .await
            .unwrap_err();
        assert!(!error.is_empty());
        assert!(db.spark_leaf_key_overrides().await.unwrap().is_empty());
        assert_eq!(
            db.spark_split_for_parent("parent")
                .await
                .unwrap()
                .unwrap()
                .status,
            "PREPARED"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pending_split_keys_cannot_be_replaced() {
        let (db, dir) = test_db();
        db.put_pending_spark_split_keys("split", "parent", &[vec![1], vec![2]])
            .await
            .unwrap();
        db.put_pending_spark_split_keys("split", "parent", &[vec![1], vec![2]])
            .await
            .unwrap();
        assert!(db
            .put_pending_spark_split_keys("split", "parent", &[vec![3], vec![4]])
            .await
            .is_err());
        assert_eq!(
            db.pending_spark_split_keys("split", "parent")
                .await
                .unwrap(),
            Some(vec![vec![1], vec![2]])
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn retiring_returned_leaf_override_is_atomic_and_restart_safe() {
        let (db, dir) = test_db();
        db.put_pending_spark_split_keys(
            "split",
            "parent",
            &[b"encrypted-a".to_vec(), b"encrypted-b".to_vec()],
        )
        .await
        .unwrap();
        db.get_or_insert_spark_split(&SparkSplitOperation {
            operation_id: "split".to_string(),
            parent_node_id: "parent".to_string(),
            parent_value_sats: 2_000,
            child_values_sats: vec![1_000, 1_000],
            plan: vec![1],
            status: "SUBMITTED".to_string(),
            child_node_ids: Vec::new(),
            last_error: None,
        })
        .await
        .unwrap();
        db.bind_pending_spark_split_keys("split", &["child-a".to_string(), "child-b".to_string()])
            .await
            .unwrap();

        db.put_pending_spark_split_keys("next-split", "other-parent", &[vec![9], vec![10]])
            .await
            .unwrap();
        for _ in 0..2 {
            db.retire_spark_leaf_keys(&["child-a".to_string()])
                .await
                .unwrap();
        }
        drop(db);

        let db = Db::open(dir.to_str().unwrap()).unwrap();
        assert_eq!(db.spark_leaf_key_override("child-a").await.unwrap(), None);
        assert_eq!(
            db.spark_leaf_key_override("child-b").await.unwrap(),
            Some(b"encrypted-b".to_vec())
        );
        assert_eq!(
            db.pending_spark_split_keys("next-split", "other-parent")
                .await
                .unwrap(),
            Some(vec![vec![9], vec![10]])
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn compat_requests_are_counted_and_pruned() {
        let (db, dir) = test_db();
        let old = "2020-01-01T00:00:00+00:00";
        let now = chrono::Utc::now().to_rfc3339();
        db.insert_request(
            "old-coop",
            "COOP_EXIT",
            "owner",
            old,
            &serde_json::json!({"exit_speed": "MEDIUM"}),
            None,
        )
        .await
        .unwrap();
        db.insert_request(
            "old-claim",
            "CLAIM_STATIC_DEPOSIT",
            "owner",
            old,
            &serde_json::json!({"transaction_id": "tx"}),
            None,
        )
        .await
        .unwrap();
        db.insert_request(
            "old-send",
            "LIGHTNING_SEND",
            "owner",
            old,
            &serde_json::json!({"amount_sats": 1}),
            None,
        )
        .await
        .unwrap();
        db.insert_request(
            "new-coop",
            "COOP_EXIT",
            "owner",
            &now,
            &serde_json::json!({"exit_speed": "MEDIUM"}),
            None,
        )
        .await
        .unwrap();

        // Only compat rows inside the window are quota-counted.
        assert_eq!(
            db.compat_request_count(
                "owner",
                &(chrono::Utc::now() - chrono::Duration::hours(24)).to_rfc3339()
            )
            .await
            .unwrap(),
            1
        );

        // Pruning removes only old compatibility rows; financial records stay.
        assert_eq!(db.prune_compat_requests(&now).await.unwrap(), 2);
        assert!(db.get_request("old-send", "owner").await.unwrap().is_some());
        assert!(db.get_request("new-coop", "owner").await.unwrap().is_some());
        assert!(db.get_request("old-coop", "owner").await.unwrap().is_none());
        assert!(db
            .get_request("old-claim", "owner")
            .await
            .unwrap()
            .is_none());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn database_and_sidecars_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let dir = std::env::temp_dir().join(format!("open-ssp-db-mode-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("ssp.sqlite");
        // A restored or copied database can arrive world-readable.
        std::fs::write(&db_path, b"").unwrap();
        let mut permissions = std::fs::metadata(&db_path).unwrap().permissions();
        permissions.set_mode(0o666);
        std::fs::set_permissions(&db_path, permissions).unwrap();

        let db = Db::open(dir.to_str().unwrap()).unwrap();

        let mode = std::fs::metadata(&db_path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);

        // A write in WAL mode creates the -wal sidecar; SQLite gives it
        // exactly the database file's mode, so it must be owner-only too.
        db.insert_request(
            "request",
            "LIGHTNING_RECEIVE",
            "owner",
            &chrono::Utc::now().to_rfc3339(),
            &serde_json::json!({"payment_hash": "hash"}),
            None,
        )
        .await
        .unwrap();
        let wal_path = dir.join("ssp.sqlite-wal");
        assert!(wal_path.exists());
        let wal_mode = std::fs::metadata(&wal_path).unwrap().permissions().mode();
        assert_eq!(wal_mode & 0o777, 0o600);
        drop(db);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
