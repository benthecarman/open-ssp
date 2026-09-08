//! Durable, at-least-once wallet notifications. Secrets never leave this module.
use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use hmac::{Hmac, Mac};
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use sha2::Sha256;
use uuid::Uuid;

use crate::db::Db;

const EVENTS: [&str; 4] = [
    "SPARK_LIGHTNING_RECEIVE_FINISHED",
    "SPARK_LIGHTNING_SEND_FINISHED",
    "SPARK_COOP_EXIT_FINISHED",
    "SPARK_STATIC_DEPOSIT_FINISHED",
];

pub fn migrate(c: &Connection) -> rusqlite::Result<()> {
    c.execute_batch("CREATE TABLE IF NOT EXISTS wallet_webhooks(id TEXT PRIMARY KEY,owner TEXT NOT NULL,url TEXT NOT NULL,secret TEXT NOT NULL,events TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS webhooks_owner ON wallet_webhooks(owner);
        CREATE TABLE IF NOT EXISTS webhook_events(id TEXT PRIMARY KEY,request_id TEXT NOT NULL UNIQUE,owner TEXT NOT NULL,event_type TEXT NOT NULL,payload TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS webhook_deliveries(event_id TEXT NOT NULL REFERENCES webhook_events(id),webhook_id TEXT NOT NULL REFERENCES wallet_webhooks(id) ON DELETE CASCADE,attempts INTEGER NOT NULL DEFAULT 0,next_attempt INTEGER NOT NULL DEFAULT 0,delivered INTEGER NOT NULL DEFAULT 0,last_error TEXT,PRIMARY KEY(event_id,webhook_id));
        CREATE INDEX IF NOT EXISTS webhook_due ON webhook_deliveries(delivered,next_attempt);")
}

/// Capture and enqueue in the caller's transaction, including when a new
/// subscription is registered. Registration never sends old completions.
fn capture(c: &Connection) -> rusqlite::Result<()> {
    let mut stmt = c.prepare("SELECT h.id,h.owner,h.request_type,h.request_status,h.payload,l.receiver,l.amount_sats,p.preimage
        FROM request_history h LEFT JOIN lightning_receives l ON l.request_id=h.id LEFT JOIN receive_payments p ON p.hash=l.hash
        WHERE h.request_status IN ('SUCCEEDED','FAILED','CANCELED') AND h.request_type IN ('LIGHTNING_SEND','LIGHTNING_RECEIVE','COOP_EXIT','CLAIM_STATIC_DEPOSIT') AND NOT EXISTS(SELECT 1 FROM webhook_events e WHERE e.request_id=h.id)")?;
    let rows = stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, String>(3)?,
            r.get::<_, String>(4)?,
            r.get::<_, Option<String>>(5)?,
            r.get::<_, Option<u64>>(6)?,
            r.get::<_, Option<String>>(7)?,
        ))
    })?;
    for row in rows {
        let (request, owner, kind, status, data, receiver, amount, preimage) = row?;
        let event_type = match kind.as_str() {
            "LIGHTNING_RECEIVE" => EVENTS[0],
            "LIGHTNING_SEND" => EVENTS[1],
            "COOP_EXIT" => EVENTS[2],
            _ => EVENTS[3],
        };
        let id = Uuid::new_v4().to_string();
        let data: Value = serde_json::from_str(&data)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
        let payload = json!({"event_id":id,"type":event_type,"request_id":request,"status":status,
            "receiver_identity_public_key":receiver,"payment_preimage":preimage,
            "htlc_amount":amount.map(|v| json!({"value":v,"unit":"SATOSHI"})),
            "network":data["network"]})
        .to_string();
        c.execute(
            "INSERT INTO webhook_events VALUES(?1,?2,?3,?4,?5)",
            params![id, request, owner, event_type, payload],
        )?;
        c.execute("INSERT INTO webhook_deliveries(event_id,webhook_id) SELECT ?1,id FROM wallet_webhooks WHERE owner=?2 AND EXISTS(SELECT 1 FROM json_each(events) WHERE value=?3)",params![id,owner,event_type])?;
    }
    Ok(())
}

pub fn allow_local(network: &str) -> bool {
    matches!(network, "REGTEST" | "LOCAL")
        && std::env::var("SSP_WEBHOOK_ALLOW_LOCAL").as_deref() == Ok("1")
}

fn parse_url(raw: &str, local: bool) -> Result<reqwest::Url, String> {
    if raw.len() > 2048 {
        return Err("webhook URL is too long".into());
    }
    let url = reqwest::Url::parse(raw).map_err(|_| "invalid webhook URL")?;
    if !(url.scheme() == "https" || local && url.scheme() == "http")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err("webhook needs an HTTPS URL without credentials or a fragment".into());
    }
    Ok(url)
}

fn public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let [a, b, _, _] = ip.octets();
            !ip.is_private()
                && !ip.is_loopback()
                && !ip.is_link_local()
                && !ip.is_broadcast()
                && !ip.is_documentation()
                && a != 0
                && a < 224
                && !(a == 100 && (64..=127).contains(&b))
                && !(a == 198 && (b == 18 || b == 19))
                && !(a == 192 && b == 0)
        }
        IpAddr::V6(ip) => {
            if let Some(v4) = ip.to_ipv4_mapped() {
                return public_ip(IpAddr::V4(v4));
            }
            // Global unicast only; exclude transition and documentation ranges.
            let s = ip.segments();
            (s[0] & 0xe000) == 0x2000
                && s[0] != 0x2002
                && !(s[0] == 0x2001 && (s[1] < 0x200 || s[1] == 0xdb8))
        }
    }
}

fn local_test_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => ip.is_loopback() || ip.is_private(),
        IpAddr::V6(ip) => match ip.to_ipv4_mapped() {
            Some(ip) => local_test_ip(IpAddr::V4(ip)),
            None => ip.is_loopback() || ip.is_unique_local(),
        },
    }
}

async fn client_for(url: &reqwest::Url, local: bool) -> Result<reqwest::Client, String> {
    let host = url
        .host_str()
        .ok_or("missing webhook host")?
        .trim_matches(['[', ']']);
    let port = url.port_or_known_default().ok_or("missing webhook port")?;
    let addresses: Vec<SocketAddr> = match host.parse::<IpAddr>() {
        Ok(ip) => vec![SocketAddr::new(ip, port)],
        Err(_) => tokio::time::timeout(
            Duration::from_secs(5),
            tokio::net::lookup_host((host, port)),
        )
        .await
        .map_err(|_| "webhook DNS timeout")?
        .map_err(|_| "webhook DNS failed")?
        .take(32)
        .collect(),
    };
    if addresses.is_empty()
        || addresses
            .iter()
            .any(|a| !public_ip(a.ip()) && !(local && local_test_ip(a.ip())))
    {
        return Err("webhook address is not public".into());
    }
    reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(10))
        .resolve_to_addrs(host, &addresses)
        .build()
        .map_err(|_| "webhook HTTP client failed".into())
}

fn signature(secret: &str, body: &str) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts every key size");
    mac.update(body.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

impl Db {
    pub async fn register_webhook(
        &self,
        owner: &str,
        input: &Value,
        local: bool,
    ) -> Result<String, String> {
        let url = parse_url(input["url"].as_str().ok_or("webhook URL required")?, local)?;
        let _ = client_for(&url, local).await?;
        let secret = input["secret"]
            .as_str()
            .filter(|s| !s.is_empty() && s.len() <= 4096)
            .ok_or("webhook secret must contain 1 to 4096 bytes")?;
        let events = input["event_types"]
            .as_array()
            .filter(|a| !a.is_empty() && a.len() <= EVENTS.len())
            .ok_or("webhook event_types required")?;
        if events
            .iter()
            .any(|v| !v.as_str().is_some_and(|v| EVENTS.contains(&v)))
        {
            return Err("unsupported webhook event type".into());
        }
        let id = Uuid::new_v4().to_string();
        self.with(|c| {
            let tx = c.unchecked_transaction()?;
            let count: u64 = tx.query_row(
                "SELECT count(*) FROM wallet_webhooks WHERE owner=?1",
                [owner],
                |r| r.get(0),
            )?;
            if count >= 10 {
                return Err(rusqlite::Error::ToSqlConversionFailure(
                    "wallet webhook limit reached".into(),
                ));
            }
            capture(&tx)?;
            tx.execute(
                "INSERT INTO wallet_webhooks VALUES(?1,?2,?3,?4,?5)",
                params![id, owner, url.as_str(), secret, json!(events).to_string()],
            )?;
            tx.commit()
        })
        .await?;
        Ok(id)
    }

    pub async fn list_webhooks(&self, owner: &str) -> Result<Vec<Value>, String> {
        self.with(|c| {
            let mut stmt=c.prepare("SELECT id,url,events FROM wallet_webhooks WHERE owner=?1 ORDER BY id")?;
            let rows=stmt.query_map([owner],|r| {let events:String=r.get(2)?; Ok(json!({"webhook_id":r.get::<_,String>(0)?,"url":r.get::<_,String>(1)?,"event_types":serde_json::from_str::<Value>(&events).unwrap_or(Value::Null)}))})?;
            rows.collect()
        }).await
    }

    pub async fn delete_webhook(&self, owner: &str, id: &str) -> Result<bool, String> {
        self.with(|c| {
            c.execute(
                "DELETE FROM wallet_webhooks WHERE id=?1 AND owner=?2",
                params![id, owner],
            )
            .map(|n| n == 1)
        })
        .await
    }

    async fn deliver_webhooks(&self, local: bool) -> Result<(), String> {
        let due = self.with(|c| {
            let tx=c.unchecked_transaction()?;
            capture(&tx)?;
            let rows = {
                let mut stmt=tx.prepare("SELECT d.event_id,d.webhook_id,w.url,w.secret,e.payload,d.attempts FROM webhook_deliveries d JOIN wallet_webhooks w ON w.id=d.webhook_id JOIN webhook_events e ON e.id=d.event_id WHERE d.delivered=0 AND d.next_attempt<=?1 ORDER BY d.next_attempt LIMIT 20")?;
                let rows = stmt.query_map([chrono::Utc::now().timestamp()],|r| Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,String>(3)?,r.get::<_,String>(4)?,r.get::<_,u32>(5)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
                rows
            };
            tx.commit()?;
            Ok(rows)
        }).await?;
        for (event, webhook, url, secret, payload, attempts) in due {
            let result = async {
                let url = parse_url(&url, local)?;
                let client = client_for(&url, local).await?;
                let response = client
                    .post(url)
                    .header("Content-Type", "application/json")
                    .header("X-Spark-Signature", signature(&secret, &payload))
                    .header("X-Spark-Event-Id", &event)
                    .body(payload)
                    .send()
                    .await
                    .map_err(|_| "webhook transport failed".to_string())?;
                if !response.status().is_success() {
                    return Err(format!("webhook HTTP {}", response.status().as_u16()));
                }
                Ok::<(), String>(())
            }
            .await;
            let delay = 5_i64.saturating_mul(1_i64 << attempts.min(14)).min(86400);
            self.with(|c|c.execute("UPDATE webhook_deliveries SET attempts=attempts+1,next_attempt=?3,delivered=?4,last_error=?5 WHERE event_id=?1 AND webhook_id=?2",params![event,webhook,chrono::Utc::now().timestamp()+delay,result.is_ok(),result.err()]).map(|_|())).await?;
        }
        Ok(())
    }
}

pub async fn run(db: Arc<Db>, local: bool) {
    loop {
        if let Err(error) = db.deliver_webhooks(local).await {
            tracing::warn!("webhook worker: {error}");
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn destination_policy_and_signature() {
        for ip in [
            "127.0.0.1",
            "10.0.0.1",
            "169.254.169.254",
            "100.64.0.1",
            "::1",
            "::ffff:127.0.0.1",
            "2001:db8::1",
            "fc00::1",
        ] {
            assert!(!public_ip(ip.parse().unwrap()), "{ip}");
        }
        assert!(public_ip("8.8.8.8".parse().unwrap()));
        for ip in [
            "127.0.0.1",
            "172.17.0.1",
            "10.0.0.1",
            "::1",
            "fc00::1",
            "::ffff:172.17.0.1",
        ] {
            assert!(local_test_ip(ip.parse().unwrap()), "{ip}");
        }
        for ip in ["169.254.169.254", "0.0.0.0", "fe80::1", "8.8.8.8"] {
            assert!(!local_test_ip(ip.parse().unwrap()), "{ip}");
        }

        assert!(parse_url("http://example.com", false).is_err());
        assert!(parse_url("https://user:secret@example.com", false).is_err());
        assert_eq!(
            signature("key", "The quick brown fox jumps over the lazy dog"),
            "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
        );
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn notifications_survive_restart_retry_and_do_not_leak_between_wallets() {
        use axum::{extract::State, http::HeaderMap, routing::post, Router};
        let received = Arc::new(tokio::sync::Mutex::new(Vec::<(String, String)>::new()));
        type Received = Arc<tokio::sync::Mutex<Vec<(String, String)>>>;
        async fn handler(
            State(received): State<Received>,
            headers: HeaderMap,
            body: String,
        ) -> axum::http::StatusCode {
            let mut records = received.lock().await;
            records.push((headers["x-spark-signature"].to_str().unwrap().into(), body));
            if records.len() == 1 {
                axum::http::StatusCode::SERVICE_UNAVAILABLE
            } else {
                axum::http::StatusCode::OK
            }
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/notify", listener.local_addr().unwrap());
        let server = tokio::spawn(
            axum::serve(
                listener,
                Router::new()
                    .route("/notify", post(handler))
                    .with_state(received.clone()),
            )
            .into_future(),
        );
        let dir = std::env::temp_dir().join(format!("ssp-webhooks-{}", Uuid::new_v4()));
        let db = Db::open(dir.to_str().unwrap()).unwrap();
        let insert = |id: &str, owner: &str| {
            let (id, owner) = (id.to_string(), owner.to_string());
            let db = db.clone();
            async move {
                db.insert_request(
                    &id,
                    "CLAIM_STATIC_DEPOSIT",
                    &owner,
                    "now",
                    &json!({"status":"SUCCEEDED"}),
                    None,
                )
                .await
                .unwrap();
            }
        };
        insert("old", "alice").await;
        let id = db
            .register_webhook(
                "alice",
                &json!({"url":url,"secret":"key","event_types":[EVENTS[3]]}),
                true,
            )
            .await
            .unwrap();
        insert("new", "alice").await;
        insert("private", "bob").await;
        assert!(db.list_webhooks("bob").await.unwrap().is_empty());
        assert!(!db.list_webhooks("alice").await.unwrap()[0]
            .to_string()
            .contains("key"));
        assert!(!db.delete_webhook("bob", &id).await.unwrap());
        db.deliver_webhooks(true).await.unwrap();
        drop(db);
        let db = Db::open(dir.to_str().unwrap()).unwrap();
        db.with(|c| c.execute("UPDATE webhook_deliveries SET next_attempt=0", []))
            .await
            .unwrap();
        db.deliver_webhooks(true).await.unwrap();
        db.deliver_webhooks(true).await.unwrap();
        let records = received.lock().await;
        assert_eq!(records.len(), 2);
        assert_eq!(records[0], records[1]);
        assert_eq!(records[0].0, signature("key", &records[0].1));
        assert_eq!(
            serde_json::from_str::<Value>(&records[0].1).unwrap()["request_id"],
            "new"
        );
        assert!(db.delete_webhook("alice", &id).await.unwrap());
        server.abort();
        drop(db);
        std::fs::remove_dir_all(dir).unwrap();
    }
    use std::future::IntoFuture;
}
