//! Wallet callbacks registered through the public Breez SDK.
use super::*;
use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
};
use breez_sdk_spark::{RegisterWebhookRequest, UnregisterWebhookRequest, WebhookEventType};
use hmac::{Hmac, Mac};
use std::{collections::HashSet, sync::Arc};
use tokio::sync::Mutex;

#[derive(Clone)]
struct Delivery {
    id: String,
    body: Vec<u8>,
    signature: String,
}
#[derive(Clone, Default)]
struct Inbox(Arc<Mutex<Vec<Delivery>>>);

async fn receive(State(inbox): State<Inbox>, headers: HeaderMap, body: Bytes) -> StatusCode {
    let mut deliveries = inbox.0.lock().await;
    let id = headers
        .get("x-spark-event-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let first = !deliveries.iter().any(|v| v.id == id);
    deliveries.push(Delivery {
        id,
        body: body.to_vec(),
        signature: headers
            .get("x-spark-signature")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_owned(),
    });
    // Force a real delivery retry for every event.
    if first {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    }
}

pub struct Callbacks {
    inbox: Inbox,
    task: tokio::task::JoinHandle<()>,
    id: String,
    secret: String,
}
impl Drop for Callbacks {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Callbacks {
    pub async fn start(wallet: &Wallet, other: &Wallet) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await?;
        let url = format!(
            "http://host.docker.internal:{}/callback",
            listener.local_addr()?.port()
        );
        let inbox = Inbox::default();
        let app = Router::new()
            .route("/callback", post(receive))
            .with_state(inbox.clone());
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let secret = "breez-regtest-callback-signature-key".to_owned();
        let id = wallet
            .sdk
            .register_webhook(RegisterWebhookRequest {
                url: url.clone(),
                secret: secret.clone(),
                event_types: vec![
                    WebhookEventType::LightningSendFinished,
                    WebhookEventType::LightningReceiveFinished,
                    WebhookEventType::CoopExitFinished,
                ],
            })
            .await?
            .webhook_id;
        let listed = wallet.sdk.list_webhooks().await?;
        ensure!(
            listed.iter().any(|v| v.id == id && v.url == url),
            "SDK did not list its registered webhook"
        );
        ensure!(
            other.sdk.list_webhooks().await?.iter().all(|v| v.id != id),
            "another wallet can see this webhook"
        );
        other
            .sdk
            .unregister_webhook(UnregisterWebhookRequest {
                webhook_id: id.clone(),
            })
            .await?;
        ensure!(
            wallet.sdk.list_webhooks().await?.iter().any(|v| v.id == id),
            "another wallet removed this webhook"
        );
        Ok(Self {
            inbox,
            task,
            id,
            secret,
        })
    }
    pub async fn verify(&self, wallet: &Wallet, timeout: Duration) -> Result<()> {
        poll("signed wallet callbacks and retries", timeout, || async {
            let deliveries = self.inbox.0.lock().await;
            let mut types = HashSet::new();
            for entry in deliveries.iter() {
                let mut mac = Hmac::<Sha256>::new_from_slice(self.secret.as_bytes())?;
                mac.update(&entry.body);
                mac.verify_slice(&hex::decode(&entry.signature)?)
                    .context("invalid callback HMAC")?;
                let data: Value = serde_json::from_slice(&entry.body)?;
                ensure!(
                    data["event_id"] == entry.id,
                    "callback ID does not match its body"
                );
                let matching: Vec<_> = deliveries.iter().filter(|v| v.id == entry.id).collect();
                if matching.len() >= 2 {
                    ensure!(
                        matching
                            .iter()
                            .all(|v| v.body == entry.body && v.signature == entry.signature),
                        "retry changed callback bytes or signature"
                    );
                    types.insert(
                        data["type"]
                            .as_str()
                            .context("callback has no type")?
                            .to_owned(),
                    );
                }
            }
            for expected in [
                "SPARK_LIGHTNING_SEND_FINISHED",
                "SPARK_LIGHTNING_RECEIVE_FINISHED",
                "SPARK_COOP_EXIT_FINISHED",
            ] {
                ensure!(
                    types.contains(expected),
                    "missing successful callback retry for {expected}"
                );
            }
            Ok(())
        })
        .await?;
        wallet
            .sdk
            .unregister_webhook(UnregisterWebhookRequest {
                webhook_id: self.id.clone(),
            })
            .await?;
        ensure!(
            wallet
                .sdk
                .list_webhooks()
                .await?
                .iter()
                .all(|v| v.id != self.id),
            "deleted webhook remains listed"
        );
        println!(
            "PASS Breez webhooks: registration, owner isolation, signed delivery, stable retries, deletion"
        );
        Ok(())
    }
}
