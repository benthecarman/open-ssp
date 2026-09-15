use std::{future::Future, str::FromStr, sync::Arc, time::Duration};

use ldk_server_client::{
    client::LdkServerClient,
    ldk_server_grpc::{
        api::{
            Bolt11ClaimForHashRequest, Bolt11FailForHashRequest, Bolt11ReceiveForHashRequest,
            Bolt11SendRequest, Bolt12ReceiveRequest, Bolt12SendRequest, DecodeInvoiceRequest,
            DecodeOfferRequest, GetPaymentDetailsRequest, ListPaymentsRequest,
        },
        events::event_envelope::Event as LdkRawEvent,
        types::{Bolt11InvoiceDescription, Payment, PaymentDirection, PaymentStatus},
    },
};
use sha2::{Digest, Sha256};

use crate::{
    config::Config,
    db::Db,
    lightning_store::{LightningSend, SendKind, SendStatus},
    spark::{LightningReceiveSwap, SparkService},
};

#[async_trait::async_trait]
trait SendLdk: Send + Sync {
    async fn submit(&self, send: &LightningSend) -> Result<String, String>;
    async fn lookup(&self, send: &LightningSend) -> Result<Option<Payment>, String>;
}

#[async_trait::async_trait]
impl SendLdk for LdkServerClient {
    async fn submit(&self, send: &LightningSend) -> Result<String, String> {
        let amount_msat = send.amount_override.map(sats_to_msats).transpose()?;
        match send.kind {
            SendKind::Bolt11 => self
                .bolt11_send(Bolt11SendRequest {
                    invoice: send.invoice.clone(),
                    amount_msat,
                    route_parameters: None,
                })
                .await
                .map(|r| r.payment_id)
                .map_err(|e| e.to_string()),
            SendKind::Bolt12 => self
                .bolt12_send(Bolt12SendRequest {
                    offer: send.invoice.clone(),
                    amount_msat,
                    quantity: None,
                    payer_note: Some(send.payer_note()),
                    route_parameters: None,
                })
                .await
                .map(|r| r.payment_id)
                .map_err(|e| e.to_string()),
        }
    }
    async fn lookup(&self, send: &LightningSend) -> Result<Option<Payment>, String> {
        if let Some(id) = send
            .payment_id
            .as_deref()
            .or_else(|| (send.kind == SendKind::Bolt11).then_some(send.expected_id.as_str()))
        {
            let payment = self
                .get_payment_details(GetPaymentDetailsRequest {
                    payment_id: id.into(),
                })
                .await
                .map_err(|e| e.to_string())?
                .payment;
            return payment
                .map(|p| validate_send_payment(send, &p).map(|()| p))
                .transpose();
        }
        // Offers get a random LDK payment ID. The durable request ID is carried
        // in the payer note and is returned by ListPayments, including pending payments.
        let mut page_token = None;
        let mut found = None;
        for _ in 0..100 {
            let page = self
                .list_payments(ListPaymentsRequest { page_token })
                .await
                .map_err(|e| e.to_string())?;
            for payment in page.payments {
                if validate_send_payment(send, &payment).is_ok() {
                    if found.is_some() {
                        return Err("multiple LDK payments match one intent".into());
                    }
                    found = Some(payment);
                }
            }
            page_token = page.next_page_token;
            if page_token.is_none() {
                return Ok(found);
            }
        }
        Err("LDK lookup exceeded 100 pages".into())
    }
}

fn validate_send_payment(send: &LightningSend, payment: &Payment) -> Result<(), String> {
    use ldk_server_client::ldk_server_grpc::types::payment_kind::Kind;
    // The invoice amount wins; a zero-amount BOLT11 invoice and a BOLT12 offer
    // both fall back to the stored intent. Both paths are overflow-checked so an
    // oversized intent reports an overflow rather than a mismatch.
    let expected_msat = match lightning_invoice::Bolt11Invoice::from_str(&send.invoice)
        .ok()
        .and_then(|invoice| invoice.amount_milli_satoshis())
    {
        Some(msat) => msat,
        None => send
            .amount_sats
            .checked_mul(1000)
            .ok_or("send amount is too large")?,
    };
    if payment.direction != PaymentDirection::Outbound as i32
        || !payment.amount_msat.is_some_and(|a| a == expected_msat)
    {
        return Err("LDK payment direction or amount does not match the send intent".into());
    }
    let known_id = send.payment_id.as_ref().is_some_and(|id| id == &payment.id);
    let matches = match (
        send.kind,
        payment.kind.as_ref().and_then(|k| k.kind.as_ref()),
    ) {
        (SendKind::Bolt11, Some(Kind::Bolt11(p))) => {
            p.hash == send.expected_id || (known_id && send.expected_id.is_empty())
        }
        (SendKind::Bolt12, Some(Kind::Bolt12Offer(p))) => {
            (p.offer_id == send.expected_id || (known_id && send.expected_id.is_empty()))
                && (known_id || p.payer_note.as_deref() == Some(send.payer_note().as_str()))
        }
        _ => false,
    };
    if !matches {
        return Err("LDK payment identity does not match the send intent".into());
    }
    Ok(())
}

async fn recover_submission<L: SendLdk + ?Sized>(
    db: &Db,
    ldk: &L,
    send: &LightningSend,
) -> Result<Option<Payment>, String> {
    let payment = ldk.lookup(send).await?;
    if let Some(payment) = &payment {
        validate_send_payment(send, payment)?;
        db.bind_lightning_payment(&send.request_id, &payment.id)
            .await?;
    }
    Ok(payment)
}

/// The pinned LDK Node derives BOLT11 PaymentId from the invoice hash and
/// rejects duplicates in both its store and channel manager. Reuse that ID
/// only after an authoritative lookup reports no record. Offers use random
/// IDs and must never pass this retry path.
async fn retry_missing_bolt11<L: SendLdk + ?Sized>(
    db: &Db,
    ldk: &L,
    send: &LightningSend,
) -> Result<(), String> {
    if send.kind != SendKind::Bolt11
        || send.status != SendStatus::Submitting
        || send.payment_id.is_some()
    {
        return Err(
            "payment needs backend reconciliation; automatic resubmission is unavailable".into(),
        );
    }
    match ldk.submit(send).await {
        Ok(id) => db.bind_lightning_payment(&send.request_id, &id).await,
        Err(error) => {
            // A duplicate error can mean the original call committed while
            // this retry was in flight. Keep the same intent and look it up.
            if recover_submission(db, ldk, send).await?.is_none() {
                db.lightning_submission_error(&send.request_id, &error)
                    .await?;
            }
            Ok(())
        }
    }
}

async fn submit_durable_send<L: SendLdk + ?Sized>(
    db: &Db,
    ldk: &L,
    send: &LightningSend,
) -> Result<(), String> {
    if !db.begin_lightning_submission(&send.request_id).await? {
        return Ok(());
    }
    // This checkpoint precedes the network call. SUBMITTING is an uncertain
    // outcome after a crash, including a crash immediately before the call.
    // Never submit it again without backend idempotency support.
    match ldk.submit(send).await {
        Ok(id) => db.bind_lightning_payment(&send.request_id, &id).await,
        Err(error) => {
            db.lightning_submission_error(&send.request_id, &error)
                .await?;
            tracing::warn!(
                request_id = send.request_id,
                "Lightning submission outcome unknown: {error}"
            );
            Ok(())
        }
    }
}

#[derive(Clone, Debug)]
pub struct CreateInvoiceResult {
    pub invoice: String,
}

#[derive(Clone, Debug)]
pub struct CreateOfferResult {
    pub offer: String,
    pub offer_id: String,
}

/// Minimal SSP view of ldk-server SubscribeEvents payloads.
#[derive(Clone, Debug)]
pub enum LnEvent {
    OutboundSucceeded {
        payment: Payment,
    },
    OutboundFailed {
        payment_id: String,
        reason: Option<String>,
    },
    InboundClaimable {
        payment_hash: String,
        amount_msat: Option<u64>,
    },
    InboundReceived {
        payment_hash: String,
    },
    InboundBolt12Received {
        offer_id: String,
        payment_hash: String,
        preimage: Option<String>,
        amount_msat: Option<u64>,
    },
}

impl LdkGrpcBackend {
    /// SubscribeEvents pump for a live backend. The upstream streaming client
    /// does not set a `grpc-timeout` header. Reconnect with capped exponential
    /// backoff when the server, proxy, or HTTP/2 connection ends the stream.
    pub async fn run_event_pump(live: Arc<LdkGrpcBackend>) {
        let mut failures = 0u32;
        loop {
            let connected_at = std::time::Instant::now();
            let mut received_event = false;
            // Bound only the connection and response-header phase. Do not put
            // a deadline on the returned server stream.
            match tokio::time::timeout(
                std::time::Duration::from_secs(15),
                live.client.subscribe_events(),
            )
            .await
            {
                Ok(Ok(mut stream)) => {
                    tracing::info!("ldk event stream connected");
                    while let Some(msg) = stream.next_message().await {
                        match msg {
                            Ok(env) => {
                                received_event = true;
                                for ev in map_envelope(env) {
                                    live.apply_ln_event(ev).await;
                                }
                            }
                            Err(e) => {
                                tracing::warn!("ldk event stream error: {e}");
                                break;
                            }
                        }
                    }
                    tracing::warn!("ldk event stream ended; reconnecting");
                }
                Ok(Err(e)) => tracing::warn!("ldk subscribe_events failed: {e}"),
                Err(_) => tracing::warn!("ldk subscribe_events connection timed out"),
            }
            if received_event || connected_at.elapsed() >= std::time::Duration::from_secs(30) {
                failures = 0;
            } else {
                failures = failures.saturating_add(1);
            }
            let delay = reconnect_delay(failures);
            tracing::info!(?delay, "waiting before ldk event stream reconnect");
            tokio::time::sleep(delay).await;
        }
    }

    /// Recover events lost during a stream gap from the durable payment list.
    pub async fn run_reconciler(live: Arc<LdkGrpcBackend>) {
        loop {
            if let Err(e) = live.reconcile_payments().await {
                tracing::warn!("ldk payment reconcile failed: {e}");
            }
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        }
    }
}

fn reconnect_delay(failures: u32) -> std::time::Duration {
    use rand::Rng;
    let exponent = failures.saturating_sub(1).min(5);
    let base_secs = (1u64 << exponent).min(30);
    let jitter_ms = rand::thread_rng().gen_range(0..=base_secs * 250);
    std::time::Duration::from_millis(base_secs * 1000 + jitter_ms)
}

fn map_envelope(env: ldk_server_client::ldk_server_grpc::events::EventEnvelope) -> Vec<LnEvent> {
    let mut out = Vec::new();
    let Some(event) = env.event else { return out };
    match event {
        LdkRawEvent::PaymentSuccessful(e) => {
            if let Some(p) = e.payment {
                out.push(LnEvent::OutboundSucceeded { payment: p });
            }
        }
        LdkRawEvent::PaymentFailed(e) => {
            if let Some(p) = e.payment {
                out.push(LnEvent::OutboundFailed {
                    payment_id: p.id,
                    reason: e.reason.map(|r| {
                        ldk_server_client::ldk_server_grpc::events::PaymentFailureReason::from_i32(
                            r,
                        )
                        .map(|r| r.as_str_name().to_string())
                        .unwrap_or_else(|| format!("UNKNOWN_{r}"))
                    }),
                });
            }
        }
        LdkRawEvent::PaymentClaimable(e) => {
            if let Some(payment) = e.payment {
                let amount_msat = payment.amount_msat;
                if let Some(hash) = bolt11_hash(Some(payment)) {
                    out.push(LnEvent::InboundClaimable {
                        payment_hash: hash,
                        amount_msat,
                    });
                }
            }
        }
        LdkRawEvent::PaymentReceived(e) => {
            if let Some(hash) = bolt11_hash(e.payment.clone()) {
                out.push(LnEvent::InboundReceived { payment_hash: hash });
            } else if let Some((offer_id, payment_hash)) = bolt12_offer_ids(e.payment.clone()) {
                out.push(LnEvent::InboundBolt12Received {
                    offer_id,
                    payment_hash,
                    preimage: bolt12_preimage(e.payment.as_ref()),
                    amount_msat: e.payment.and_then(|payment| payment.amount_msat),
                });
            }
        }
        _ => {}
    }
    out
}

fn bolt12_preimage(payment: Option<&Payment>) -> Option<String> {
    use ldk_server_client::ldk_server_grpc::types::payment_kind::Kind;
    match payment?.kind.as_ref()?.kind.as_ref()? {
        Kind::Bolt12Offer(offer) => offer.preimage.clone(),
        _ => None,
    }
}

fn bolt12_offer_ids(
    p: Option<ldk_server_client::ldk_server_grpc::types::Payment>,
) -> Option<(String, String)> {
    let p = p?;
    let kind = p.kind?;
    match kind.kind? {
        ldk_server_client::ldk_server_grpc::types::payment_kind::Kind::Bolt12Offer(offer) => {
            Some((offer.offer_id, offer.hash?))
        }
        _ => None,
    }
}

fn bolt11_hash(p: Option<ldk_server_client::ldk_server_grpc::types::Payment>) -> Option<String> {
    let p = p?;
    let kind = p.kind?;
    match kind.kind? {
        ldk_server_client::ldk_server_grpc::types::payment_kind::Kind::Bolt11(b) => Some(b.hash),
        _ => None,
    }
}

#[async_trait::async_trait]
trait ReceiveSpark: Send + Sync {
    async fn swap_receive(
        &self,
        owner: &str,
        payment_hash: &str,
        invoice: &str,
        amount_sats: u64,
    ) -> Result<LightningReceiveSwap, String>;
}

#[async_trait::async_trait]
impl ReceiveSpark for SparkService {
    async fn swap_receive(
        &self,
        owner: &str,
        payment_hash: &str,
        invoice: &str,
        amount_sats: u64,
    ) -> Result<LightningReceiveSwap, String> {
        self.swap_for_lightning_receive(owner, payment_hash, invoice, amount_sats, 0)
            .await
    }
}

#[async_trait::async_trait]
trait ReceiveLdk: Send + Sync {
    async fn claim_receive(
        &self,
        payment_hash: &str,
        amount_msat: u64,
        preimage: &str,
    ) -> Result<(), String>;
    async fn fail_receive(&self, payment_hash: &str) -> Result<(), String>;
}

#[async_trait::async_trait]
impl ReceiveLdk for LdkServerClient {
    async fn claim_receive(
        &self,
        payment_hash: &str,
        amount_msat: u64,
        preimage: &str,
    ) -> Result<(), String> {
        self.bolt11_claim_for_hash(Bolt11ClaimForHashRequest {
            payment_hash: Some(payment_hash.to_string()),
            claimable_amount_msat: Some(amount_msat),
            preimage: preimage.to_string(),
        })
        .await
        .map(|_| ())
        .map_err(|e| format!("claim Lightning receive {payment_hash}: {e}"))
    }

    async fn fail_receive(&self, payment_hash: &str) -> Result<(), String> {
        self.bolt11_fail_for_hash(Bolt11FailForHashRequest {
            payment_hash: payment_hash.to_string(),
        })
        .await
        .map(|_| ())
        .map_err(|e| format!("fail Lightning receive {payment_hash}: {e}"))
    }
}

const RECEIVE_RETRY_DELAYS: [Duration; 4] = [
    Duration::from_millis(100),
    Duration::from_millis(250),
    Duration::from_millis(500),
    Duration::from_secs(1),
];
const RECEIVE_OPERATION_TIMEOUT: Duration = Duration::from_secs(15);

async fn retry_bounded<T, F, Fut>(mut operation: F, delays: &[Duration]) -> Result<T, String>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, String>>,
{
    let mut last_error = None;
    for attempt in 0..=delays.len() {
        match tokio::time::timeout(RECEIVE_OPERATION_TIMEOUT, operation()).await {
            Ok(Ok(value)) => return Ok(value),
            Ok(Err(error)) => last_error = Some(error),
            Err(_) => last_error = Some("receive operation timed out".to_string()),
        }
        if let Some(delay) = delays.get(attempt) {
            tokio::time::sleep(*delay).await;
        }
    }
    Err(last_error.unwrap_or_else(|| "operation failed without an error".to_string()))
}

fn validate_preimage(payment_hash: &str, preimage: &str) -> Result<(), String> {
    let bytes = hex::decode(preimage).map_err(|e| format!("preimage is not hex: {e}"))?;
    if bytes.len() != 32 {
        return Err("preimage must be 32 bytes".to_string());
    }
    let digest = hex::encode(Sha256::digest(bytes));
    if digest != payment_hash.to_lowercase() {
        return Err("preimage does not match the Lightning payment hash".to_string());
    }
    Ok(())
}

fn is_definitive_swap_failure(error: &str) -> bool {
    [
        "Insufficient",
        "insufficient",
        "Unselectable",
        "unselectable",
        "NEEDS_TOPUP",
    ]
    .iter()
    .any(|needle| error.contains(needle))
}

async fn fail_unfunded_receive<L: ReceiveLdk + ?Sized>(
    db: &Db,
    ldk: &L,
    payment_hash: &str,
    delays: &[Duration],
) {
    match retry_bounded(|| ldk.fail_receive(payment_hash), delays).await {
        Ok(()) => {
            let _ = db.set_receive_status(payment_hash, "HTLC_FAILED").await;
        }
        Err(error) => tracing::error!(
            payment_hash,
            "could not fail unfunded Lightning receive: {error}"
        ),
    }
}

/// Process one claimable receive under a lock shared by the event stream and
/// reconciler. The database checkpoint separates the operator commit from the
/// LDK claim, so an LDK retry never repeats the Spark transfer.
async fn process_standard_receive<S, L>(
    db: &Db,
    receive_lock: &tokio::sync::Mutex<()>,
    spark: &S,
    ldk: &L,
    payment_hash: &str,
    amount_msat: Option<u64>,
    delays: &[Duration],
) -> Result<bool, String>
where
    S: ReceiveSpark + ?Sized,
    L: ReceiveLdk + ?Sized,
{
    let _guard = receive_lock.lock().await;
    if db.internal_send(payment_hash).await?.is_some() {
        // Do not change the receive's local settlement state when rejecting
        // an external HTLC for an invoice reserved by a local payer.
        retry_bounded(|| ldk.fail_receive(payment_hash), delays).await?;
        return Ok(true);
    }
    let Some(mut receive) = db.lightning_receive_for_hash(payment_hash).await? else {
        return Ok(false);
    };
    if receive.status.as_str() == "TRANSFER_COMPLETED" || receive.status.as_str() == "HTLC_FAILED" {
        return Ok(true);
    }
    let expected_msat = receive
        .amount_sats
        .checked_mul(1000)
        .ok_or_else(|| "Lightning receive amount is too large".to_string())?;
    let actual_msat =
        amount_msat.ok_or_else(|| "claimable Lightning payment has no amount".to_string())?;
    if actual_msat != expected_msat {
        fail_unfunded_receive(db, ldk, payment_hash, delays).await;
        return Err(format!(
            "claimable amount is {actual_msat} msat; expected {expected_msat} msat"
        ));
    }
    db.mark_receive_claimable(payment_hash, actual_msat).await?;

    if receive.transfer_id.is_none() || receive.preimage.is_none() {
        let swap = match retry_bounded(
            || {
                spark.swap_receive(
                    &receive.receiver,
                    payment_hash,
                    &receive.invoice,
                    receive.amount_sats,
                )
            },
            delays,
        )
        .await
        {
            Ok(swap) => swap,
            Err(error) => {
                db.set_receive_status(payment_hash, "TRANSFER_CREATION_FAILED")
                    .await?;
                // A connection error can hide a successful operator commit.
                // Leave that HTLC held for reconciliation. Only fail now when
                // no transfer could have been committed.
                if is_definitive_swap_failure(&error) {
                    fail_unfunded_receive(db, ldk, payment_hash, delays).await;
                }
                return Err(format!("Spark receive swap failed: {error}"));
            }
        };
        if let Err(error) = validate_preimage(payment_hash, &swap.preimage) {
            db.set_receive_status(payment_hash, "PAYMENT_PREIMAGE_RECOVERING_FAILED")
                .await?;
            return Err(error);
        }
        db.commit_lightning_receive_swap(
            payment_hash,
            &swap.transfer_id,
            &swap.preimage,
            &receive.request_id,
            &receive.owner,
        )
        .await?;
        receive.transfer_id = Some(swap.transfer_id);
        receive.preimage = Some(swap.preimage);
    }

    if receive.claim_submitted {
        return Ok(true);
    }
    let preimage = receive
        .preimage
        .as_deref()
        .ok_or_else(|| "committed Spark receive has no preimage".to_string())?;
    validate_preimage(payment_hash, preimage)?;
    retry_bounded(
        || ldk.claim_receive(payment_hash, expected_msat, preimage),
        delays,
    )
    .await?;
    db.mark_receive_claim_submitted(payment_hash).await?;
    Ok(true)
}

#[async_trait::async_trait]
trait InternalSpark: ReceiveSpark {
    async fn verify_sender(&self, send: &LightningSend) -> Result<(), String>;
    async fn settle_sender(&self, send: &LightningSend, preimage: &str) -> Result<(), String>;
}
#[async_trait::async_trait]
impl InternalSpark for SparkService {
    async fn verify_sender(&self, send: &LightningSend) -> Result<(), String> {
        self.verify_lightning_send(
            &send.owner,
            &send.outbound_transfer_id,
            &send.expected_id,
            send.amount_sats,
        )
        .await
    }
    async fn settle_sender(&self, send: &LightningSend, preimage: &str) -> Result<(), String> {
        self.settle_lightning_send(&send.outbound_transfer_id, &send.expected_id, preimage)
            .await
    }
}

async fn process_internal_send<S: InternalSpark + ?Sized>(
    db: &Db,
    lock: &tokio::sync::Mutex<()>,
    spark: &S,
    send: &LightningSend,
) -> Result<(), String> {
    let _guard = lock.lock().await;
    if matches!(
        db.payment_status(&send.request_id).await?.as_str(),
        "SUCCEEDED" | "FAILED"
    ) {
        return Ok(());
    }
    if let Err(error) = db.reserve_internal_send(send).await {
        // No operator call was made. The sender's conditional transfer can
        // return through the standard preimage-swap expiry path.
        db.set_payment(&send.request_id, "FAILED").await?;
        return Err(error);
    }
    let receive = db
        .lightning_receive_for_hash(&send.expected_id)
        .await?
        .ok_or("local receive missing")?;
    let preimage = match receive.preimage {
        Some(preimage) => preimage,
        None => {
            spark.verify_sender(send).await?;
            let swap = spark
                .swap_receive(
                    &receive.receiver,
                    &send.expected_id,
                    &receive.invoice,
                    receive.amount_sats,
                )
                .await?;
            validate_preimage(&send.expected_id, &swap.preimage)?;
            db.commit_lightning_receive_swap(
                &send.expected_id,
                &swap.transfer_id,
                &swap.preimage,
                &receive.request_id,
                &receive.owner,
            )
            .await?;
            swap.preimage
        }
    };
    validate_preimage(&send.expected_id, &preimage)?;
    spark.settle_sender(send, &preimage).await?;
    db.finish_internal_send(send).await
}

#[derive(Clone)]
pub struct LdkGrpcBackend {
    pub client: LdkServerClient,
    pub node_id: String,
    db: Arc<Db>,
    spark: Arc<SparkService>,
    receive_lock: Arc<tokio::sync::Mutex<()>>,
    invoice_network: bitcoin::Network,
}

impl LdkGrpcBackend {
    pub async fn connect(
        config: &Config,
        db: Arc<Db>,
        spark: Arc<SparkService>,
    ) -> Result<Self, String> {
        if config.ldk_grpc_addr.is_empty() {
            return Err("LDK_GRPC_ADDR unset".to_string());
        }
        let api_key = if !config.ldk_api_key.is_empty() {
            config.ldk_api_key.clone()
        } else if !config.ldk_api_key_file.is_empty() {
            // On-disk key is raw bytes; ldk-server hex-encodes before HMAC.
            let raw = std::fs::read(&config.ldk_api_key_file)
                .map_err(|e| format!("read LDK_API_KEY_FILE: {e}"))?;
            hex::encode(raw).trim().to_string()
        } else {
            return Err("LDK_API_KEY or LDK_API_KEY_FILE required for live mode".to_string());
        };
        if api_key.is_empty() {
            return Err("empty LDK api key".to_string());
        }
        let cert_pem = std::fs::read(&config.ldk_tls_cert_file)
            .map_err(|e| format!("read LDK_TLS_CERT_FILE {}: {e}", config.ldk_tls_cert_file))?;
        let client = LdkServerClient::new(config.ldk_grpc_addr.clone(), api_key, &cert_pem)?;
        let info = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            client.get_node_info(ldk_server_client::ldk_server_grpc::api::GetNodeInfoRequest {}),
        )
        .await
        .map_err(|_| "get_node_info timed out".to_string())?
        .map_err(|e| format!("get_node_info: {e}"))?;
        Ok(Self {
            client,
            node_id: info.node_id,
            db,
            spark,
            receive_lock: Arc::new(tokio::sync::Mutex::new(())),
            invoice_network: invoice_network(&config.network)?,
        })
    }

    pub async fn prepare_send(
        &self,
        owner: &str,
        transfer: &str,
        invoice: &str,
        amount: Option<u64>,
    ) -> Result<LightningSend, String> {
        uuid::Uuid::parse_str(transfer).map_err(|_| "invalid Spark funding transfer ID")?;
        let (kind, expected_id, total) = if invoice.to_ascii_lowercase().starts_with("lno1") {
            let decoded = self
                .client
                .decode_offer(DecodeOfferRequest {
                    offer: invoice.into(),
                })
                .await
                .map_err(|e| e.to_string())?;
            (
                SendKind::Bolt12,
                decoded.offer_id,
                amount.ok_or("BOLT12 sends require amount_sats")?,
            )
        } else {
            let decoded =
                lightning_invoice::Bolt11Invoice::from_str(invoice).map_err(|e| e.to_string())?;
            if !send_network_matches(decoded.network(), self.invoice_network) {
                return Err("Lightning invoice network mismatch".into());
            }
            if decoded.would_expire(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|e| e.to_string())?,
            ) {
                return Err("Lightning invoice expired".into());
            }
            let sats = decoded
                .amount_milli_satoshis()
                .map(|a| a.div_ceil(1000))
                .or(amount)
                .ok_or("zero-amount invoice needs amount_sats")?;
            (SendKind::Bolt11, decoded.payment_hash().to_string(), sats)
        };
        sats_to_msats(total)?;
        self.verify_lightning_send_funding(owner, transfer, invoice, amount)
            .await?;
        Ok(LightningSend {
            request_id: uuid::Uuid::new_v4().to_string(),
            owner: owner.into(),
            outbound_transfer_id: transfer.into(),
            invoice: invoice.into(),
            amount_sats: total,
            amount_override: amount,
            kind,
            expected_id,
            payment_id: None,
            status: SendStatus::Prepared,
        })
    }

    pub async fn submit_send(&self, send: &LightningSend) -> Result<(), String> {
        if self
            .db
            .lightning_receive_hash_for_invoice(&send.invoice)
            .await?
            .is_some()
        {
            return process_internal_send(&self.db, &self.receive_lock, self.spark.as_ref(), send)
                .await;
        }
        if send.status != SendStatus::Prepared {
            return Ok(());
        }
        // A prepared request can survive a restart before submission. Recheck
        // that its funding is still claimable before crossing the checkpoint.
        self.verify_lightning_send_funding(
            &send.owner,
            &send.outbound_transfer_id,
            &send.invoice,
            send.amount_override,
        )
        .await?;
        submit_durable_send(&self.db, &self.client, send).await
    }

    async fn recover_send(&self, send: &LightningSend) -> Result<(), String> {
        if matches!(send.status, SendStatus::Succeeded | SendStatus::Failed) {
            return Ok(());
        }
        if self
            .db
            .lightning_receive_hash_for_invoice(&send.invoice)
            .await?
            .is_some()
        {
            return process_internal_send(&self.db, &self.receive_lock, self.spark.as_ref(), send)
                .await;
        }
        if send.status == SendStatus::Prepared {
            return self.submit_send(send).await;
        }
        if let Some(payment) = recover_submission(&self.db, &self.client, send).await? {
            self.observe_payment(&payment.id).await;
        } else if send.kind == SendKind::Bolt11
            && send.status == SendStatus::Submitting
            && send.payment_id.is_none()
        {
            self.verify_lightning_send_funding(
                &send.owner,
                &send.outbound_transfer_id,
                &send.invoice,
                send.amount_override,
            )
            .await?;
            retry_missing_bolt11(&self.db, &self.client, send).await?;
        } else {
            self.db.lightning_submission_error(&send.request_id,"LDK has no matching payment; inspect the backend and its event log. This intent will not be resubmitted without an idempotent backend contract.").await?;
        }
        Ok(())
    }

    async fn settle_succeeded_payment(&self, payment: &Payment) -> Result<(), String> {
        let payment_id = payment.id.clone();
        let Some(send) = self.db.lightning_send_for_payment(&payment_id).await? else {
            return Err(format!(
                "no Lightning send request for payment {payment_id}"
            ));
        };
        validate_send_payment(&send, payment)?;
        let Some(kind) = payment.kind.as_ref().and_then(|kind| kind.kind.as_ref()) else {
            return Err(format!("payment {payment_id} has no payment kind"));
        };
        match kind {
            ldk_server_client::ldk_server_grpc::types::payment_kind::Kind::Bolt11(bolt11) => {
                let preimage = bolt11
                    .preimage
                    .as_deref()
                    .ok_or_else(|| format!("payment {payment_id} succeeded without a preimage"))?;
                self.spark
                    .settle_lightning_send(&send.outbound_transfer_id, &bolt11.hash, preimage)
                    .await?;
            }
            ldk_server_client::ldk_server_grpc::types::payment_kind::Kind::Bolt12Offer(offer)
                if send.kind == SendKind::Bolt12 =>
            {
                let hash = offer
                    .hash
                    .as_deref()
                    .ok_or_else(|| format!("payment {payment_id} succeeded without a hash"))?;
                let preimage = offer
                    .preimage
                    .as_deref()
                    .ok_or_else(|| format!("payment {payment_id} succeeded without a preimage"))?;
                validate_preimage(hash, preimage)?;
                self.db
                    .record_lightning_proof(&send.request_id, hash, preimage)
                    .await?;
            }
            _ => {
                return Err(format!(
                    "payment {payment_id} has an unexpected payment kind"
                ))
            }
        }
        self.db.set_payment(&payment_id, "SUCCEEDED").await
    }

    async fn fail_managed_payment(
        &self,
        payment_id: &str,
        reason: Option<&str>,
    ) -> Result<(), String> {
        let Some(send) = self.db.lightning_send_for_payment(payment_id).await? else {
            return Ok(());
        };
        if send.status == SendStatus::Succeeded {
            return Ok(());
        }
        self.db.record_lightning_failure(payment_id, reason).await?;
        if send.status == SendStatus::Failed {
            return Ok(());
        }
        if send.kind == SendKind::Bolt12 {
            self.db.set_payment(payment_id, "REFUNDING").await?;
            self.spark
                .refund_bolt12_send(&send.owner, &send.outbound_transfer_id, send.amount_sats)
                .await?;
        }
        self.db.set_payment(payment_id, "FAILED").await
    }

    async fn finish_bolt12_receive(
        &self,
        offer_id: &str,
        payment_hash: &str,
        preimage: Option<&str>,
        amount_msat: Option<u64>,
    ) -> Result<(), String> {
        let _guard = self.receive_lock.lock().await;
        let Some(receive) = self.db.lightning_receive_for_hash(offer_id).await? else {
            return Ok(());
        };
        if receive.status.as_str() == "TRANSFER_COMPLETED" {
            return Ok(());
        }
        let expected_msat = receive
            .amount_sats
            .checked_mul(1000)
            .ok_or_else(|| "BOLT12 receive amount is too large".to_string())?;
        if !amount_msat.is_some_and(|amount| amount >= expected_msat) {
            return Err(format!(
                "BOLT12 receive has {amount_msat:?} msat; expected at least {expected_msat}"
            ));
        }
        let preimage = preimage.ok_or("BOLT12 receive succeeded without a preimage")?;
        validate_preimage(payment_hash, preimage)?;
        self.db.bind_bolt12_receive(offer_id, payment_hash).await?;
        self.db
            .record_lightning_proof(&receive.request_id, payment_hash, preimage)
            .await?;
        let transfer_id = self
            .spark
            .settle_lightning_receive(&receive.receiver, payment_hash, receive.amount_sats)
            .await?;
        self.db
            .commit_bolt12_receive(
                offer_id,
                payment_hash,
                &transfer_id,
                &receive.request_id,
                &receive.owner,
            )
            .await
    }

    async fn is_managed_outbound(&self, payment_id: &str) -> Result<bool, String> {
        Ok(self
            .db
            .lightning_send_for_payment(payment_id)
            .await?
            .is_some())
    }

    async fn finish_received_payment(&self, payment_hash: &str) -> Result<bool, String> {
        let _guard = self.receive_lock.lock().await;
        if self.db.internal_send(payment_hash).await?.is_some() {
            return Ok(true);
        }
        let Some(receive) = self.db.lightning_receive_for_hash(payment_hash).await? else {
            return Ok(false);
        };
        let transfer_id = match receive.transfer_id {
            Some(id) => id,
            None => self
                .db
                .transfer_for_request(&receive.request_id, &receive.owner)
                .await?
                .ok_or("Lightning settled before its Spark transfer")?,
        };
        self.db
            .insert_transfer(
                &transfer_id,
                &receive.request_id,
                "LIGHTNING_RECEIVE",
                "TRANSFER_COMPLETED",
                &receive.owner,
            )
            .await?;
        self.db
            .set_receive_status(payment_hash, "TRANSFER_COMPLETED")
            .await?;
        Ok(true)
    }

    async fn process_inbound_claimable(
        &self,
        payment_hash: &str,
        amount_msat: Option<u64>,
    ) -> Result<bool, String> {
        process_standard_receive(
            self.db.as_ref(),
            self.receive_lock.as_ref(),
            self.spark.as_ref(),
            &self.client,
            payment_hash,
            amount_msat,
            &RECEIVE_RETRY_DELAYS,
        )
        .await
    }

    async fn reconcile_payments(&self) -> Result<(), String> {
        for send in self.db.unresolved_lightning_sends().await? {
            if let Err(error) = self.recover_send(&send).await {
                self.db
                    .lightning_submission_error(&send.request_id, &error)
                    .await?;
                tracing::debug!(
                    request_id = send.request_id,
                    "send recovery pending: {error}"
                );
            }
        }
        let mut page_token = None;
        for _ in 0..100 {
            let page = self
                .client
                .list_payments(ListPaymentsRequest { page_token })
                .await
                .map_err(|e| e.to_string())?;
            for payment in page.payments {
                if payment.direction == PaymentDirection::Outbound as i32 {
                    if !self.is_managed_outbound(&payment.id).await? {
                        continue;
                    }
                    match payment.status {
                        value if value == PaymentStatus::Succeeded as i32 => {
                            if let Err(error) = self.settle_succeeded_payment(&payment).await {
                                tracing::warn!(
                                    payment_id = %payment.id,
                                    "Lightning paid but Spark settlement is pending: {error}"
                                );
                                self.db.set_payment(&payment.id, "SETTLING").await?;
                            }
                        }
                        value if value == PaymentStatus::Failed as i32 => {
                            // Failure reasons arrive in PaymentFailed events, not
                            // payment snapshots. Preserve any stored event reason.
                            self.fail_managed_payment(&payment.id, None).await?;
                        }
                        _ => self.db.set_payment(&payment.id, "PENDING").await?,
                    }
                    continue;
                }
                if let Some((offer_id, payment_hash)) = bolt12_offer_ids(Some(payment.clone())) {
                    if payment.status == PaymentStatus::Succeeded as i32 {
                        if let Err(error) = self
                            .finish_bolt12_receive(
                                &offer_id,
                                &payment_hash,
                                bolt12_preimage(Some(&payment)).as_deref(),
                                payment.amount_msat,
                            )
                            .await
                        {
                            tracing::warn!(
                                offer_id,
                                payment_hash,
                                "BOLT12 Spark payout is pending: {error}"
                            );
                        }
                    }
                    continue;
                }
                let Some(payment_hash) = bolt11_hash(Some(payment.clone())) else {
                    continue;
                };
                match payment.status {
                    value if value == PaymentStatus::Succeeded as i32 => {
                        if let Err(error) = self.finish_received_payment(&payment_hash).await {
                            tracing::warn!(
                                payment_hash,
                                "Lightning received but Spark payout is pending: {error}"
                            );
                        }
                    }
                    value if value == PaymentStatus::Failed as i32 => {
                        if self
                            .db
                            .lightning_receive_for_hash(&payment_hash)
                            .await?
                            .is_some()
                        {
                            self.db.fail_external_receive(&payment_hash).await?;
                        }
                    }
                    _ => match self
                        .process_inbound_claimable(&payment_hash, payment.amount_msat)
                        .await
                    {
                        Ok(true) => {}
                        Ok(false) => {}
                        Err(error) => tracing::warn!(
                            payment_hash,
                            "Spark payout or Lightning claim is pending: {error}"
                        ),
                    },
                }
            }
            page_token = page.next_page_token;
            if page_token.is_none() {
                break;
            }
        }
        if page_token.is_some() {
            return Err("ldk payment reconciliation exceeded 100 pages".to_string());
        }
        for payment_hash in self
            .db
            .expired_receive_hashes(chrono::Utc::now().timestamp())
            .await?
        {
            // The list can become stale while an internal send reserves or
            // funds this receive. Recheck under the same lock before failing
            // the invoice or deleting a secret needed for payout recovery.
            let _guard = self.receive_lock.lock().await;
            let Some(receive) = self.db.lightning_receive_for_hash(&payment_hash).await? else {
                continue;
            };
            if self.db.internal_send(&payment_hash).await?.is_some()
                || receive.transfer_id.is_some()
                || matches!(
                    receive.status.as_str(),
                    "TRANSFER_COMPLETED" | "HTLC_FAILED"
                )
                || self
                    .db
                    .transfer_for_request(&receive.request_id, &receive.owner)
                    .await?
                    .is_some()
            {
                continue;
            }
            if retry_bounded(
                || self.client.fail_receive(&payment_hash),
                &RECEIVE_RETRY_DELAYS,
            )
            .await
            .is_ok()
            {
                self.db
                    .set_receive_status(&payment_hash, "HTLC_FAILED")
                    .await?;
            }
        }
        Ok(())
    }
}

fn sats_to_msats(sats: u64) -> Result<u64, String> {
    sats.checked_mul(1000)
        .ok_or_else(|| "amount_sats is too large".to_string())
}

// Some Signet clients issue invoices with the older Testnet currency code.
// Both codes refer to test coins; mainnet and regtest remain distinct.
fn send_network_matches(invoice: bitcoin::Network, configured: bitcoin::Network) -> bool {
    use bitcoin::Network::{Signet, Testnet};
    invoice == configured || matches!((invoice, configured), (Testnet, Signet) | (Signet, Testnet))
}

fn invoice_network(network: &str) -> Result<bitcoin::Network, String> {
    match network.to_ascii_uppercase().as_str() {
        "MAINNET" => Ok(bitcoin::Network::Bitcoin),
        "TESTNET" => Ok(bitcoin::Network::Testnet),
        "SIGNET" => Ok(bitcoin::Network::Signet),
        "REGTEST" | "LOCAL" => Ok(bitcoin::Network::Regtest),
        _ => Err(format!("unsupported Lightning invoice network {network}")),
    }
}

fn validate_created_invoice(
    invoice: &str,
    payment_hash: &str,
    amount_sats: u64,
    network: bitcoin::Network,
) -> Result<(), String> {
    let invoice = lightning_invoice::Bolt11Invoice::from_str(invoice)
        .map_err(|e| format!("decode created BOLT11 invoice: {e}"))?;
    if invoice.payment_hash().to_string() != payment_hash.to_lowercase() {
        return Err("created invoice payment hash does not match the wallet hash".to_string());
    }
    if invoice.amount_milli_satoshis() != Some(sats_to_msats(amount_sats)?) {
        return Err("created invoice amount does not match the requested amount".to_string());
    }
    if invoice.network() != network {
        return Err("created invoice network does not match the SSP network".to_string());
    }
    Ok(())
}

fn description_of(memo: &str) -> Option<Bolt11InvoiceDescription> {
    use ldk_server_client::ldk_server_grpc::types::bolt11_invoice_description::Kind;
    if memo.is_empty() {
        return None;
    }
    Some(Bolt11InvoiceDescription {
        kind: Some(Kind::Direct(memo.to_string())),
    })
}

impl LdkGrpcBackend {
    // Decision: 0 fee.
    pub async fn fee_estimate_msat(&self, _invoice: &str, _amount_sats: Option<u64>) -> u64 {
        0
    }

    pub async fn verify_lightning_send_funding(
        &self,
        owner: &str,
        outbound_transfer_id: &str,
        invoice: &str,
        amount_sats: Option<u64>,
    ) -> Result<(), String> {
        if invoice.to_ascii_lowercase().starts_with("lno1") {
            let amount_sats =
                amount_sats.ok_or_else(|| "BOLT12 sends require amount_sats".to_string())?;
            if amount_sats == 0 {
                return Err("Lightning send amount must be positive".to_string());
            }
            return self
                .spark
                .verify_bolt12_send(owner, outbound_transfer_id, amount_sats)
                .await;
        }
        let decoded = self
            .client
            .decode_invoice(DecodeInvoiceRequest {
                invoice: invoice.to_string(),
            })
            .await
            .map_err(|error| format!("decode invoice: {error}"))?;
        let amount_msat = match decoded.amount_msat {
            Some(value) => {
                if amount_sats.is_some() {
                    return Err("amount_sats is only valid for zero-amount invoices".to_string());
                }
                value
            }
            None => sats_to_msats(
                amount_sats.ok_or_else(|| "zero-amount invoice needs amount_sats".to_string())?,
            )?,
        };
        let total_sats = amount_msat
            .checked_add(999)
            .ok_or_else(|| "invoice amount is too large".to_string())?
            / 1000;
        if total_sats == 0 {
            return Err("Lightning send amount must be positive".to_string());
        }
        self.spark
            .verify_lightning_send(
                owner,
                outbound_transfer_id,
                &decoded.payment_hash.to_lowercase(),
                total_sats,
            )
            .await
    }

    pub async fn reconcile_request(&self, id: &str) -> Result<String, String> {
        let send = self
            .db
            .lightning_send_for_payment(id)
            .await?
            .ok_or("Lightning send request not found")?;
        self.recover_send(&send).await?;
        self.db.payment_status(&send.request_id).await
    }

    pub async fn payment_status(&self, payment_id: &str) -> String {
        let mut cached = self.db.payment_status(payment_id).await.unwrap_or_default();
        let send = match self.db.lightning_send_for_payment(payment_id).await {
            Ok(Some(send)) => send,
            _ => return cached,
        };
        if let Err(error) = self.recover_send(&send).await {
            tracing::debug!(
                request_id = send.request_id,
                "send recovery pending: {error}"
            );
        }
        cached = self
            .db
            .payment_status(&send.request_id)
            .await
            .unwrap_or(cached);
        cached
    }

    async fn observe_payment(&self, payment_id: &str) -> String {
        let cached = self.db.payment_status(payment_id).await.unwrap_or_default();
        match self
            .client
            .get_payment_details(GetPaymentDetailsRequest {
                payment_id: payment_id.to_string(),
            })
            .await
        {
            Ok(resp) => match resp.payment {
                Some(p) if p.status == PaymentStatus::Succeeded as i32 => {
                    if cached == "SUCCEEDED" {
                        cached
                    } else {
                        match self.settle_succeeded_payment(&p).await {
                            Ok(()) => "SUCCEEDED".to_string(),
                            Err(error) => {
                                tracing::warn!(
                                    payment_id,
                                    "Lightning paid but Spark settlement is pending: {error}"
                                );
                                let _ = self.db.set_payment(payment_id, "SETTLING").await;
                                "SETTLING".to_string()
                            }
                        }
                    }
                }
                Some(p) if p.status == PaymentStatus::Failed as i32 => {
                    // The pinned client exposes failure reasons only on events.
                    match self.fail_managed_payment(&p.id, None).await {
                        Ok(()) => "FAILED".to_string(),
                        Err(error) => {
                            tracing::warn!(payment_id, "BOLT12 refund is pending: {error}");
                            "REFUNDING".to_string()
                        }
                    }
                }
                Some(_) => {
                    if cached.is_empty() || cached == "UNKNOWN" {
                        "PENDING".to_string()
                    } else {
                        cached
                    }
                }
                None => cached,
            },
            Err(_) => cached,
        }
    }

    pub async fn create_invoice(
        &self,
        amount_sats: u64,
        payment_hash_hex: &str,
        memo: &str,
        expiry_secs: u32,
    ) -> Result<CreateInvoiceResult, String> {
        let request = Bolt11ReceiveForHashRequest {
            amount_msat: Some(sats_to_msats(amount_sats)?),
            description: description_of(memo),
            expiry_secs,
            payment_hash: payment_hash_hex.to_string(),
        };
        let resp = retry_bounded(
            || {
                let request = request.clone();
                async {
                    self.client
                        .bolt11_receive_for_hash(request)
                        .await
                        .map_err(|e| e.to_string())
                }
            },
            &RECEIVE_RETRY_DELAYS,
        )
        .await?;
        validate_created_invoice(
            &resp.invoice,
            payment_hash_hex,
            amount_sats,
            self.invoice_network,
        )?;
        Ok(CreateInvoiceResult {
            invoice: resp.invoice,
        })
    }

    pub async fn create_bolt12_offer(
        &self,
        amount_sats: u64,
        memo: &str,
        expiry_secs: u32,
    ) -> Result<CreateOfferResult, String> {
        let response = self
            .client
            .bolt12_receive(Bolt12ReceiveRequest {
                description: memo.to_string(),
                amount_msat: Some(sats_to_msats(amount_sats)?),
                expiry_secs: Some(expiry_secs),
                quantity: None,
            })
            .await
            .map_err(|e| format!("create BOLT12 offer: {e}"))?;
        Ok(CreateOfferResult {
            offer: response.offer,
            offer_id: response.offer_id.to_lowercase(),
        })
    }

    pub async fn fail_hold(&self, payment_hash_hex: &str) -> bool {
        self.client
            .bolt11_fail_for_hash(Bolt11FailForHashRequest {
                payment_hash: payment_hash_hex.to_string(),
            })
            .await
            .is_ok()
    }

    async fn apply_ln_event(&self, event: LnEvent) {
        match event {
            LnEvent::OutboundSucceeded { payment } => {
                match self.is_managed_outbound(&payment.id).await {
                    Ok(true) => {}
                    Ok(false) => return,
                    Err(error) => {
                        tracing::warn!(
                            payment_id = %payment.id,
                            "could not classify outbound Lightning payment: {error}"
                        );
                        return;
                    }
                }
                if let Err(error) = self.settle_succeeded_payment(&payment).await {
                    tracing::warn!(
                        payment_id = %payment.id,
                        "Lightning paid but Spark settlement is pending: {error}"
                    );
                    let _ = self.db.set_payment(&payment.id, "SETTLING").await;
                }
            }
            LnEvent::OutboundFailed { payment_id, reason } => {
                match self.is_managed_outbound(&payment_id).await {
                    Ok(true) => {}
                    Ok(false) => return,
                    Err(error) => {
                        tracing::warn!(
                            payment_id,
                            "could not classify outbound Lightning payment: {error}"
                        );
                        return;
                    }
                }
                if let Err(error) = self
                    .fail_managed_payment(&payment_id, reason.as_deref())
                    .await
                {
                    tracing::warn!(payment_id, "BOLT12 refund is pending: {error}");
                }
            }
            LnEvent::InboundClaimable {
                payment_hash,
                amount_msat,
            } => {
                match self
                    .process_inbound_claimable(&payment_hash, amount_msat)
                    .await
                {
                    Ok(true) => {
                        tracing::info!(
                            "committed Spark receive and submitted LDK claim {payment_hash}"
                        );
                    }
                    Ok(false) => {}
                    Err(error) => tracing::warn!(
                        payment_hash,
                        "Spark payout or Lightning claim is pending: {error}"
                    ),
                }
            }
            LnEvent::InboundReceived { payment_hash } => {
                if let Err(error) = self.finish_received_payment(&payment_hash).await {
                    tracing::warn!(
                        payment_hash,
                        "Lightning received but Spark payout is pending: {error}"
                    );
                }
            }
            LnEvent::InboundBolt12Received {
                offer_id,
                payment_hash,
                preimage,
                amount_msat,
            } => {
                if let Err(error) = self
                    .finish_bolt12_receive(
                        &offer_id,
                        &payment_hash,
                        preimage.as_deref(),
                        amount_msat,
                    )
                    .await
                {
                    tracing::warn!(
                        offer_id,
                        payment_hash,
                        "BOLT12 Spark payout is pending: {error}"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex as SyncMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct LostReplyLdk {
        db: Db,
        calls: AtomicUsize,
        payment: SyncMutex<Option<Payment>>,
    }
    #[async_trait::async_trait]
    impl SendLdk for LostReplyLdk {
        async fn submit(&self, send: &LightningSend) -> Result<String, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let stored = self
                .db
                .lightning_send_for_payment(&send.request_id)
                .await?
                .unwrap();
            assert_eq!(stored.status, SendStatus::Submitting);
            assert!(self
                .db
                .find_by_idempotency(&send.owner, "key")
                .await?
                .is_some());
            assert_eq!(
                self.db
                    .transfer_for_request(&send.request_id, &send.owner)
                    .await?,
                Some(send.outbound_transfer_id.clone())
            );
            *self.payment.lock() = Some(send_payment(send));
            Err("response lost after LDK accepted payment".into())
        }
        async fn lookup(&self, _send: &LightningSend) -> Result<Option<Payment>, String> {
            Ok(self.payment.lock().clone())
        }
    }
    fn send_intent(kind: SendKind) -> LightningSend {
        LightningSend {
            request_id: uuid::Uuid::new_v4().to_string(),
            owner: "owner".into(),
            outbound_transfer_id: uuid::Uuid::new_v4().to_string(),
            invoice: "test-invoice".into(),
            amount_sats: 1234,
            amount_override: None,
            kind,
            expected_id: "expected".into(),
            payment_id: None,
            status: SendStatus::Prepared,
        }
    }
    fn send_payment(send: &LightningSend) -> Payment {
        use ldk_server_client::ldk_server_grpc::types::{
            payment_kind::Kind, Bolt11, Bolt12Offer, PaymentKind,
        };
        Payment {
            id: "ldk-payment".into(),
            amount_msat: Some(send.amount_sats * 1000),
            direction: PaymentDirection::Outbound as i32,
            status: PaymentStatus::Pending as i32,
            kind: Some(PaymentKind {
                kind: Some(match send.kind {
                    SendKind::Bolt11 => Kind::Bolt11(Bolt11 {
                        hash: send.expected_id.clone(),
                        ..Default::default()
                    }),
                    SendKind::Bolt12 => Kind::Bolt12Offer(Bolt12Offer {
                        offer_id: send.expected_id.clone(),
                        payer_note: Some(send.payer_note()),
                        ..Default::default()
                    }),
                }),
            }),
            ..Default::default()
        }
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn lost_send_reply_recovers_after_restart_without_resubmission() {
        for kind in [SendKind::Bolt11, SendKind::Bolt12] {
            let dir = std::env::temp_dir().join(format!("open-ssp-send-{}", uuid::Uuid::new_v4()));
            let db = Db::open(dir.to_str().unwrap()).unwrap();
            let send = send_intent(kind);
            db.prepare_lightning_send(&send, "key", "REGTEST")
                .await
                .unwrap();
            let ldk = LostReplyLdk {
                db: db.clone(),
                calls: AtomicUsize::new(0),
                payment: SyncMutex::new(None),
            };
            // Concurrent/repeated attempts can cross the durable checkpoint only once.
            let (a, b) = tokio::join!(
                submit_durable_send(&db, &ldk, &send),
                submit_durable_send(&db, &ldk, &send)
            );
            a.unwrap();
            b.unwrap();
            assert_eq!(ldk.calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                db.payment_status(&send.request_id).await.unwrap(),
                "SUBMITTING"
            );
            let payment = ldk.payment.lock().clone();
            drop(ldk);
            drop(db);
            let db = Db::open(dir.to_str().unwrap()).unwrap();
            let ldk = LostReplyLdk {
                db: db.clone(),
                calls: AtomicUsize::new(0),
                payment: SyncMutex::new(payment),
            };
            let stored = db
                .lightning_send_for_payment(&send.request_id)
                .await
                .unwrap()
                .unwrap();
            submit_durable_send(&db, &ldk, &stored).await.unwrap();
            recover_submission(&db, &ldk, &stored)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(ldk.calls.load(Ordering::SeqCst), 0);
            assert_eq!(
                db.payment_status(&send.request_id).await.unwrap(),
                "PENDING"
            );
            assert_eq!(
                db.lightning_send_for_payment("ldk-payment")
                    .await
                    .unwrap()
                    .unwrap()
                    .outbound_transfer_id,
                send.outbound_transfer_id
            );
            std::fs::remove_dir_all(dir).unwrap();
        }
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn missing_bolt11_retries_but_bolt12_and_bound_payments_do_not() {
        let dir = std::env::temp_dir().join(format!("open-ssp-send-{}", uuid::Uuid::new_v4()));
        let db = Db::open(dir.to_str().unwrap()).unwrap();
        let send = send_intent(SendKind::Bolt11);
        db.prepare_lightning_send(&send, "key", "REGTEST")
            .await
            .unwrap();
        assert!(db
            .begin_lightning_submission(&send.request_id)
            .await
            .unwrap());
        drop(db);
        let db = Db::open(dir.to_str().unwrap()).unwrap();
        let ldk = LostReplyLdk {
            db: db.clone(),
            calls: AtomicUsize::new(0),
            payment: SyncMutex::new(None),
        };
        submit_durable_send(&db, &ldk, &send).await.unwrap();
        assert!(recover_submission(&db, &ldk, &send)
            .await
            .unwrap()
            .is_none());
        assert_eq!(ldk.calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            db.payment_status(&send.request_id).await.unwrap(),
            "SUBMITTING"
        );
        let mut stored = db
            .lightning_send_for_payment(&send.request_id)
            .await
            .unwrap()
            .unwrap();
        stored.kind = SendKind::Bolt12;
        assert!(retry_missing_bolt11(&db, &ldk, &stored).await.is_err());
        assert_eq!(ldk.calls.load(Ordering::SeqCst), 0);
        stored.kind = SendKind::Bolt11;
        retry_missing_bolt11(&db, &ldk, &stored).await.unwrap();
        assert_eq!(ldk.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            db.payment_status(&send.request_id).await.unwrap(),
            "PENDING"
        );
        let bound = db
            .lightning_send_for_payment(&send.request_id)
            .await
            .unwrap()
            .unwrap();
        assert!(retry_missing_bolt11(&db, &ldk, &bound).await.is_err());
        assert_eq!(ldk.calls.load(Ordering::SeqCst), 1);
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn recovered_send_must_match_amount_direction_and_identity() {
        for kind in [SendKind::Bolt11, SendKind::Bolt12] {
            let send = send_intent(kind);
            let original = send_payment(&send);
            assert!(validate_send_payment(&send, &original).is_ok());
            let mut payment = original.clone();
            payment.amount_msat = Some(1);
            assert!(validate_send_payment(&send, &payment).is_err());
            payment = original.clone();
            payment.direction = PaymentDirection::Inbound as i32;
            assert!(validate_send_payment(&send, &payment).is_err());
            let mut other = send.clone();
            other.expected_id = "different".into();
            assert!(validate_send_payment(&other, &original).is_err());
            if kind == SendKind::Bolt12 {
                other = send.clone();
                other.request_id = "other-request".into();
                assert!(validate_send_payment(&other, &original).is_err());
            }
        }
    }
    #[derive(Default)]
    struct MockSpark {
        calls: AtomicUsize,
        failures: AtomicUsize,
        preimage: String,
        error_message: Option<String>,
        log: Arc<SyncMutex<Vec<&'static str>>>,
    }

    #[async_trait::async_trait]
    impl ReceiveSpark for MockSpark {
        async fn swap_receive(
            &self,
            _owner: &str,
            _payment_hash: &str,
            _invoice: &str,
            _amount_sats: u64,
        ) -> Result<LightningReceiveSwap, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.log.lock().push("spark");
            if let Some(error) = &self.error_message {
                return Err(error.clone());
            }
            if self
                .failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                    value.checked_sub(1)
                })
                .is_ok()
            {
                return Err("operator unavailable".to_string());
            }
            Ok(LightningReceiveSwap {
                transfer_id: "00000000-0000-4000-8000-000000000001".to_string(),
                preimage: self.preimage.clone(),
            })
        }
    }

    #[derive(Default)]
    struct MockLdk {
        claims: AtomicUsize,
        failures: AtomicUsize,
        failed_holds: AtomicUsize,
        log: Arc<SyncMutex<Vec<&'static str>>>,
    }

    #[async_trait::async_trait]
    impl ReceiveLdk for MockLdk {
        async fn claim_receive(
            &self,
            _payment_hash: &str,
            _amount_msat: u64,
            _preimage: &str,
        ) -> Result<(), String> {
            self.claims.fetch_add(1, Ordering::SeqCst);
            self.log.lock().push("claim");
            if self
                .failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                    value.checked_sub(1)
                })
                .is_ok()
            {
                Err("ldk unavailable".to_string())
            } else {
                Ok(())
            }
        }

        async fn fail_receive(&self, _payment_hash: &str) -> Result<(), String> {
            self.failed_holds.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    async fn receive_fixture() -> (Db, std::path::PathBuf, String, String) {
        let dir = std::env::temp_dir().join(format!("open-ssp-receive-{}", uuid::Uuid::new_v4()));
        let db = Db::open(dir.to_str().unwrap()).unwrap();
        let preimage = "01".repeat(32);
        let payment_hash = hex::encode(Sha256::digest(hex::decode(&preimage).unwrap()));
        db.insert_request(
            "request",
            "LIGHTNING_RECEIVE",
            "request-owner",
            &chrono::Utc::now().to_rfc3339(),
            &serde_json::json!({
                "payment_hash": payment_hash,
                "amount_sats": 5_000,
                "invoice": "ln-invoice",
                "receiver_identity_pubkey": "receiver",
                "expiry_secs": 300,
            }),
            None,
        )
        .await
        .unwrap();
        db.set_receive_status(&payment_hash, "INVOICE_CREATED")
            .await
            .unwrap();
        (db, dir, payment_hash, preimage)
    }

    fn mock_spark(preimage: String, log: Arc<SyncMutex<Vec<&'static str>>>) -> MockSpark {
        MockSpark {
            preimage,
            log,
            ..Default::default()
        }
    }

    #[async_trait::async_trait]
    impl InternalSpark for MockSpark {
        async fn verify_sender(&self, _: &LightningSend) -> Result<(), String> {
            Ok(())
        }
        async fn settle_sender(&self, _: &LightningSend, _: &str) -> Result<(), String> {
            self.log.lock().push("settle_sender");
            Ok(())
        }
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn internal_payment_resumes_after_payout_and_rejects_external_htlcs() {
        let (db, dir, hash, preimage) = receive_fixture().await;
        let mut send = send_intent(SendKind::Bolt11);
        send.expected_id = hash.clone();
        send.invoice = "ln-invoice".into();
        send.amount_sats = 5_000;
        db.prepare_lightning_send(&send, "internal", "REGTEST")
            .await
            .unwrap();
        db.reserve_internal_send(&send).await.unwrap();
        // Crash after an operator reply was durably checkpointed.
        db.commit_lightning_receive_swap(
            &hash,
            "00000000-0000-4000-8000-000000000001",
            &preimage,
            "request",
            "request-owner",
        )
        .await
        .unwrap();
        drop(db);
        let db = Db::open(dir.to_str().unwrap()).unwrap();
        let lock = tokio::sync::Mutex::new(());
        let spark = mock_spark(preimage, Arc::new(SyncMutex::new(Vec::new())));
        let ldk = MockLdk::default();
        process_standard_receive(&db, &lock, &spark, &ldk, &hash, Some(5_000_000), &[])
            .await
            .unwrap();
        db.fail_external_receive(&hash).await.unwrap();
        process_internal_send(&db, &lock, &spark, &send)
            .await
            .unwrap();
        process_internal_send(&db, &lock, &spark, &send)
            .await
            .unwrap();
        assert_eq!(spark.calls.load(Ordering::SeqCst), 0);
        assert_eq!(ldk.claims.load(Ordering::SeqCst), 0);
        assert_eq!(ldk.failed_holds.load(Ordering::SeqCst), 1);
        assert_eq!(
            db.payment_status(&send.request_id).await.unwrap(),
            "SUCCEEDED"
        );
        assert_eq!(
            db.receive_status(&hash).await.unwrap(),
            "TRANSFER_COMPLETED"
        );
        assert_eq!(*spark.log.lock(), vec!["settle_sender"]);
        drop(db);
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn external_htlc_wins_before_internal_reservation() {
        let (db, dir, hash, preimage) = receive_fixture().await;
        let mut send = send_intent(SendKind::Bolt11);
        send.expected_id = hash.clone();
        send.invoice = "ln-invoice".into();
        send.amount_sats = 5_000;
        db.prepare_lightning_send(&send, "internal", "REGTEST")
            .await
            .unwrap();
        db.mark_receive_claimable(&hash, 5_000_000).await.unwrap();
        let spark = mock_spark(preimage, Arc::new(SyncMutex::new(Vec::new())));
        assert!(
            process_internal_send(&db, &tokio::sync::Mutex::new(()), &spark, &send)
                .await
                .is_err()
        );
        assert_eq!(spark.calls.load(Ordering::SeqCst), 0);
        assert_eq!(db.payment_status(&send.request_id).await.unwrap(), "FAILED");
        assert_eq!(db.receive_status(&hash).await.unwrap(), "HTLC_RECEIVED");
        drop(db);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn reconnect_backoff_is_bounded() {
        for failures in 0..100 {
            let delay = reconnect_delay(failures);
            assert!(delay >= std::time::Duration::from_secs(1));
            assert!(delay <= std::time::Duration::from_millis(37_500));
        }
    }

    #[test]
    fn millisatoshi_conversion_rejects_overflow() {
        assert_eq!(
            sats_to_msats(21_000_000 * 100_000_000),
            Ok(2_100_000_000_000_000_000)
        );
        assert!(sats_to_msats(u64::MAX).is_err());
    }

    #[test]
    fn failed_event_keeps_backend_reason() {
        use ldk_server_client::ldk_server_grpc::events::{
            EventEnvelope, PaymentFailed, PaymentFailureReason,
        };
        let events = map_envelope(EventEnvelope {
            event: Some(LdkRawEvent::PaymentFailed(PaymentFailed {
                payment: Some(Payment {
                    id: "payment".into(),
                    ..Default::default()
                }),
                reason: Some(PaymentFailureReason::InvoiceRequestExpired as i32),
            })),
        });
        assert!(
            matches!(events.as_slice(), [LnEvent::OutboundFailed {payment_id,reason:Some(reason)}]
            if payment_id == "payment" && reason == "PAYMENT_FAILURE_REASON_INVOICE_REQUEST_EXPIRED")
        );
    }

    #[test]
    fn bolt12_receive_event_keeps_offer_and_payment_ids() {
        use ldk_server_client::ldk_server_grpc::{
            events::{EventEnvelope, PaymentReceived},
            types::{payment_kind, Bolt12Offer, PaymentKind},
        };

        let payment = Payment {
            id: "payment-id".to_string(),
            kind: Some(PaymentKind {
                kind: Some(payment_kind::Kind::Bolt12Offer(Bolt12Offer {
                    hash: Some("payment-hash".to_string()),
                    offer_id: "offer-id".to_string(),
                    ..Default::default()
                })),
            }),
            amount_msat: Some(1_001_000),
            ..Default::default()
        };
        let events = map_envelope(EventEnvelope {
            event: Some(LdkRawEvent::PaymentReceived(PaymentReceived {
                payment: Some(payment),
                custom_records: Vec::new(),
            })),
        });

        assert!(matches!(
            events.as_slice(),
            [LnEvent::InboundBolt12Received {
                offer_id,
                payment_hash,
                amount_msat: Some(1_001_000),
                preimage: None,
            }] if offer_id == "offer-id" && payment_hash == "payment-hash"
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn wallet_created_receive_commits_and_claims() {
        let (db, dir, hash, preimage) = receive_fixture().await;
        let log = Arc::new(SyncMutex::new(Vec::new()));
        let spark = mock_spark(preimage, log.clone());
        let ldk = MockLdk {
            log: log.clone(),
            ..Default::default()
        };
        let lock = tokio::sync::Mutex::new(());

        assert!(
            process_standard_receive(&db, &lock, &spark, &ldk, &hash, Some(5_000_000), &[],)
                .await
                .unwrap()
        );
        let receive = db.lightning_receive_for_hash(&hash).await.unwrap().unwrap();
        assert!(receive.transfer_id.is_some());
        assert_eq!(receive.preimage, Some("01".repeat(32)));
        assert!(receive.claim_submitted);
        assert_eq!(*log.lock(), vec!["spark", "claim"]);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mismatched_operator_preimage_is_not_claimed() {
        let (db, dir, hash, _) = receive_fixture().await;
        let spark = mock_spark("02".repeat(32), Arc::default());
        let ldk = MockLdk::default();

        let error = process_standard_receive(
            &db,
            &tokio::sync::Mutex::new(()),
            &spark,
            &ldk,
            &hash,
            Some(5_000_000),
            &[],
        )
        .await
        .unwrap_err();
        assert!(error.contains("does not match"));
        assert_eq!(ldk.claims.load(Ordering::SeqCst), 0);
        assert_eq!(ldk.failed_holds.load(Ordering::SeqCst), 0);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn duplicate_claimable_event_does_not_repeat_transfer_or_claim() {
        let (db, dir, hash, preimage) = receive_fixture().await;
        let spark = mock_spark(preimage, Arc::default());
        let ldk = MockLdk::default();
        let lock = tokio::sync::Mutex::new(());

        for _ in 0..2 {
            process_standard_receive(&db, &lock, &spark, &ldk, &hash, Some(5_000_000), &[])
                .await
                .unwrap();
        }
        assert_eq!(spark.calls.load(Ordering::SeqCst), 1);
        assert_eq!(ldk.claims.load(Ordering::SeqCst), 1);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn restart_after_spark_commit_resumes_only_ldk_claim() {
        let (db, dir, hash, preimage) = receive_fixture().await;
        db.commit_lightning_receive_swap(
            &hash,
            "00000000-0000-4000-8000-000000000001",
            &preimage,
            "request",
            "request-owner",
        )
        .await
        .unwrap();
        let spark = mock_spark(preimage, Arc::default());
        let ldk = MockLdk::default();

        process_standard_receive(
            &db,
            &tokio::sync::Mutex::new(()),
            &spark,
            &ldk,
            &hash,
            Some(5_000_000),
            &[],
        )
        .await
        .unwrap();
        assert_eq!(spark.calls.load(Ordering::SeqCst), 0);
        assert_eq!(ldk.claims.load(Ordering::SeqCst), 1);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn operator_failure_keeps_hold_without_claiming() {
        let (db, dir, hash, preimage) = receive_fixture().await;
        let spark = MockSpark {
            failures: AtomicUsize::new(1),
            ..mock_spark(preimage, Arc::default())
        };
        let ldk = MockLdk::default();

        assert!(process_standard_receive(
            &db,
            &tokio::sync::Mutex::new(()),
            &spark,
            &ldk,
            &hash,
            Some(5_000_000),
            &[],
        )
        .await
        .is_err());
        assert_eq!(ldk.claims.load(Ordering::SeqCst), 0);
        assert_eq!(ldk.failed_holds.load(Ordering::SeqCst), 0);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ldk_claim_retries_without_repeating_spark_transfer() {
        let (db, dir, hash, preimage) = receive_fixture().await;
        let spark = mock_spark(preimage, Arc::default());
        let ldk = MockLdk {
            failures: AtomicUsize::new(2),
            ..Default::default()
        };
        let delays = [Duration::ZERO, Duration::ZERO];

        process_standard_receive(
            &db,
            &tokio::sync::Mutex::new(()),
            &spark,
            &ldk,
            &hash,
            Some(5_000_000),
            &delays,
        )
        .await
        .unwrap();
        assert_eq!(spark.calls.load(Ordering::SeqCst), 1);
        assert_eq!(ldk.claims.load(Ordering::SeqCst), 3);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn claimable_amount_must_match_invoice_amount() {
        let (db, dir, hash, preimage) = receive_fixture().await;
        let spark = mock_spark(preimage, Arc::default());
        let ldk = MockLdk::default();

        assert!(process_standard_receive(
            &db,
            &tokio::sync::Mutex::new(()),
            &spark,
            &ldk,
            &hash,
            Some(4_999_000),
            &[],
        )
        .await
        .is_err());
        assert_eq!(spark.calls.load(Ordering::SeqCst), 0);
        assert_eq!(ldk.claims.load(Ordering::SeqCst), 0);
        assert_eq!(ldk.failed_holds.load(Ordering::SeqCst), 1);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn send_networks_preserve_legacy_signet_invoices() {
        use bitcoin::Network::{Bitcoin, Regtest, Signet, Testnet};
        assert!(send_network_matches(Testnet, Signet));
        assert!(send_network_matches(Signet, Testnet));
        assert!(send_network_matches(Regtest, Regtest));
        assert!(!send_network_matches(Bitcoin, Signet));
        assert!(!send_network_matches(Testnet, Regtest));
    }

    #[test]
    fn receive_invoice_networks_are_explicit() {
        assert_eq!(
            invoice_network("MAINNET").unwrap(),
            bitcoin::Network::Bitcoin
        );
        assert_eq!(
            invoice_network("TESTNET").unwrap(),
            bitcoin::Network::Testnet
        );
        assert_eq!(invoice_network("SIGNET").unwrap(), bitcoin::Network::Signet);
        assert_eq!(invoice_network("LOCAL").unwrap(), bitcoin::Network::Regtest);
        assert!(invoice_network("unknown").is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unselectable_amount_fails_the_hold_for_refund() {
        let (db, dir, hash, preimage) = receive_fixture().await;
        let spark = MockSpark {
            // Exact-conservation leaf selection reports unrepresentable
            // amounts this way; the string must stay a definitive failure.
            error_message: Some("unselectable amount".to_string()),
            ..mock_spark(preimage, Arc::new(SyncMutex::new(Vec::new())))
        };
        let ldk = MockLdk::default();

        assert!(process_standard_receive(
            &db,
            &tokio::sync::Mutex::new(()),
            &spark,
            &ldk,
            &hash,
            Some(5_000_000),
            &[],
        )
        .await
        .is_err());

        assert_eq!(spark.calls.load(Ordering::SeqCst), 1);
        assert_eq!(ldk.claims.load(Ordering::SeqCst), 0);
        // The hold is failed so the payer is refunded instead of waiting
        // for expiry cleanup.
        assert_eq!(ldk.failed_holds.load(Ordering::SeqCst), 1);
        assert_eq!(db.receive_status(&hash).await.unwrap(), "HTLC_FAILED");
        std::fs::remove_dir_all(dir).unwrap();
    }
}
