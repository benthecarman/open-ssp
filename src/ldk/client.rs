//! Transport boundary for the subset of LDK used by SSP settlement.
//!
//! Keep the existing payment DTOs at this boundary so both transports feed the
//! same identity checks and durable recovery code. No gRPC server is started in
//! embedded mode.
use std::{fs::File, path::Path, str::FromStr, sync::Arc};

use fs2::FileExt;
use ldk_node::{
    entropy::NodeEntropy,
    lightning::{ln::channelmanager::PaymentId, offers::offer::Offer},
    lightning_invoice::{Bolt11Invoice, Bolt11InvoiceDescription, Description},
    lightning_types::payment::{PaymentHash, PaymentPreimage},
    payment::{self, PaymentDetails},
    Builder, Node,
};
use ldk_server_client::{
    client::LdkServerClient,
    ldk_server_grpc::{api::*, types},
};

use super::{ClaimableReceive, LdkBackend, LnEvent, ReceiveLdk, SendLdk};
use crate::{
    config::{Config, LdkBackendMode, LdkChainSource},
    lightning_store::{LightningSend, SendKind},
};

#[derive(Clone)]
pub(super) enum LdkClient {
    Server(Arc<LdkServerClient>),
    Embedded(Arc<EmbeddedNode>),
}

pub(super) struct EmbeddedNode {
    node: Arc<Node>,
    // Held until the node is dropped, including all outstanding blocking calls.
    _directory_lock: File,
}

impl EmbeddedNode {
    fn build(config: &Config, network: bitcoin::Network) -> Result<Self, String> {
        let listen = config
            .ldk_node_listen_addr
            .parse()
            .map_err(|e| format!("LDK_NODE_LISTEN_ADDR: {e}"))?;
        let dir = if config.ldk_node_data_dir.is_empty() {
            Path::new(&config.data_dir).join("ldk-node")
        } else {
            config.ldk_node_data_dir.clone().into()
        };
        let mut builder = Builder::from_config(ldk_node::config::Config {
            network,
            storage_dir_path: dir.to_string_lossy().into_owned(),
            // SSP supplies preimages only after the Spark transfer is durable.
            manually_handle_unknown_bolt11_payments: true,
            ..Default::default()
        });
        configure_chain_source(&mut builder, config)?;
        std::fs::create_dir_all(&dir).map_err(|e| format!("create node directory: {e}"))?;
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join(".lock"))
            .map_err(|e| e.to_string())?;
        lock.try_lock_exclusive()
            .map_err(|e| format!("LDK node directory is already in use: {e}"))?;
        let entropy = load_entropy(&dir, config.ldk_node_seed_required)?;
        builder
            .set_listening_addresses(vec![listen])
            .map_err(|e| e.to_string())?;
        let node = builder
            .build_with_fs_store(entropy)
            .map_err(|e| format!("build LDK node: {e}"))?;
        Ok(Self {
            node: Arc::new(node),
            _directory_lock: lock,
        })
    }

    async fn call<T: Send + 'static>(
        self: &Arc<Self>,
        operation: impl FnOnce(&Node) -> Result<T, String> + Send + 'static,
    ) -> Result<T, String> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || operation(&this.node))
            .await
            .map_err(|e| format!("LDK node task: {e}"))?
    }
}

fn configure_chain_source(builder: &mut Builder, config: &Config) -> Result<(), String> {
    match config.ldk_node_chain_source {
        LdkChainSource::Esplora => {
            if config.ldk_node_esplora_url.trim().is_empty() {
                return Err("LDK_NODE_ESPLORA_URL is required for the esplora chain source".into());
            }
            let url = reqwest::Url::parse(&config.ldk_node_esplora_url)
                .map_err(|e| format!("LDK_NODE_ESPLORA_URL: {e}"))?;
            if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
                return Err("LDK_NODE_ESPLORA_URL must be an HTTP(S) URL".into());
            }
            builder.set_chain_source_esplora(config.ldk_node_esplora_url.clone(), None);
        }
        LdkChainSource::Bitcoind => {
            // LDK takes a host and port, not a URL or a Core wallet endpoint.
            let host = &config.ldk_node_bitcoind_rpc_host;
            if host.is_empty()
                || reqwest::Url::parse(&format!("http://{host}"))
                    .map(|url| {
                        !url.host_str()
                            .is_some_and(|parsed| parsed.eq_ignore_ascii_case(host))
                            || url.port().is_some()
                            || url.path() != "/"
                            || url.query().is_some()
                            || url.fragment().is_some()
                            || !url.username().is_empty()
                            || url.password().is_some()
                    })
                    .unwrap_or(true)
            {
                return Err(
                    "LDK_NODE_BITCOIND_RPC_HOST must be a host without a scheme, port, or path"
                        .into(),
                );
            }
            if config.ldk_node_bitcoind_rpc_port == 0 {
                return Err("LDK_NODE_BITCOIND_RPC_PORT must be nonzero".into());
            }
            if config.ldk_node_bitcoind_rpc_user.trim().is_empty() {
                return Err(
                    "LDK_NODE_BITCOIND_RPC_USER is required for the bitcoind chain source".into(),
                );
            }
            let password = if !config.ldk_node_bitcoind_rpc_password_file.is_empty() {
                std::fs::read_to_string(&config.ldk_node_bitcoind_rpc_password_file)
                    .map_err(|_| "cannot read LDK_NODE_BITCOIND_RPC_PASSWORD_FILE")?
                    .trim_end_matches(['\r', '\n'])
                    .to_owned()
            } else {
                config.ldk_node_bitcoind_rpc_password.clone()
            };
            if password.is_empty() {
                return Err(
                    "LDK_NODE_BITCOIND_RPC_PASSWORD or a nonempty password file is required".into(),
                );
            }
            builder.set_chain_source_bitcoind_rpc(
                host.clone(),
                config.ldk_node_bitcoind_rpc_port,
                config.ldk_node_bitcoind_rpc_user.clone(),
                password,
                config.ldk_node_bitcoind_rescan_from_height,
            );
        }
    }
    Ok(())
}

fn load_entropy(dir: &Path, required: bool) -> Result<NodeEntropy, String> {
    use std::io::Write;
    let path = dir.join("seed");
    match std::fs::read(&path) {
        Ok(bytes) => {
            let seed = bytes
                .try_into()
                .map_err(|_| "LDK seed must contain exactly 64 bytes")?;
            Ok(NodeEntropy::from_seed_bytes(seed))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            if required || dir.join("fs_store").exists() {
                return Err("LDK seed missing; restore the node directory or disable LDK_NODE_SEED_REQUIRED for first boot".into());
            }
            let mut seed = [0u8; 64];
            rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut seed);
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options
                .open(&path)
                .map_err(|e| format!("create LDK seed: {e}"))?;
            file.write_all(&seed)
                .and_then(|()| file.sync_all())
                .map_err(|e| format!("persist LDK seed: {e}"))?;
            File::open(dir)
                .and_then(|dir| dir.sync_all())
                .map_err(|e| e.to_string())?;
            Ok(NodeEntropy::from_seed_bytes(seed))
        }
        Err(e) => Err(format!("read LDK seed: {e}")),
    }
}

impl LdkClient {
    pub async fn connect(
        config: &Config,
        network: bitcoin::Network,
    ) -> Result<(Self, String), String> {
        match config.ldk_backend {
            LdkBackendMode::Embedded => {
                let config = config.clone();
                let embedded =
                    tokio::task::spawn_blocking(move || EmbeddedNode::build(&config, network))
                        .await
                        .map_err(|e| e.to_string())??;
                let embedded = Arc::new(embedded);
                embedded
                    .call(|node| node.start().map_err(|e| format!("start LDK node: {e}")))
                    .await?;
                let id = embedded.node.node_id().to_string();
                tracing::info!(node_id = id, "embedded LDK node started");
                Ok((Self::Embedded(embedded), id))
            }
            LdkBackendMode::Server => {
                if config.ldk_grpc_addr.is_empty() {
                    return Err("LDK_GRPC_ADDR unset".into());
                }
                let api_key = if !config.ldk_api_key.is_empty() {
                    config.ldk_api_key.clone()
                } else if !config.ldk_api_key_file.is_empty() {
                    hex::encode(
                        std::fs::read(&config.ldk_api_key_file)
                            .map_err(|e| format!("read LDK_API_KEY_FILE: {e}"))?,
                    )
                } else {
                    return Err("LDK_API_KEY or LDK_API_KEY_FILE required for server mode".into());
                };
                if api_key.is_empty() {
                    return Err("empty LDK api key".into());
                }
                let cert = std::fs::read(&config.ldk_tls_cert_file)
                    .map_err(|e| format!("read LDK_TLS_CERT_FILE: {e}"))?;
                let client = LdkServerClient::new(config.ldk_grpc_addr.clone(), api_key, &cert)?;
                let info = tokio::time::timeout(
                    std::time::Duration::from_secs(15),
                    client.get_node_info(GetNodeInfoRequest {}),
                )
                .await
                .map_err(|_| "get_node_info timed out")?
                .map_err(|e| e.to_string())?;
                Ok((Self::Server(Arc::new(client)), info.node_id))
            }
        }
    }

    pub async fn stop(&self) -> Result<(), String> {
        match self {
            Self::Server(_) => Ok(()),
            Self::Embedded(node) => {
                node.call(|node| node.stop().map_err(|e| e.to_string()))
                    .await
            }
        }
    }

    pub async fn send(&self, send: &LightningSend) -> Result<String, String> {
        let node = match self {
            Self::Server(client) => return client.submit(send).await,
            Self::Embedded(node) => node,
        };
        let send = send.clone();
        node.call(move |node| {
            let amount = send.amount_override.map(super::sats_to_msats).transpose()?;
            let id = match send.kind {
                SendKind::Bolt11 => {
                    let invoice =
                        Bolt11Invoice::from_str(&send.invoice).map_err(|e| e.to_string())?;
                    match amount {
                        Some(amount) => node
                            .bolt11_payment()
                            .send_using_amount(&invoice, amount, None),
                        None => node.bolt11_payment().send(&invoice, None),
                    }
                }
                SendKind::Bolt12 => {
                    let offer = Offer::from_str(&send.invoice)
                        .map_err(|e| format!("invalid offer: {e:?}"))?;
                    match amount {
                        Some(amount) => node.bolt12_payment().send_using_amount(
                            &offer,
                            amount,
                            None,
                            Some(send.payer_note()),
                            None,
                        ),
                        None => {
                            node.bolt12_payment()
                                .send(&offer, None, Some(send.payer_note()), None)
                        }
                    }
                }
            }
            .map_err(|e| e.to_string())?;
            Ok(id.to_string())
        })
        .await
    }

    pub async fn offer_id(&self, offer: &str) -> Result<String, String> {
        match self {
            Self::Server(client) => client
                .decode_offer(DecodeOfferRequest {
                    offer: offer.into(),
                })
                .await
                .map(|r| r.offer_id)
                .map_err(|e| e.to_string()),
            Self::Embedded(_) => Offer::from_str(offer)
                .map(|o| hex::encode(o.id().0))
                .map_err(|e| format!("invalid offer: {e:?}")),
        }
    }

    pub async fn get_payment_details(
        &self,
        request: GetPaymentDetailsRequest,
    ) -> Result<GetPaymentDetailsResponse, String> {
        match self {
            Self::Server(client) => client
                .get_payment_details(request)
                .await
                .map_err(|e| e.to_string()),
            Self::Embedded(node) => {
                node.call(move |node| {
                    let id = PaymentId(hex32(&request.payment_id)?);
                    let payment = node
                        .payment(&id)
                        .map_err(|e| e.to_string())?
                        .map(payment_snapshot);
                    Ok(GetPaymentDetailsResponse { payment })
                })
                .await
            }
        }
    }

    pub async fn list_payments(
        &self,
        request: ListPaymentsRequest,
    ) -> Result<ListPaymentsResponse, String> {
        match self {
            Self::Server(client) => client
                .list_payments(request)
                .await
                .map_err(|e| e.to_string()),
            Self::Embedded(node) => {
                node.call(move |node| {
                    let page = node
                        .list_payments(request.page_token.map(payment::PageToken::new))
                        .map_err(|e| e.to_string())?;
                    Ok(ListPaymentsResponse {
                        payments: page.payments.into_iter().map(payment_snapshot).collect(),
                        next_page_token: page.next_page_token.map(|p| p.to_string()),
                    })
                })
                .await
            }
        }
    }

    pub async fn bolt11_receive_for_hash(
        &self,
        request: Bolt11ReceiveForHashRequest,
    ) -> Result<Bolt11ReceiveForHashResponse, String> {
        match self {
            Self::Server(client) => client
                .bolt11_receive_for_hash(request)
                .await
                .map_err(|e| e.to_string()),
            Self::Embedded(node) => {
                node.call(move |node| {
                    let memo = match request.description.and_then(|d| d.kind) {
                        Some(types::bolt11_invoice_description::Kind::Direct(memo)) => memo,
                        None => String::new(),
                        _ => return Err("SSP only creates direct invoice descriptions".into()),
                    };
                    let description = Bolt11InvoiceDescription::Direct(
                        Description::new(memo).map_err(|e| e.to_string())?,
                    );
                    let hash = PaymentHash(hex32(&request.payment_hash)?);
                    let invoice = match request.amount_msat {
                        Some(amount) => node.bolt11_payment().receive_for_hash(
                            amount,
                            &description,
                            request.expiry_secs,
                            hash,
                        ),
                        None => node.bolt11_payment().receive_variable_amount_for_hash(
                            &description,
                            request.expiry_secs,
                            hash,
                        ),
                    }
                    .map_err(|e| e.to_string())?;
                    Ok(Bolt11ReceiveForHashResponse {
                        invoice: invoice.to_string(),
                    })
                })
                .await
            }
        }
    }

    pub async fn bolt12_receive(
        &self,
        request: Bolt12ReceiveRequest,
    ) -> Result<Bolt12ReceiveResponse, String> {
        match self {
            Self::Server(client) => client
                .bolt12_receive(request)
                .await
                .map_err(|e| e.to_string()),
            Self::Embedded(node) => {
                node.call(move |node| {
                    let offer = match request.amount_msat {
                        Some(amount) => node.bolt12_payment().receive(
                            amount,
                            &request.description,
                            request.expiry_secs,
                            request.quantity,
                        ),
                        None => node
                            .bolt12_payment()
                            .receive_variable_amount(&request.description, request.expiry_secs),
                    }
                    .map_err(|e| e.to_string())?;
                    Ok(Bolt12ReceiveResponse {
                        offer_id: hex::encode(offer.id().0),
                        offer: offer.to_string(),
                    })
                })
                .await
            }
        }
    }

    pub async fn run_embedded_events(&self, backend: &LdkBackend) {
        let Self::Embedded(node) = self else { return };
        loop {
            let event = node.node.next_event_async().await;
            let mapped = node
                .call(move |node| map_event(event, |id| event_payment(node, id)))
                .await;
            match mapped {
                Ok(Some(event)) => {
                    // Failure reasons exist only in events, not payment snapshots.
                    // Keep the queued event if that information cannot be saved.
                    if let LnEvent::OutboundFailed { payment_id, reason } = &event {
                        if let Err(error) = backend
                            .db
                            .record_lightning_failure(payment_id, reason.as_deref())
                            .await
                        {
                            tracing::warn!("persist embedded LDK failure: {error}");
                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                            continue;
                        }
                    }
                    backend.apply_ln_event(event).await;
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!("read embedded LDK event: {error}");
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    continue;
                }
            }
            // Payment snapshots and SSP checkpoints recover incomplete settlement
            // after acknowledgement, just as after a remote stream disconnect.
            if let Err(error) = node
                .call(|node| node.event_handled().map_err(|e| e.to_string()))
                .await
            {
                tracing::error!("acknowledge embedded LDK event: {error}");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}

#[async_trait::async_trait]
impl ReceiveLdk for LdkClient {
    async fn claim_receive(
        &self,
        id: &str,
        amount_msat: u64,
        preimage: &str,
    ) -> Result<(), String> {
        match self {
            Self::Server(client) => client.claim_receive(id, amount_msat, preimage).await,
            Self::Embedded(node) => {
                let id = PaymentId(hex32(id)?);
                let preimage = PaymentPreimage(hex32(preimage)?);
                node.call(move |node| {
                    node.bolt11_payment()
                        .claim_for_id(id, amount_msat, preimage)
                        .map_err(|e| e.to_string())
                })
                .await
            }
        }
    }
    async fn fail_receive(&self, id: &str) -> Result<(), String> {
        match self {
            Self::Server(client) => client.fail_receive(id).await,
            Self::Embedded(node) => {
                let id = PaymentId(hex32(id)?);
                node.call(move |node| {
                    node.bolt11_payment()
                        .fail_for_id(id)
                        .map_err(|e| e.to_string())
                })
                .await
            }
        }
    }
}

fn hex32(value: &str) -> Result<[u8; 32], String> {
    let mut bytes = [0u8; 32];
    hex::decode_to_slice(value, &mut bytes)
        .map_err(|e| format!("expected 32-byte hex value: {e}"))?;
    Ok(bytes)
}

fn payment_snapshot(payment: PaymentDetails) -> types::Payment {
    use types::payment_kind::Kind;
    let kind = match payment.kind {
        payment::PaymentKind::Bolt11 {
            hash,
            preimage,
            secret,
            counterparty_skimmed_fee_msat,
        } => Some(Kind::Bolt11(types::Bolt11 {
            hash: hash.to_string(),
            preimage: preimage.map(|p| p.to_string()),
            secret: secret.map(|s| s.0.to_vec().into()),
            counterparty_skimmed_fee_msat,
        })),
        payment::PaymentKind::Bolt12Offer {
            hash,
            preimage,
            secret,
            offer_id,
            payer_note,
            quantity,
        } => Some(Kind::Bolt12Offer(types::Bolt12Offer {
            hash: hash.map(|h| h.to_string()),
            preimage: preimage.map(|p| p.to_string()),
            secret: secret.map(|s| s.0.to_vec().into()),
            offer_id: hex::encode(offer_id.0),
            payer_note: payer_note.map(|s| s.0),
            quantity,
        })),
        // SSP settlement ignores on-chain, keysend and refund payment kinds.
        _ => None,
    };
    types::Payment {
        payment_id: payment.id.to_string(),
        kind: kind.map(|kind| types::PaymentKind { kind: Some(kind) }),
        amount_msat: payment.amount_msat,
        fee_paid_msat: payment.fee_paid_msat,
        direction: match payment.direction {
            payment::PaymentDirection::Inbound => types::PaymentDirection::Inbound as i32,
            payment::PaymentDirection::Outbound => types::PaymentDirection::Outbound as i32,
        },
        status: match payment.status {
            payment::PaymentStatus::Pending => types::PaymentStatus::Pending as i32,
            payment::PaymentStatus::Succeeded => types::PaymentStatus::Succeeded as i32,
            payment::PaymentStatus::Failed => types::PaymentStatus::Failed as i32,
        },
        latest_update_timestamp: payment.latest_update_timestamp,
    }
}

fn map_event(
    event: ldk_node::Event,
    payment: impl FnOnce(PaymentId) -> Result<types::Payment, String>,
) -> Result<Option<LnEvent>, String> {
    use ldk_node::Event;
    let result = match event {
        Event::PaymentSuccessful { payment_id, .. } => Some(LnEvent::OutboundSucceeded {
            payment: payment(payment_id)?,
        }),
        Event::PaymentFailed {
            payment_id, reason, ..
        } => Some(LnEvent::OutboundFailed {
            payment_id: payment_id.to_string(),
            reason: reason.map(failure_reason),
        }),
        Event::PaymentClaimable {
            payment_id,
            payment_hash,
            claimable_amount_msat,
            ..
        } => Some(LnEvent::InboundClaimable(ClaimableReceive {
            payment_id: payment_id.to_string(),
            payment_hash: payment_hash.to_string(),
            amount_msat: Some(claimable_amount_msat),
        })),
        Event::PaymentReceived { payment_id, .. } => {
            let payment = payment(payment_id)?;
            if let Some(hash) = super::bolt11_hash(Some(payment.clone())) {
                Some(LnEvent::InboundReceived {
                    payment_id: payment_id.to_string(),
                    payment_hash: hash,
                })
            } else if let Some((offer_id, payment_hash)) =
                super::bolt12_offer_ids(Some(payment.clone()))
            {
                Some(LnEvent::InboundBolt12Received {
                    offer_id,
                    payment_hash,
                    preimage: super::bolt12_preimage(Some(&payment)),
                    amount_msat: payment.amount_msat,
                })
            } else {
                None
            }
        }
        _ => None,
    };
    Ok(result)
}

fn failure_reason(reason: ldk_node::lightning::events::PaymentFailureReason) -> String {
    use ldk_node::lightning::events::PaymentFailureReason as NodeReason;
    use ldk_server_client::ldk_server_grpc::events::PaymentFailureReason as ApiReason;
    let reason = match reason {
        NodeReason::RecipientRejected => ApiReason::RecipientRejected,
        NodeReason::UserAbandoned => ApiReason::UserAbandoned,
        NodeReason::RetriesExhausted => ApiReason::RetriesExhausted,
        NodeReason::PaymentExpired => ApiReason::PaymentExpired,
        NodeReason::RouteNotFound => ApiReason::RouteNotFound,
        NodeReason::UnexpectedError => ApiReason::UnexpectedError,
        NodeReason::UnknownRequiredFeatures => ApiReason::UnknownRequiredFeatures,
        NodeReason::InvoiceRequestExpired => ApiReason::InvoiceRequestExpired,
        NodeReason::InvoiceRequestRejected => ApiReason::InvoiceRequestRejected,
        NodeReason::BlindedPathCreationFailed => ApiReason::BlindedPathCreationFailed,
    };
    reason.as_str_name().to_string()
}

fn event_payment(node: &Node, id: PaymentId) -> Result<types::Payment, String> {
    node.payment(&id)
        .map_err(|e| e.to_string())?
        .map(payment_snapshot)
        .ok_or_else(|| format!("LDK event payment {id} missing from store"))
}

#[cfg(test)]
mod tests;
