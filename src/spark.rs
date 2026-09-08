use std::{
    fs::OpenOptions,
    io::Write,
    os::unix::fs::OpenOptionsExt,
    path::Path,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use ::spark::{
    operator::rpc::spark::{
        initiate_preimage_swap_request::Reason as PreimageSwapReason, InitiatePreimageSwapRequest,
        InvoiceAmount, InvoiceAmountProof, StartTransferRequest,
    },
    operator::{rpc::DefaultConnectionManager, OperatorPool},
    services::{
        LeafKeyTweak, LeafSplitDraft, LeafSplitPlan, LeafSplitService, SubmittedLeafSplit,
        Transfer as SparkTransfer, TransferService, TransferType,
    },
    session_store::InMemorySessionStore,
    signer::SparkSigner as CoreSparkSigner,
    tree::{select_leaves_by_exact_amounts, LeafLike, TreeNode, TreeNodeStatus, TreeServiceError},
};
use bip39::{Language, Mnemonic};
use bitcoin::{
    consensus::deserialize,
    hashes::{sha256, Hash as BitcoinHash},
    Transaction,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use spark_wallet::{
    DefaultSigner, Network, OperatorPoolConfig, Preimage, PreimageRequestRole,
    PreimageRequestStatus, SparkAddress, SparkSignerAdapter, SparkWallet, SparkWalletConfig,
    TransferId, TransferStatus, WalletLeaf, WalletTransfer,
};

use crate::{
    config::Config,
    db::{Db, SparkSplitOperation},
};

#[derive(Debug, Serialize)]
pub struct SparkHealth {
    pub address: String,
    pub identity_pubkey: String,
    pub available_sats: u64,
    pub owned_sats: u64,
    pub needs_topup: bool,
}

#[derive(Debug)]
pub struct SwapFill {
    pub transfer_id: String,
    pub leaves: Vec<serde_json::Value>,
    pub expires_at: Option<String>,
}

// Swap V3 sends only the CPFP refund. The operator stores its verified
// adaptor signature in the witness; it is not the identity-key signature.
fn swap_leaf_response(id: &str, raw: &str) -> Result<serde_json::Value, String> {
    let mut tx: Transaction =
        deserialize(&hex::decode(raw).map_err(|e| e.to_string())?).map_err(|e| e.to_string())?;
    let input = tx.input.first_mut().ok_or("swap refund has no input")?;
    let signature = input
        .witness
        .iter()
        .next()
        .filter(|s| s.len() == 64)
        .ok_or("swap refund has no 64-byte adaptor signature")?;
    let signature = hex::encode(signature);
    for input in &mut tx.input {
        input.witness.clear();
    }
    Ok(serde_json::json!({
        "leaf_id": id,
        "raw_unsigned_refund_transaction": hex::encode(bitcoin::consensus::serialize(&tx)),
        "adaptor_signed_signature": signature,
        "direct_raw_unsigned_refund_transaction": null,
        "direct_adaptor_signed_signature": null,
        "direct_from_cpfp_raw_unsigned_refund_transaction": null,
        "direct_from_cpfp_adaptor_signed_signature": null,
    }))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LightningReceiveSwap {
    pub transfer_id: String,
    pub preimage: String,
}

pub struct SparkService {
    wallet: Arc<SparkWallet>,
    identity_secret: bitcoin::secp256k1::SecretKey,
    raw_signer: Arc<DefaultSigner>,
    network: Network,
    identity: spark_wallet::PublicKey,
    operator_pool: Arc<OperatorPool>,
    private_pool: Option<Arc<OperatorPool>>,
    transfer_service: Arc<TransferService>,
    split_service: Option<Arc<LeafSplitService>>,
    db: Arc<Db>,
    minimum_split_child_sats: u64,
    liquidity_lock: tokio::sync::Mutex<()>,
    needs_topup: AtomicBool,
}

impl SparkService {
    pub async fn coop_exit_leaves(
        &self,
        owner: &str,
        ids: &[String],
    ) -> Result<Vec<crate::coop_exit::ExitLeaf>, String> {
        use ::spark::operator::rpc::spark::{
            query_nodes_request::Source, QueryNodesRequest, TreeNodeIds,
        };
        let request = QueryNodesRequest {
            source: Some(Source::NodeIds(TreeNodeIds {
                node_ids: ids.to_vec(),
            })),
            network: self.network.to_proto_network() as i32,
            ..Default::default()
        };
        let response = if let Some(pool) = &self.private_pool {
            pool.get_coordinator().client.query_ssp_nodes(request).await
        } else {
            self.operator_pool
                .get_coordinator()
                .client
                .query_nodes(request)
                .await
        }
        .map_err(|error| error.to_string())?;
        let owner = spark_wallet::PublicKey::from_str(owner).map_err(|error| error.to_string())?;
        ids.iter().map(|id| {
            let node = response.nodes.get(id).ok_or("withdrawal leaf unavailable; the operator must permit this SSP to read the leaf")?;
            if node.owner_identity_public_key != owner.serialize() || node.network != self.network.to_proto_network() as i32 {
                return Err("withdrawal leaf owner or network mismatch".into());
            }
            if node.status != "AVAILABLE" || node.value == 0 {
                return Err("withdrawal leaf is not available".into());
            }
            Ok(crate::coop_exit::ExitLeaf { id:id.clone(), value:node.value })
        }).collect()
    }

    pub async fn coop_exit_transfer_exists(&self, id: &str) -> Result<bool, String> {
        let id = TransferId::from_str(id).map_err(|error| error.to_string())?;
        Ok(self
            .transfer_service
            .query_transfer(&id)
            .await
            .map_err(|error| error.to_string())?
            .is_some())
    }

    pub async fn verify_coop_exit(
        &self,
        record: &crate::coop_exit::ExitRecord,
    ) -> Result<(), String> {
        let id = TransferId::from_str(&record.transfer_id).map_err(|error| error.to_string())?;
        let transfer = self
            .transfer_service
            .query_transfer(&id)
            .await
            .map_err(|error| error.to_string())?
            .ok_or("conditional withdrawal transfer is not available yet")?;
        validate_coop_exit_transfer(&transfer, record, &self.identity)
    }

    pub async fn claim_coop_exit(
        &self,
        record: &crate::coop_exit::ExitRecord,
    ) -> Result<(), String> {
        let _guard = self.liquidity_lock.lock().await;
        let id = TransferId::from_str(&record.transfer_id).map_err(|error| error.to_string())?;
        let transfer = self
            .wallet
            .get_transfer(&id)
            .await
            .map_err(|error| error.to_string())?
            .ok_or("withdrawal Spark transfer not found")?;
        if transfer.transfer_type != TransferType::CooperativeExit
            || transfer.receiver_id != self.identity
        {
            return Err("withdrawal transfer is not addressed to this SSP".into());
        }
        if transfer.status != TransferStatus::Completed {
            self.wallet
                .process_transfer(transfer)
                .await
                .map_err(|error| error.to_string())?;
        }
        let transfer = self
            .wallet
            .get_transfer(&id)
            .await
            .map_err(|error| error.to_string())?
            .ok_or("withdrawal Spark transfer disappeared")?;
        if transfer.status != TransferStatus::Completed {
            return Err("withdrawal Spark claim is pending".into());
        }
        Ok(())
    }

    pub async fn connect(config: &Config, db: Arc<Db>) -> Result<Arc<Self>, String> {
        let network = parse_network(&config.network)?;
        let mnemonic =
            load_or_create_mnemonic(&config.spark_mnemonic_file, config.spark_mnemonic_required)?;
        let seed = mnemonic.to_seed("");
        let signer = Arc::new(DefaultSigner::new(&seed, network).map_err(|e| e.to_string())?);
        let raw_signer = signer.clone();
        let identity_secret = ::spark::signer::identity_master_key(&seed, network, None)
            .map_err(|e| e.to_string())?
            .private_key;
        let signer: Arc<dyn CoreSparkSigner> =
            Arc::new(SparkSignerAdapter::new(signer).with_leaf_key_override_store(db.clone()));
        let identity = signer
            .get_identity_public_key()
            .await
            .map_err(|e| e.to_string())?;
        if !config.ssp_identity_pubkey.is_empty()
            && identity.to_string() != config.ssp_identity_pubkey.to_lowercase()
        {
            return Err(format!(
                "embedded Spark identity {identity} does not match SSP_IDENTITY_PUBKEY {}",
                config.ssp_identity_pubkey
            ));
        }

        let hosts = csv(&config.so_hosts);
        let pubkeys = csv(&config.so_identity_pubkeys);
        if hosts.is_empty() || hosts.len() != pubkeys.len() {
            return Err(
                "SO_HOSTS and SO_IDENTITY_PUBKEYS must have the same nonzero length".to_string(),
            );
        }
        let cert_files = csv(&config.so_cert_files);
        if !cert_files.is_empty() && cert_files.len() != hosts.len() {
            return Err("SO_CERT_FILES must be empty or match SO_HOSTS".to_string());
        }
        let mut operators = Vec::with_capacity(hosts.len());
        for (index, (host, pubkey)) in hosts.iter().zip(&pubkeys).enumerate() {
            let cert = if cert_files.is_empty() || cert_files[index].is_empty() {
                None
            } else {
                Some(
                    std::fs::read(&cert_files[index])
                        .map_err(|e| format!("read SO certificate {}: {e}", cert_files[index]))?,
                )
            };
            let address = if host.contains("://") {
                host.clone()
            } else {
                format!("https://{host}")
            };
            operators.push(
                SparkWalletConfig::create_operator_config(
                    index,
                    &format!("{:064x}", index + 1),
                    &address,
                    cert.as_deref(),
                    pubkey,
                )
                .map_err(|e| format!("operator {index}: {e}"))?,
            );
        }

        let mut wallet_config = SparkWalletConfig::default_config(network);
        wallet_config.operator_pool =
            OperatorPoolConfig::new(0, operators).map_err(|e| e.to_string())?;
        wallet_config.service_provider_config = SparkWalletConfig::create_service_provider_config(
            &config.ssp_public_url,
            &identity.to_string(),
            Some("graphql/spark/rc".to_string()),
        )
        .map_err(|e| e.to_string())?;
        wallet_config.split_secret_threshold = config.frost_threshold as u32;
        wallet_config.leaf_auto_optimize_enabled = false;

        // Keep an authenticated operator client beside the embedded wallet.
        // SparkWallet does not expose its pool, but receive settlement needs the
        // existing low-level InitiatePreimageSwapV3 RPC and generated types.
        let sessions = Arc::new(InMemorySessionStore::default());
        let operator_pool = Arc::new(
            OperatorPool::connect(
                &wallet_config.operator_pool,
                Arc::new(DefaultConnectionManager::new()),
                sessions.clone(),
                signer.clone(),
                None,
            )
            .await
            .map_err(|e| format!("connect receive operator clients: {e}"))?,
        );
        let transfer_service = Arc::new(TransferService::new(
            signer.clone(),
            network,
            wallet_config.split_secret_threshold,
            operator_pool.clone(),
            None,
        ));

        let ssp_hosts = csv(&config.ssp_operator_hosts);
        let ssp_cert_files = csv(&config.ssp_operator_cert_files);
        let mut private_pool_for_service = None;
        let split_service = if ssp_hosts.is_empty() {
            None
        } else {
            if ssp_hosts.len() != pubkeys.len() {
                return Err(
                    "SSP_OPERATOR_HOSTS must be empty or have the same length as SO_IDENTITY_PUBKEYS"
                        .to_string(),
                );
            }
            if !ssp_cert_files.is_empty() && ssp_cert_files.len() != ssp_hosts.len() {
                return Err(
                    "SSP_OPERATOR_CERT_FILES must be empty or match SSP_OPERATOR_HOSTS".to_string(),
                );
            }
            let mut private_operators = Vec::with_capacity(ssp_hosts.len());
            for (index, (host, pubkey)) in ssp_hosts.iter().zip(&pubkeys).enumerate() {
                let cert = if ssp_cert_files.is_empty() || ssp_cert_files[index].is_empty() {
                    None
                } else {
                    Some(std::fs::read(&ssp_cert_files[index]).map_err(|e| {
                        format!(
                            "read SSP operator certificate {}: {e}",
                            ssp_cert_files[index]
                        )
                    })?)
                };
                let address = if host.contains("://") {
                    host.clone()
                } else {
                    format!("https://{host}")
                };
                private_operators.push(
                    SparkWalletConfig::create_operator_config(
                        index,
                        &format!("{:064x}", index + 1),
                        &address,
                        cert.as_deref(),
                        pubkey,
                    )
                    .map_err(|e| format!("SSP operator {index}: {e}"))?,
                );
            }
            let private_config =
                OperatorPoolConfig::new(0, private_operators).map_err(|e| e.to_string())?;
            let private_pool = Arc::new(
                OperatorPool::connect(
                    &private_config,
                    Arc::new(DefaultConnectionManager::new()),
                    sessions,
                    signer.clone(),
                    None,
                )
                .await
                .map_err(|e| format!("connect SSP operator clients: {e}"))?,
            );
            private_pool_for_service = Some(private_pool.clone());
            Some(Arc::new(
                LeafSplitService::new(network, operator_pool.clone(), private_pool, signer.clone())
                    .await
                    .map_err(|e| format!("create leaf split service: {e}"))?,
            ))
        };

        let wallet = Arc::new(
            SparkWallet::connect(wallet_config, signer)
                .await
                .map_err(|e| format!("connect embedded Spark wallet: {e}"))?,
        );
        let service = Arc::new(Self {
            wallet,
            identity_secret,
            raw_signer,
            network,
            identity,
            operator_pool,
            private_pool: private_pool_for_service,
            transfer_service,
            split_service,
            db,
            minimum_split_child_sats: config.ssp_min_split_child_sats,
            liquidity_lock: tokio::sync::Mutex::new(()),
            needs_topup: AtomicBool::new(false),
        });
        service.recover_incomplete_splits().await?;
        Ok(service)
    }

    pub fn identity(&self) -> String {
        self.identity.to_string()
    }

    pub async fn start_background_processing(&self) {
        self.wallet.start_background_processing().await;
    }

    pub async fn health(&self) -> Result<SparkHealth, String> {
        let _guard = self.liquidity_lock.lock().await;
        self.wallet.sync().await.map_err(|e| e.to_string())?;
        let leaves = self.wallet.list_leaves().await.map_err(|e| e.to_string())?;
        let available_sats = leaves.available.iter().map(|leaf| leaf.value).sum();
        let owned_sats = available_sats
            + leaves
                .available_missing_from_operators
                .iter()
                .map(|leaf| leaf.value)
                .sum::<u64>();
        let address = self
            .wallet
            .get_spark_address()
            .and_then(|address| {
                address
                    .to_address_string()
                    .map_err(|e| spark_wallet::SparkWalletError::Generic(e.to_string()))
            })
            .map_err(|e| e.to_string())?;
        Ok(SparkHealth {
            address,
            identity_pubkey: self.identity(),
            available_sats,
            owned_sats,
            needs_topup: available_sats == 0 || self.needs_topup.load(Ordering::Relaxed),
        })
    }

    pub async fn generate_deposit_address(&self) -> Result<String, String> {
        self.wallet
            .generate_deposit_address()
            .await
            .map(|result| result.address.to_string())
            .map_err(|e| e.to_string())
    }

    pub async fn claim_deposit(
        &self,
        transaction_hex: &str,
        vout: u32,
    ) -> Result<Vec<u64>, String> {
        let bytes = hex::decode(transaction_hex).map_err(|e| format!("transaction hex: {e}"))?;
        let tx: Transaction = deserialize(&bytes).map_err(|e| format!("transaction: {e}"))?;
        self.wallet
            .claim_deposit(tx, vout)
            .await
            .map(|leaves| leaves.into_iter().map(|leaf| leaf.value).collect())
            .map_err(|e| e.to_string())
    }

    pub async fn static_deposit_address(
        &self,
        owner: &str,
        address: &str,
    ) -> Result<::spark::operator::rpc::spark::DepositAddressQueryResult, String> {
        use ::spark::operator::rpc::spark::QueryStaticDepositAddressesRequest;
        let owner = spark_wallet::PublicKey::from_str(owner).map_err(|e| e.to_string())?;
        let response = self
            .private_pool
            .as_ref()
            .ok_or("private SSP operator endpoints are required")?
            .get_coordinator()
            .client
            .query_ssp_static_deposit_addresses(QueryStaticDepositAddressesRequest {
                identity_public_key: owner.serialize().to_vec(),
                network: self.network.to_proto_network() as i32,
                limit: 2,
                offset: 0,
                deposit_address: Some(address.into()),
                hash_variant: 0,
            })
            .await
            .map_err(|e| e.to_string())?;
        let mut addresses = response.deposit_addresses;
        if addresses.len() != 1 || addresses[0].deposit_address != address {
            return Err("static deposit address does not belong to the session wallet".into());
        }
        Ok(addresses.remove(0))
    }

    pub fn validate_static_authorization(
        &self,
        quote: &crate::static_deposits::DepositQuote,
        encrypted: &str,
        signature: &str,
    ) -> Result<(), String> {
        use bitcoin::secp256k1::{ecdsa::Signature, Message, PublicKey, Secp256k1, SecretKey};
        let encrypted = hex::decode(encrypted).map_err(|_| "invalid encrypted deposit key")?;
        if encrypted.len() != 129 {
            return Err("invalid encrypted deposit key length".into());
        }
        let plaintext = utils::ecies::decrypt(&self.identity_secret.secret_bytes(), &encrypted)
            .map_err(|_| "cannot decrypt static deposit key")?;
        let key = SecretKey::from_slice(&plaintext).map_err(|_| "invalid static deposit key")?;
        if hex::encode(PublicKey::from_secret_key(&Secp256k1::new(), &key).serialize())
            != quote.signing_key
        {
            return Err("static deposit key does not match the address".into());
        }
        let mut statement = b"claim_static_deposit".to_vec();
        statement.extend(self.network.to_string().as_bytes());
        statement.extend(quote.txid.as_bytes());
        statement.extend(quote.vout.to_le_bytes());
        statement.push(::spark::operator::rpc::spark::UtxoSwapRequestType::Fixed as u8);
        statement.extend(quote.credit.to_le_bytes());
        statement.extend(hex::decode(&quote.signature).map_err(|_| "invalid quote signature")?);
        let digest: [u8; 32] = Sha256::digest(&statement).into();
        let signature =
            Signature::from_der(&hex::decode(signature).map_err(|_| "invalid user signature")?)
                .map_err(|_| "invalid DER user signature")?;
        let owner =
            PublicKey::from_str(&quote.owner).map_err(|_| "invalid static deposit owner")?;
        Secp256k1::verification_only()
            .verify_ecdsa(&Message::from_digest(digest), &signature, &owner)
            .map_err(|_| "invalid static deposit authorization".into())
    }

    pub(crate) async fn deposit_liquidity_lock(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.liquidity_lock.lock().await
    }

    pub(crate) fn instant_authorization(
        &self,
        quote: &crate::static_deposits::DepositQuote,
        secret: Option<&str>,
        encrypted: Option<&str>,
        signature: &str,
    ) -> Result<String, String> {
        use bitcoin::secp256k1::{ecdsa::Signature, Message, PublicKey, Secp256k1};
        let key = decode_instant_deposit_key(&self.identity_secret, secret, encrypted)?;
        if hex::encode(PublicKey::from_secret_key(&Secp256k1::new(), &key).serialize())
            != quote.signing_key
        {
            return Err("deposit key does not match the quoted address".into());
        }
        let digest = crate::instant_deposits::authorization_digest(quote)?;
        let signature = hex::decode(signature).map_err(|_| "invalid user signature")?;
        let signature = Signature::from_der(&signature)
            .or_else(|_| Signature::from_compact(&signature))
            .map_err(|_| "invalid user signature")?;
        let owner = PublicKey::from_str(&quote.owner).map_err(|_| "invalid deposit owner")?;
        Secp256k1::verification_only()
            .verify_ecdsa(&Message::from_digest(digest), &signature, &owner)
            .map_err(|_| "invalid instant deposit authorization")?;
        utils::ecies::encrypt(&self.identity.serialize(), &key.secret_bytes())
            .map(hex::encode)
            .map_err(|_| "cannot encrypt deposit key".into())
    }

    /// The caller holds the liquidity lock across preparation, persistence and submission.
    pub(crate) async fn submit_instant_reserve(
        &self,
        quote: &crate::static_deposits::DepositQuote,
        transfer_id: &str,
        plan: &crate::static_deposits::StaticPlan,
    ) -> Result<(), String> {
        use ::spark::operator::rpc::spark_ssp_internal::{
            ReserveInstantDepositRequest, StaticDepositSwapRequest,
        };
        use prost::Message;
        let req =
            StaticDepositSwapRequest::decode(plan.request.as_slice()).map_err(|e| e.to_string())?;
        let response = self
            .private_pool
            .as_ref()
            .ok_or("private SSP operator endpoints required")?
            .get_coordinator()
            .client
            .reserve_instant_deposit(ReserveInstantDepositRequest {
                on_chain_utxo: req.on_chain_utxo,
                ssp_signature: req.ssp_signature,
                user_signature: req.user_signature,
                transfer: req.transfer,
                destination_address: quote.address.clone(),
                value_sats: (quote.credit + quote.fee) as i64,
                credit_amount_sats: quote.credit as i64,
            })
            .await
            .map_err(|e| e.to_string())?;
        let transfer = response
            .transfer
            .ok_or("operator returned no instant deposit transfer")?;
        if transfer.id != transfer_id
            || transfer.total_value != quote.credit
            || transfer.sender_identity_public_key != self.identity.serialize()
            || transfer.receiver_identity_public_key
                != hex::decode(&quote.owner).map_err(|e| e.to_string())?
        {
            return Err("operator returned a different instant deposit transfer".into());
        }
        Ok(())
    }

    pub async fn prepare_static_claim(
        &self,
        quote: &crate::static_deposits::DepositQuote,
        encrypted: &str,
        signature: &str,
        transfer_id: &str,
        spend: &Transaction,
    ) -> Result<crate::static_deposits::StaticPlan, String> {
        use ::spark::{
            operator::rpc::{
                spark::{SigningJob, Utxo},
                spark_ssp_internal::StaticDepositSwapRequest,
            },
            signer::Signer,
        };
        use prost::Message;
        self.wallet.sync().await.map_err(|e| e.to_string())?;
        self.ensure_exact_liquidity(quote.credit).await?;
        let leaves = self
            .wallet
            .list_leaves()
            .await
            .map_err(|e| e.to_string())?
            .available
            .into_iter()
            .map(wallet_leaf_to_tree_node)
            .collect::<Result<Vec<_>, _>>()?;
        let selected =
            select_leaves_by_exact_amounts(&leaves, &[quote.credit]).map_err(|e| e.to_string())?;
        let tweaks = selected
            .into_iter()
            .map(|node| LeafKeyTweak {
                node,
                incoming_key: None,
            })
            .collect::<Vec<_>>();
        let owner = spark_wallet::PublicKey::from_str(&quote.owner).map_err(|e| e.to_string())?;
        let id = TransferId::from_str(transfer_id)?;
        let prepared = self
            .transfer_service
            .prepare_transfer_request(
                &id,
                &tweaks,
                &owner,
                None,
                Some(std::time::SystemTime::now() + Duration::from_secs(24 * 3600)),
                None,
            )
            .await
            .map_err(|e| e.to_string())?;
        let nonce = self
            .raw_signer
            .generate_random_signing_commitment()
            .await
            .map_err(|e| e.to_string())?;
        let req = StaticDepositSwapRequest {
            on_chain_utxo: Some(Utxo {
                txid: hex::decode(&quote.txid).map_err(|e| e.to_string())?,
                vout: quote.vout,
                network: self.network.to_proto_network() as i32,
                ..Default::default()
            }),
            ssp_signature: hex::decode(&quote.signature).map_err(|e| e.to_string())?,
            user_signature: hex::decode(signature).map_err(|e| e.to_string())?,
            transfer: Some(prepared.transfer_request),
            spend_tx_signing_job: Some(SigningJob {
                signing_public_key: hex::decode(&quote.signing_key).map_err(|e| e.to_string())?,
                raw_tx: bitcoin::consensus::serialize(spend),
                signing_nonce_commitment: Some(
                    nonce
                        .commitments
                        .try_into()
                        .map_err(|e: ::spark::services::ServiceError| e.to_string())?,
                ),
            }),
            hash_variant: 0,
            confirmation_threshold: Some(3),
        };
        Ok(crate::static_deposits::StaticPlan {
            request: req.encode_to_vec(),
            nonce_ciphertext: nonce.nonces_ciphertext,
            encrypted_key: hex::decode(encrypted).map_err(|e| e.to_string())?,
            prev_output: quote.prev_output.clone(),
        })
    }

    pub async fn submit_static_claim(
        &self,
        quote: &crate::static_deposits::DepositQuote,
        transfer_id: &str,
        plan: &crate::static_deposits::StaticPlan,
        instant: bool,
    ) -> Result<Transaction, String> {
        use ::spark::{
            operator::rpc::spark_ssp_internal::StaticDepositSwapRequest,
            services::SigningResult,
            signer::{
                AggregateFrostRequest, FrostSigningCommitmentsWithNonces, SecretSource,
                SignFrostRequest, Signer,
            },
        };
        use prost::Message;
        let req =
            StaticDepositSwapRequest::decode(plan.request.as_slice()).map_err(|e| e.to_string())?;
        let job = req
            .spend_tx_signing_job
            .clone()
            .ok_or("deposit plan lacks signing job")?;
        let client = &self
            .private_pool
            .as_ref()
            .ok_or("private SSP operator endpoints required")?
            .get_coordinator()
            .client;
        let response = if instant {
            client
                .recover_instant_deposit(
                    ::spark::operator::rpc::spark_ssp_internal::RecoverInstantDepositRequest {
                        on_chain_utxo: req.on_chain_utxo,
                        spend_tx_signing_job: Some(job.clone()),
                        transfer_id: transfer_id.into(),
                    },
                )
                .await
        } else {
            client.initiate_static_deposit_swap(req).await
        }
        .map_err(|e| e.to_string())?;
        let transfer = response
            .transfer
            .ok_or("operator returned no static deposit transfer")?;
        if transfer.id != transfer_id
            || transfer.receiver_identity_public_key
                != hex::decode(&quote.owner).map_err(|e| e.to_string())?
            || transfer.sender_identity_public_key != self.identity.serialize()
        {
            return Err("operator returned a different deposit transfer".into());
        }
        let result: SigningResult = response
            .spend_tx_signing_result
            .as_ref()
            .ok_or("operator returned no deposit signing result")?
            .try_into()
            .map_err(|e: ::spark::services::ServiceError| e.to_string())?;
        let verifying_key =
            spark_wallet::PublicKey::from_str(&quote.verifying_key).map_err(|e| e.to_string())?;
        let signing_key =
            spark_wallet::PublicKey::from_str(&quote.signing_key).map_err(|e| e.to_string())?;
        let prev: bitcoin::TxOut =
            deserialize(&hex::decode(&plan.prev_output).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
        let mut spend: Transaction = deserialize(&job.raw_tx).map_err(|e| e.to_string())?;
        let sighash =
            ::spark::bitcoin::sighash_from_tx(&spend, 0, &prev).map_err(|e| e.to_string())?;
        let nonce = FrostSigningCommitmentsWithNonces {
            commitments: job
                .signing_nonce_commitment
                .ok_or("missing deposit nonce")?
                .try_into()
                .map_err(|e: ::spark::services::ServiceError| e.to_string())?,
            nonces_ciphertext: plan.nonce_ciphertext.clone(),
        };
        let key = SecretSource::new_encrypted(plan.encrypted_key.clone());
        let user_share = self
            .raw_signer
            .sign_frost(SignFrostRequest {
                message: sighash.as_byte_array(),
                public_key: &signing_key,
                private_key: &key,
                verifying_key: &verifying_key,
                self_nonce_commitment: &nonce,
                statechain_commitments: result.signing_commitments.clone(),
                adaptor_public_key: None,
            })
            .await
            .map_err(|e| e.to_string())?;
        let signature = ::spark::utils::frost::aggregate_frost(AggregateFrostRequest {
            message: sighash.as_byte_array(),
            statechain_signatures: result.signature_shares,
            statechain_public_keys: result.public_keys,
            verifying_key: &verifying_key,
            statechain_commitments: result.signing_commitments,
            self_commitment: &nonce.commitments,
            public_key: &signing_key,
            self_signature: &user_share,
            adaptor_public_key: None,
        })
        .map_err(|e| e.to_string())?;
        let bytes = signature.serialize().map_err(|e| e.to_string())?;
        // Validate against the actual output script, not operator metadata.
        if !prev.script_pubkey.is_p2tr() {
            return Err("static deposit is not Taproot".into());
        }
        let output_key =
            bitcoin::secp256k1::XOnlyPublicKey::from_slice(&prev.script_pubkey.as_bytes()[2..])
                .map_err(|e| e.to_string())?;
        bitcoin::secp256k1::Secp256k1::verification_only()
            .verify_schnorr(
                &bitcoin::secp256k1::schnorr::Signature::from_slice(&bytes)
                    .map_err(|e| e.to_string())?,
                &bitcoin::secp256k1::Message::from_digest(*sighash.as_byte_array()),
                &output_key,
            )
            .map_err(|_| "deposit recovery signature is invalid")?;
        spend.input[0].witness.push(bytes);
        Ok(spend)
    }

    pub(crate) fn authentication_challenge_mac(&self, challenge: &[u8]) -> Vec<u8> {
        use hmac::{Hmac, Mac};
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.identity_secret.secret_bytes())
            .expect("HMAC accepts a 32-byte key");
        mac.update(b"open-ssp-auth-challenge-v1");
        mac.update(challenge);
        mac.finalize().into_bytes().to_vec()
    }

    pub fn sign_digest(&self, digest: [u8; 32]) -> String {
        let signature = bitcoin::secp256k1::Secp256k1::new().sign_ecdsa(
            &bitcoin::secp256k1::Message::from_digest(digest),
            &self.identity_secret,
        );
        hex::encode(signature.serialize_der())
    }

    pub async fn sign_message(&self, message: &str) -> Result<String, String> {
        self.wallet
            .sign_message(message)
            .await
            .map(|signature| hex::encode(signature.serialize_der()))
            .map_err(|e| e.to_string())
    }

    pub async fn verify_lightning_send(
        &self,
        owner: &str,
        outbound_transfer_id: &str,
        payment_hash: &str,
        amount_sats: u64,
    ) -> Result<(), String> {
        let request = self
            .find_htlc(outbound_transfer_id, payment_hash)
            .await?
            .ok_or_else(|| "matching preimage swap was not found".to_string())?;
        let transfer = request
            .transfer
            .ok_or_else(|| "preimage swap has no transfer".to_string())?;
        if transfer.sender_identity_public_key.to_string() != owner.to_lowercase() {
            return Err("preimage swap sender does not match session owner".to_string());
        }
        if transfer.receiver_identity_public_key != self.identity {
            return Err("preimage swap receiver does not match the SSP".to_string());
        }
        if transfer.total_value != amount_sats {
            return Err(format!(
                "preimage swap has {} sats; expected {amount_sats}",
                transfer.total_value
            ));
        }
        if request.status != PreimageRequestStatus::WaitingForPreimage || request.preimage.is_some()
        {
            return Err("preimage swap is not waiting for payment".to_string());
        }
        Ok(())
    }

    /// Verify an unconditional Spark transfer that prepays a BOLT12 send.
    /// BOLT12 offers do not expose a payment hash before the invoice-request
    /// exchange, so they cannot use the hash-locked BOLT11 funding path.
    pub async fn verify_bolt12_send(
        &self,
        owner: &str,
        outbound_transfer_id: &str,
        amount_sats: u64,
    ) -> Result<(), String> {
        let transfer_id = TransferId::from_str(outbound_transfer_id)?;
        let transfer = self.wait_for_completed_transfer(&transfer_id).await?;
        let owner_key = spark_wallet::PublicKey::from_str(owner).map_err(|e| e.to_string())?;
        validate_transfer(&transfer, owner_key, self.identity, amount_sats)?;
        if transfer.transfer_type != TransferType::Transfer {
            return Err("BOLT12 funding must be a standard Spark transfer".to_string());
        }
        Ok(())
    }

    /// Return prepaid BOLT12 funding after a terminal Lightning failure.
    /// The deterministic ID makes retries and reconciliation idempotent.
    pub async fn refund_bolt12_send(
        &self,
        owner: &str,
        outbound_transfer_id: &str,
        amount_sats: u64,
    ) -> Result<String, String> {
        let _guard = self.liquidity_lock.lock().await;
        self.wallet.sync().await.map_err(|e| e.to_string())?;
        let owner_key = spark_wallet::PublicKey::from_str(owner).map_err(|e| e.to_string())?;
        let receiver = SparkAddress::new(owner_key, self.network, None);
        let refund_id = counter_transfer_id(&format!("bolt12-refund:{outbound_transfer_id}"));
        let transfer = match self.find_transfer(&refund_id).await? {
            Some(transfer) => transfer,
            None => self
                .wallet
                .transfer(amount_sats, &receiver, Some(refund_id.clone()))
                .await
                .map_err(|e| self.liquidity_error(e.to_string()))?,
        };
        validate_transfer(&transfer, self.identity, owner_key, amount_sats)?;
        self.needs_topup.store(false, Ordering::Relaxed);
        Ok(transfer.id.to_string())
    }

    pub async fn settle_lightning_send(
        &self,
        outbound_transfer_id: &str,
        payment_hash: &str,
        preimage_hex: &str,
    ) -> Result<(), String> {
        let preimage = Preimage::from_hex(preimage_hex).map_err(|e| e.to_string())?;
        if preimage.compute_hash().to_string() != payment_hash.to_lowercase() {
            return Err("preimage does not match the payment hash".to_string());
        }
        let request = self
            .find_htlc(outbound_transfer_id, payment_hash)
            .await?
            .ok_or_else(|| "matching preimage swap was not found".to_string())?;
        if request.status == PreimageRequestStatus::PreimageShared
            && request.preimage.is_some()
            && request
                .transfer
                .as_ref()
                .is_some_and(|transfer| funding_transfer_committed(&transfer.status))
        {
            return Ok(());
        }
        if request.status == PreimageRequestStatus::Returned {
            return Err("preimage swap can no longer be settled".to_string());
        }
        let transfer = self
            .wallet
            .claim_htlc(&preimage)
            .await
            .map_err(|e| e.to_string())?;
        if transfer.id.to_string() != outbound_transfer_id {
            return Err("settled transfer id does not match the funded transfer".to_string());
        }
        if !funding_transfer_committed(&transfer.status) {
            return Err("sender funding has not committed".to_string());
        }
        Ok(())
    }

    async fn receive_transfer_id(&self, hash: &str) -> Result<TransferId, String> {
        let quoted: Option<String> = self
            .db
            .with(|c| {
                use rusqlite::OptionalExtension;
                c.query_row(
                    "SELECT quote_id FROM receive_quote_uses WHERE payment_hash=?1",
                    [hash],
                    |r| r.get(0),
                )
                .optional()
            })
            .await?;
        match quoted {
            Some(id) => id
                .parse()
                .map_err(|e| format!("invalid quoted transfer ID: {e}")),
            None => payment_transfer_id(hash),
        }
    }

    pub async fn settle_lightning_receive(
        &self,
        owner: &str,
        payment_hash: &str,
        amount_sats: u64,
    ) -> Result<String, String> {
        let _guard = self.liquidity_lock.lock().await;
        self.wallet.sync().await.map_err(|e| e.to_string())?;
        let transfer_id = self.receive_transfer_id(payment_hash).await?;
        let owner_key = spark_wallet::PublicKey::from_str(owner).map_err(|e| e.to_string())?;
        let receiver = SparkAddress::new(owner_key, self.network, None);
        let transfer = match self.find_transfer(&transfer_id).await? {
            Some(transfer) => transfer,
            None => {
                self.ensure_exact_liquidity(amount_sats).await?;
                self.wallet
                    .transfer(amount_sats, &receiver, Some(transfer_id.clone()))
                    .await
                    .map_err(|e| self.liquidity_error(e.to_string()))?
            }
        };
        validate_transfer(&transfer, self.identity, owner_key, amount_sats)?;
        self.needs_topup.store(false, Ordering::Relaxed);
        Ok(transfer.id.to_string())
    }

    /// Atomically transfer SSP leaves and redeem the wallet's operator-held
    /// preimage shares through InitiatePreimageSwapV3(REASON_RECEIVE).
    pub async fn swap_for_lightning_receive(
        &self,
        owner: &str,
        payment_hash: &str,
        invoice: &str,
        amount_sats: u64,
        fee_sats: u64,
    ) -> Result<LightningReceiveSwap, String> {
        if fee_sats != 0 {
            return Err("Spark receive swaps do not support a fee".to_string());
        }
        let payment_hash_bytes =
            hex::decode(payment_hash).map_err(|e| format!("decode Lightning payment hash: {e}"))?;
        let payment_hash = sha256::Hash::from_slice(&payment_hash_bytes)
            .map_err(|e| format!("Lightning payment hash: {e}"))?;
        let owner_key = spark_wallet::PublicKey::from_str(owner).map_err(|e| e.to_string())?;
        let transfer_id = self.receive_transfer_id(&payment_hash.to_string()).await?;

        let _guard = self.liquidity_lock.lock().await;
        self.wallet.sync().await.map_err(|e| e.to_string())?;
        if let Some(recovered) = self
            .recover_lightning_receive_swap(&transfer_id, &payment_hash, owner_key, amount_sats)
            .await?
        {
            return Ok(recovered);
        }
        self.ensure_exact_liquidity(amount_sats).await?;
        let leaves = self.wallet.list_leaves().await.map_err(|e| e.to_string())?;
        let mut available = leaves
            .available
            .into_iter()
            .map(wallet_leaf_to_tree_node)
            .collect::<Result<Vec<_>, _>>()?;
        available.sort_by(|a, b| a.value.cmp(&b.value).then_with(|| a.id.cmp(&b.id)));
        let selected = select_receive_leaves(&available, amount_sats)
            .map_err(|e| self.liquidity_error(e.to_string()))?;
        let selected_total_sats = selected.iter().try_fold(0u64, |sum, leaf| {
            sum.checked_add(leaf.value)
                .ok_or_else(|| "selected receive leaf total overflow".to_string())
        })?;
        tracing::info!(
            payment_hash = %payment_hash,
            invoice_amount_sats = amount_sats,
            transfer_total_sats = selected_total_sats,
            leaf_count = selected.len(),
            "selected Spark liquidity for Lightning receive"
        );
        let leaf_tweaks = selected
            .into_iter()
            .map(|node| LeafKeyTweak {
                node,
                incoming_key: None,
            })
            .collect::<Vec<_>>();
        let expiry = std::time::SystemTime::now() + Duration::from_secs(30 * 60);
        let prepared = self
            .transfer_service
            .prepare_transfer_request(
                &transfer_id,
                &leaf_tweaks,
                &owner_key,
                // A receive commits a normal SSP-to-wallet transfer. The
                // payment hash belongs to the enclosing preimage swap. If it
                // is also set here, the SDK creates HTLC refund transactions,
                // which do not match the operators' normal transfer ladder.
                None,
                Some(expiry),
                None,
            )
            .await
            .map_err(|e| e.to_string())?;

        let response = self
            .operator_pool
            .get_coordinator()
            .client
            .initiate_preimage_swap_v3(build_lightning_receive_swap_request(
                &payment_hash,
                invoice,
                amount_sats,
                owner_key,
                fee_sats,
                prepared.transfer_request,
            ))
            .await
            .map_err(|e| format!("InitiatePreimageSwapV3(REASON_RECEIVE): {e}"))?;

        let preimage = Preimage::try_from(response.preimage)
            .map_err(|_| "operator response did not contain a 32-byte preimage".to_string())?;
        let transfer: SparkTransfer = response
            .transfer
            .ok_or_else(|| "operator receive swap did not return a transfer".to_string())?
            .try_into()
            .map_err(|e: ::spark::services::ServiceError| e.to_string())?;
        let result = validate_lightning_receive_swap(
            transfer,
            preimage,
            &payment_hash,
            &transfer_id,
            self.identity,
            owner_key,
            amount_sats,
        )?;
        // The direct operator call bypasses the wallet's local transfer cache.
        // Keep the committed result even if refreshing the cache is unavailable.
        if let Err(error) = self.wallet.sync().await {
            tracing::warn!(%error, "receive committed; Spark balance refresh is pending");
        }
        self.needs_topup.store(false, Ordering::Relaxed);
        Ok(result)
    }

    async fn recover_lightning_receive_swap(
        &self,
        transfer_id: &TransferId,
        payment_hash: &sha256::Hash,
        receiver: spark_wallet::PublicKey,
        amount_sats: u64,
    ) -> Result<Option<LightningReceiveSwap>, String> {
        let result = self
            .wallet
            .query_htlc(
                vec![transfer_id.to_string()],
                vec![payment_hash.to_string()],
                None,
                PreimageRequestRole::Sender,
                None,
            )
            .await
            .map_err(|e| format!("recover receive swap: {e}"))?;
        let Some(request) = result.items.into_iter().find(|request| {
            request.payment_hash == *payment_hash
                && request
                    .transfer
                    .as_ref()
                    .is_some_and(|transfer| transfer.id == *transfer_id)
        }) else {
            return Ok(None);
        };
        let Some(preimage) = request.preimage else {
            return Ok(None);
        };
        let transfer = request
            .transfer
            .ok_or_else(|| "recovered receive swap has no transfer".to_string())?;
        validate_lightning_receive_swap(
            transfer,
            preimage,
            payment_hash,
            transfer_id,
            self.identity,
            receiver,
            amount_sats,
        )
        .map(Some)
    }

    pub async fn fill_swap(
        self: &Arc<Self>,
        owner: &str,
        outbound_transfer_id: &str,
        adaptor_pubkey: &str,
        targets: &[u64],
        received_total_sats: u64,
        payout_total_sats: u64,
    ) -> Result<SwapFill, String> {
        if targets.is_empty() || targets.contains(&0) {
            return Err("swap targets must be positive".to_string());
        }
        if payout_total_sats != received_total_sats {
            return Err("Swap V3 fees are not supported by the operator protocol".to_string());
        }
        let target_total = targets.iter().try_fold(0u64, |sum, value| {
            sum.checked_add(*value)
                .ok_or_else(|| "swap target total overflow".to_string())
        })?;
        if target_total > payout_total_sats {
            return Err("swap targets exceed the payout".to_string());
        }

        // Resolve and validate the funding transfer before taking the
        // liquidity lock: a missing or underfunded caller-supplied id must
        // not occupy the lock (wait_for_transfer polls for up to 15 s) that
        // both Lightning receive payout paths also need. The transfer is
        // re-fetched and revalidated under the lock before anything moves.
        let primary_id = TransferId::from_str(outbound_transfer_id)?;
        let primary = self.wait_for_transfer(&primary_id).await?;
        let owner_key = spark_wallet::PublicKey::from_str(owner).map_err(|e| e.to_string())?;
        validate_transfer(&primary, owner_key, self.identity, received_total_sats)?;
        validate_swap_primary_claimable(&primary.status)?;

        let _guard = self.liquidity_lock.lock().await;
        let primary = self
            .find_transfer(&primary_id)
            .await?
            .ok_or_else(|| "outbound swap transfer was not found for the SSP".to_string())?;
        validate_transfer(&primary, owner_key, self.identity, received_total_sats)?;

        // Dead primaries are rejected on the creation path only: an
        // idempotent retry whose counter transfer already exists keeps
        // working regardless of the primary's final state. Rejecting before
        // the wallet sync and counter RPC keeps expired or returned
        // caller-supplied ids from occupying the liquidity lock.
        let counter_id = counter_transfer_id(outbound_transfer_id);
        let counter = match self.find_transfer(&counter_id).await? {
            Some(counter) => counter,
            None => {
                validate_swap_primary_claimable(&primary.status)?;
                self.create_counter_transfer(
                    &primary_id,
                    owner_key,
                    adaptor_pubkey,
                    targets,
                    received_total_sats,
                    target_total,
                    counter_id,
                )
                .await?
            }
        };
        validate_transfer(&counter, self.identity, owner_key, received_total_sats)?;
        let leaves = counter
            .leaves
            .iter()
            .map(|leaf| swap_leaf_response(&leaf.leaf.id.to_string(), &leaf.intermediate_refund_tx))
            .collect::<Result<Vec<_>, _>>()?;
        if leaves.is_empty() {
            return Err("counter transfer has no leaves".to_string());
        }
        self.needs_topup.store(false, Ordering::Relaxed);
        let service = self.clone();
        tokio::spawn(async move { service.reconcile_swap_claim(primary_id).await });
        Ok(SwapFill {
            transfer_id: counter.id.to_string(),
            leaves,
            expires_at: counter
                .expiry_time
                .map(|time| chrono::DateTime::<chrono::Utc>::from(time).to_rfc3339()),
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_counter_transfer(
        &self,
        primary_id: &TransferId,
        owner_key: spark_wallet::PublicKey,
        adaptor_pubkey: &str,
        targets: &[u64],
        received_total_sats: u64,
        target_total: u64,
        counter_id: TransferId,
    ) -> Result<WalletTransfer, String> {
        self.wallet.sync().await.map_err(|e| e.to_string())?;
        let adaptor =
            spark_wallet::PublicKey::from_str(adaptor_pubkey).map_err(|e| e.to_string())?;
        let receiver = SparkAddress::new(owner_key, self.network, None);
        let mut amounts = targets.to_vec();
        let change = received_total_sats - target_total;
        if change > 0 {
            amounts.push(change);
        }
        self.ensure_denominated_liquidity(&amounts).await?;
        self.wallet
            .transfer_swap_counter(amounts, &receiver, primary_id.clone(), adaptor, counter_id)
            .await
            .map_err(|e| self.liquidity_error(e.to_string()))
    }

    async fn ensure_exact_liquidity(&self, amount_sats: u64) -> Result<(), String> {
        for _ in 0..32 {
            let leaves = self.wallet.list_leaves().await.map_err(|e| e.to_string())?;
            let mut available = leaves
                .available
                .into_iter()
                .map(wallet_leaf_to_tree_node)
                .collect::<Result<Vec<_>, _>>()?;
            available.sort_by(|a, b| a.value.cmp(&b.value).then_with(|| a.id.cmp(&b.id)));
            if select_receive_leaves(&available, amount_sats).is_ok() {
                return Ok(());
            }
            let plan = plan_just_in_time_split(
                &available,
                amount_sats,
                self.minimum_split_child_sats,
            )
            .ok_or_else(|| self.liquidity_error("amount cannot be represented by available leaves without creating a child below the configured split floor".to_string()))?;
            self.execute_leaf_split(&plan.parent, vec![plan.needed_sats, plan.change_sats])
                .await?;
            self.wallet.sync().await.map_err(|e| e.to_string())?;
        }
        Err("leaf split reconciliation exceeded its progress limit".to_string())
    }

    async fn ensure_denominated_liquidity(&self, amounts: &[u64]) -> Result<(), String> {
        for _ in 0..32 {
            let leaves = self.wallet.list_leaves().await.map_err(|e| e.to_string())?;
            let mut available = leaves
                .available
                .into_iter()
                .map(wallet_leaf_to_tree_node)
                .collect::<Result<Vec<_>, _>>()?;
            available.sort_by(|a, b| a.value.cmp(&b.value).then_with(|| a.id.cmp(&b.id)));
            if select_leaves_by_exact_amounts(&available, amounts).is_ok() {
                return Ok(());
            }
            let (parent, needed_sats, change_sats) = plan_denomination_split(
                &available,
                amounts,
                self.minimum_split_child_sats,
            )
            .ok_or_else(|| self.liquidity_error("swap denominations cannot be produced without creating a child below the configured split floor".to_string()))?;
            self.execute_leaf_split(&parent, vec![needed_sats, change_sats])
                .await?;
            self.wallet.sync().await.map_err(|e| e.to_string())?;
        }
        Err("denomination split reconciliation exceeded its progress limit".to_string())
    }

    async fn execute_leaf_split(
        &self,
        parent: &TreeNode,
        child_values: Vec<u64>,
    ) -> Result<(), String> {
        let service = self.split_service.as_ref().ok_or_else(|| {
            self.liquidity_error(
                "just-in-time splitting is disabled; configure SSP_OPERATOR_HOSTS".to_string(),
            )
        })?;
        let operation_id = split_operation_id(&parent.id);
        let operation = match self
            .db
            .spark_split_for_parent(&parent.id.to_string())
            .await?
        {
            Some(operation) => operation,
            None => {
                let draft = service
                    .draft_split(operation_id.clone(), parent, child_values.clone())
                    .await
                    .map_err(|e| e.to_string())?;
                self.db
                    .get_or_insert_spark_split(&SparkSplitOperation {
                        operation_id: operation_id.clone(),
                        parent_node_id: parent.id.to_string(),
                        parent_value_sats: parent.value,
                        child_values_sats: child_values.clone(),
                        plan: serde_json::to_vec(&draft).map_err(|e| e.to_string())?,
                        status: "DRAFT".to_string(),
                        child_node_ids: Vec::new(),
                        last_error: None,
                    })
                    .await?
            }
        };
        if operation.parent_node_id != parent.id.to_string()
            || operation.parent_value_sats != parent.value
            || operation.child_values_sats != child_values
        {
            return Err(
                "persisted split does not match the requested parent and values".to_string(),
            );
        }
        self.resume_leaf_split(operation, Some(parent)).await
    }

    async fn resume_leaf_split(
        &self,
        mut operation: SparkSplitOperation,
        parent: Option<&TreeNode>,
    ) -> Result<(), String> {
        let service = self
            .split_service
            .as_ref()
            .ok_or_else(|| "just-in-time splitting is disabled".to_string())?;
        let operation_id = operation.operation_id.clone();
        let result = async {
            loop {
                match operation.status.as_str() {
                    "DRAFT" => {
                        let parent = parent.ok_or_else(|| {
                            "cannot resume a draft split because its parent is unavailable"
                                .to_string()
                        })?;
                        let draft: LeafSplitDraft =
                            serde_json::from_slice(&operation.plan).map_err(|e| e.to_string())?;
                        let plan = service
                            .prepare_split(&draft, parent)
                            .await
                            .map_err(|e| e.to_string())?;
                        let encoded = serde_json::to_vec(&plan).map_err(|e| e.to_string())?;
                        self.db
                            .save_prepared_spark_split(&operation.operation_id, &encoded)
                            .await?;
                    }
                    "PREPARED" | "SUBMITTING" => {
                        let plan: LeafSplitPlan =
                            serde_json::from_slice(&operation.plan).map_err(|e| e.to_string())?;
                        self.db
                            .mark_spark_split_submitting(&operation.operation_id)
                            .await?;
                        let submitted = service
                            .submit_split(plan)
                            .await
                            .map_err(|e| e.to_string())?;
                        let encoded = serde_json::to_vec(&submitted).map_err(|e| e.to_string())?;
                        self.db
                            .save_submitted_spark_split(&operation.operation_id, &encoded)
                            .await?;
                    }
                    "SUBMITTED" => {
                        let submitted: SubmittedLeafSplit =
                            serde_json::from_slice(&operation.plan).map_err(|e| e.to_string())?;
                        let split = service
                            .finalize_split(&submitted)
                            .await
                            .map_err(|e| e.to_string())?;
                        if split.children.len() != operation.child_values_sats.len() {
                            return Err(
                                "finalized split returned an unexpected child count".to_string()
                            );
                        }
                        self.db
                            .mark_spark_split_completed(&operation.operation_id)
                            .await?;
                    }
                    "COMPLETED" => return Ok(()),
                    status => return Err(format!("unknown Spark split status {status}")),
                }
                operation = self
                    .db
                    .spark_split_for_parent(&operation.parent_node_id)
                    .await?
                    .ok_or_else(|| "Spark split checkpoint disappeared".to_string())?;
            }
        }
        .await;
        if let Err(error) = &result {
            let _ = self.db.record_spark_split_error(&operation_id, error).await;
        }
        result
    }

    async fn recover_incomplete_splits(&self) -> Result<(), String> {
        if self.split_service.is_none() {
            return Ok(());
        }
        let operations = self.db.incomplete_spark_splits().await?;
        if operations.is_empty() {
            return Ok(());
        }
        self.wallet.sync().await.map_err(|e| e.to_string())?;
        let leaves = self.wallet.list_leaves().await.map_err(|e| e.to_string())?;
        let available = leaves
            .available
            .into_iter()
            .chain(leaves.available_missing_from_operators)
            .map(wallet_leaf_to_tree_node)
            .collect::<Result<Vec<_>, _>>()?;
        for operation in operations {
            let parent = available
                .iter()
                .find(|leaf| leaf.id.to_string() == operation.parent_node_id);
            if operation.status == "DRAFT" && parent.is_none() {
                tracing::warn!(
                    operation_id = %operation.operation_id,
                    parent_node_id = %operation.parent_node_id,
                    "cannot resume draft Spark split until its parent is available"
                );
                continue;
            }
            self.resume_leaf_split(operation, parent).await?;
        }
        self.wallet.sync().await.map_err(|e| e.to_string())
    }

    async fn find_htlc(
        &self,
        transfer_id: &str,
        payment_hash: &str,
    ) -> Result<Option<spark_wallet::PreimageRequestWithTransfer>, String> {
        let result = self
            .wallet
            .query_htlc(
                vec![transfer_id.to_string()],
                vec![payment_hash.to_lowercase()],
                None,
                PreimageRequestRole::Receiver,
                None,
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(result.items.into_iter().find(|request| {
            request.payment_hash.to_string() == payment_hash.to_lowercase()
                && request
                    .transfer
                    .as_ref()
                    .is_some_and(|transfer| transfer.id.to_string() == transfer_id)
        }))
    }

    async fn wait_for_transfer(&self, id: &TransferId) -> Result<WalletTransfer, String> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            match self.find_transfer(id).await {
                Ok(Some(transfer)) => return Ok(transfer),
                Ok(None) => {}
                Err(error) => tracing::debug!(%error, %id, "Spark transfer lookup retry"),
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("outbound swap transfer was not found for the SSP".to_string());
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    async fn wait_for_completed_transfer(&self, id: &TransferId) -> Result<WalletTransfer, String> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            if let Err(error) = self.wallet.sync().await {
                tracing::debug!(%error, %id, "Spark wallet sync retry");
            }
            match self.find_transfer(id).await {
                Ok(Some(transfer)) if transfer.status == TransferStatus::Completed => {
                    return Ok(transfer);
                }
                Ok(Some(_)) | Ok(None) => {}
                Err(error) => tracing::debug!(%error, %id, "Spark transfer lookup retry"),
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("BOLT12 funding transfer is not complete".to_string());
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    async fn find_transfer(&self, id: &TransferId) -> Result<Option<WalletTransfer>, String> {
        self.wallet
            .get_transfer(id)
            .await
            .map_err(|e| e.to_string())
    }

    /// Recover historical response data without creating another transfer.
    pub async fn swap_details(&self, id: &str) -> Result<SwapFill, String> {
        let transfer = self
            .find_transfer(&id.parse()?)
            .await?
            .ok_or("swap counter transfer is unavailable")?;
        let leaves = transfer
            .leaves
            .iter()
            .map(|leaf| swap_leaf_response(&leaf.leaf.id.to_string(), &leaf.intermediate_refund_tx))
            .collect::<Result<Vec<_>, _>>()?;
        if leaves.is_empty() {
            return Err("swap counter transfer has no refund data".into());
        }
        Ok(SwapFill {
            transfer_id: transfer.id.to_string(),
            leaves,
            expires_at: transfer
                .expiry_time
                .map(|time| chrono::DateTime::<chrono::Utc>::from(time).to_rfc3339()),
        })
    }

    pub async fn run_swap_history(self: Arc<Self>) {
        loop {
            let pending = self.db.with(|c| {
                let mut statement = c.prepare("SELECT id,json_extract(payload,'$.outbound_transfer_spark_id') FROM requests WHERE kind='LEAVES_SWAP' AND json_extract(payload,'$.outbound_transfer_spark_id') IS NOT NULL AND COALESCE(json_extract(payload,'$.status'),'')!='SUCCEEDED' LIMIT 1000")?;
                let rows = statement.query_map([],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?)))?.collect::<rusqlite::Result<Vec<_>>>();
                rows
            }).await;
            match pending {
                Ok(pending) => {
                    for (id, primary) in pending {
                        let Ok(primary) = primary.parse() else {
                            continue;
                        };
                        match self.find_transfer(&primary).await {
                        Ok(Some(transfer)) if transfer.status == TransferStatus::Completed => {
                            if let Err(error) = self.db.with(|c|c.execute("UPDATE requests SET payload=json_set(payload,'$.status','SUCCEEDED') WHERE id=?1",[&id]).map(|_|())).await {
                                tracing::warn!(%id,%error,"swap history persistence failed");
                            }
                        },
                        Ok(Some(transfer)) if matches!(transfer.status,TransferStatus::SenderKeyTweaked | TransferStatus::ReceiverKeyTweaked | TransferStatus::ReceiverRefundSigned) => {
                            if let Err(error) = self.wallet.process_transfer(transfer).await { tracing::debug!(%id,%error,"swap claim will retry"); }
                        },
                        Ok(_) => {},
                        Err(error) => tracing::debug!(%id,%error,"swap history lookup will retry"),
                    }
                    }
                }
                Err(error) => tracing::warn!(%error,"swap history query failed"),
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    async fn reconcile_swap_claim(self: Arc<Self>, primary_id: TransferId) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(300);
        let mut delay = Duration::from_millis(250);
        while tokio::time::Instant::now() < deadline {
            match self.find_transfer(&primary_id).await {
                Ok(Some(transfer)) if transfer.status == TransferStatus::Completed => return,
                Ok(Some(transfer))
                    if matches!(
                        transfer.status,
                        TransferStatus::SenderKeyTweaked
                            | TransferStatus::ReceiverKeyTweaked
                            | TransferStatus::ReceiverRefundSigned
                    ) =>
                {
                    match self.wallet.process_transfer(transfer).await {
                        Ok(_) => return,
                        Err(error) => tracing::warn!(%error, %primary_id, "swap claim retry"),
                    }
                }
                Ok(_) => {}
                Err(error) => tracing::warn!(%error, %primary_id, "swap lookup retry"),
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(5));
        }
        tracing::error!(%primary_id, "swap claim reconciliation timed out");
    }

    fn liquidity_error(&self, error: String) -> String {
        if error.contains("available balance")
            || error.contains("Insufficient")
            || error.contains("Target amounts")
        {
            self.needs_topup.store(true, Ordering::Relaxed);
            format!("NEEDS_TOPUP: {error}")
        } else {
            error
        }
    }
}

fn validate_coop_exit_transfer(
    transfer: &SparkTransfer,
    record: &crate::coop_exit::ExitRecord,
    identity: &spark_wallet::PublicKey,
) -> Result<(), String> {
    let sender =
        spark_wallet::PublicKey::from_str(&record.owner).map_err(|error| error.to_string())?;
    if transfer.id.to_string() != record.transfer_id
        || transfer.sender_identity_public_key != sender
        || transfer.receiver_identity_public_key != *identity
        || transfer.transfer_type != TransferType::CooperativeExit
    {
        return Err("withdrawal transfer identity or type mismatch".into());
    }
    // The committed operator flow keeps these leaves locked until the exact
    // exit transaction confirms. Earlier or returned transfers cannot fund a payout.
    if transfer.status != TransferStatus::SenderKeyTweakPending {
        return Err(format!(
            "withdrawal transfer is not locked: {:?}",
            transfer.status
        ));
    }
    let value = record
        .leaves
        .iter()
        .try_fold(0u64, |sum, leaf| sum.checked_add(leaf.value))
        .ok_or("withdrawal value overflow")?;
    if transfer.total_value != value || transfer.leaves.len() != record.leaves.len() {
        return Err("withdrawal transfer value or leaf count mismatch".into());
    }
    let connector: Transaction =
        deserialize(&hex::decode(&record.raw_connector).map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?;
    let connector_id = connector.compute_txid();
    let mut used_outputs = std::collections::HashSet::new();
    for expected in &record.leaves {
        let mut matches = transfer
            .leaves
            .iter()
            .filter(|leaf| leaf.leaf.id.to_string() == expected.id);
        let leaf = matches
            .next()
            .ok_or("withdrawal transfer has different leaves")?;
        if matches.next().is_some() || leaf.leaf.value != expected.value {
            return Err("withdrawal transfer has duplicate or changed leaves".into());
        }
        let connector_output = leaf
            .intermediate_refund_tx
            .input
            .get(1)
            .ok_or("withdrawal refund has no connector input")?
            .previous_output;
        if connector_output.txid != connector_id
            || connector_output.vout as usize >= record.leaves.len()
            || !used_outputs.insert(connector_output)
        {
            return Err(
                "withdrawal connector outputs must be distinct and cover the leaves".into(),
            );
        }
        for refund in [
            Some(&leaf.intermediate_refund_tx),
            leaf.intermediate_direct_refund_tx.as_ref(),
            leaf.intermediate_direct_from_cpfp_refund_tx.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            if refund.input.len() != 2 || refund.input[1].previous_output != connector_output {
                return Err("withdrawal refund does not bind the expected connector output".into());
            }
        }
        if leaf.intermediate_direct_from_cpfp_refund_tx.is_none() {
            return Err("withdrawal transfer is missing its direct refund".into());
        }
    }
    Ok(())
}

fn funding_transfer_committed(status: &TransferStatus) -> bool {
    matches!(
        status,
        TransferStatus::SenderKeyTweaked
            | TransferStatus::ReceiverKeyTweaked
            | TransferStatus::ReceiverKeyTweakLocked
            | TransferStatus::ReceiverKeyTweakApplied
            | TransferStatus::ReceiverRefundSigned
            | TransferStatus::Completed
    )
}

fn build_lightning_receive_swap_request(
    payment_hash: &sha256::Hash,
    invoice: &str,
    amount_sats: u64,
    receiver: spark_wallet::PublicKey,
    fee_sats: u64,
    transfer_request: StartTransferRequest,
) -> InitiatePreimageSwapRequest {
    InitiatePreimageSwapRequest {
        payment_hash: payment_hash.to_byte_array().to_vec(),
        invoice_amount: Some(InvoiceAmount {
            value_sats: amount_sats,
            invoice_amount_proof: Some(InvoiceAmountProof {
                bolt11_invoice: invoice.to_string(),
            }),
        }),
        reason: PreimageSwapReason::Receive as i32,
        transfer: None,
        receiver_identity_public_key: receiver.serialize().to_vec(),
        fee_sats,
        transfer_request: Some(transfer_request),
    }
}

fn validate_lightning_receive_swap(
    transfer: SparkTransfer,
    preimage: Preimage,
    payment_hash: &sha256::Hash,
    transfer_id: &TransferId,
    sender: spark_wallet::PublicKey,
    receiver: spark_wallet::PublicKey,
    invoice_amount_sats: u64,
) -> Result<LightningReceiveSwap, String> {
    if preimage.compute_hash() != *payment_hash {
        return Err("operator preimage does not match the Lightning payment hash".to_string());
    }
    if transfer.id != *transfer_id
        || transfer.sender_identity_public_key != sender
        || transfer.receiver_identity_public_key != receiver
        || transfer.transfer_type != TransferType::PreimageSwap
    {
        return Err("operator receive swap returned a mismatched transfer".to_string());
    }
    // Value conservation must be exact: a transfer above the invoice amount
    // pays the wallet SSP-owned sats that Lightning never collected.
    if transfer.total_value != invoice_amount_sats {
        return Err(format!(
            "operator receive swap transferred {} sats; invoice requires exactly {invoice_amount_sats}",
            transfer.total_value
        ));
    }
    Ok(LightningReceiveSwap {
        transfer_id: transfer.id.to_string(),
        preimage: preimage.encode_hex(),
    })
}

/// Select exactly the invoice amount. A covering whole-leaf set would
/// overpay the wallet with SSP-owned sats, so amounts the ladder cannot
/// represent are rejected (the receive then fails its hold invoice and the
/// payer is refunded).
fn select_receive_leaves<L: LeafLike>(
    leaves: &[L],
    amount_sats: u64,
) -> Result<Vec<L>, TreeServiceError> {
    select_leaves_by_exact_amounts(leaves, &[amount_sats])
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct JustInTimeSplit<L> {
    /// Whole leaves used alongside the newly-created `needed` child.
    selected: Vec<L>,
    parent: L,
    needed_sats: u64,
    change_sats: u64,
}

/// Find one leaf to split after exact whole-leaf selection fails. Existing
/// leaves may cover part of the target; the parent becomes `[needed, change]`.
/// Both children honor the configured local floor. Operators accept any
/// positive amount, but deployments that require standalone L1 relayability
/// should configure their Bitcoin dust floor here.
fn plan_just_in_time_split<L: LeafLike + Clone>(
    leaves: &[L],
    target_sats: u64,
    minimum_child_sats: u64,
) -> Option<JustInTimeSplit<L>> {
    if target_sats == 0 || minimum_child_sats == 0 {
        return None;
    }

    let mut best: Option<JustInTimeSplit<L>> = None;
    for (parent_index, parent) in leaves.iter().enumerate() {
        let mut candidates = leaves
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != parent_index)
            .map(|(_, leaf)| leaf.clone())
            .collect::<Vec<_>>();
        candidates.sort_by_key(|leaf| std::cmp::Reverse(leaf.leaf_value()));

        let maximum_existing = target_sats.saturating_sub(minimum_child_sats);
        let mut selected = Vec::new();
        let mut selected_sats = 0u64;
        for candidate in candidates {
            let Some(next) = selected_sats.checked_add(candidate.leaf_value()) else {
                continue;
            };
            if next <= maximum_existing {
                selected.push(candidate);
                selected_sats = next;
            }
        }

        let needed_sats = target_sats - selected_sats;
        let Some(change_sats) = parent.leaf_value().checked_sub(needed_sats) else {
            continue;
        };
        if needed_sats < minimum_child_sats || change_sats < minimum_child_sats {
            continue;
        }
        let plan = JustInTimeSplit {
            selected,
            parent: parent.clone(),
            needed_sats,
            change_sats,
        };
        let score = (
            plan.change_sats,
            plan.selected.len(),
            plan.parent.leaf_value(),
        );
        if best.as_ref().is_none_or(|current| {
            score
                < (
                    current.change_sats,
                    current.selected.len(),
                    current.parent.leaf_value(),
                )
        }) {
            best = Some(plan);
        }
    }
    best
}

fn plan_denomination_split<L: LeafLike + Clone>(
    leaves: &[L],
    denominations: &[u64],
    minimum_child_sats: u64,
) -> Option<(L, u64, u64)> {
    if denominations.is_empty() || denominations.contains(&0) || minimum_child_sats == 0 {
        return None;
    }
    let mut remaining = leaves.to_vec();
    let mut missing = None;
    for denomination in denominations {
        if let Some(index) = remaining
            .iter()
            .position(|leaf| leaf.leaf_value() == *denomination)
        {
            remaining.remove(index);
        } else {
            missing = Some(*denomination);
            break;
        }
    }
    let missing = missing?;
    if missing < minimum_child_sats {
        return None;
    }
    remaining
        .into_iter()
        .filter_map(|parent| {
            let change = parent.leaf_value().checked_sub(missing)?;
            (change >= minimum_child_sats).then_some((parent, missing, change))
        })
        .min_by_key(|(parent, _, change)| (*change, parent.leaf_value()))
}

fn wallet_leaf_to_tree_node(leaf: WalletLeaf) -> Result<TreeNode, String> {
    Ok(TreeNode {
        id: leaf.id,
        tree_id: leaf.tree_id,
        value: leaf.value,
        parent_node_id: leaf.parent_node_id,
        node_tx: leaf.node_tx,
        refund_tx: leaf.refund_tx,
        direct_tx: leaf.direct_tx,
        direct_refund_tx: leaf.direct_refund_tx,
        direct_from_cpfp_refund_tx: leaf.direct_from_cpfp_refund_tx,
        vout: leaf.vout,
        verifying_public_key: leaf.verifying_public_key,
        owner_identity_public_key: leaf.owner_identity_public_key,
        signing_keyshare: leaf
            .signing_keyshare
            .ok_or_else(|| "available Spark leaf has no signing keyshare".to_string())?,
        status: TreeNodeStatus::Available,
    })
}

fn validate_transfer(
    transfer: &WalletTransfer,
    sender: spark_wallet::PublicKey,
    receiver: spark_wallet::PublicKey,
    total_sats: u64,
) -> Result<(), String> {
    if transfer.sender_id != sender {
        return Err("transfer sender does not match".to_string());
    }
    if transfer.receiver_id != receiver {
        return Err("transfer receiver does not match".to_string());
    }
    if transfer.total_value_sat != total_sats {
        return Err(format!(
            "transfer has {} sats; expected {total_sats}",
            transfer.total_value_sat
        ));
    }
    Ok(())
}

fn csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_string)
        .collect()
}
/// A swap primary must still be able to fund the counter transfer. Expired
/// and returned transfers have already sent their value back to the sender,
/// so they can never fund a swap; rejecting them locally (before the wallet
/// sync and counter-transfer RPC) keeps dead caller-supplied ids out of the
/// liquidity lock. The coordinator's atomic validation remains the final
/// authority for every other state.
fn validate_swap_primary_claimable(status: &TransferStatus) -> Result<(), String> {
    match status {
        TransferStatus::Expired | TransferStatus::Returned => Err(format!(
            "outbound swap transfer is {status} and can no longer fund a swap"
        )),
        _ => Ok(()),
    }
}

fn parse_network(value: &str) -> Result<Network, String> {
    match value.to_ascii_uppercase().as_str() {
        "MAINNET" => Ok(Network::Mainnet),
        "TESTNET" => Ok(Network::Testnet),
        "SIGNET" => Ok(Network::Signet),
        "LOCAL" | "REGTEST" => Ok(Network::Regtest),
        _ => Err(format!("unsupported Spark network {value}")),
    }
}

fn load_or_create_mnemonic(path: &str, required: bool) -> Result<Mnemonic, String> {
    match std::fs::read_to_string(path) {
        Ok(value) => {
            // An existing mnemonic may have arrived through a copy or a
            // restore with wider permissions than creation used. Refuse to
            // continue if it cannot be secured.
            crate::fs::restrict_to_owner(Path::new(path))?;
            return Mnemonic::parse_in_normalized(Language::English, value.trim())
                .map_err(|e| format!("parse Spark mnemonic: {e}"));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && !required => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(format!(
                "Spark mnemonic {path} is required; restore the funded wallet mnemonic"
            ));
        }
        Err(error) => return Err(format!("read Spark mnemonic {path}: {error}")),
    }
    let mnemonic = Mnemonic::generate_in(Language::English, 12)
        .map_err(|e| format!("generate Spark mnemonic: {e}"))?;
    let path = Path::new(path);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("create mnemonic directory: {e}"))?;
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|e| format!("create Spark mnemonic {}: {e}", path.display()))?;
    writeln!(file, "{mnemonic}").map_err(|e| format!("write Spark mnemonic: {e}"))?;
    file.sync_all()
        .map_err(|e| format!("sync Spark mnemonic: {e}"))?;
    Ok(mnemonic)
}

fn payment_transfer_id(payment_hash: &str) -> Result<TransferId, String> {
    let bytes = hex::decode(payment_hash).map_err(|e| e.to_string())?;
    if bytes.len() != 32 {
        return Err("payment hash must be 32 bytes".to_string());
    }
    deterministic_transfer_id(&bytes[..16])
}

fn counter_transfer_id(primary_id: &str) -> TransferId {
    let digest = Sha256::digest(format!("swap-counter:{primary_id}").as_bytes());
    deterministic_transfer_id(&digest[..16]).expect("sha256 prefix is 16 bytes")
}

fn split_operation_id(parent_id: &::spark::tree::TreeNodeId) -> String {
    let digest = Sha256::digest(format!("leaf-split:{parent_id}").as_bytes());
    deterministic_transfer_id(&digest[..16])
        .expect("sha256 prefix is 16 bytes")
        .to_string()
}

fn deterministic_transfer_id(source: &[u8]) -> Result<TransferId, String> {
    let mut bytes: [u8; 16] = source
        .try_into()
        .map_err(|_| "transfer id source must be 16 bytes".to_string())?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Ok(TransferId::from_bytes(bytes))
}

// Both fields are accepted for older clients, but they cannot be combined.
fn decode_instant_deposit_key(
    identity: &bitcoin::secp256k1::SecretKey,
    raw: Option<&str>,
    encrypted: Option<&str>,
) -> Result<bitcoin::secp256k1::SecretKey, String> {
    let bytes = match (raw, encrypted) {
        (Some(raw), None) if raw.len() == 64 => {
            hex::decode(raw).map_err(|_| "invalid deposit key")?
        }
        (None, Some(encrypted)) if encrypted.len() == 258 => {
            let ciphertext = hex::decode(encrypted).map_err(|_| "invalid encrypted deposit key")?;
            utils::ecies::decrypt(&identity.secret_bytes(), &ciphertext)
                .map_err(|_| "cannot decrypt instant deposit key")?
        }
        _ => return Err("exactly one valid deposit key share required".into()),
    };
    bitcoin::secp256k1::SecretKey::from_slice(&bytes).map_err(|_| "invalid deposit key".into())
}

#[cfg(test)]
mod tests {
    #[test]
    fn instant_key_accepts_legacy_and_upstream_encryption() {
        use bitcoin::secp256k1::{PublicKey, Secp256k1, SecretKey};
        let identity = SecretKey::from_slice(&[1; 32]).unwrap();
        let key = SecretKey::from_slice(&[2; 32]).unwrap();
        let raw = hex::encode(key.secret_bytes());
        let ciphertext = hex::encode(
            utils::ecies::encrypt(
                &PublicKey::from_secret_key(&Secp256k1::new(), &identity).serialize(),
                &key.secret_bytes(),
            )
            .unwrap(),
        );
        assert_eq!(
            super::decode_instant_deposit_key(&identity, Some(&raw), None).unwrap(),
            key
        );
        assert_eq!(
            super::decode_instant_deposit_key(&identity, None, Some(&ciphertext)).unwrap(),
            key
        );
        assert!(
            super::decode_instant_deposit_key(&identity, Some(&raw), Some(&ciphertext)).is_err()
        );
        assert!(super::decode_instant_deposit_key(&identity, None, None).is_err());
        let wrong_identity = SecretKey::from_slice(&[3; 32]).unwrap();
        assert!(
            super::decode_instant_deposit_key(&wrong_identity, None, Some(&ciphertext)).is_err()
        );
        let mut corrupted = hex::decode(&ciphertext).unwrap();
        *corrupted.last_mut().unwrap() ^= 1;
        assert!(
            super::decode_instant_deposit_key(&identity, None, Some(&hex::encode(corrupted)))
                .is_err()
        );
    }

    use super::*;

    #[test]
    fn swap_response_preserves_operator_refund_and_signature() {
        let mut tx = Transaction {
            version: bitcoin::transaction::Version(3),
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![bitcoin::TxIn::default()],
            output: vec![bitcoin::TxOut {
                value: bitcoin::Amount::from_sat(1000),
                script_pubkey: bitcoin::ScriptBuf::new(),
            }],
        };
        let raw = |tx: &Transaction| hex::encode(bitcoin::consensus::serialize(tx));
        assert!(swap_leaf_response("leaf", &raw(&tx)).is_err());
        tx.input[0].witness.push([42; 64]);
        let result = swap_leaf_response("leaf", &raw(&tx)).unwrap();
        let mut restored: Transaction = deserialize(
            &hex::decode(result["raw_unsigned_refund_transaction"].as_str().unwrap()).unwrap(),
        )
        .unwrap();
        assert!(restored.input[0].witness.is_empty());
        restored.input[0]
            .witness
            .push(hex::decode(result["adaptor_signed_signature"].as_str().unwrap()).unwrap());
        assert_eq!(restored, tx);
        assert!(result["direct_adaptor_signed_signature"].is_null());
        assert!(swap_leaf_response("leaf", "not hex").is_err());
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct TestLeaf {
        id: u8,
        value: u64,
    }

    impl LeafLike for TestLeaf {
        type Id = u8;

        fn leaf_id(&self) -> &Self::Id {
            &self.id
        }

        fn leaf_value(&self) -> u64 {
            self.value
        }
    }

    #[test]
    fn maps_all_supported_networks() {
        assert_eq!(parse_network("MAINNET").unwrap(), Network::Mainnet);
        assert_eq!(parse_network("TESTNET").unwrap(), Network::Testnet);
        assert_eq!(parse_network("SIGNET").unwrap(), Network::Signet);
        assert_eq!(parse_network("LOCAL").unwrap(), Network::Regtest);
    }

    #[test]
    fn payment_ids_match_the_previous_sidecar_format() {
        let hash = "00112233445566778899aabbccddeeff00000000000000000000000000000000";
        assert_eq!(
            payment_transfer_id(hash).unwrap().to_string(),
            "00112233-4455-4677-8899-aabbccddeeff"
        );
    }

    #[test]
    fn receive_swap_request_uses_existing_receive_protocol() {
        let secp = bitcoin::secp256k1::Secp256k1::new();
        let secret = bitcoin::secp256k1::SecretKey::from_slice(&[3; 32]).unwrap();
        let receiver = bitcoin::secp256k1::PublicKey::from_secret_key(&secp, &secret);
        let payment_hash = sha256::Hash::hash(b"wallet preimage");
        let transfer_request = StartTransferRequest {
            transfer_id: "transfer".to_string(),
            ..Default::default()
        };

        let request = build_lightning_receive_swap_request(
            &payment_hash,
            "ln-invoice",
            5_000,
            receiver,
            0,
            transfer_request,
        );

        assert_eq!(request.reason, PreimageSwapReason::Receive as i32);
        assert_eq!(request.payment_hash, payment_hash.to_byte_array());
        assert_eq!(request.receiver_identity_public_key, receiver.serialize());
        assert!(request.transfer.is_none());
        assert_eq!(request.fee_sats, 0);
        let amount = request.invoice_amount.unwrap();
        assert_eq!(amount.value_sats, 5_000);
        assert_eq!(
            amount.invoice_amount_proof.unwrap().bolt11_invoice,
            "ln-invoice"
        );
        assert_eq!(request.transfer_request.unwrap().transfer_id, "transfer");
    }

    #[test]
    fn receive_leaf_selection_rejects_unrepresentable_amounts() {
        let leaves = [
            TestLeaf {
                id: 1,
                value: 1_000,
            },
            TestLeaf {
                id: 2,
                value: 2_000,
            },
            TestLeaf {
                id: 3,
                value: 4_000,
            },
        ];

        // A 68-sat invoice must never claim a whole 1,000-sat leaf.
        assert!(select_receive_leaves(&leaves, 68).is_err());
        // 1,500 sats cannot be composed from whole 1,000-sat leaves either.
        assert!(select_receive_leaves(&leaves, 1_500).is_err());
    }

    #[test]
    fn dead_swap_primaries_are_rejected() {
        assert!(validate_swap_primary_claimable(&TransferStatus::SenderInitiated).is_ok());
        assert!(validate_swap_primary_claimable(&TransferStatus::SenderKeyTweaked).is_ok());
        assert!(validate_swap_primary_claimable(&TransferStatus::Completed).is_ok());
        for dead in [TransferStatus::Expired, TransferStatus::Returned] {
            let error = validate_swap_primary_claimable(&dead).unwrap_err();
            assert!(error.contains("can no longer fund a swap"));
        }
    }

    #[test]
    fn receive_leaf_selection_prefers_an_exact_set() {
        let leaves = [
            TestLeaf {
                id: 1,
                value: 1_000,
            },
            TestLeaf {
                id: 2,
                value: 2_000,
            },
            TestLeaf {
                id: 3,
                value: 4_000,
            },
        ];

        let exact = select_receive_leaves(&leaves, 3_000).unwrap();
        assert_eq!(exact.iter().map(|leaf| leaf.value).sum::<u64>(), 3_000);
    }

    #[test]
    fn receive_leaf_selection_combines_two_leaves() {
        let equal_leaves = [
            TestLeaf {
                id: 4,
                value: 1_000,
            },
            TestLeaf {
                id: 5,
                value: 1_000,
            },
        ];
        let combined = select_receive_leaves(&equal_leaves, 2_000).unwrap();
        assert_eq!(combined, equal_leaves);
    }

    #[test]
    fn split_planner_uses_one_larger_leaf_for_needed_and_change() {
        let leaves = [
            TestLeaf {
                id: 1,
                value: 10_000,
            },
            TestLeaf {
                id: 2,
                value: 20_000,
            },
        ];
        let plan = plan_just_in_time_split(&leaves, 7_321, 1).unwrap();
        assert!(plan.selected.is_empty());
        assert_eq!(plan.parent.id, 1);
        assert_eq!(plan.needed_sats, 7_321);
        assert_eq!(plan.change_sats, 2_679);
    }

    #[test]
    fn split_planner_combines_existing_leaves_with_one_split() {
        let leaves = [
            TestLeaf { id: 1, value: 400 },
            TestLeaf { id: 2, value: 600 },
            TestLeaf {
                id: 3,
                value: 1_000,
            },
        ];
        let plan = plan_just_in_time_split(&leaves, 1_500, 1).unwrap();
        assert_eq!(
            plan.selected.iter().map(|leaf| leaf.value).sum::<u64>(),
            1_400
        );
        assert_eq!(plan.parent.value, 600);
        assert_eq!(plan.needed_sats, 100);
        assert_eq!(plan.change_sats, 500);
    }

    #[test]
    fn split_planner_enforces_both_child_floors() {
        let leaves = [TestLeaf {
            id: 1,
            value: 1_000,
        }];
        assert!(plan_just_in_time_split(&leaves, 900, 101).is_none());
        assert!(plan_just_in_time_split(&leaves, 100, 101).is_none());

        let plan = plan_just_in_time_split(&leaves, 670, 330).unwrap();
        assert_eq!((plan.needed_sats, plan.change_sats), (670, 330));
    }

    #[test]
    fn split_planner_can_split_a_previous_split_child_again() {
        // The planner intentionally has no concept of tree depth: once the
        // signer can resolve a child key, it is an ordinary eligible parent.
        let change_child = [TestLeaf {
            id: 9,
            value: 2_679,
        }];
        let plan = plan_just_in_time_split(&change_child, 2_500, 1).unwrap();
        assert_eq!(plan.parent.id, 9);
        assert_eq!((plan.needed_sats, plan.change_sats), (2_500, 179));
    }

    #[test]
    fn denomination_planner_preserves_existing_matches() {
        let leaves = [
            TestLeaf { id: 1, value: 500 },
            TestLeaf {
                id: 2,
                value: 2_000,
            },
        ];
        let (parent, needed, change) = plan_denomination_split(&leaves, &[500, 700], 330).unwrap();
        assert_eq!(parent.id, 2);
        assert_eq!((needed, change), (700, 1_300));
    }

    #[test]
    fn denomination_planner_rejects_unexitable_child() {
        let leaves = [TestLeaf {
            id: 1,
            value: 1_000,
        }];
        assert!(plan_denomination_split(&leaves, &[100], 330).is_none());
        assert!(plan_denomination_split(&leaves, &[800], 330).is_none());
    }

    #[test]
    fn receive_swap_validation_requires_exact_value() {
        let secp = bitcoin::secp256k1::Secp256k1::new();
        let sender_secret = bitcoin::secp256k1::SecretKey::from_slice(&[2; 32]).unwrap();
        let receiver_secret = bitcoin::secp256k1::SecretKey::from_slice(&[3; 32]).unwrap();
        let sender = bitcoin::secp256k1::PublicKey::from_secret_key(&secp, &sender_secret);
        let receiver = bitcoin::secp256k1::PublicKey::from_secret_key(&secp, &receiver_secret);
        let preimage = Preimage::from_hex(&"01".repeat(32)).unwrap();
        let payment_hash = preimage.compute_hash();
        let transfer_id = payment_transfer_id(&payment_hash.to_string()).unwrap();
        let transfer = SparkTransfer {
            id: transfer_id.clone(),
            sender_identity_public_key: sender,
            receiver_identity_public_key: receiver,
            status: ::spark::services::TransferStatus::SenderKeyTweaked,
            total_value: 12_345,
            expiry_time: None,
            leaves: Vec::new(),
            created_time: None,
            updated_time: None,
            transfer_type: TransferType::PreimageSwap,
            spark_invoice: None,
        };

        // Exactly the invoice amount is accepted...
        assert!(validate_lightning_receive_swap(
            transfer.clone(),
            preimage.clone(),
            &payment_hash,
            &transfer_id,
            sender,
            receiver,
            12_345,
        )
        .is_ok());
        // ...but over- and under-valued operator transfers are both rejected.
        for invoice_amount_sats in [100u64, 12_346] {
            assert!(validate_lightning_receive_swap(
                transfer.clone(),
                preimage.clone(),
                &payment_hash,
                &transfer_id,
                sender,
                receiver,
                invoice_amount_sats,
            )
            .unwrap_err()
            .contains("requires exactly"));
        }
    }
    #[test]
    fn required_mnemonic_does_not_create_a_new_identity() {
        let path =
            std::env::temp_dir().join(format!("open-ssp-required-mnemonic-{}", std::process::id()));
        let error = load_or_create_mnemonic(path.to_str().unwrap(), true).unwrap_err();
        assert!(error.contains("is required"));
        assert!(!path.exists());
    }

    #[test]
    fn withdrawal_requires_the_locked_leaves_and_the_exact_connector() {
        use ::spark::{
            services::TransferLeaf,
            tree::{SigningKeyshare, TreeNodeId},
        };
        use bitcoin::{
            secp256k1::{Secp256k1, SecretKey},
            OutPoint,
        };
        let key = |byte| {
            spark_wallet::PublicKey::from_secret_key(
                &Secp256k1::new(),
                &SecretKey::from_slice(&[byte; 32]).unwrap(),
            )
        };
        let sender = key(1);
        let receiver = key(2);
        let mut record = crate::coop_exit::tests::record();
        record.owner = sender.to_string();
        let connector: Transaction =
            deserialize(&hex::decode(&record.raw_connector).unwrap()).unwrap();
        let mut refund = connector.clone();
        refund.input.push(refund.input[0].clone());
        refund.input[1].previous_output = OutPoint::new(connector.compute_txid(), 0);
        let leaf = TreeNode {
            id: TreeNodeId::from_str(&record.leaves[0].id).unwrap(),
            tree_id: uuid::Uuid::new_v4().to_string(),
            value: 10_000,
            parent_node_id: None,
            node_tx: connector.clone(),
            refund_tx: Some(refund.clone()),
            direct_tx: None,
            direct_refund_tx: None,
            direct_from_cpfp_refund_tx: Some(refund.clone()),
            vout: 0,
            verifying_public_key: sender,
            owner_identity_public_key: Some(sender),
            signing_keyshare: SigningKeyshare {
                owner_identifiers: vec![],
                threshold: 2,
                public_key: sender,
            },
            status: TreeNodeStatus::TransferLocked,
        };
        let transfer = SparkTransfer {
            id: TransferId::from_str(&record.transfer_id).unwrap(),
            sender_identity_public_key: sender,
            receiver_identity_public_key: receiver,
            status: TransferStatus::SenderKeyTweakPending,
            total_value: 10_000,
            expiry_time: None,
            created_time: None,
            updated_time: None,
            spark_invoice: None,
            transfer_type: TransferType::CooperativeExit,
            leaves: vec![TransferLeaf {
                leaf,
                secret_cipher: vec![],
                signature: None,
                intermediate_refund_tx: refund.clone(),
                intermediate_direct_refund_tx: None,
                intermediate_direct_from_cpfp_refund_tx: Some(refund),
            }],
        };
        assert!(validate_coop_exit_transfer(&transfer, &record, &receiver).is_ok());
        let mut changed = transfer.clone();
        changed.transfer_type = TransferType::Transfer;
        assert!(validate_coop_exit_transfer(&changed, &record, &receiver).is_err());
        changed = transfer.clone();
        changed.receiver_identity_public_key = sender;
        assert!(validate_coop_exit_transfer(&changed, &record, &receiver).is_err());
        changed = transfer.clone();
        changed.total_value -= 1;
        assert!(validate_coop_exit_transfer(&changed, &record, &receiver).is_err());
        changed = transfer.clone();
        changed.leaves[0].intermediate_refund_tx.input[1]
            .previous_output
            .vout = 1;
        assert!(validate_coop_exit_transfer(&changed, &record, &receiver).is_err());
        for status in [
            TransferStatus::SenderInitiated,
            TransferStatus::Returned,
            TransferStatus::Expired,
        ] {
            changed = transfer.clone();
            changed.status = status;
            assert!(validate_coop_exit_transfer(&changed, &record, &receiver).is_err());
        }
        let mut different_connector = connector;
        different_connector.output[0].value = bitcoin::Amount::from_sat(331);
        record.raw_connector = bitcoin::consensus::encode::serialize_hex(&different_connector);
        assert!(validate_coop_exit_transfer(&transfer, &record, &receiver).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn existing_mnemonic_permissions_are_tightened() {
        use std::os::unix::fs::PermissionsExt;

        let path =
            std::env::temp_dir().join(format!("open-ssp-mnemonic-mode-{}", uuid::Uuid::new_v4()));
        let mnemonic = Mnemonic::generate_in(Language::English, 12).unwrap();
        std::fs::write(&path, mnemonic.to_string()).unwrap();
        let mut permissions = std::fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o644);
        std::fs::set_permissions(&path, permissions).unwrap();

        load_or_create_mnemonic(path.to_str().unwrap(), true).unwrap();

        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
        std::fs::remove_file(&path).unwrap();
    }
}
