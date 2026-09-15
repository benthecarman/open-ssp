//! Cooperative withdrawals. Bitcoin inputs and the exact payout transaction
//! remain reserved until the corresponding Spark transfer is recovered.

use std::{collections::HashSet, str::FromStr, sync::Arc, time::Duration};

use bitcoin::{
    absolute::LockTime,
    consensus::{deserialize, encode::serialize_hex},
    transaction::Version,
    Address, Amount, Denomination, Network, OutPoint, ScriptBuf, Sequence, Transaction, TxIn,
    TxOut, Witness,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{db::Db, spark::SparkService};

const MAX_LEAVES: usize = 100;
const QUOTE_SECONDS: i64 = 300;
const CONNECTOR_SATS: u64 = 330;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ExitLeaf {
    pub id: String,
    pub value: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ExitQuote {
    pub id: String,
    pub owner: String,
    pub address: String,
    pub leaves: Vec<ExitLeaf>,
    pub created_at: i64,
    pub expires_at: i64,
    pub user_fee: u64,
    pub rates: [u64; 3],
    pub fees: [u64; 3],
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ExitRecord {
    pub id: String,
    pub owner: String,
    pub transfer_id: String,
    pub idempotency_key: String,
    pub fingerprint: String,
    pub quote: ExitQuote,
    pub leaves: Vec<ExitLeaf>,
    pub speed: String,
    pub payout_sats: u64,
    pub fee_sats: u64,
    pub created_at: i64,
    pub expires_at: i64,
    pub raw_exit: String,
    pub raw_connector: String,
    pub signed_exit: Option<String>,
    pub status: String,
}

#[derive(Deserialize, Serialize)]
struct ExitInput {
    leaf_external_ids: Vec<String>,
    withdrawal_address: String,
    exit_speed: String,
    #[serde(default = "yes")]
    withdraw_all: bool,
    fee_leaf_external_ids: Option<Vec<String>>,
    fee_quote_id: Option<String>,
    idempotency_key: Option<String>,
    user_outbound_transfer_external_id: Option<String>,
}

fn yes() -> bool {
    true
}
fn now() -> i64 {
    chrono::Utc::now().timestamp()
}
fn timestamp(value: i64) -> String {
    chrono::DateTime::from_timestamp(value, 0)
        .expect("stored timestamp")
        .to_rfc3339()
}
fn sats(value: u64) -> Value {
    json!({"original_value": value, "original_unit": "SATOSHI",
        "preferred_currency_unit": "SATOSHI", "preferred_currency_value_rounded": value})
}
fn total(leaves: &[ExitLeaf]) -> Result<u64, String> {
    leaves.iter().try_fold(0u64, |sum, leaf| {
        sum.checked_add(leaf.value)
            .filter(|sum| *sum <= 2_100_000_000_000_000)
            .ok_or_else(|| "withdrawal amount is out of range".to_string())
    })
}
fn ids(value: &Value) -> Result<Vec<String>, String> {
    let ids: Vec<String> = serde_json::from_value(value.clone())
        .map_err(|_| "leaf_external_ids must be an array of UUIDs")?;
    validate_ids(&ids)?;
    Ok(ids)
}
fn validate_ids(ids: &[String]) -> Result<(), String> {
    if ids.is_empty() || ids.len() > MAX_LEAVES {
        return Err(format!("withdrawal requires 1 to {MAX_LEAVES} leaves"));
    }
    let mut seen = HashSet::new();
    for id in ids {
        let parsed = Uuid::parse_str(id).map_err(|_| "invalid leaf UUID")?;
        if parsed.to_string() != *id || !seen.insert(id) {
            return Err("leaf IDs must be distinct canonical UUIDs".to_string());
        }
    }
    Ok(())
}
fn speed_index(speed: &str) -> Result<usize, String> {
    match speed {
        "FAST" => Ok(0),
        "MEDIUM" => Ok(1),
        "SLOW" => Ok(2),
        _ => Err("invalid exit_speed".into()),
    }
}

struct BitcoinWallet {
    http: reqwest::Client,
    url: String,
    user: String,
    password: String,
}

impl BitcoinWallet {
    async fn rpc(&self, method: &str, params: Value) -> Result<Value, String> {
        let response = self
            .http
            .post(&self.url)
            .basic_auth(&self.user, Some(&self.password))
            .json(&json!({"jsonrpc":"2.0", "id":"ssp", "method":method, "params":params}))
            .send()
            .await
            .map_err(|error| format!("Bitcoin {method}: {}", error.without_url()))?;
        let status = response.status();
        let body: Value = response
            .json()
            .await
            .map_err(|_| format!("Bitcoin {method}: invalid RPC response ({status})"))?;
        if !body["error"].is_null() || !status.is_success() {
            return Err(format!("Bitcoin {method}: {}", body["error"]));
        }
        body.get("result")
            .cloned()
            .ok_or_else(|| format!("Bitcoin {method}: missing result"))
    }

    async fn address(&self, network: Network) -> Result<Address, String> {
        let value = self.rpc("getrawchangeaddress", json!(["bech32m"])).await?;
        parse_address(
            value.as_str().ok_or("Bitcoin returned no address")?,
            network,
        )
    }
}

fn parse_address(value: &str, network: Network) -> Result<Address, String> {
    Address::from_str(value)
        .map_err(|error| error.to_string())?
        .require_network(network)
        .map_err(|error| error.to_string())
}

pub struct CoopExitService {
    db: Arc<Db>,
    spark: Arc<dyn CoopSpark>,
    bitcoin: BitcoinWallet,
    network: Network,
    network_name: String,
    user_fee: u64,
    lock: tokio::sync::Mutex<()>,
}

#[async_trait::async_trait]
trait CoopSpark: Send + Sync {
    async fn leaves(&self, owner: &str, ids: &[String]) -> Result<Vec<ExitLeaf>, String>;
    async fn transfer_exists(&self, id: &str) -> Result<bool, String>;
    async fn verify(&self, record: &ExitRecord) -> Result<(), String>;
    async fn claim(&self, record: &ExitRecord) -> Result<(), String>;
}

#[async_trait::async_trait]
impl CoopSpark for SparkService {
    async fn leaves(&self, owner: &str, ids: &[String]) -> Result<Vec<ExitLeaf>, String> {
        self.coop_exit_leaves(owner, ids).await
    }
    async fn transfer_exists(&self, id: &str) -> Result<bool, String> {
        self.coop_exit_transfer_exists(id).await
    }
    async fn verify(&self, record: &ExitRecord) -> Result<(), String> {
        self.verify_coop_exit(record).await
    }
    async fn claim(&self, record: &ExitRecord) -> Result<(), String> {
        self.claim_coop_exit(record).await
    }
}

impl CoopExitService {
    pub async fn from_env(
        db: Arc<Db>,
        spark: Arc<SparkService>,
        network_name: &str,
    ) -> Result<Option<Arc<Self>>, String> {
        let url = std::env::var("COOP_BITCOIN_RPC_URL").unwrap_or_default();
        if url.is_empty() {
            return Ok(None);
        }
        let parsed = reqwest::Url::parse(&url).map_err(|_| "invalid COOP_BITCOIN_RPC_URL")?;
        if !matches!(parsed.scheme(), "http" | "https")
            || !parsed.path().starts_with("/wallet/")
            || parsed.path() == "/wallet/"
        {
            return Err(
                "COOP_BITCOIN_RPC_URL must name a dedicated /wallet/<name> endpoint".into(),
            );
        }
        let network = match network_name {
            "REGTEST" | "LOCAL" => Network::Regtest,
            "SIGNET" => Network::Signet,
            "TESTNET" => Network::Testnet,
            "MAINNET" => Network::Bitcoin,
            _ => return Err("unsupported Bitcoin network".into()),
        };
        let bitcoin = BitcoinWallet {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(15))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|error| error.to_string())?,
            url,
            user: std::env::var("COOP_BITCOIN_RPC_USER")
                .map_err(|_| "COOP_BITCOIN_RPC_USER is required")?,
            password: match std::env::var("COOP_BITCOIN_RPC_PASSWORD_FILE") {
                Ok(path) => std::fs::read_to_string(path)
                    .map_err(|_| "cannot read COOP_BITCOIN_RPC_PASSWORD_FILE")?
                    .trim_end()
                    .to_string(),
                Err(_) => std::env::var("COOP_BITCOIN_RPC_PASSWORD")
                    .map_err(|_| "COOP_BITCOIN_RPC_PASSWORD or its file is required")?,
            },
        };
        let chain = bitcoin.rpc("getblockchaininfo", json!([])).await?;
        let expected = match network {
            Network::Bitcoin => "main",
            Network::Testnet => "test",
            Network::Signet => "signet",
            _ => "regtest",
        };
        if chain["chain"].as_str() != Some(expected) {
            return Err("withdrawal Bitcoin node network mismatch".into());
        }
        let wallet = bitcoin.rpc("getwalletinfo", json!([])).await?;
        if wallet["private_keys_enabled"].as_bool() != Some(true)
            || wallet.get("scanning").is_some_and(|v| v != false)
        {
            return Err("withdrawal wallet must have private keys and finish scanning".into());
        }
        db.init_coop_exits().await?;
        let user_fee = std::env::var("COOP_EXIT_FEE_SATS")
            .unwrap_or_else(|_| "0".into())
            .parse::<u64>()
            .map_err(|_| "invalid COOP_EXIT_FEE_SATS")?;
        Ok(Some(Arc::new(Self {
            db,
            spark,
            bitcoin,
            network,
            network_name: network_name.into(),
            user_fee,
            lock: tokio::sync::Mutex::new(()),
        })))
    }

    pub(crate) async fn bitcoin_rpc(&self, method: &str, params: Value) -> Result<Value, String> {
        self.bitcoin.rpc(method, params).await
    }
    pub(crate) async fn bitcoin_change_address(&self) -> Result<Address, String> {
        let value = self
            .bitcoin
            .rpc("getrawchangeaddress", json!(["bech32"]))
            .await?;
        parse_address(
            value
                .as_str()
                .ok_or("Bitcoin returned no recovery address")?,
            self.network,
        )
    }

    pub async fn quote(&self, owner: &str, input: &Value) -> Result<ExitQuote, String> {
        let leaf_ids = ids(&input["leaf_external_ids"])?;
        let address = parse_address(
            input["withdrawal_address"]
                .as_str()
                .ok_or("withdrawal_address is required")?,
            self.network,
        )?;
        let leaves = self.spark.leaves(owner, &leaf_ids).await?;
        let mut rates = [0; 3];
        for (index, blocks) in [2, 6, 12].iter().enumerate() {
            let estimate = self
                .bitcoin
                .rpc("estimatesmartfee", json!([blocks, "CONSERVATIVE"]))
                .await?;
            rates[index] = match estimate.get("feerate") {
                Some(value) => btc_amount(value)?.div_ceil(1000).max(1),
                None if self.network == Network::Regtest => 1,
                None => return Err("Bitcoin fee estimate unavailable".into()),
            };
        }
        // One native-SegWit input, payout, connector funding, and change.
        // Reserve an upper bound for the signed input; any surplus is miner fee.
        let vbytes = 11 + 69 + 2 * 43 + 8 + 1 + address.script_pubkey().len() as u64;
        let mut fees = [0; 3];
        for (fee, rate) in fees.iter_mut().zip(rates) {
            *fee = vbytes.checked_mul(rate).ok_or("withdrawal fee overflow")?;
        }
        let quote = ExitQuote {
            id: Uuid::new_v4().to_string(),
            owner: owner.into(),
            address: address.to_string(),
            leaves,
            created_at: now(),
            expires_at: now() + QUOTE_SECONDS,
            user_fee: self.user_fee,
            rates,
            fees,
        };
        self.db.save_coop_quote(&quote).await?;
        Ok(quote)
    }

    pub fn quote_response(&self, quote: &ExitQuote) -> Value {
        json!({"__typename":"CoopExitFeeQuote", "id":quote.id,
            "created_at":timestamp(quote.created_at), "updated_at":timestamp(quote.created_at),
            "expires_at":timestamp(quote.expires_at), "network":self.network_name,
            "total_amount":sats(total(&quote.leaves).unwrap_or(0)),
            "user_fee_fast":sats(quote.user_fee), "user_fee_medium":sats(quote.user_fee), "user_fee_slow":sats(quote.user_fee),
            "l1_broadcast_fee_fast":sats(quote.fees[0]), "l1_broadcast_fee_medium":sats(quote.fees[1]), "l1_broadcast_fee_slow":sats(quote.fees[2])})
    }

    pub async fn request(&self, owner: &str, input: &Value) -> Result<Value, String> {
        let request: ExitInput = serde_json::from_value(input.clone())
            .map_err(|error| format!("invalid withdrawal: {error}"))?;
        validate_ids(&request.leaf_external_ids)?;
        let speed = speed_index(&request.exit_speed)?;
        let transfer_id = request
            .user_outbound_transfer_external_id
            .as_deref()
            .ok_or("user_outbound_transfer_external_id is required")?;
        if Uuid::parse_str(transfer_id)
            .map_err(|_| "invalid transfer UUID")?
            .to_string()
            != transfer_id
        {
            return Err("transfer ID must be a canonical UUID".into());
        }
        let key = request
            .idempotency_key
            .as_deref()
            .filter(|key| !key.is_empty())
            .unwrap_or(transfer_id);
        if key.len() > 200 {
            return Err("idempotency_key is too long".into());
        }
        let fingerprint = hex::encode(Sha256::digest(
            serde_json::to_vec(&request).map_err(|error| error.to_string())?,
        ));
        let _guard = self.lock.lock().await;
        if let Some(record) = self.db.coop_exit_by_key(owner, key, transfer_id).await? {
            if record.fingerprint != fingerprint {
                return Err(
                    "withdrawal key or transfer ID was already used for another request".into(),
                );
            }
            return self.response(&record).await;
        }
        if self.db.coop_exit_pending_for_owner(owner).await? {
            return Err("complete the pending withdrawal before requesting another".into());
        }
        let quote = match request.fee_quote_id.as_deref() {
            Some(id) => self
                .db
                .coop_quote(id, owner)
                .await?
                .ok_or("withdrawal quote not found")?,
            None => self.quote(owner, input).await?,
        };
        let address = parse_address(&request.withdrawal_address, self.network)?;
        if quote.expires_at <= now() || quote.address != address.to_string() {
            return Err("withdrawal quote expired or does not match the address".into());
        }
        // SDKs can split the quoted leaves to separate the payout and fee.
        // The Bitcoin fee depends on the fixed transaction shape, not leaf IDs.
        // Check ownership and values of the actual request leaves again.
        let mut leaves = self.spark.leaves(owner, &request.leaf_external_ids).await?;
        let payout = withdrawal_amount(
            &request,
            &quote,
            speed,
            &mut leaves,
            self.spark.as_ref(),
            owner,
        )
        .await?;
        if payout < address.script_pubkey().minimal_non_dust().to_sat() {
            return Err("withdrawal payout is below the address dust limit".into());
        }
        let reserved = self.db.coop_reserved_inputs().await?;
        let intermediate = CONNECTOR_SATS * (leaves.len() as u64 + 1);
        let needed = payout
            .checked_add(quote.fees[speed])
            .and_then(|v| v.checked_add(intermediate + CONNECTOR_SATS))
            .ok_or("withdrawal amount overflow")?;
        let unspent = self.bitcoin.rpc("listunspent", json!([1])).await?;
        let mut candidates = Vec::new();
        for coin in unspent
            .as_array()
            .ok_or("Bitcoin returned invalid unspent outputs")?
        {
            if coin["spendable"] != true || coin["safe"] != true {
                continue;
            }
            let value = btc_amount(&coin["amount"])?;
            let script =
                ScriptBuf::from_hex(coin["scriptPubKey"].as_str().ok_or("missing coin script")?)
                    .map_err(|error| error.to_string())?;
            if !(script.is_p2tr() || script.is_p2wpkh()) || value < needed {
                continue;
            }
            let outpoint = OutPoint::new(
                coin["txid"]
                    .as_str()
                    .ok_or("missing coin txid")?
                    .parse()
                    .map_err(|_| "invalid coin txid")?,
                u32::try_from(coin["vout"].as_u64().ok_or("missing coin vout")?)
                    .map_err(|_| "invalid coin vout")?,
            );
            if !reserved.contains(&outpoint.to_string()) {
                candidates.push((value, outpoint));
            }
        }
        candidates.sort_by_key(|(value, outpoint)| (*value, *outpoint));
        let (coin_value, outpoint) = candidates.first().copied().ok_or("withdrawal needs one unreserved confirmed SegWit UTXO large enough for payout, fees, and connector funding")?;
        let intermediate_address = self.bitcoin.address(self.network).await?;
        let change_address = self.bitcoin.address(self.network).await?;
        let exit = transaction(
            vec![outpoint],
            vec![
                TxOut {
                    value: Amount::from_sat(payout),
                    script_pubkey: address.script_pubkey(),
                },
                TxOut {
                    value: Amount::from_sat(intermediate),
                    script_pubkey: intermediate_address.script_pubkey(),
                },
                TxOut {
                    value: Amount::from_sat(coin_value - payout - intermediate - quote.fees[speed]),
                    script_pubkey: change_address.script_pubkey(),
                },
            ],
        );
        let mut connector_outputs = Vec::with_capacity(leaves.len() + 1);
        for _ in 0..=leaves.len() {
            connector_outputs.push(TxOut {
                value: Amount::from_sat(CONNECTOR_SATS),
                script_pubkey: self.bitcoin.address(self.network).await?.script_pubkey(),
            });
        }
        let connector = transaction(
            vec![OutPoint::new(exit.compute_txid(), 1)],
            connector_outputs,
        );
        let record = ExitRecord {
            id: Uuid::new_v4().to_string(),
            owner: owner.into(),
            transfer_id: transfer_id.into(),
            idempotency_key: key.into(),
            fingerprint,
            quote: quote.clone(),
            leaves,
            speed: request.exit_speed,
            payout_sats: payout,
            fee_sats: quote.fees[speed],
            created_at: now(),
            expires_at: now() + QUOTE_SECONDS,
            raw_exit: serialize_hex(&exit),
            raw_connector: serialize_hex(&connector),
            signed_exit: None,
            status: "INITIATED".into(),
        };
        self.db
            .insert_coop_exit(&record, &outpoint.to_string())
            .await?;
        self.response(&record).await
    }

    pub async fn complete(&self, owner: &str, input: &Value) -> Result<Value, String> {
        let transfer_id = input["user_outbound_transfer_external_id"]
            .as_str()
            .ok_or("transfer ID is required")?;
        let _guard = self.lock.lock().await;
        let mut record = self
            .db
            .coop_exit_by_key(owner, "", transfer_id)
            .await?
            .ok_or("withdrawal request not found")?;
        if input
            .get("coop_exit_request_id")
            .and_then(Value::as_str)
            .is_some_and(|id| id != record.id)
        {
            return Err("withdrawal request ID does not match the transfer".into());
        }
        if record.status == "EXPIRED" {
            return Err("withdrawal request expired".into());
        }
        self.advance(&mut record).await?;
        self.response(&record).await
    }

    pub async fn get(&self, id: &str, owner: &str) -> Result<Option<Value>, String> {
        match self.db.coop_exit(id, owner).await? {
            Some(record) => Ok(Some(self.response(&record).await?)),
            None => Ok(None),
        }
    }

    pub async fn response(&self, record: &ExitRecord) -> Result<Value, String> {
        let updated = self.db.request_updated_at(&record.id).await?;
        let tx: Transaction = hex::decode(&record.raw_exit)
            .map_err(|e| e.to_string())
            .and_then(|raw| deserialize(&raw).map_err(|e| e.to_string()))?;
        Ok(
            json!({"__typename":"CoopExitRequest", "id":record.id, "network":self.network_name,
            "created_at":timestamp(record.created_at), "updated_at":updated,
            "expires_at":timestamp(record.expires_at), "withdrawal_address":record.quote.address,
            "fee":sats(record.quote.user_fee), "l1_broadcast_fee":sats(record.fee_sats),
            "fee_quote":self.quote_response(&record.quote), "exit_speed":record.speed,
            "status":record.status, "raw_connector_transaction":record.raw_connector,
            "raw_coop_exit_transaction":record.raw_exit, "coop_exit_txid":tx.compute_txid().to_string(),
            "transfer_spark_id":record.transfer_id, "transfer":null}),
        )
    }

    /// Raise the package fee by spending only the SSP change output. The
    /// payout and connector parent transaction IDs remain unchanged.
    pub async fn bump_fee(
        &self,
        id: &str,
        fee_rate: u64,
        max_fee_sats: u64,
    ) -> Result<Value, String> {
        if !(1..=10_000).contains(&fee_rate) || max_fee_sats == 0 {
            return Err(
                "fee_rate must be 1 to 10000 sat/vB and max_fee_sats must be positive".into(),
            );
        }
        let _guard = self.lock.lock().await;
        let record: ExitRecord = self
            .db
            .with(|c| c.query_row("SELECT data FROM coop_exits WHERE id=?1", [id], decode_row))
            .await?;
        if record.status != "TX_BROADCASTED" {
            return Err("only an unconfirmed broadcast withdrawal can be bumped".into());
        }
        let parent: Transaction = deserialize(
            &hex::decode(
                record
                    .signed_exit
                    .as_deref()
                    .ok_or("withdrawal is unsigned")?,
            )
            .map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
        let parent_txid = parent.compute_txid().to_string();
        let observed = self
            .bitcoin
            .rpc("gettransaction", json!([parent_txid]))
            .await?;
        if observed["confirmations"].as_i64() != Some(0) {
            return Err("withdrawal is confirmed or conflicted".into());
        }
        let existing: Option<(String, u64)> = self
            .db
            .with(|c| {
                use rusqlite::OptionalExtension;
                c.query_row(
                    "SELECT raw,fee_rate FROM coop_exit_bumps WHERE request_id=?1",
                    [id],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()
            })
            .await?;
        if let Some((raw, previous_rate)) = existing {
            if previous_rate != fee_rate {
                return Err("a fee bump already exists; retry with its original fee_rate".into());
            }
            let child: Transaction = deserialize(&hex::decode(&raw).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
            let change = parent
                .output
                .get(2)
                .ok_or("withdrawal has no SSP change")?
                .value
                .to_sat();
            let payout = child
                .output
                .first()
                .ok_or("fee bump has no payout output")?
                .value
                .to_sat();
            let fee = change
                .checked_sub(payout)
                .ok_or("fee bump output exceeds the parent change")?;
            if fee > max_fee_sats {
                return Err("stored fee bump exceeds max_fee_sats".into());
            }
            self.bitcoin.rpc("sendrawtransaction", json!([raw])).await?;
            return Ok(
                json!({"txid":child.compute_txid().to_string(),"fee_sats":fee,"parent_txid":parent_txid}),
            );
        }
        let coin = self
            .bitcoin
            .rpc("gettxout", json!([parent_txid, 2, true]))
            .await?;
        if coin.is_null() {
            return Err("withdrawal change is not available for a fee bump".into());
        }
        let destination = self.bitcoin.address(self.network).await?;
        let (child, fee) = cpfp_transaction(
            &parent,
            record.fee_sats,
            fee_rate,
            max_fee_sats,
            destination.script_pubkey(),
        )?;
        let signed = self
            .bitcoin
            .rpc(
                "signrawtransactionwithwallet",
                json!([serialize_hex(&child)]),
            )
            .await?;
        if signed["complete"] != true {
            return Err("Bitcoin wallet could not sign fee bump".into());
        }
        let raw = signed["hex"]
            .as_str()
            .ok_or("Bitcoin returned no fee bump transaction")?;
        validate_signed_exit(&child, raw)?;
        self.db
            .with(|c| {
                c.execute(
                    "INSERT INTO coop_exit_bumps(request_id,raw,fee_rate) VALUES(?1,?2,?3)",
                    (id, raw, fee_rate),
                )
                .map(|_| ())
            })
            .await?;
        self.bitcoin.rpc("sendrawtransaction", json!([raw])).await?;
        Ok(
            json!({"txid":child.compute_txid().to_string(),"fee_sats":fee,"parent_txid":parent_txid}),
        )
    }

    async fn advance(&self, record: &mut ExitRecord) -> Result<(), String> {
        if matches!(record.status.as_str(), "SUCCEEDED" | "EXPIRED") {
            return Ok(());
        }
        let exit: Transaction =
            deserialize(&hex::decode(&record.raw_exit).map_err(|error| error.to_string())?)
                .map_err(|error| error.to_string())?;
        let txid = exit.compute_txid().to_string();
        if record.signed_exit.is_none() {
            // The operator's signed connector refunds bind these exact leaves
            // to this payout. No Bitcoin signature exists before this check.
            self.spark.verify(record).await?;
            self.db
                .insert_transfer(
                    &record.transfer_id,
                    &record.id,
                    "COOPERATIVE_EXIT",
                    "PENDING",
                    &record.owner,
                )
                .await?;
            let signed = self
                .bitcoin
                .rpc("signrawtransactionwithwallet", json!([record.raw_exit]))
                .await?;
            if signed["complete"] != true {
                return Err("Bitcoin wallet could not sign the withdrawal".into());
            }
            let raw = signed["hex"]
                .as_str()
                .ok_or("Bitcoin returned no signed transaction")?;
            validate_signed_exit(&exit, raw)?;
            record.signed_exit = Some(raw.into());
            record.status = "INBOUND_TRANSFER_CHECKED".into();
            self.db.update_coop_exit(record).await?;
        }
        let observed = self.bitcoin.rpc("gettransaction", json!([txid])).await;
        let confirmations = observed
            .as_ref()
            .ok()
            .and_then(|tx| tx["confirmations"].as_i64())
            .unwrap_or(0);
        if confirmations < 0 {
            return Err("withdrawal transaction is conflicted; input reservation retained".into());
        }
        if confirmations == 0 {
            // A lost reply never creates a replacement payout. Broadcast the
            // same durable transaction and retain its reservation on errors.
            self.bitcoin
                .rpc("sendrawtransaction", json!([record.signed_exit]))
                .await?;
            record.status = "TX_BROADCASTED".into();
            self.db.update_coop_exit(record).await?;
            let bump: Option<String> = self
                .db
                .with(|c| {
                    use rusqlite::OptionalExtension;
                    c.query_row(
                        "SELECT raw FROM coop_exit_bumps WHERE request_id=?1",
                        [&record.id],
                        |r| r.get(0),
                    )
                    .optional()
                })
                .await?;
            if let Some(raw) = bump {
                self.bitcoin.rpc("sendrawtransaction", json!([raw])).await?;
            }
            return Ok(());
        }
        record.status = "ON_CHAIN_TX_CONFIRMED".into();
        self.db.update_coop_exit(record).await?;
        self.spark.claim(record).await?;
        record.status = "SUCCEEDED".into();
        self.db.update_coop_exit(record).await?;
        Ok(())
    }

    pub async fn run(self: Arc<Self>) {
        loop {
            {
                let _guard = self.lock.lock().await;
                match self.db.pending_coop_exits().await {
                    Ok(records) => {
                        for mut record in records {
                            if record.signed_exit.is_none() && record.expires_at <= now() {
                                // Never free a payout coin if the wallet already
                                // submitted the conditional transfer, even if it
                                // lost its complete-request response.
                                match self.spark.transfer_exists(&record.transfer_id).await {
                                    Ok(false) => {
                                        record.status = "EXPIRED".into();
                                        if let Err(error) = self.db.update_coop_exit(&record).await
                                        {
                                            tracing::warn!("expire withdrawal: {error}");
                                        }
                                        continue;
                                    }
                                    Err(error) => {
                                        tracing::warn!("check withdrawal expiry: {error}");
                                        continue;
                                    }
                                    Ok(true) => {}
                                }
                            }
                            if let Err(error) = self.advance(&mut record).await {
                                tracing::debug!(
                                    request_id = record.id,
                                    "withdrawal pending: {error}"
                                );
                            }
                        }
                    }
                    Err(error) => tracing::warn!("reconcile withdrawals: {error}"),
                }
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
}

fn cpfp_transaction(
    parent: &Transaction,
    parent_fee: u64,
    rate: u64,
    max_fee: u64,
    destination: ScriptBuf,
) -> Result<(Transaction, u64), String> {
    let change = parent.output.get(2).ok_or("withdrawal has no SSP change")?;
    if !(change.script_pubkey.is_p2tr() || change.script_pubkey.is_p2wpkh()) {
        return Err("fee bump needs SegWit SSP change".into());
    }
    let mut child = transaction(
        vec![OutPoint::new(parent.compute_txid(), 2)],
        vec![TxOut {
            value: change.value,
            script_pubkey: destination,
        }],
    );
    // Upper bound for a P2WPKH witness, which is larger than a P2TR witness.
    let vbytes = child.vsize() as u64 + 28;
    let package_fee = rate
        .checked_mul(parent.vsize() as u64 + vbytes)
        .ok_or("fee overflow")?;
    let fee = package_fee
        .saturating_sub(parent_fee)
        .max(rate.checked_mul(vbytes).ok_or("fee overflow")?);
    if fee > max_fee {
        return Err("fee bump exceeds max_fee_sats".into());
    }
    let value = change
        .value
        .to_sat()
        .checked_sub(fee)
        .ok_or("SSP change cannot fund the fee bump")?;
    if value < child.output[0].script_pubkey.minimal_non_dust().to_sat() {
        return Err("fee bump leaves a dust output".into());
    }
    child.output[0].value = Amount::from_sat(value);
    Ok((child, fee))
}

async fn withdrawal_amount(
    request: &ExitInput,
    quote: &ExitQuote,
    speed: usize,
    leaves: &mut Vec<ExitLeaf>,
    spark: &dyn CoopSpark,
    owner: &str,
) -> Result<u64, String> {
    let fees = quote
        .user_fee
        .checked_add(quote.fees[speed])
        .ok_or("withdrawal fee overflow")?;
    let amount = total(leaves)?;
    let fee_ids = request.fee_leaf_external_ids.as_deref().unwrap_or_default();
    if request.withdraw_all {
        if !fee_ids.is_empty() {
            return Err("withdraw_all cannot include fee leaves".into());
        }
        amount
            .checked_sub(fees)
            .ok_or_else(|| "withdrawal amount does not cover fees".into())
    } else {
        validate_ids(fee_ids)?;
        let fee_leaves = spark.leaves(owner, fee_ids).await?;
        if total(&fee_leaves)? != fees {
            return Err("fee leaves must exactly cover the quoted fees".into());
        }
        leaves.extend(fee_leaves);
        validate_ids(
            &leaves
                .iter()
                .map(|leaf| leaf.id.clone())
                .collect::<Vec<_>>(),
        )?;
        Ok(amount)
    }
}

fn transaction(inputs: Vec<OutPoint>, output: Vec<TxOut>) -> Transaction {
    Transaction {
        // Spark's cooperative exit protocol requires version-3 connectors.
        version: Version(3),
        lock_time: LockTime::ZERO,
        input: inputs
            .into_iter()
            .map(|previous_output| TxIn {
                previous_output,
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            })
            .collect(),
        output,
    }
}

pub(crate) fn btc_amount(value: &Value) -> Result<u64, String> {
    let text = match value.as_str() {
        Some(text) => text.to_owned(),
        None => value
            .as_f64()
            .ok_or("Bitcoin amount is not a number")?
            .to_string(),
    };
    Amount::from_str_in(&text, Denomination::Bitcoin)
        .map(|amount| amount.to_sat())
        .map_err(|error| format!("invalid Bitcoin amount: {error}"))
}

fn validate_signed_exit(unsigned: &Transaction, raw: &str) -> Result<(), String> {
    let signed: Transaction = deserialize(&hex::decode(raw).map_err(|error| error.to_string())?)
        .map_err(|error| error.to_string())?;
    if signed.compute_txid() != unsigned.compute_txid()
        || signed.input.iter().any(|input| input.witness.is_empty())
    {
        return Err("Bitcoin signing changed the payout transaction or omitted a witness".into());
    }
    Ok(())
}

impl Db {
    async fn init_coop_exits(&self) -> Result<(), String> {
        self.with(|db| db.execute_batch("CREATE TABLE IF NOT EXISTS coop_exit_quotes(id TEXT PRIMARY KEY, owner TEXT NOT NULL, expires_at INTEGER NOT NULL, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS coop_exits(id TEXT PRIMARY KEY, owner TEXT NOT NULL, transfer_id TEXT NOT NULL UNIQUE, idem TEXT NOT NULL, status TEXT NOT NULL, data TEXT NOT NULL, UNIQUE(owner,idem));
            CREATE TABLE IF NOT EXISTS coop_exit_inputs(outpoint TEXT PRIMARY KEY, request_id TEXT NOT NULL REFERENCES coop_exits(id));
            CREATE TABLE IF NOT EXISTS coop_exit_bumps(request_id TEXT PRIMARY KEY REFERENCES coop_exits(id),raw TEXT NOT NULL,fee_rate INTEGER NOT NULL);")).await
    }

    async fn save_coop_quote(&self, quote: &ExitQuote) -> Result<(), String> {
        let data = serde_json::to_string(quote).map_err(|error| error.to_string())?;
        self.with(|db| {
            db.execute("DELETE FROM coop_exit_quotes WHERE expires_at<=?1", [now()])?;
            // Quotes do not reserve coins. Keep bounded storage per identity.
            db.execute("DELETE FROM coop_exit_quotes WHERE owner=?1 AND id NOT IN (SELECT id FROM coop_exit_quotes WHERE owner=?1 ORDER BY expires_at DESC LIMIT 19)", [&quote.owner])?;
            db.execute("INSERT INTO coop_exit_quotes VALUES(?1,?2,?3,?4)", (&quote.id, &quote.owner, quote.expires_at, data)).map(|_| ())
        }).await
    }

    async fn coop_quote(&self, id: &str, owner: &str) -> Result<Option<ExitQuote>, String> {
        self.with(|db| {
            use rusqlite::OptionalExtension;
            db.query_row(
                "SELECT data FROM coop_exit_quotes WHERE id=?1 AND owner=?2",
                (id, owner),
                decode_row,
            )
            .optional()
        })
        .await
    }

    async fn coop_exit(&self, id: &str, owner: &str) -> Result<Option<ExitRecord>, String> {
        self.with(|db| {
            use rusqlite::OptionalExtension;
            db.query_row(
                "SELECT data FROM coop_exits WHERE id=?1 AND owner=?2",
                (id, owner),
                decode_row,
            )
            .optional()
        })
        .await
    }

    async fn coop_exit_by_key(
        &self,
        owner: &str,
        key: &str,
        transfer: &str,
    ) -> Result<Option<ExitRecord>, String> {
        self.with(|db| {
            use rusqlite::OptionalExtension;
            db.query_row(
                "SELECT data FROM coop_exits WHERE owner=?1 AND (idem=?2 OR transfer_id=?3)",
                (owner, key, transfer),
                decode_row,
            )
            .optional()
        })
        .await
    }

    async fn coop_exit_pending_for_owner(&self, owner: &str) -> Result<bool, String> {
        self.with(|db| db.query_row("SELECT EXISTS(SELECT 1 FROM coop_exits WHERE owner=?1 AND status NOT IN ('SUCCEEDED','EXPIRED'))", [owner], |row| row.get(0))).await
    }

    async fn coop_reserved_inputs(&self) -> Result<HashSet<String>, String> {
        self.with(|db| {
            let mut statement = db.prepare("SELECT outpoint FROM coop_exit_inputs")?;
            let rows = statement.query_map([], |row| row.get(0))?;
            rows.collect()
        })
        .await
    }

    async fn insert_coop_exit(&self, record: &ExitRecord, outpoint: &str) -> Result<(), String> {
        let data = serde_json::to_string(record).map_err(|error| error.to_string())?;
        let exit: Transaction =
            deserialize(&hex::decode(&record.raw_exit).map_err(|error| error.to_string())?)
                .map_err(|error| error.to_string())?;
        let connector_funding = OutPoint::new(exit.compute_txid(), 1).to_string();
        self.with(|db| {
            let tx = db.unchecked_transaction()?;
            tx.execute("INSERT INTO coop_exits VALUES(?1,?2,?3,?4,?5,?6)", (&record.id,&record.owner,&record.transfer_id,&record.idempotency_key,&record.status,data))?;
            tx.execute("INSERT INTO coop_exit_inputs VALUES(?1,?2)", (outpoint,&record.id))?;
            // Keep the connector funding available until Spark recovery, even
            // after the exit confirms and Core lists this output as spendable.
            tx.execute("INSERT INTO coop_exit_inputs VALUES(?1,?2)", (connector_funding,&record.id))?;
            // Keep the SSP change for CPFP until this withdrawal is complete.
            tx.execute("INSERT INTO coop_exit_inputs VALUES(?1,?2)", (OutPoint::new(exit.compute_txid(),2).to_string(),&record.id))?;
            let payload = json!({"total_amount_sats":record.leaves.iter().map(|leaf| leaf.value).sum::<u64>()}).to_string();
            tx.execute("INSERT INTO requests(id,kind,owner,created_at,payload) VALUES(?1,'COOP_EXIT_V2',?2,?3,?4)", (&record.id,&record.owner,timestamp(record.created_at),payload))?;
            tx.commit()
        }).await
    }

    async fn update_coop_exit(&self, record: &ExitRecord) -> Result<(), String> {
        let data = serde_json::to_string(record).map_err(|error| error.to_string())?;
        self.with(|db| {
            let tx = db.unchecked_transaction()?;
            tx.execute(
                "UPDATE coop_exits SET status=?2,data=?3 WHERE id=?1",
                (&record.id, &record.status, data),
            )?;
            if matches!(record.status.as_str(), "SUCCEEDED" | "EXPIRED") {
                tx.execute(
                    "DELETE FROM coop_exit_inputs WHERE request_id=?1",
                    [&record.id],
                )?;
            }
            if record.status == "SUCCEEDED" {
                tx.execute(
                    "UPDATE transfers SET status='COMPLETED' WHERE request_id=?1 AND owner=?2",
                    (&record.id, &record.owner),
                )?;
            }
            tx.commit()
        })
        .await
    }

    async fn pending_coop_exits(&self) -> Result<Vec<ExitRecord>, String> {
        self.with(|db| {
            let mut statement = db.prepare("SELECT data FROM coop_exits WHERE status NOT IN ('SUCCEEDED','EXPIRED') ORDER BY rowid")?;
            let rows = statement.query_map([], decode_row)?;
            rows.collect()
        }).await
    }
}

fn decode_row<T: serde::de::DeserializeOwned>(row: &rusqlite::Row<'_>) -> rusqlite::Result<T> {
    let raw: String = row.get(0)?;
    serde_json::from_str(&raw).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use axum::{extract::State, routing::post, Json, Router};
    use std::sync::{
        atomic::{AtomicBool, AtomicI64, Ordering},
        Mutex,
    };

    pub(crate) fn record() -> ExitRecord {
        let leaves = vec![ExitLeaf {
            id: Uuid::new_v4().to_string(),
            value: 10_000,
        }];
        let exit = transaction(
            vec![OutPoint::null()],
            vec![TxOut {
                value: Amount::from_sat(9_790),
                script_pubkey: ScriptBuf::new(),
            }],
        );
        let connector = transaction(
            vec![OutPoint::new(exit.compute_txid(), 1)],
            vec![
                TxOut {
                    value: Amount::from_sat(330),
                    script_pubkey: ScriptBuf::new(),
                },
                TxOut {
                    value: Amount::from_sat(330),
                    script_pubkey: ScriptBuf::new(),
                },
            ],
        );
        ExitRecord {
            id: Uuid::new_v4().to_string(),
            owner: "alice".into(),
            transfer_id: Uuid::new_v4().to_string(),
            idempotency_key: Uuid::new_v4().to_string(),
            fingerprint: "fixed".into(),
            quote: ExitQuote {
                id: Uuid::new_v4().to_string(),
                owner: "alice".into(),
                address: "unused".into(),
                leaves: leaves.clone(),
                created_at: now(),
                expires_at: now() + 300,
                user_fee: 0,
                rates: [1; 3],
                fees: [210; 3],
            },
            leaves,
            speed: "FAST".into(),
            payout_sats: 9_790,
            fee_sats: 210,
            created_at: now(),
            expires_at: now() + 300,
            raw_exit: serialize_hex(&exit),
            raw_connector: serialize_hex(&connector),
            signed_exit: None,
            status: "INITIATED".into(),
        }
    }

    #[test]
    fn cpfp_preserves_payout_and_connector_and_enforces_budget() {
        let script = ScriptBuf::from_bytes([vec![0x51, 0x20], vec![2; 32]].concat());
        let mut parent: Transaction =
            deserialize(&hex::decode(record().raw_exit).unwrap()).unwrap();
        parent.output.push(TxOut {
            value: Amount::from_sat(660),
            script_pubkey: script.clone(),
        });
        parent.output.push(TxOut {
            value: Amount::from_sat(10_000),
            script_pubkey: script.clone(),
        });
        parent.input[0].witness.push([0; 64]);
        let original = parent.clone();
        let (child, fee) = cpfp_transaction(&parent, 210, 5, 5_000, script.clone()).unwrap();
        assert_eq!(parent, original);
        assert_eq!(
            child.input[0].previous_output,
            OutPoint::new(parent.compute_txid(), 2)
        );
        assert_eq!(child.output[0].value.to_sat() + fee, 10_000);
        assert!(210 + fee >= 5 * (parent.vsize() as u64 + child.vsize() as u64 + 28));
        assert!(cpfp_transaction(&parent, 210, 5, fee - 1, script.clone()).is_err());
        parent.output[2].value = Amount::from_sat(fee + 100);
        assert!(cpfp_transaction(&parent, 210, 5, 5_000, script).is_err());
    }

    #[test]
    fn bitcoin_amounts_preserve_satoshis_and_reject_fractional_sats() {
        assert_eq!(btc_amount(&json!(0.00000001)).unwrap(), 1);
        assert_eq!(btc_amount(&json!(0.00001234)).unwrap(), 1234);
        assert_eq!(btc_amount(&json!(1.23456789)).unwrap(), 123456789);
        assert!(btc_amount(&json!(-1)).is_err());
        assert!(btc_amount(&json!(0.000000001)).is_err());
    }

    #[test]
    fn signing_must_preserve_the_committed_payout() {
        let record = record();
        let unsigned: Transaction = deserialize(&hex::decode(record.raw_exit).unwrap()).unwrap();
        assert!(validate_signed_exit(&unsigned, &serialize_hex(&unsigned)).is_err());
        let mut signed = unsigned.clone();
        signed.input[0].witness.push([1; 64]);
        assert!(validate_signed_exit(&unsigned, &serialize_hex(&signed)).is_ok());
        signed.output[0].value = Amount::from_sat(1);
        assert!(validate_signed_exit(&unsigned, &serialize_hex(&signed)).is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reservations_and_financial_records_survive_restart_and_pruning() {
        let path = std::env::temp_dir().join(format!("ssp-coop-{}", Uuid::new_v4()));
        let record = record();
        {
            let db = Db::open(path.to_str().unwrap()).unwrap();
            db.init_coop_exits().await.unwrap();
            db.insert_coop_exit(&record, "coin:0").await.unwrap();
            let mut competing = record.clone();
            competing.id = Uuid::new_v4().to_string();
            competing.transfer_id = Uuid::new_v4().to_string();
            competing.idempotency_key = Uuid::new_v4().to_string();
            assert!(db.insert_coop_exit(&competing, "coin:0").await.is_err());
            assert!(db
                .get_request(&competing.id, "alice")
                .await
                .unwrap()
                .is_none());
            db.prune_compat_requests("9999-01-01T00:00:00+00:00")
                .await
                .unwrap();
        }
        let db = Db::open(path.to_str().unwrap()).unwrap();
        db.init_coop_exits().await.unwrap();
        assert!(db.get_request(&record.id, "alice").await.unwrap().is_some());
        assert!(db.coop_exit(&record.id, "bob").await.unwrap().is_none());
        assert_eq!(
            db.coop_reserved_inputs().await.unwrap(),
            HashSet::from([
                OutPoint::new(
                    deserialize::<Transaction>(&hex::decode(&record.raw_exit).unwrap())
                        .unwrap()
                        .compute_txid(),
                    2
                )
                .to_string(),
                "coin:0".into(),
                OutPoint::new(
                    deserialize::<Transaction>(&hex::decode(&record.raw_exit).unwrap())
                        .unwrap()
                        .compute_txid(),
                    1,
                )
                .to_string(),
            ])
        );
        assert_eq!(db.pending_coop_exits().await.unwrap().len(), 1);
        drop(db);
        std::fs::remove_dir_all(path).unwrap();
    }

    struct MockSpark {
        valid: AtomicBool,
        fail_claim: AtomicBool,
        calls: Mutex<Vec<&'static str>>,
    }
    #[async_trait::async_trait]
    impl CoopSpark for MockSpark {
        async fn leaves(&self, _: &str, ids: &[String]) -> Result<Vec<ExitLeaf>, String> {
            Ok(ids
                .iter()
                .map(|id| ExitLeaf {
                    id: id.clone(),
                    value: 210,
                })
                .collect())
        }
        async fn transfer_exists(&self, _: &str) -> Result<bool, String> {
            Ok(self.valid.load(Ordering::SeqCst))
        }
        async fn verify(&self, _: &ExitRecord) -> Result<(), String> {
            self.calls.lock().unwrap().push("verify");
            if self.valid.load(Ordering::SeqCst) {
                Ok(())
            } else {
                Err("unfunded".into())
            }
        }
        async fn claim(&self, _: &ExitRecord) -> Result<(), String> {
            self.calls.lock().unwrap().push("claim");
            if self.fail_claim.swap(false, Ordering::SeqCst) {
                Err("claim reply lost".into())
            } else {
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn payout_conserves_value_for_full_and_separate_fee_withdrawals() {
        let record = record();
        let spark = MockSpark {
            valid: AtomicBool::new(false),
            fail_claim: AtomicBool::new(false),
            calls: Mutex::new(vec![]),
        };
        let mut input = ExitInput {
            leaf_external_ids: record.leaves.iter().map(|leaf| leaf.id.clone()).collect(),
            withdrawal_address: record.quote.address.clone(),
            exit_speed: "FAST".into(),
            withdraw_all: true,
            fee_leaf_external_ids: None,
            fee_quote_id: Some(record.quote.id.clone()),
            idempotency_key: None,
            user_outbound_transfer_external_id: Some(record.transfer_id.clone()),
        };
        let mut leaves = record.leaves.clone();
        let payout = withdrawal_amount(&input, &record.quote, 0, &mut leaves, &spark, "alice")
            .await
            .unwrap();
        assert_eq!(payout, 9_790);
        assert_eq!(total(&leaves).unwrap(), payout + record.quote.fees[0]);
        let mut excessive_fee = record.quote.clone();
        excessive_fee.user_fee = 10_000;
        assert!(
            withdrawal_amount(&input, &excessive_fee, 0, &mut leaves, &spark, "alice")
                .await
                .is_err()
        );

        input.fee_leaf_external_ids = Some(vec![Uuid::new_v4().to_string()]);
        assert!(
            withdrawal_amount(&input, &record.quote, 0, &mut leaves, &spark, "alice")
                .await
                .is_err()
        );
        input.withdraw_all = false;
        let payout = withdrawal_amount(&input, &record.quote, 0, &mut leaves, &spark, "alice")
            .await
            .unwrap();
        assert_eq!(payout, 10_000);
        assert_eq!(total(&leaves).unwrap(), payout + record.quote.fees[0]);

        // A payout leaf cannot also pay the fee.
        input.fee_leaf_external_ids = Some(vec![record.leaves[0].id.clone()]);
        assert!(withdrawal_amount(
            &input,
            &record.quote,
            0,
            &mut record.leaves.clone(),
            &spark,
            "alice"
        )
        .await
        .is_err());
        let mut wrong_fee = record.quote.clone();
        wrong_fee.fees[0] += 1;
        assert!(withdrawal_amount(
            &input,
            &wrong_fee,
            0,
            &mut record.leaves.clone(),
            &spark,
            "alice"
        )
        .await
        .is_err());
    }

    struct MockBitcoin {
        db: Arc<Db>,
        record: ExitRecord,
        calls: Mutex<Vec<String>>,
        confirmations: AtomicI64,
        lose_broadcast_reply: AtomicBool,
    }
    async fn rpc(State(state): State<Arc<MockBitcoin>>, Json(request): Json<Value>) -> Json<Value> {
        let method = request["method"].as_str().unwrap();
        state.calls.lock().unwrap().push(method.into());
        let result = match method {
            "signrawtransactionwithwallet" => {
                let mut tx: Transaction =
                    deserialize(&hex::decode(&state.record.raw_exit).unwrap()).unwrap();
                tx.input[0].witness.push([1; 64]);
                json!({"complete":true,"hex":serialize_hex(&tx)})
            }
            "gettransaction" => json!({"confirmations":state.confirmations.load(Ordering::SeqCst)}),
            "sendrawtransaction" => {
                let persisted = state
                    .db
                    .coop_exit(&state.record.id, &state.record.owner)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(persisted.status, "INBOUND_TRANSFER_CHECKED");
                assert_eq!(
                    persisted.signed_exit.as_deref(),
                    request["params"][0].as_str()
                );
                state.confirmations.store(6, Ordering::SeqCst);
                if state.lose_broadcast_reply.swap(false, Ordering::SeqCst) {
                    return Json(
                        json!({"error":{"code":-1,"message":"reply lost after acceptance"}}),
                    );
                }
                json!("txid")
            }
            _ => panic!("unexpected Bitcoin RPC {method}"),
        };
        Json(json!({"result":result,"error":null}))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lost_broadcast_and_claim_replies_resume_the_same_withdrawal() {
        let path = std::env::temp_dir().join(format!("ssp-coop-{}", Uuid::new_v4()));
        let db = Arc::new(Db::open(path.to_str().unwrap()).unwrap());
        db.init_coop_exits().await.unwrap();
        let mut record = record();
        db.insert_coop_exit(&record, "coin:0").await.unwrap();
        let bitcoin = Arc::new(MockBitcoin {
            db: db.clone(),
            record: record.clone(),
            calls: Mutex::new(vec![]),
            confirmations: AtomicI64::new(0),
            lose_broadcast_reply: AtomicBool::new(true),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new()
            .route("/", post(rpc))
            .with_state(bitcoin.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let spark = Arc::new(MockSpark {
            valid: AtomicBool::new(false),
            fail_claim: AtomicBool::new(true),
            calls: Mutex::new(vec![]),
        });
        let service = CoopExitService {
            db: db.clone(),
            spark: spark.clone(),
            bitcoin: BitcoinWallet {
                http: reqwest::Client::new(),
                url,
                user: "test".into(),
                password: "test".into(),
            },
            network: Network::Regtest,
            network_name: "REGTEST".into(),
            user_fee: 0,
            lock: tokio::sync::Mutex::new(()),
        };
        assert!(service
            .advance(&mut record)
            .await
            .unwrap_err()
            .contains("unfunded"));
        assert!(bitcoin.calls.lock().unwrap().is_empty());
        spark.valid.store(true, Ordering::SeqCst);
        assert!(service.advance(&mut record).await.is_err());
        // Rebuild all per-request state from the database, as on restart.
        let mut restored = db
            .coop_exit(&record.id, &record.owner)
            .await
            .unwrap()
            .unwrap();
        assert!(restored.signed_exit.is_some());
        assert!(service
            .advance(&mut restored)
            .await
            .unwrap_err()
            .contains("claim reply lost"));
        assert!(!db.coop_reserved_inputs().await.unwrap().is_empty());
        let mut restored = db
            .coop_exit(&record.id, &record.owner)
            .await
            .unwrap()
            .unwrap();
        service.advance(&mut restored).await.unwrap();
        assert_eq!(restored.status, "SUCCEEDED");
        assert!(db.coop_reserved_inputs().await.unwrap().is_empty());
        service.advance(&mut restored).await.unwrap();
        let calls = bitcoin.calls.lock().unwrap().clone();
        assert_eq!(
            calls
                .iter()
                .filter(|method| *method == "signrawtransactionwithwallet")
                .count(),
            1
        );
        assert_eq!(
            calls
                .iter()
                .filter(|method| *method == "sendrawtransaction")
                .count(),
            1
        );
        assert_eq!(
            *spark.calls.lock().unwrap(),
            vec!["verify", "verify", "claim", "claim"]
        );
        server.abort();
        drop(service);
        drop(bitcoin);
        drop(db);
        std::fs::remove_dir_all(path).unwrap();
    }
}
