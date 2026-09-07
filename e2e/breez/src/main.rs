mod regtest;

use std::{env, path::PathBuf, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use breez_sdk_spark::{
    BreezSdk, ChainApiType, FeePolicy, GetInfoRequest, GetPaymentRequest, ListPaymentsRequest,
    Network, OnchainConfirmationSpeed, Payment, PaymentDetails, PaymentRequest, PaymentStatus,
    PaymentType, PrepareSendPaymentRequest, ReceivePaymentMethod, ReceivePaymentRequest,
    SdkBuilder, Seed, SendPaymentMethod, SendPaymentOptions, SendPaymentRequest,
    SignMessageRequest, SparkConfig, SparkSigningOperator, SparkSspConfig, SyncWalletRequest,
    default_config,
};
use reqwest::Client;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::{process::Command, time::Instant};

const OPERATOR_IDENTITIES: [&str; 3] = [
    "0322ca18fc489ae25418a0e768273c2c61cabb823edfb14feb891e9bec62016510",
    "0341727a6c41b168f07eb50865ab8c397a53c7eef628ac1020956b705e43b6cb27",
    "0305ab8d485cc752394de4981f8a5ae004f2becfea6f432c9a59d5022d8764f0a6",
];

struct SdkLogger;

impl breez_sdk_spark::Logger for SdkLogger {
    fn log(&self, entry: breez_sdk_spark::LogEntry) {
        eprintln!("Breez {}: {}", entry.level, entry.line);
    }
}

struct TestConfig {
    admin_token: String,
    ssp_container: String,
    bitcoin_rpc_url: String,
    bitcoin_rpc_user: String,
    bitcoin_rpc_password: String,
    bitcoin_rpc_wallet: String,
    chain_service_url: String,
    cert_dir: PathBuf,
    send_amount_sats: u64,
    receive_amount_sats: u64,
    repeated_receive_amount_sats: u64,
    ssp_funding_leaf_sats: u64,
    timeout: Duration,
}

#[derive(Clone)]
struct LdkClient {
    container: String,
    api_key: String,
}

struct Wallet {
    name: &'static str,
    sdk: BreezSdk,
    _storage: TempDir,
    ssp_url: &'static str,
    ldk: LdkClient,
}

fn optional_env(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

impl TestConfig {
    fn from_env(admin_token: String, cert_dir: PathBuf, ssp_container: String) -> Result<Self> {
        let send_amount_sats = optional_env("BREEZ_SEND_AMOUNT_SATS", "1000")
            .parse()
            .context("BREEZ_SEND_AMOUNT_SATS is not an integer")?;
        let receive_amount_sats = optional_env("BREEZ_RECEIVE_AMOUNT_SATS", "2000")
            .parse()
            .context("BREEZ_RECEIVE_AMOUNT_SATS is not an integer")?;
        let repeated_receive_amount_sats =
            optional_env("BREEZ_REPEATED_RECEIVE_AMOUNT_SATS", "1333")
                .parse()
                .context("BREEZ_REPEATED_RECEIVE_AMOUNT_SATS is not an integer")?;
        let ssp_funding_leaf_sats: u64 = optional_env("BREEZ_SSP_FUNDING_LEAF_SATS", "5000")
            .parse()
            .context("BREEZ_SSP_FUNDING_LEAF_SATS is not an integer")?;
        let timeout_secs = optional_env("BREEZ_E2E_TIMEOUT_SECS", "180")
            .parse()
            .context("BREEZ_E2E_TIMEOUT_SECS is not an integer")?;
        ensure!(
            send_amount_sats > 0,
            "BREEZ_SEND_AMOUNT_SATS must be positive"
        );
        ensure!(
            receive_amount_sats > 0,
            "BREEZ_RECEIVE_AMOUNT_SATS must be positive"
        );
        ensure!(
            repeated_receive_amount_sats > 0,
            "BREEZ_REPEATED_RECEIVE_AMOUNT_SATS must be positive"
        );
        ensure!(
            ssp_funding_leaf_sats > receive_amount_sats + repeated_receive_amount_sats,
            "BREEZ_SSP_FUNDING_LEAF_SATS must cover both split receives"
        );

        Ok(Self {
            admin_token,
            ssp_container,
            bitcoin_rpc_url: optional_env(
                "BITCOIN_RPC_URL",
                &format!(
                    "http://127.0.0.1:{}",
                    optional_env("BITCOIN_RPC_PORT", "8332")
                ),
            ),
            bitcoin_rpc_user: optional_env("BITCOIN_RPC_USER", "testutil"),
            bitcoin_rpc_password: optional_env("BITCOIN_RPC_PASSWORD", "testutilpassword"),
            bitcoin_rpc_wallet: optional_env("BITCOIN_RPC_WALLET", "default"),
            chain_service_url: optional_env("BREEZ_CHAIN_SERVICE_URL", "http://127.0.0.1:30000"),
            cert_dir,
            send_amount_sats,
            receive_amount_sats,
            repeated_receive_amount_sats,
            ssp_funding_leaf_sats,
            timeout: Duration::from_secs(timeout_secs),
        })
    }
}

async fn command_output(program: &str, args: &[&str]) -> Result<String> {
    let output = Command::new(program)
        .kill_on_drop(true)
        .args(args)
        .output()
        .await
        .with_context(|| format!("could not run {program}"))?;
    ensure!(
        output.status.success(),
        "{} failed: {}",
        program,
        String::from_utf8_lossy(&output.stderr).trim()
    );
    String::from_utf8(output.stdout).context("command output was not UTF-8")
}

impl LdkClient {
    async fn connect(container: String) -> Result<Self> {
        let output = Command::new("docker")
            .kill_on_drop(true)
            .args(["exec", &container, "cat", "/data/regtest/api_key"])
            .output()
            .await?;
        ensure!(output.status.success(), "could not read the LDK API key");
        ensure!(!output.stdout.is_empty(), "LDK API key is empty");
        let key = hex::encode(output.stdout);
        Ok(Self {
            container,
            api_key: key,
        })
    }

    async fn output(&self, args: &[&str]) -> Result<String> {
        let mut command_args = vec![
            "exec",
            self.container.as_str(),
            "ldk-server-cli",
            "--base-url",
            "localhost:3536",
            "--api-key",
            self.api_key.as_str(),
            "--tls-cert",
            "/data/tls.crt",
        ];
        command_args.extend_from_slice(args);
        command_output("docker", &command_args).await
    }

    async fn json(&self, args: &[&str]) -> Result<Value> {
        serde_json::from_str(&self.output(args).await?).context("LDK CLI output was not JSON")
    }
}

async fn http_json(
    client: &Client,
    method: reqwest::Method,
    url: &str,
    body: Option<Value>,
    bearer: Option<&str>,
) -> Result<Value> {
    let mut request = client
        .request(method.clone(), url)
        .header("Accept", "application/json");
    if let Some(body) = body {
        request = request.json(&body);
    }
    if let Some(token) = bearer {
        request = request.bearer_auth(token);
    }
    let response = request
        .send()
        .await
        .with_context(|| format!("{method} {url} failed"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .context("could not read HTTP response")?;
    ensure!(
        status.is_success(),
        "{method} {url} failed: HTTP {status}: {text}"
    );
    serde_json::from_str(&text).with_context(|| format!("{method} {url} did not return JSON"))
}

async fn admin_json(
    client: &Client,
    config: &TestConfig,
    ssp_url: &str,
    path: &str,
    body: Option<Value>,
) -> Result<Value> {
    let method = if body.is_some() {
        reqwest::Method::POST
    } else {
        reqwest::Method::GET
    };
    http_json(
        client,
        method,
        &format!("{}{path}", ssp_url.trim_end_matches('/')),
        body,
        Some(&config.admin_token),
    )
    .await
}

async fn graphql_json(
    client: &Client,
    wallet: &Wallet,
    session: Option<&str>,
    operation: &str,
    variables: Value,
) -> Result<Value> {
    let url = format!("{}/graphql/spark/rc", wallet.ssp_url);
    let mut request = client.post(&url).json(&json!({
        "operationName": operation,
        "query": format!("mutation {operation} {{ result }}"),
        "variables": variables,
    }));
    if let Some(session) = session {
        request = request.bearer_auth(session);
    }
    let response = request
        .send()
        .await
        .with_context(|| format!("{operation} request failed"))?;
    let status = response.status();
    let body: Value = response
        .json()
        .await
        .with_context(|| format!("{operation} returned invalid JSON"))?;
    ensure!(
        status.is_success() && body.get("errors").is_none(),
        "{operation} failed with HTTP {status}: {body}"
    );
    Ok(body["data"].clone())
}

async fn authenticate_wallet(client: &Client, wallet: &Wallet) -> Result<String> {
    let identity = wallet
        .sdk
        .sign_message(SignMessageRequest {
            message: "identity-probe".to_string(),
            compact: false,
        })
        .await?;
    let challenge = graphql_json(
        client,
        wallet,
        None,
        "GetChallenge",
        json!({ "public_key": identity.pubkey }),
    )
    .await?["get_challenge"]["protected_challenge"]
        .as_str()
        .context("GetChallenge returned no protected challenge")?
        .to_string();
    let decoded = URL_SAFE_NO_PAD
        .decode(&challenge)
        .context("SSP challenge is not base64url")?;
    let signed = wallet
        .sdk
        .sign_message(SignMessageRequest {
            message: String::from_utf8(decoded).context("SSP challenge is not UTF-8")?,
            compact: false,
        })
        .await?;
    let verified = graphql_json(
        client,
        wallet,
        None,
        "VerifyChallenge",
        json!({ "input": {
            "identity_public_key": signed.pubkey,
            "protected_challenge": challenge,
            "signature": signed.signature,
        }}),
    )
    .await?;
    verified["verify_challenge"]["session_token"]
        .as_str()
        .context("VerifyChallenge returned no session token")
        .map(str::to_string)
}

async fn bitcoin_rpc(
    client: &Client,
    config: &TestConfig,
    method: &str,
    params: Value,
) -> Result<Value> {
    let url = format!(
        "{}/wallet/{}",
        config.bitcoin_rpc_url.trim_end_matches('/'),
        config.bitcoin_rpc_wallet
    );
    let response = client
        .post(&url)
        .basic_auth(&config.bitcoin_rpc_user, Some(&config.bitcoin_rpc_password))
        .json(&json!({
            "jsonrpc": "1.0",
            "id": method,
            "method": method,
            "params": params,
        }))
        .send()
        .await
        .with_context(|| format!("bitcoind {method} request failed"))?;
    let status = response.status();
    let value: Value = response
        .json()
        .await
        .context("bitcoind returned invalid JSON")?;
    ensure!(
        status.is_success(),
        "bitcoind {method} returned HTTP {status}: {value}"
    );
    if !value["error"].is_null() {
        bail!("bitcoind {method}: {}", value["error"]);
    }
    Ok(value["result"].clone())
}

fn local_config(
    config: &TestConfig,
    ssp_url: &str,
    ssp_identity: &str,
) -> Result<breez_sdk_spark::Config> {
    let mut sdk_config = default_config(Network::Regtest);
    sdk_config.api_key = None;
    sdk_config.lnurl_domain = None;
    sdk_config.sync_interval_secs = 2;
    sdk_config.real_time_sync_server_url = None;
    sdk_config.prefer_spark_over_lightning = false;
    sdk_config.use_default_external_input_parsers = false;
    sdk_config.private_enabled_default = false;
    sdk_config.leaf_optimization_config.auto_enabled = false;
    sdk_config.token_optimization_config.auto_enabled = false;

    let current = sdk_config
        .spark_config
        .as_ref()
        .context("Breez regtest config has no Spark configuration")?;
    let mut signing_operators = Vec::with_capacity(3);
    for (id, identity_public_key) in OPERATOR_IDENTITIES.iter().enumerate() {
        let cert_path = config.cert_dir.join(format!("server_{id}.crt"));
        let ca_cert_pem = std::fs::read_to_string(&cert_path)
            .with_context(|| format!("could not read {}", cert_path.display()))?;
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
            base_url: ssp_url.to_string(),
            identity_public_key: ssp_identity.to_string(),
            schema_endpoint: Some("graphql/spark/rc".to_string()),
        },
        expected_withdraw_bond_sats: current.expected_withdraw_bond_sats,
        expected_withdraw_relative_block_locktime: current
            .expected_withdraw_relative_block_locktime,
        max_token_transaction_inputs: None,
    });
    Ok(sdk_config)
}

async fn connect_wallet(
    client: &Client,
    config: &TestConfig,
    name: &'static str,
    ssp_url: &'static str,
    ldk: LdkClient,
    seed_byte: u8,
) -> Result<Wallet> {
    let status = admin_json(client, config, ssp_url, "/status", None).await?;
    ensure!(
        status["ldk_mode"] == "live",
        "{name} SSP does not use live Lightning"
    );
    let identity = status["ssp_identity_pubkey"]
        .as_str()
        .context("SSP status has no identity public key")?;
    let storage = tempfile::Builder::new()
        .prefix(&format!("breez-{name}-"))
        .tempdir()
        .context("could not create Breez storage directory")?;
    let sdk = SdkBuilder::new(
        local_config(config, ssp_url, identity)?,
        Seed::Entropy(vec![seed_byte; 32]),
    )
    .with_default_storage(storage.path().to_string_lossy().into_owned())
    .with_rest_chain_service(
        config.chain_service_url.clone(),
        ChainApiType::Esplora,
        None,
    )
    .build()
    .await
    .with_context(|| format!("could not connect {name}"))?;
    sdk.get_info(GetInfoRequest {
        ensure_synced: Some(true),
    })
    .await
    .with_context(|| format!("could not sync {name}"))?;

    Ok(Wallet {
        name,
        sdk,
        _storage: storage,
        ssp_url,
        ldk,
    })
}

async fn wallet_balance(wallet: &Wallet) -> Result<u64> {
    wallet.sdk.sync_wallet(SyncWalletRequest {}).await?;
    Ok(wallet
        .sdk
        .get_info(GetInfoRequest {
            ensure_synced: Some(false),
        })
        .await?
        .balance_sats)
}

async fn ssp_available_balance(client: &Client, config: &TestConfig, ssp_url: &str) -> Result<u64> {
    let status = admin_json(client, config, ssp_url, "/status", None).await?;
    status["spark"]["available_sats"]
        .as_u64()
        .context("SSP status has no available Spark balance")
}

async fn restart_ssp(client: &Client, config: &TestConfig, ssp_url: &str) -> Result<()> {
    let container = &config.ssp_container;
    command_output("docker", &["restart", container]).await?;
    poll("SSP restart", config.timeout, || async {
        admin_json(client, config, ssp_url, "/status", None)
            .await
            .map(|_| ())
    })
    .await
}

async fn fund_ssp(
    client: &Client,
    config: &TestConfig,
    ssp_url: &str,
    amount_sats: u64,
) -> Result<()> {
    let balance_before = ssp_available_balance(client, config, ssp_url).await?;
    let balance_after = balance_before
        .checked_add(amount_sats)
        .context("expected SSP balance overflow")?;
    let deposit = admin_json(
        client,
        config,
        ssp_url,
        "/admin/spark/deposit-address",
        Some(json!({})),
    )
    .await?;
    let address = deposit["address"]
        .as_str()
        .context("SSP deposit response has no address")?;
    let txid = send_regtest_deposit(client, config, address, amount_sats).await?;
    let miner_address = bitcoin_rpc(client, config, "getnewaddress", json!([]))
        .await?
        .as_str()
        .context("getnewaddress did not return an address")?
        .to_string();
    bitcoin_rpc(
        client,
        config,
        "generatetoaddress",
        json!([3, miner_address]),
    )
    .await?;
    let tx = bitcoin_rpc(client, config, "getrawtransaction", json!([txid, true])).await?;
    let output = tx["vout"]
        .as_array()
        .context("transaction has no outputs")?
        .iter()
        .find(|output| {
            output["scriptPubKey"]["address"].as_str() == Some(address)
                || output["scriptPubKey"]["addresses"]
                    .as_array()
                    .is_some_and(|addresses| {
                        addresses
                            .iter()
                            .any(|value| value.as_str() == Some(address))
                    })
        })
        .context("transaction has no SSP deposit output")?;
    let vout = output["n"]
        .as_u64()
        .context("deposit output has no index")?;
    let transaction_hex = tx["hex"]
        .as_str()
        .context("getrawtransaction did not return transaction hex")?;
    admin_json(
        client,
        config,
        ssp_url,
        "/admin/spark/claim-deposit",
        Some(json!({ "transaction_hex": transaction_hex, "vout": vout })),
    )
    .await?;
    poll("SSP deposit availability", config.timeout, || async {
        let available = ssp_available_balance(client, config, ssp_url).await?;
        ensure!(
            available >= balance_after,
            "SSP available balance is {available}; expected at least {balance_after}"
        );
        Ok(())
    })
    .await?;
    Ok(())
}

async fn send_regtest_deposit(
    client: &Client,
    config: &TestConfig,
    address: &str,
    amount_sats: u64,
) -> Result<String> {
    const FEE_SATS: u64 = 1_000;

    let unspent = bitcoin_rpc(client, config, "listunspent", json!([1])).await?;
    let input = unspent
        .as_array()
        .context("listunspent did not return an array")?
        .iter()
        .find(|utxo| utxo["spendable"].as_bool() == Some(true))
        .context("Bitcoin wallet has no confirmed spendable output")?;
    let input_sats = (input["amount"]
        .as_f64()
        .context("listunspent output has no amount")?
        * 100_000_000.0)
        .round() as u64;
    ensure!(
        input_sats > amount_sats + FEE_SATS,
        "Bitcoin wallet output cannot fund the SSP deposit"
    );

    let change_address = bitcoin_rpc(client, config, "getrawchangeaddress", json!([]))
        .await?
        .as_str()
        .context("getrawchangeaddress did not return an address")?
        .to_string();
    let mut outputs = serde_json::Map::new();
    outputs.insert(
        address.to_string(),
        json!(amount_sats as f64 / 100_000_000.0),
    );
    outputs.insert(
        change_address,
        json!((input_sats - amount_sats - FEE_SATS) as f64 / 100_000_000.0),
    );
    let raw = bitcoin_rpc(
        client,
        config,
        "createrawtransaction",
        json!([[{
            "txid": input["txid"],
            "vout": input["vout"],
            "sequence": 0
        }], Value::Object(outputs)]),
    )
    .await?
    .as_str()
    .context("createrawtransaction did not return transaction hex")?
    .to_string();
    let signed = bitcoin_rpc(client, config, "signrawtransactionwithwallet", json!([raw])).await?;
    ensure!(
        signed["complete"].as_bool() == Some(true),
        "Bitcoin wallet did not complete the deposit signature"
    );
    bitcoin_rpc(
        client,
        config,
        "sendrawtransaction",
        json!([signed["hex"], 0]),
    )
    .await?
    .as_str()
    .context("sendrawtransaction did not return a transaction ID")
    .map(str::to_string)
}

fn bolt11_hash(payment: &Value) -> Option<String> {
    payment["kind"]["kind"]["bolt11"]["hash"]
        .as_str()
        .or_else(|| payment["kind"]["bolt11"]["hash"].as_str())
        .map(str::to_lowercase)
}

fn bolt12_offer(payment: &Value) -> Option<&Value> {
    payment["kind"]["kind"]["bolt12_offer"]
        .as_object()
        .map(|_| &payment["kind"]["kind"]["bolt12_offer"])
        .or_else(|| {
            payment["kind"]["bolt12_offer"]
                .as_object()
                .map(|_| &payment["kind"]["bolt12_offer"])
        })
}

async fn succeeded_bolt12_payment(
    ldk: &LdkClient,
    offer_id: &str,
    direction: &str,
    amount_sats: u64,
) -> Result<Value> {
    let payments = ldk.json(&["list-payments"]).await?;
    let matches: Vec<&Value> = payments["list"]
        .as_array()
        .context("LDK list-payments response has no list")?
        .iter()
        .filter(|payment| {
            bolt12_offer(payment).and_then(|offer| offer["offer_id"].as_str()) == Some(offer_id)
                && payment["direction"].as_str() == Some(direction)
        })
        .collect();
    ensure!(
        matches.len() == 1,
        "expected one {direction} BOLT12 payment for offer {offer_id}; found {}",
        matches.len()
    );
    let payment = matches[0];
    let expected_msat = amount_sats * 1000;
    let amount_matches = match direction {
        "INBOUND" => payment["amount_msat"]
            .as_u64()
            .is_some_and(|amount| amount >= expected_msat),
        _ => payment["amount_msat"].as_u64() == Some(expected_msat),
    };
    ensure!(
        payment["status"] == "SUCCEEDED" && amount_matches,
        "BOLT12 payment is not complete: {payment}"
    );
    let details = bolt12_offer(payment).context("BOLT12 payment has no offer details")?;
    let hash = details["hash"]
        .as_str()
        .context("BOLT12 payment has no hash")?;
    let preimage = details["preimage"]
        .as_str()
        .context("BOLT12 payment has no preimage")?;
    ensure!(
        hex::encode(Sha256::digest(hex::decode(preimage)?)) == hash,
        "BOLT12 payment preimage does not match its hash"
    );
    Ok(payment.clone())
}

async fn succeeded_ldk_payment(
    ldk: &LdkClient,
    payment_hash: &str,
    direction: &str,
    amount_sats: u64,
) -> Result<Value> {
    let payments = ldk.json(&["list-payments"]).await?;
    let matches: Vec<&Value> = payments["list"]
        .as_array()
        .context("LDK list-payments response has no list")?
        .iter()
        .filter(|payment| {
            bolt11_hash(payment).as_deref() == Some(payment_hash)
                && payment["direction"].as_str() == Some(direction)
        })
        .collect();
    ensure!(
        matches.len() == 1,
        "expected one {direction} LDK payment for {payment_hash}; found {}",
        matches.len()
    );
    let payment = matches[0];
    ensure!(
        payment["status"] == "SUCCEEDED"
            && payment["amount_msat"].as_u64() == Some(amount_sats * 1000),
        "LDK payment is not complete: {payment}"
    );
    Ok(payment.clone())
}

async fn bolt11_payment_count(ldk: &LdkClient, payment_hash: &str) -> Result<usize> {
    let payments = ldk.json(&["list-payments"]).await?;
    Ok(payments["list"]
        .as_array()
        .context("LDK list-payments response has no list")?
        .iter()
        .filter(|payment| bolt11_hash(payment).as_deref() == Some(payment_hash))
        .count())
}

async fn received_payment(wallet: &Wallet, invoice: &str) -> Result<Payment> {
    let payments = wallet
        .sdk
        .list_payments(ListPaymentsRequest::default())
        .await?;
    let matches: Vec<&Payment> = payments
        .payments
        .iter()
        .filter(|payment| {
            payment.payment_type == PaymentType::Receive
                && matches!(
                    &payment.details,
                    Some(PaymentDetails::Lightning { invoice: value, .. }) if value == invoice
                )
        })
        .collect();
    ensure!(
        matches.len() == 1,
        "{} has {} receives for the invoice",
        wallet.name,
        matches.len()
    );
    let payment = matches[0];
    ensure!(
        payment.status == PaymentStatus::Completed,
        "{} receive is {}",
        wallet.name,
        payment.status
    );
    Ok(payment.clone())
}

async fn completed_payment(wallet: &Wallet, payment_id: &str) -> Result<Payment> {
    let payment = wallet
        .sdk
        .get_payment(GetPaymentRequest {
            payment_id: payment_id.to_string(),
        })
        .await?
        .payment;
    ensure!(
        payment.status != PaymentStatus::Failed,
        "{} payment {payment_id} failed",
        wallet.name
    );
    ensure!(
        payment.status == PaymentStatus::Completed,
        "{} payment {payment_id} is {}",
        wallet.name,
        payment.status
    );
    Ok(payment)
}

async fn poll<T, F, Fut>(label: &str, timeout: Duration, mut check: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let deadline = Instant::now() + timeout;
    let mut last_error = None;
    while Instant::now() < deadline {
        match check().await {
            Ok(value) => return Ok(value),
            Err(error) => last_error = Some(error),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    match last_error {
        Some(error) => Err(error).with_context(|| format!("{label} timed out")),
        None => bail!("{label} timed out"),
    }
}

async fn exact_balance(wallet: &Wallet, expected: u64) -> Result<u64> {
    let balance = wallet_balance(wallet).await?;
    ensure!(
        balance == expected,
        "{} balance is {balance}; expected {expected}",
        wallet.name
    );
    Ok(balance)
}

async fn decode_payment_hash(ldk: &LdkClient, invoice: &str) -> Result<String> {
    let decoded = ldk.json(&["decode-invoice", invoice]).await?;
    Ok(decoded["payment_hash"]
        .as_str()
        .context("decoded invoice has no payment hash")?
        .to_lowercase())
}

async fn setup_lightning(
    client: &Client,
    config: &TestConfig,
    ldk_a: &LdkClient,
    ldk_b: &LdkClient,
) -> Result<()> {
    let node_a = ldk_a.json(&["get-node-info"]).await?;
    let node_b = ldk_b.json(&["get-node-info"]).await?;
    let node_b_id = node_b["node_id"]
        .as_str()
        .context("second LDK node has no node ID")?;
    ensure!(
        node_a["node_id"].as_str().is_some(),
        "first LDK node has no node ID"
    );

    // Repeated `up` calls can reuse the existing chain funds and channel.
    for (node, minimum, amount_btc) in [(ldk_a, 100_000_000, 2), (ldk_b, 500_000, 1)] {
        let balances = node.json(&["get-balances"]).await?;
        if balances["spendable_onchain_balance_sats"]
            .as_u64()
            .unwrap_or(0)
            <= minimum
        {
            let address = node.json(&["onchain-receive"]).await?["address"]
                .as_str()
                .context("LDK node returned no on-chain address")?
                .to_owned();
            bitcoin_rpc(
                client,
                config,
                "sendtoaddress",
                json!([address, amount_btc]),
            )
            .await?;
        }
    }
    let miner_address = bitcoin_rpc(client, config, "getnewaddress", json!([]))
        .await?
        .as_str()
        .context("getnewaddress did not return an address")?
        .to_string();
    bitcoin_rpc(
        client,
        config,
        "generatetoaddress",
        json!([6, miner_address]),
    )
    .await?;
    poll("first Lightning node funding", config.timeout, || async {
        let balances = ldk_a.json(&["get-balances"]).await?;
        ensure!(
            balances["spendable_onchain_balance_sats"]
                .as_u64()
                .unwrap_or(0)
                > 100_000_000,
            "first Lightning node funding is not confirmed"
        );
        Ok(())
    })
    .await?;

    let _ = ldk_a
        .json(&["connect-peer", node_b_id, "ldk-server-2:9735", "--persist"])
        .await;
    let channels = ldk_a.json(&["list-channels"]).await?;
    let has_channel = channels["channels"].as_array().is_some_and(|channels| {
        channels
            .iter()
            .any(|channel| channel["counterparty_node_id"].as_str() == Some(node_b_id))
    });
    if !has_channel {
        ldk_a
            .json(&["open-channel", node_b_id, "ldk-server-2:9735", "2000000sat"])
            .await?;
        bitcoin_rpc(
            client,
            config,
            "generatetoaddress",
            json!([6, miner_address]),
        )
        .await?;
    }
    poll("Lightning channel readiness", config.timeout, || async {
        let response = ldk_a.json(&["list-channels"]).await?;
        let ready = response["channels"].as_array().is_some_and(|channels| {
            channels.iter().any(|channel| {
                channel["counterparty_node_id"].as_str() == Some(node_b_id)
                    && channel["is_channel_ready"].as_bool() == Some(true)
            })
        });
        ensure!(ready, "Lightning channel is not ready");
        Ok(())
    })
    .await?;

    let balances = ldk_b.json(&["get-balances"]).await?;
    if balances["total_lightning_balance_sats"]
        .as_u64()
        .unwrap_or(0)
        < 500_000
    {
        let receive = ldk_b
            .json(&["bolt11-receive", "500000sat", "-d", "bootstrap"])
            .await?;
        let invoice = receive["invoice"]
            .as_str()
            .context("liquidity invoice is missing")?;
        ldk_a.json(&["bolt11-send", invoice]).await?;
    }
    poll(
        "bidirectional Lightning liquidity",
        config.timeout,
        || async {
            let balances = ldk_b.json(&["get-balances"]).await?;
            ensure!(
                balances["total_lightning_balance_sats"]
                    .as_u64()
                    .unwrap_or(0)
                    >= 500_000,
                "second Lightning node has no outbound liquidity"
            );
            Ok(())
        },
    )
    .await?;
    println!("bidirectional Lightning liquidity ready");
    Ok(())
}

async fn bootstrap_wallet(
    wallet: &Wallet,
    payer: &LdkClient,
    amount_sats: u64,
    spark_credit_sats: u64,
    timeout: Duration,
) -> Result<()> {
    let balance_before = wallet_balance(wallet).await?;
    let balance_after = balance_before
        .checked_add(spark_credit_sats)
        .context("expected bootstrap balance overflow")?;
    let invoice = wallet
        .sdk
        .receive_payment(ReceivePaymentRequest {
            payment_method: ReceivePaymentMethod::Bolt11Invoice {
                description: "breez-bootstrap-receive".to_string(),
                amount_sats: Some(amount_sats),
                expiry_secs: Some(300),
                payment_hash: None,
            },
        })
        .await?
        .payment_request;
    let payment_hash = decode_payment_hash(&wallet.ldk, &invoice).await?;
    payer.json(&["bolt11-send", &invoice]).await?;

    let payment = poll("bootstrap Breez receive", timeout, || {
        received_payment(wallet, &invoice)
    })
    .await?;
    ensure!(
        payment.amount == u128::from(spark_credit_sats) && payment.fees == 0,
        "unexpected bootstrap payment: {payment:?}"
    );
    poll("bootstrap wallet balance", timeout, || {
        exact_balance(wallet, balance_after)
    })
    .await?;
    poll("bootstrap outbound LDK payment", timeout, || {
        succeeded_ldk_payment(payer, &payment_hash, "OUTBOUND", amount_sats)
    })
    .await?;
    poll("bootstrap inbound LDK payment", timeout, || {
        succeeded_ldk_payment(&wallet.ldk, &payment_hash, "INBOUND", amount_sats)
    })
    .await?;
    println!(
        "PASS exact receive: {} invoice was {amount_sats} sats; Spark credited {spark_credit_sats} sats",
        wallet.name
    );
    Ok(())
}

async fn pay_between(
    client: &Client,
    sender: &Wallet,
    receiver: &Wallet,
    amount_sats: u64,
    receiver_credit_sats: u64,
    label: &str,
    timeout: Duration,
) -> Result<String> {
    let sender_before = wallet_balance(sender).await?;
    let receiver_before = wallet_balance(receiver).await?;
    let invoice = receiver
        .sdk
        .receive_payment(ReceivePaymentRequest {
            payment_method: ReceivePaymentMethod::Bolt11Invoice {
                description: format!("breez-{label}"),
                amount_sats: Some(amount_sats),
                expiry_secs: Some(300),
                payment_hash: None,
            },
        })
        .await
        .with_context(|| format!("{label} receiver invoice creation failed"))?
        .payment_request;
    let payment_hash = decode_payment_hash(&receiver.ldk, &invoice)
        .await
        .with_context(|| format!("{label} invoice decoding failed"))?;
    let prepared = sender
        .sdk
        .prepare_send_payment(PrepareSendPaymentRequest {
            payment_request: PaymentRequest::Input {
                input: invoice.clone(),
            },
            amount: None,
            token_identifier: None,
            conversion_options: None,
            fee_policy: None,
        })
        .await
        .with_context(|| format!("{label} send preparation failed"))?;
    ensure!(
        prepared.amount == u128::from(amount_sats),
        "prepared amount is {}; expected {amount_sats}",
        prepared.amount
    );
    let sent = sender
        .sdk
        .send_payment(SendPaymentRequest {
            prepare_response: prepared,
            options: None,
            idempotency_key: None,
        })
        .await
        .with_context(|| format!("{label} send execution failed"))?;
    let sender_payment = poll(&format!("{label} Breez send"), timeout, || {
        completed_payment(sender, &sent.payment.id)
    })
    .await?;
    let receiver_payment = poll(&format!("{label} Breez receive"), timeout, || {
        received_payment(receiver, &invoice)
    })
    .await?;

    ensure!(
        sender_payment.payment_type == PaymentType::Send,
        "Breez sender did not record a send"
    );
    ensure!(
        sender_payment.amount == u128::from(amount_sats),
        "Breez send amount does not match the invoice"
    );
    ensure!(
        receiver_payment.amount == u128::from(receiver_credit_sats),
        "Breez receive amount is {}; expected {receiver_credit_sats} from the Spark transfer",
        receiver_payment.amount
    );
    ensure!(
        sender_payment.fees == 0 && receiver_payment.fees == 0,
        "local direct Lightning payment charged a fee"
    );
    let sender_expected = sender_before
        .checked_sub(amount_sats)
        .context("sender balance is smaller than the payment")?;
    let receiver_expected = receiver_before
        .checked_add(receiver_credit_sats)
        .context("receiver balance overflow")?;
    poll(&format!("{label} sender balance"), timeout, || {
        exact_balance(sender, sender_expected)
    })
    .await?;
    poll(&format!("{label} receiver balance"), timeout, || {
        exact_balance(receiver, receiver_expected)
    })
    .await?;
    poll(&format!("{label} outbound LDK payment"), timeout, || {
        succeeded_ldk_payment(&sender.ldk, &payment_hash, "OUTBOUND", amount_sats)
    })
    .await?;
    poll(&format!("{label} inbound LDK payment"), timeout, || {
        succeeded_ldk_payment(&receiver.ldk, &payment_hash, "INBOUND", amount_sats)
    })
    .await?;

    let session = authenticate_wallet(client, sender).await?;
    let transfers = graphql_json(
        client,
        sender,
        Some(&session),
        "Transfers",
        json!({"transfer_spark_ids": [sender_payment.id]}),
    )
    .await?;
    let request = &transfers["transfers"][0]["user_request"];
    let request_id = request["id"]
        .as_str()
        .context("send transfer has no SSP request")?;
    ensure!(
        request["idempotency_key"] == sender_payment.id,
        "send idempotency key does not match funding transfer: {request}"
    );
    let replay = graphql_json(
        client,
        sender,
        Some(&session),
        "RequestLightningSend",
        json!({"encoded_invoice":invoice,"user_outbound_transfer_external_id":sender_payment.id}),
    )
    .await?;
    ensure!(
        replay["request_lightning_send"]["request"]["id"] == request_id,
        "send replay changed the request ID"
    );
    ensure!(
        bolt11_payment_count(&sender.ldk, &payment_hash).await? == 1,
        "send replay created another payment"
    );
    exact_balance(sender, sender_expected).await?;
    println!("PASS send replay preserved request, payment count, and Spark balance");

    let htlc = match receiver_payment.details {
        Some(PaymentDetails::Lightning { htlc_details, .. }) => htlc_details,
        _ => bail!("Breez receive has no Lightning details"),
    };
    ensure!(
        htlc.payment_hash == payment_hash,
        "Breez HTLC payment hash does not match the invoice"
    );
    let preimage_hex = htlc.preimage.context("Breez receive has no preimage")?;
    let preimage = hex::decode(&preimage_hex).context("Breez preimage is not hex")?;
    ensure!(
        preimage.len() == 32 && hex::encode(Sha256::digest(&preimage)) == payment_hash,
        "LDK receive preimage does not match the wallet invoice hash"
    );

    println!(
        "PASS {label}: {} sent {amount_sats} sats to {}; Spark credited {receiver_credit_sats} sats through {payment_hash}",
        sender.name, receiver.name,
    );
    Ok(payment_hash)
}

async fn reject_unsafe_internal_payment(
    client: &Client,
    config: &TestConfig,
    sender: &Wallet,
    receiver: &Wallet,
    amount_sats: u64,
) -> Result<()> {
    ensure!(
        sender.ssp_url == receiver.ssp_url,
        "internal payment wallets do not use the same SSP"
    );
    let sender_before = wallet_balance(sender).await?;
    let receiver_before = wallet_balance(receiver).await?;
    let ssp_before = ssp_available_balance(client, config, sender.ssp_url).await?;
    let invoice = receiver
        .sdk
        .receive_payment(ReceivePaymentRequest {
            payment_method: ReceivePaymentMethod::Bolt11Invoice {
                description: "breez-internal-ssp-payment".to_string(),
                amount_sats: Some(amount_sats),
                expiry_secs: Some(300),
                payment_hash: None,
            },
        })
        .await?
        .payment_request;
    let payment_hash = decode_payment_hash(&receiver.ldk, &invoice).await?;
    ensure!(
        bolt11_payment_count(&sender.ldk, &payment_hash).await? == 0,
        "LDK already has a payment for the internal invoice"
    );

    let error = sender
        .sdk
        .prepare_send_payment(PrepareSendPaymentRequest {
            payment_request: PaymentRequest::Input {
                input: invoice.clone(),
            },
            amount: None,
            token_identifier: None,
            conversion_options: None,
            fee_policy: None,
        })
        .await
        .expect_err("unsafe internal invoice was accepted for funding");
    ensure!(
        error.to_string().contains("cannot be paid internally"),
        "unexpected internal payment error: {error}"
    );
    exact_balance(sender, sender_before).await?;
    exact_balance(receiver, receiver_before).await?;
    ensure!(
        ssp_available_balance(client, config, sender.ssp_url).await? == ssp_before,
        "rejected internal payment changed SSP balance"
    );
    ensure!(
        bolt11_payment_count(&sender.ldk, &payment_hash).await? == 0,
        "rejected internal payment created an LDK payment"
    );
    println!("PASS unsafe internal invoice rejected before funding: {payment_hash}");
    Ok(())
}

async fn send_bolt12(
    client: &Client,
    config: &TestConfig,
    sender: &Wallet,
    recipient_ldk: &LdkClient,
    amount_sats: u64,
) -> Result<()> {
    let sender_before = wallet_balance(sender).await?;
    let status = admin_json(client, config, sender.ssp_url, "/status", None).await?;
    let ssp_address = status["spark"]["address"]
        .as_str()
        .context("SSP status has no Spark address")?;
    let offer_response = recipient_ldk
        .json(&[
            "bolt12-receive",
            "breez-bolt12-send",
            &format!("{amount_sats}sat"),
            "--expiry-secs",
            "300",
        ])
        .await?;
    let offer = offer_response["offer"]
        .as_str()
        .context("LDK BOLT12 receive response has no offer")?;
    let offer_id = offer_response["offer_id"]
        .as_str()
        .context("LDK BOLT12 receive response has no offer ID")?;

    let prepared = sender
        .sdk
        .prepare_send_payment(PrepareSendPaymentRequest {
            payment_request: PaymentRequest::Input {
                input: ssp_address.to_string(),
            },
            amount: Some(u128::from(amount_sats)),
            token_identifier: None,
            conversion_options: None,
            fee_policy: None,
        })
        .await?;
    let funding = sender
        .sdk
        .send_payment(SendPaymentRequest {
            prepare_response: prepared,
            options: None,
            idempotency_key: None,
        })
        .await?
        .payment;
    ensure!(
        funding.status == PaymentStatus::Completed,
        "BOLT12 funding transfer is {}",
        funding.status
    );

    let session = authenticate_wallet(client, sender).await?;
    let response = graphql_json(
        client,
        sender,
        Some(&session),
        "RequestLightningSend",
        json!({ "input": {
            "encoded_invoice": offer,
            "amount_sats": amount_sats,
            "user_outbound_transfer_external_id": funding.id,
        }}),
    )
    .await?;
    let request = &response["request_lightning_send"]["request"];
    let request_id = request["id"]
        .as_str()
        .context("BOLT12 send response has no request ID")?
        .to_string();
    ensure!(
        request["status"] == "LIGHTNING_PAYMENT_INITIATED",
        "unexpected BOLT12 send status: {request}"
    );

    poll("BOLT12 SSP send", config.timeout, || async {
        let response = graphql_json(
            client,
            sender,
            Some(&session),
            "UserRequest",
            json!({ "request_id": request_id }),
        )
        .await?;
        ensure!(
            response["user_request"]["status"] != "LIGHTNING_PAYMENT_FAILED",
            "BOLT12 send failed"
        );
        ensure!(
            response["user_request"]["status"] == "LIGHTNING_PAYMENT_SUCCEEDED",
            "BOLT12 send is not complete"
        );
        Ok(())
    })
    .await?;
    poll("BOLT12 sender balance", config.timeout, || {
        exact_balance(sender, sender_before - amount_sats)
    })
    .await?;
    poll("BOLT12 outbound LDK payment", config.timeout, || {
        succeeded_bolt12_payment(&sender.ldk, offer_id, "OUTBOUND", amount_sats)
    })
    .await?;
    poll("BOLT12 inbound LDK payment", config.timeout, || {
        succeeded_bolt12_payment(recipient_ldk, offer_id, "INBOUND", amount_sats)
    })
    .await?;
    println!(
        "PASS BOLT12 send: {} prepaid and sent {amount_sats} sats for offer {offer_id}",
        sender.name
    );
    Ok(())
}

async fn receive_bolt12(
    client: &Client,
    config: &TestConfig,
    receiver: &Wallet,
    payer_ldk: &LdkClient,
    amount_sats: u64,
) -> Result<()> {
    let receiver_before = wallet_balance(receiver).await?;
    let session = authenticate_wallet(client, receiver).await?;
    let response = graphql_json(
        client,
        receiver,
        Some(&session),
        "RequestBolt12Receive",
        json!({ "input": {
            "amount_sats": amount_sats,
            "network": "REGTEST",
            "memo": "breez-bolt12-receive",
            "expiry_secs": 300,
        }}),
    )
    .await?;
    let request = &response["request_lightning_receive"]["request"];
    let request_id = request["id"]
        .as_str()
        .context("BOLT12 receive response has no request ID")?
        .to_string();
    let offer = request["invoice"]["encoded_invoice"]
        .as_str()
        .context("BOLT12 receive response has no offer")?;
    let offer_id = request["invoice"]["payment_hash"]
        .as_str()
        .context("BOLT12 receive response has no offer ID")?;
    ensure!(offer.to_ascii_lowercase().starts_with("lno1"));
    payer_ldk.json(&["bolt12-send", offer]).await?;

    poll("BOLT12 SSP receive", config.timeout, || async {
        let response = graphql_json(
            client,
            receiver,
            Some(&session),
            "UserRequest",
            json!({ "request_id": request_id }),
        )
        .await?;
        ensure!(
            response["user_request"]["status"] == "TRANSFER_COMPLETED",
            "BOLT12 receive is not complete"
        );
        Ok(())
    })
    .await?;
    poll("BOLT12 receiver balance", config.timeout, || {
        exact_balance(receiver, receiver_before + amount_sats)
    })
    .await?;
    poll("BOLT12 payer LDK payment", config.timeout, || {
        succeeded_bolt12_payment(payer_ldk, offer_id, "OUTBOUND", amount_sats)
    })
    .await?;
    poll("BOLT12 receiver LDK payment", config.timeout, || {
        succeeded_bolt12_payment(&receiver.ldk, offer_id, "INBOUND", amount_sats)
    })
    .await?;
    println!(
        "PASS BOLT12 receive: {} received {amount_sats} Spark sats for offer {offer_id}",
        receiver.name
    );
    Ok(())
}

async fn reject_request(
    client: &Client,
    wallet: &Wallet,
    session: Option<&str>,
    operation: &str,
    input: Value,
    expected: &str,
) -> Result<()> {
    let error = graphql_json(client, wallet, session, operation, input)
        .await
        .expect_err("request should have failed");
    ensure!(
        error.to_string().to_lowercase().contains(expected),
        "unexpected {operation} error: {error}"
    );
    Ok(())
}

async fn negative_lightning(
    client: &Client,
    config: &TestConfig,
    wallet: &Wallet,
    payer: &LdkClient,
) -> Result<()> {
    let before = wallet_balance(wallet).await?;
    let session = authenticate_wallet(client, wallet).await?;
    reject_request(
        client,
        wallet,
        None,
        "UserRequest",
        json!({"request_id":"missing"}),
        "unauthorized",
    )
    .await?;
    reject_request(
        client,
        wallet,
        Some(&session),
        "RequestLightningReceive",
        json!({"amount_sats":1000,"network":"REGTEST","payment_hash":"not-a-hash"}),
        "payment_hash must be 32 bytes hex",
    )
    .await?;
    let invoice = payer
        .json(&["bolt11-receive", "700sat", "-d", "negative-send"])
        .await?;
    let hash = invoice["payment_hash"]
        .as_str()
        .context("negative invoice has no hash")?;
    reject_request(
        client,
        wallet,
        Some(&session),
        "RequestLightningSend",
        json!({"encoded_invoice":invoice["invoice"]}),
        "user_outbound_transfer_external_id is required",
    )
    .await?;
    reject_request(client, wallet, Some(&session), "RequestLightningSend",
        json!({"encoded_invoice":invoice["invoice"],"user_outbound_transfer_external_id":"00000000-0000-4000-8000-000000000099"}), "matching preimage swap was not found").await?;
    ensure!(
        bolt11_payment_count(&wallet.ldk, hash).await? == 0,
        "unfunded send reached LDK"
    );
    // This unpaid invoice uses a client-created hash and needs no shares.
    let hash = hex::encode(Sha256::digest(b"expiry-only-test-preimage"));
    let expiring = graphql_json(
        client,
        wallet,
        Some(&session),
        "RequestLightningReceive",
        json!({"amount_sats":800,"network":"REGTEST","payment_hash":hash,"expiry_secs":1}),
    )
    .await?;
    let request_id = expiring["request_lightning_receive"]["request"]["id"]
        .as_str()
        .context("expiry request has no ID")?;
    poll("expired wallet invoice", config.timeout, || async {
        let response = graphql_json(
            client,
            wallet,
            Some(&session),
            "UserRequest",
            json!({"request_id":request_id}),
        )
        .await?;
        ensure!(
            response["user_request"]["status"] == "HTLC_FAILED",
            "expired invoice is not failed yet"
        );
        Ok(())
    })
    .await?;
    exact_balance(wallet, before).await?;
    println!("PASS auth, malformed hash, missing funding, unknown funding, and invoice expiry");
    Ok(())
}

async fn missed_receive(
    client: &Client,
    config: &TestConfig,
    wallet: &Wallet,
    payer: &LdkClient,
) -> Result<()> {
    let before = wallet_balance(wallet).await?;
    let ssp_before = ssp_available_balance(client, config, wallet.ssp_url).await?;
    let amount = 777;
    let invoice = wallet
        .sdk
        .receive_payment(ReceivePaymentRequest {
            payment_method: ReceivePaymentMethod::Bolt11Invoice {
                description: "missed-event".into(),
                amount_sats: Some(amount),
                expiry_secs: Some(300),
                payment_hash: None,
            },
        })
        .await?
        .payment_request;
    let hash = decode_payment_hash(&wallet.ldk, &invoice).await?;
    let container = &config.ssp_container;
    let start = Instant::now();
    command_output("docker", &["stop", "--time", "10", container]).await?;
    let stop_elapsed = start.elapsed();
    let held = async {
        ensure!(
            stop_elapsed < Duration::from_secs(5),
            "SSP graceful stop took {stop_elapsed:?}"
        );
        payer.json(&["bolt11-send", &invoice]).await?;
        poll(
            "held payment while SSP is offline",
            config.timeout,
            || async {
                let list = wallet.ldk.json(&["list-payments"]).await?;
                ensure!(
                    list["list"]
                        .as_array()
                        .context("missing LDK payments")?
                        .iter()
                        .any(|p| bolt11_hash(p).as_deref() == Some(hash.as_str())
                            && p["status"] == "PENDING"),
                    "payment is not held"
                );
                Ok(())
            },
        )
        .await
    }
    .await;
    command_output("docker", &["start", container]).await?;
    held?;
    poll("SSP restart", config.timeout, || async {
        admin_json(client, config, wallet.ssp_url, "/status", None)
            .await
            .map(|_| ())
    })
    .await?;
    poll("reconciled wallet receive", config.timeout, || {
        received_payment(wallet, &invoice)
    })
    .await?;
    poll("reconciled Spark balance", config.timeout, || {
        exact_balance(wallet, before + amount)
    })
    .await?;
    poll("reconciled inbound LDK payment", config.timeout, || {
        succeeded_ldk_payment(&wallet.ldk, &hash, "INBOUND", amount)
    })
    .await?;
    poll("reconciled outbound LDK payment", config.timeout, || {
        succeeded_ldk_payment(payer, &hash, "OUTBOUND", amount)
    })
    .await?;
    poll("reconciled SSP debit", config.timeout, || async {
        let balance = ssp_available_balance(client, config, wallet.ssp_url).await?;
        ensure!(
            balance == ssp_before - amount,
            "SSP balance did not reflect the receive debit"
        );
        Ok(())
    })
    .await?;
    println!("PASS missed receive recovered after restart; graceful stop took {stop_elapsed:?}");
    Ok(())
}

async fn run(
    client: &Client,
    config: &TestConfig,
    wallet_a: &Wallet,
    wallet_b: &Wallet,
    wallet_c: &Wallet,
) -> Result<()> {
    println!("fund one coarse SSP leaf per receiver");
    for wallet in [wallet_a, wallet_b] {
        fund_ssp(client, config, wallet.ssp_url, config.ssp_funding_leaf_sats).await?;
    }
    // Neither receiver has an exact leaf, so these payments require an
    // on-demand split rather than consuming pre-sized liquidity.
    bootstrap_wallet(
        wallet_b,
        &wallet_a.ldk,
        config.receive_amount_sats,
        config.receive_amount_sats,
        config.timeout,
    )
    .await
    .context("wallet B exact bootstrap receive failed")?;
    bootstrap_wallet(
        wallet_a,
        &wallet_b.ldk,
        config.receive_amount_sats,
        config.receive_amount_sats,
        config.timeout,
    )
    .await
    .context("wallet A exact bootstrap receive failed")?;

    println!("restart SSP A and split its previous change child again");
    restart_ssp(client, config, wallet_a.ssp_url).await?;
    bootstrap_wallet(
        wallet_a,
        &wallet_b.ldk,
        config.repeated_receive_amount_sats,
        config.repeated_receive_amount_sats,
        config.timeout,
    )
    .await
    .context("wallet A repeated split receive failed after SSP restart")?;

    println!("seed exact leaves for the Lightning send matrix");
    for _ in 0..3 {
        fund_ssp(client, config, wallet_a.ssp_url, config.send_amount_sats).await?;
    }
    for _ in 0..3 {
        fund_ssp(client, config, wallet_b.ssp_url, config.send_amount_sats).await?;
    }
    bootstrap_wallet(
        wallet_b,
        &wallet_a.ldk,
        config.send_amount_sats,
        config.send_amount_sats,
        config.timeout,
    )
    .await
    .context("wallet B send-liquidity bootstrap failed")?;
    bootstrap_wallet(
        wallet_a,
        &wallet_b.ldk,
        config.send_amount_sats,
        config.send_amount_sats,
        config.timeout,
    )
    .await
    .context("wallet A send-liquidity bootstrap failed")?;
    println!("reject unsafe internal payment before funding");
    reject_unsafe_internal_payment(client, config, wallet_a, wallet_c, config.send_amount_sats)
        .await
        .context("unsafe internal payment rejection failed")?;

    println!("send from wallet B to wallet A over Lightning");
    let first_hash = pay_between(
        client,
        wallet_b,
        wallet_a,
        config.send_amount_sats,
        config.send_amount_sats,
        "ssp-2-send-ssp-1-receive",
        config.timeout,
    )
    .await
    .context("wallet B to wallet A payment failed")?;
    println!("send from wallet A to wallet B over Lightning");
    let second_hash = pay_between(
        client,
        wallet_a,
        wallet_b,
        config.send_amount_sats,
        config.send_amount_sats,
        "ssp-1-send-ssp-2-receive",
        config.timeout,
    )
    .await
    .context("wallet A to wallet B payment failed")?;
    ensure!(
        first_hash != second_hash,
        "the two wallet invoices reused a payment hash"
    );
    negative_lightning(client, config, wallet_a, &wallet_b.ldk).await?;
    missed_receive(client, config, wallet_a, &wallet_b.ldk).await?;
    println!("withdraw wallet A's remaining Spark balance to Bitcoin");
    withdraw_bitcoin(client, config, wallet_a)
        .await
        .context("cooperative withdrawal failed")?;
    // The pinned Breez SDK cannot parse the BOLT12 extension's history.
    // Complete standard SDK checks first, then refill for the extension tests.
    bootstrap_wallet(
        wallet_a,
        &wallet_b.ldk,
        config.send_amount_sats,
        config.send_amount_sats,
        config.timeout,
    )
    .await?;
    println!("send from wallet A through its SSP to a BOLT12 offer");
    send_bolt12(
        client,
        config,
        wallet_a,
        &wallet_b.ldk,
        config.send_amount_sats,
    )
    .await
    .context("wallet A BOLT12 send failed")?;
    println!("receive to wallet A through its SSP BOLT12 offer");
    receive_bolt12(
        client,
        config,
        wallet_a,
        &wallet_b.ldk,
        config.send_amount_sats,
    )
    .await
    .context("wallet A BOLT12 receive failed")?;
    println!("BREEZ LN AND WITHDRAWAL E2E PASS");
    Ok(())
}

async fn withdraw_bitcoin(client: &Client, config: &TestConfig, wallet: &Wallet) -> Result<()> {
    let before = wallet_balance(wallet).await?;
    fund_ssp(client, config, wallet.ssp_url, 10_000).await?;
    let ssp_before = ssp_available_balance(client, config, wallet.ssp_url).await?;
    let address = bitcoin_rpc(
        client,
        config,
        "getnewaddress",
        json!(["coop-exit-recipient", "bech32m"]),
    )
    .await?
    .as_str()
    .context("Bitcoin returned no withdrawal address")?
    .to_string();
    let prepared = wallet
        .sdk
        .prepare_send_payment(PrepareSendPaymentRequest {
            payment_request: PaymentRequest::Input {
                input: address.clone(),
            },
            amount: Some(before.into()),
            token_identifier: None,
            conversion_options: None,
            fee_policy: Some(FeePolicy::FeesIncluded),
        })
        .await
        .context("prepare Bitcoin withdrawal")?;
    let SendPaymentMethod::BitcoinAddress { fee_quote, .. } = &prepared.payment_method else {
        bail!("withdrawal was not prepared as a Bitcoin payment");
    };
    let fee = fee_quote.speed_fast.total_fee_sat();
    ensure!(before > fee, "wallet cannot cover the withdrawal fee");
    let sent = wallet
        .sdk
        .send_payment(SendPaymentRequest {
            prepare_response: prepared,
            options: Some(SendPaymentOptions::BitcoinAddress {
                confirmation_speed: OnchainConfirmationSpeed::Fast,
            }),
            idempotency_key: None,
        })
        .await
        .context("send Bitcoin withdrawal")?;
    restart_ssp(client, config, wallet.ssp_url).await?;
    let mining_address = bitcoin_rpc(client, config, "getnewaddress", json!([])).await?;
    bitcoin_rpc(
        client,
        config,
        "generatetoaddress",
        json!([12, mining_address]),
    )
    .await?;
    let received = bitcoin_rpc(client, config, "getreceivedbyaddress", json!([address, 1])).await?;
    let received_sats = (received
        .as_f64()
        .context("invalid Bitcoin received amount")?
        * 100_000_000.0)
        .round() as u64;
    ensure!(
        received_sats == before - fee,
        "Bitcoin payout {received_sats} does not equal {}",
        before - fee
    );
    poll("withdrawal Spark recovery", config.timeout, || async {
        let current = ssp_available_balance(client, config, wallet.ssp_url).await?;
        ensure!(
            current == ssp_before + before,
            "SSP balance {current}; expected {}",
            ssp_before + before
        );
        Ok(())
    })
    .await?;
    poll("withdrawal wallet balance", config.timeout, || {
        exact_balance(wallet, 0)
    })
    .await?;
    let payment = poll("Breez withdrawal record", config.timeout, || {
        completed_payment(wallet, &sent.payment.id)
    })
    .await?;
    let Some(PaymentDetails::Withdraw { tx_id }) = payment.details else {
        bail!("Breez withdrawal details missing");
    };
    let session = authenticate_wallet(client, wallet).await?;
    for _ in 0..2 {
        let completed = graphql_json(
            client,
            wallet,
            Some(&session),
            "CompleteCoopExit",
            json!({"input":{"user_outbound_transfer_external_id":sent.payment.id}}),
        )
        .await?;
        let request = &completed["complete_coop_exit"]["request"];
        ensure!(
            request["status"] == "SUCCEEDED",
            "withdrawal not settled: {request}"
        );
        ensure!(
            request["coop_exit_txid"] == tx_id,
            "retry changed the payout transaction"
        );
        let history = graphql_json(
            client,
            wallet,
            Some(&session),
            "UserRequest",
            json!({"request_id":request["id"]}),
        )
        .await?;
        ensure!(
            history["user_request"]["status"] == "SUCCEEDED",
            "withdrawal history missing: {history}"
        );
    }
    let received_again =
        bitcoin_rpc(client, config, "getreceivedbyaddress", json!([address, 1])).await?;
    ensure!(
        received_again == received,
        "completion retry created another payout"
    );
    println!(
        "PASS cooperative withdrawal: {before} Spark sats became {received_sats} Bitcoin sats with {fee} sats in fees; SSP recovered the Spark leaves after restart"
    );
    Ok(())
}

async fn acceptance(config: TestConfig, ldk_a: LdkClient, ldk_b: LdkClient) -> Result<()> {
    breez_sdk_spark::init_logging(
        None,
        Some(Box::new(SdkLogger)),
        Some("off,spark=warn,spark_wallet=warn".into()),
    )?;
    let started = Instant::now();
    for id in 0..3 {
        let cert = config.cert_dir.join(format!("server_{id}.crt"));
        ensure!(
            cert.is_file(),
            "missing operator certificate {}",
            cert.display()
        );
    }

    let client = Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .context("could not build HTTP client")?;
    println!("create bidirectional Lightning liquidity");
    setup_lightning(&client, &config, &ldk_a, &ldk_b).await?;

    println!("connect Breez wallets");
    let wallet_a = connect_wallet(
        &client,
        &config,
        "wallet-a",
        "http://127.0.0.1:5000",
        ldk_a.clone(),
        0x0a,
    )
    .await?;
    let wallet_b = connect_wallet(
        &client,
        &config,
        "wallet-b",
        "http://127.0.0.1:5001",
        ldk_b,
        0x0b,
    )
    .await?;
    let wallet_c = connect_wallet(
        &client,
        &config,
        "wallet-c",
        "http://127.0.0.1:5000",
        ldk_a,
        0x0c,
    )
    .await?;

    let result = run(&client, &config, &wallet_a, &wallet_b, &wallet_c).await;
    let disconnect_c = wallet_c.sdk.disconnect().await;
    let disconnect_b = wallet_b.sdk.disconnect().await;
    let disconnect_a = wallet_a.sdk.disconnect().await;
    result?;
    disconnect_c.context("could not disconnect wallet-c")?;
    disconnect_b.context("could not disconnect wallet-b")?;
    disconnect_a.context("could not disconnect wallet-a")?;
    println!("completed in {:.1}s", started.elapsed().as_secs_f64());
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    regtest::run().await
}
