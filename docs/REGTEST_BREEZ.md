# Run open-ssp on regtest with the Breez SDK

This guide uses the **Breez SDK - Spark Rust SDK**, at the revision pinned in
[`e2e/breez/Cargo.toml`](../e2e/breez/Cargo.toml). Run the services and the SDK
client on the same Linux host. All coins, operator keys, and credentials in
this setup are for local regtest use only.

Use `cargo regtest up` to start a funded development stack, then connect your
own Breez wallet. Use `cargo regtest test` for the separate acceptance suite.

This guide funds client wallets through Lightning. Breez static on-chain
deposits are not implemented end to end: the SSP still has test-only quote
and placeholder claim paths. A Breez API key does not enable those operations.
The acceptance suite checks authentication, BOLT11 payments, and real
cooperative Bitcoin withdrawals. It does not establish compatibility with
every flow in an application that uses Breez.

## 1. Install the tools

You need Git, Docker with Compose v2, and a current stable Rust toolchain
installed through rustup. On Debian or Ubuntu, install the host build tools:

```sh
sudo apt-get update
sudo apt-get install -y build-essential pkg-config libssl-dev \
  libprotobuf-dev protobuf-compiler git curl
rustup update stable
```

Check Docker access and Rust:

```sh
docker info
docker compose version
cargo +stable --version
protoc --version
```

The Rust CLI runs Docker Compose and Git directly. It handles service readiness,
certificates, funding, channel setup, and tests. You do not need Bash helper
functions, Node.js, or a JavaScript SDK build for this guide. Docker Compose
still defines the service containers.

## 2. Clone the repository and its sources

```sh
git clone --recurse-submodules https://github.com/benthecarman/open-ssp.git
cd open-ssp
```

For an existing checkout, run this from the repository root:

```sh
cargo regtest init
```

`cargo regtest` is a repository Cargo alias for the Rust program in
`e2e/breez`. Run it from the repository root. The first invocation compiles
that program and its pinned Breez SDK dependency.

The submodules `vendor/spark` and `vendor/ldk-server` record the operator and
Lightning node revisions. `cargo regtest init` runs
`git submodule update --init --recursive`; run it again after pulling changes
that update those pins. Use `git submodule status` to inspect the revisions.
See [the source dependency notes](../e2e/upstream/README.md) for updates.

The end-client test uses upstream
[Breez SDK at `c7eecfe670798a8b8332ce412044cbd49123687a`](https://github.com/breez/spark-sdk/tree/c7eecfe670798a8b8332ce412044cbd49123687a).
Cargo fetches this dependency. The SSP uses the separate Spark crate pins in
its root `Cargo.toml`. The client does not need the SSP's SDK fork for the
BOLT11 flows in this guide.

## 3. Start the development stack

```sh
cargo regtest up
cargo regtest status
```

`up` builds and starts Bitcoin Core, an automatic miner, Electrs with an
Esplora API, PostgreSQL, three Spark Operators, two LDK nodes, and two SSPs.
It waits for readiness, opens and funds a Lightning channel in both directions,
and gives each SSP at least 10,000 sats of Spark liquidity. It copies the
operator certificates to `.regtest/open-ssp-regtest/operator-certs`.

The development project is `open-ssp-regtest`. Repeated `up` calls preserve
its volumes and reuse its channel. They add funding when needed. The command
prints `Regtest is ready` after setup completes. On failure, it prints logs
and keeps the development data for inspection.

`status` shows the containers, authenticated status for each SSP, and the
Esplora block height. Each SSP must have `ldk_mode: live`, `spark_error: null`,
and a Spark wallet. Use `spark.available_sats` to check its available liquidity.
A container health check alone does not prove that payments work.

| Service | Address from the host |
|---|---|
| SSP A / SSP B | `http://127.0.0.1:5000` / `http://127.0.0.1:5001` |
| Spark Operators 0, 1, 2 | `https://localhost:8535`, `:8536`, `:8537` |
| Esplora | `http://127.0.0.1:30000` |
| Bitcoin RPC | `http://127.0.0.1:8332` |
| LDK gRPC A / B | `localhost:3536` / `localhost:3537` |
| LDK peer A / B | `localhost:19735` / `localhost:19736` |

These ports bind to loopback. Run the SDK client on the same host. The operator
certificates include `localhost`, so use that name for SDK TLS connections.
The public `/identity` endpoint returns `identityPublicKey` for client setup.
The connection function below reads it automatically.

Use these commands to manage the stack:

| Command | Result |
|---|---|
| `cargo regtest stop` | Stop the containers and keep all data |
| `cargo regtest start` | Resume stopped containers and wait for SSP readiness |
| `cargo regtest down` | Remove containers and keep volumes; use `up` to recreate |
| `cargo regtest reset` | **Delete this project's containers and volumes** |
| `cargo regtest logs ssp ldk-server` | Show recent logs for selected services |
| `cargo regtest fund a 1000` | Add one 1,000-sat Spark leaf to SSP A |
| `cargo regtest ldk b list-channels` | Inspect LDK B's channel |
| `cargo regtest certs` | Refresh and print the local operator certificate directory |
| `cargo regtest --help` | Show all commands |

The default admin token is `regtest-spark-admin-token`. The CLI passes it to
Compose and uses it for admin requests. This is a local test credential.
Set `SPARK_ADMIN_TOKEN` consistently if you change it. No Breez API key is used
by this local setup.

## 4. Run the Breez acceptance tests

Stop the development stack first to release its host ports:

```sh
cargo regtest stop
cargo regtest test
cargo regtest start
```

`test` uses a separate project, `open-ssp-breez-e2e`. It **deletes that project's
volumes before each run**, then creates a fresh stack. It checks BOLT11 receives
and sends, recovery after restart, invalid requests, a Bitcoin withdrawal,
BOLT12 extensions, and repeated operator splits. The final success message is
`PASS Breez regtest acceptance and operator split checks`.

The runner removes test containers and volumes on success, failure, or Ctrl-C.
To keep them for inspection, use `cargo regtest test --keep`. This still resets
the test project at startup. To inspect and remove the retained test stack:

```sh
cargo regtest --project open-ssp-breez-e2e status
cargo regtest --project open-ssp-breez-e2e reset
```

The test wallets use temporary storage, which is removed when the client exits.
Use your own seed and persistent storage for application development. After a
full network reset, use a fresh regtest wallet and current operator certificates.

## 5. Connect your own Breez Rust wallet

Run `cargo regtest certs` to print the certificate directory. Set your
application's `BREEZ_OPERATOR_CERT_DIR` environment variable to that absolute
path. You can also copy the certificates to another directory:

```sh
cargo regtest certs /absolute/path/to/your-app/operator-certs
```

Add these dependencies to your Rust application's `Cargo.toml`:

```toml
[dependencies]
anyhow = "1"
breez-sdk-spark = { git = "https://github.com/breez/spark-sdk.git", rev = "c7eecfe670798a8b8332ce412044cbd49123687a", features = ["sqlite"] }
reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls"] }
serde_json = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
```

Use this connection function. It reads the SSP identity from the local service
and configures the same operators as the test runner:

```rust
use anyhow::{Context, Result};
use breez_sdk_spark::{
    BreezSdk, ChainApiType, Network, SdkBuilder, Seed, SparkConfig,
    SparkSigningOperator, SparkSspConfig, default_config,
};

async fn connect_regtest(seed: Seed, storage_dir: String) -> Result<BreezSdk> {
    let ssp_url = "http://127.0.0.1:5000";
    let identity: serde_json::Value = reqwest::get(format!("{ssp_url}/identity"))
        .await?
        .error_for_status()?
        .json()
        .await?;
    let identity_public_key = identity["identityPublicKey"]
        .as_str()
        .context("missing SSP identityPublicKey")?
        .to_owned();

    let cert_dir = std::path::PathBuf::from(
        std::env::var("BREEZ_OPERATOR_CERT_DIR")?,
    );
    let operator_keys = [
        "0322ca18fc489ae25418a0e768273c2c61cabb823edfb14feb891e9bec62016510",
        "0341727a6c41b168f07eb50865ab8c397a53c7eef628ac1020956b705e43b6cb27",
        "0305ab8d485cc752394de4981f8a5ae004f2becfea6f432c9a59d5022d8764f0a6",
    ];
    let mut signing_operators = Vec::new();
    for (id, key) in operator_keys.iter().enumerate() {
        signing_operators.push(SparkSigningOperator {
            id: id as u32,
            identifier: format!("{:064x}", id + 1),
            address: format!("https://localhost:{}", 8535 + id),
            identity_public_key: (*key).to_owned(),
            ca_cert_pem: Some(std::fs::read_to_string(
                cert_dir.join(format!("server_{id}.crt")),
            )?),
        });
    }

    let mut config = default_config(Network::Regtest);
    config.api_key = None;
    config.lnurl_domain = None;
    config.real_time_sync_server_url = None;
    config.use_default_external_input_parsers = false;
    config.prefer_spark_over_lightning = false;
    config.private_enabled_default = false;
    config.sync_interval_secs = 2;
    config.leaf_optimization_config.auto_enabled = false;
    config.token_optimization_config.auto_enabled = false;
    let defaults = config.spark_config.take().context("missing Spark defaults")?;
    config.spark_config = Some(SparkConfig {
        coordinator_identifier: format!("{:064x}", 1),
        threshold: 2,
        signing_operators,
        ssp_config: SparkSspConfig {
            base_url: ssp_url.to_owned(),
            identity_public_key,
            schema_endpoint: Some("graphql/spark/rc".to_owned()),
        },
        expected_withdraw_bond_sats: defaults.expected_withdraw_bond_sats,
        expected_withdraw_relative_block_locktime:
            defaults.expected_withdraw_relative_block_locktime,
        max_token_transaction_inputs: None,
    });

    Ok(SdkBuilder::new(config, seed)
        .with_default_storage(storage_dir)
        .with_rest_chain_service(
            "http://127.0.0.1:30000".to_owned(), ChainApiType::Esplora, None,
        )
        .build()
        .await?)
}
```

Call this function from your async application with your own regtest `Seed`
and a persistent storage directory. Keep the same seed and directory across
restarts. Do not reuse the acceptance test's fixed seeds. For a second wallet,
use another seed and directory, and change `ssp_url` to port `5001`.

To receive a test payment, add this function to the same application. It
prints an invoice and keeps the wallet connected for three minutes while you
pay it from the other LDK node:

```rust
async fn receive_test(sdk: &BreezSdk) -> Result<()> {
    use breez_sdk_spark::{
        GetInfoRequest, ReceivePaymentMethod, ReceivePaymentRequest,
    };
    let invoice = sdk.receive_payment(ReceivePaymentRequest {
        payment_method: ReceivePaymentMethod::Bolt11Invoice {
            description: "local regtest receive".to_owned(),
            amount_sats: Some(1000),
            expiry_secs: Some(300),
            payment_hash: None,
        },
    }).await?;
    println!("Pay this invoice from LDK B: {}", invoice.payment_request);
    for _ in 0..90 {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let info = sdk.get_info(GetInfoRequest {
            ensure_synced: Some(true),
        }).await?;
        println!("Spark balance: {} sats", info.balance_sats);
    }
    Ok(())
}
```

The development stack already has a funded channel and SSP liquidity. To add
an exact 1,000-sat leaf for another receive and pay the invoice from LDK B, run
these commands in another terminal at the repository root:

```sh
cargo regtest fund a 1000
cargo regtest ldk b bolt11-send '<invoice printed by your wallet>'
```

A fresh wallet should reach a balance of 1,000 sats. Receives need both SSP
Spark liquidity and inbound Lightning capacity. If you connect the wallet to
SSP B, use `cargo regtest fund b 1000` and pay from `cargo regtest ldk a` instead.

To test an outgoing payment from a funded wallet on SSP A, create an invoice
on LDK B:

```sh
cargo regtest ldk b bolt11-receive 500sat -d regtest-send
```

Pass the returned invoice to this function in your application:

```rust
async fn send_test(sdk: &BreezSdk, invoice: String) -> Result<()> {
    use breez_sdk_spark::{
        PaymentRequest, PrepareSendPaymentRequest, SendPaymentRequest,
    };
    let prepared = sdk.prepare_send_payment(PrepareSendPaymentRequest {
        payment_request: PaymentRequest::Input { input: invoice },
        amount: None,
        token_identifier: None,
        conversion_options: None,
        fee_policy: None,
    }).await?;
    let result = sdk.send_payment(SendPaymentRequest {
        prepare_response: prepared,
        options: None,
        idempotency_key: None,
    }).await?;
    println!("{result:?}");
    Ok(())
}
```

Check the final payment status and balance through the SDK. Call
`sdk.disconnect().await?` when the application finishes. The full
[Breez acceptance client](../e2e/breez/src/main.rs) includes payment polling
and settlement checks.

## Troubleshooting

| Symptom | Check or action |
|---|---|
| Port already in use | Stop the conflicting stack. Project names isolate volumes, not host ports. |
| Bitcoin RPC port conflict | Set `BITCOIN_RPC_PORT=18443` for the CLI. It also selects that port for host RPC requests unless `BITCOIN_RPC_URL` overrides it. |
| Missing source checkout | Run `cargo regtest init`. Remove stale `SPARK_REF` or `LDK_SERVER_REF` overrides. |
| Missing compiler or `protoc` | Install the build dependencies in section 1. |
| HTTP 401 | Use the same `SPARK_ADMIN_TOKEN` as the running containers. |
| Operator TLS error | Run `cargo regtest certs`, use `https://localhost`, and reconnect the SDK. |
| Operators never become ready | Use `cargo regtest logs spark-operator-0 postgres` and check the submodule pin. |
| SSP startup or connection fails | Use `cargo regtest logs ssp ldk-server` and `cargo regtest status`. A live LDK backend is required. |
| Chain data requests fail | Check `cargo regtest status`. Set the SDK builder's local Esplora service explicitly. |
| Invoice is rejected as an internal payment | Pay through the opposite SSP/LDK node. Same-SSP Lightning payments are rejected. |
| Receive fails or `spark.needs_topup` is true | Use `cargo regtest fund a 1000` for Spark liquidity and inspect `cargo regtest ldk a list-channels` for inbound capacity. |
| A slow machine times out during payment checks | Set `BREEZ_E2E_TIMEOUT_SECS=600` for `cargo regtest test`. Service startup has separate timeouts. |

Use `--project NAME` before the command to manage another project. The
`REGTEST_PROJECT` environment variable also selects a project. `test` accepts
`BREEZ_E2E_PROJECT_NAME` as a fallback for CI compatibility. A custom project
passed to `test` will have its volumes deleted, so keep it separate from your
development project.

For source development, `SPARK_REF` and `LDK_SERVER_REF` override the submodule
paths. Operator builds use a clean detached worktree at the Spark checkout's
`HEAD`; they exclude uncommitted changes. `SPARK_OPERATOR_COMMIT` can select
another commit available in that checkout. Temporary worktrees are removed
after the command exits.

The fixture automatically mines a block about every ten seconds. Wait for
chain and operator synchronization after funding. Test BOLT11 first: the
pinned Breez SDK cannot parse the project's BOLT12 extension history, so the
acceptance client runs those extension checks last. See
[API coverage](SSP_API_COVERAGE.md) for other flow limits.
