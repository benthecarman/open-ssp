# Run open-ssp on regtest with the Breez SDK

This guide uses the **Breez SDK - Spark Rust SDK**, at the revision pinned in
[`e2e/breez/Cargo.toml`](../e2e/breez/Cargo.toml). Run the services and the SDK
client on the same Linux host. All coins, operator keys, and credentials in
this setup are for local regtest use only.

Use `cargo regtest up` to start a funded development stack, then connect your
own Breez wallet. Use `cargo regtest test` for the separate acceptance suite.

Client wallets can receive Lightning payments and confirmed static on-chain
deposits. The acceptance suite checks standard Breez clients with private leaf
queries enabled. It covers same-SSP and cross-SSP BOLT11 payments, deposits,
cooperative Bitcoin withdrawals, fee bumps, and restart recovery.

## 1. Install the tools

The native runner currently supports **Linux x86_64**. Install Git, a current
stable Rust toolchain through rustup, Go (the version in
`vendor/spark/spark/go.mod`), and PostgreSQL server tools. On Debian or Ubuntu:

```sh
sudo apt-get update
sudo apt-get install -y build-essential pkg-config libssl-dev libzmq3-dev \
  libprotobuf-dev protobuf-compiler postgresql libpq-dev libclang-dev git curl unzip
rustup update stable
```

Check the host tools:

```sh
cargo +stable --version
go version
protoc --version
pg_config --bindir
```

Run the fixture as a regular user: PostgreSQL's `initdb` refuses root. Set
`PGBIN` to the directory containing `initdb`, `postgres`, and `psql` if it is not
the directory reported by `pg_config`. The fixture starts its own PostgreSQL
instance on port 54329; it does not use your system database.

Rust launches and manages the real service binaries. Docker is not required.
The first `cargo regtest build` downloads checksum-verified Bitcoin Core 29.0
and Atlas Community 1.0.0 for migrations. It builds Electrs from revision
`8c06d8010e43f793b1a65f83695ea846e5cd83ed`, which adds Esplora package
broadcasting, and verifies the checkout revision before compilation.
Exact source and artifact pins live in
[`native/tools.rs`](../e2e/breez/src/native/tools.rs). It also builds the Spark
operator (Go), Spark signer (Rust), LDK server/client, and SSP from source.
The signer uses optimization level 1 so its curve arithmetic fits DKG RPC
deadlines on small CI runners. Electrs uses a release build; the other Rust services use development builds.
Downloaded tools and compiler outputs persist under `.regtest/native-tools`
and `.regtest/native-build`; resetting test data keeps these caches.

Core 29 and the bundled Electrs support package relay for clients that use
zero-fee commitments. Clients can broadcast packages through Bitcoin RPC or
Esplora's `/txs/package` endpoint.

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

The submodules `vendor/spark`, `vendor/ldk-server`, and `vendor/breez-sdk`
record the operator, Lightning node, and SSP wallet source revisions. `cargo regtest init` runs
`git submodule update --init --recursive`; run it again after pulling changes
that update those pins. Use `git submodule status` to inspect the revisions.
See [the source dependency notes](../e2e/upstream/README.md) for updates.

Both the end-client test and the SSP use the `vendor/breez-sdk` submodule.
It is based on Breez `main` at
`a3fac0e8f1f38e7e3dca110a22f37dd3264e2bde`, which includes upstream
instant-deposit support from PR #1012. The fork retains the SSP counter-transfer,
leaf-splitting, and private operator APIs, plus Rust request-history and BOLT12
extensions. The test consumes this source through a Cargo path dependency;
the parent repository's gitlink pins its revision. The SSP issues the protobuf
authentication challenge required by current Breez clients.

The earlier `c7eecfe` pin was itself a fork commit, with four SSP patches. All
nine fork commits were included in the rebase. Standard clients can use
upstream's `fetch_claim_deposit_quote` and `claim_deposit`; the test also uses
fork wrappers around upstream's lower-level quote/claim methods for negative
and replay checks.

## 3. Start the development stack

```sh
cargo regtest up
cargo regtest status
```

`up` builds and starts Bitcoin Core, an automatic miner, Electrs with an
Esplora API, PostgreSQL, three Spark Operators, two LDK nodes, and two SSPs.
It waits for readiness, opens and funds a Lightning channel in both directions,
and gives each SSP at least 10,000 sats of Spark liquidity. Operator certificates are stored in
`.regtest/open-ssp-regtest/native/tls`.

The development project is `open-ssp-regtest`. Repeated `up` calls preserve
its native data and reuse its channel. They add funding when needed. The command
prints `Regtest is ready` after setup completes. On failure, it prints logs
and keeps the development data for inspection.

`status` shows the processes, authenticated status for each SSP, and the
Esplora block height. Each SSP must have `ldk_mode: live`, `spark_error: null`,
and a Spark wallet. Use `spark.available_sats` to check its available liquidity.
A running process alone does not prove that payments work.

| Service | Address from the host |
|---|---|
| SSP A / SSP B | `http://127.0.0.1:5000` / `http://127.0.0.1:5001` |
| Spark Operators 0, 1, 2 | `https://localhost:8535`, `:8536`, `:8537` |
| Esplora | `http://127.0.0.1:30000` |
| Bitcoin RPC | `http://127.0.0.1:8332` |
| LDK gRPC A / B | `localhost:3536` / `localhost:3537` |
| PostgreSQL | `127.0.0.1:54329` |
| Operator SSP APIs | `localhost:18535`, `:18536`, `:18537` |
| LDK peer A / B | `localhost:19735` / `localhost:19736` |

These ports bind to loopback. Run the SDK client on the same host. The operator
certificates include `localhost`, so use that name for SDK TLS connections.
The public `/identity` endpoint returns `identityPublicKey` for client setup.
The connection function below reads it automatically.

Use these commands to manage the stack:

| Command | Result |
|---|---|
| `cargo regtest stop` | Stop the processes and keep all data |
| `cargo regtest start` | Resume stopped processes and wait for SSP readiness |
| `cargo regtest down` | Stop processes and keep all data (same as `stop`) |
| `cargo regtest reset` | **Stop processes and delete this project's native data** |
| `cargo regtest logs ssp ldk-server` | Show recent logs for selected services |
| `cargo regtest fund a 1000` | Add one 1,000-sat Spark leaf to SSP A |
| `cargo regtest settlements a` | List unresolved SSP A settlements |
| `cargo regtest reconcile a REQUEST_ID` | Reconcile one Lightning send |
| `cargo regtest bump a REQUEST_ID 5 5000` | Spend at most 5,000 sats on a 5 sat/vB withdrawal fee bump |
| `cargo regtest ldk b list-channels` | Inspect LDK B's channel |
| `cargo regtest certs` | Print the local operator certificate directory |
| `cargo regtest --help` | Show all commands |

The default admin token is `regtest-spark-admin-token`. The CLI passes it to
the services and uses it for admin requests. This is a local test credential.
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
native data before each run**, then creates a fresh stack. It checks BOLT11 receives
and sends, recovery after restart, invalid requests, a Bitcoin withdrawal,
BOLT12 extensions, and repeated operator splits. The final success message is
`PASS Breez regtest acceptance and operator split checks`.

The runner stops test processes and deletes their data on success, failure, or Ctrl-C.
To keep them for inspection, use `cargo regtest test --keep`. This still resets
the test project at startup. To inspect and remove the retained test stack:

```sh
cargo regtest --project open-ssp-breez-e2e status
cargo regtest --project open-ssp-breez-e2e reset
```

For repeated runs against binaries you have already built, use
`cargo regtest test --no-build`. This still creates fresh test data and runs the
entire suite. Rebuild after changing SSP, operator, or LDK sources; the default
`cargo regtest test` does this automatically. `--no-build` can be combined with
`--keep`.

The runner prints `TIMING` lines for native builds, setup, Lightning provisioning,
individual waits, acceptance, teardown, and the total run. Compiling the runner
itself happens before its total timer starts. CI caches native service builds,
Go modules, and Cargo dependencies, runs `cargo regtest build` followed by
`cargo regtest test --no-build --keep`, and uploads `e2e.log` plus full service
logs as an artifact before cleanup. Native build caches are saved before
acceptance, so a test failure does not discard those successful builds. A separate
job still builds and publishes the deployment Docker image after tests pass.

Processes have separate process groups and logs under
`.regtest/<project>/native`. The runner checks saved PID start times before
stopping a service, rejects occupied ports, and serializes commands that mutate
state within a repository. Only one stack can use the fixed ports at a time.
`cargo regtest miner stop` and `cargo regtest miner start` control automatic
mining for manual tests.

When migrating an existing Docker fixture, stop it once with its old Compose
command before using native commands. Native `reset` never deletes Docker
volumes, and existing Docker data is not imported:

```sh
SPARK_ADMIN_TOKEN=regtest-spark-admin-token \
  docker compose -p open-ssp-regtest -f docker-compose.regtest.yml stop
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
breez-sdk-spark = { path = "vendor/breez-sdk/crates/breez-sdk/core", features = ["sqlite"] }
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
    config.private_enabled_default = true;
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
| Port already in use | Stop the conflicting stack. Project names isolate data, not host ports. |
| Bitcoin RPC port conflict | Set `BITCOIN_RPC_PORT=18443` for the CLI. It also selects that port for host RPC requests unless `BITCOIN_RPC_URL` overrides it. |
| Missing source checkout | Run `cargo regtest init`. Remove stale `SPARK_REF` or `LDK_SERVER_REF` overrides. |
| Missing compiler or `protoc` | Install the build dependencies in section 1. |
| HTTP 401 | Use the same `SPARK_ADMIN_TOKEN` as the running services. |
| Operator TLS error | Run `cargo regtest certs`, use `https://localhost`, and reconnect the SDK. |
| Operators never become ready | Use `cargo regtest logs spark-operator-0 postgres` and check the submodule pin. |
| SSP startup or connection fails | Use `cargo regtest logs ssp ldk-server` and `cargo regtest status`. A live LDK backend is required. |
| Chain data requests fail | Check `cargo regtest status`. Set the SDK builder's local Esplora service explicitly. |
| Same-SSP payment fails | Check SSP Spark liquidity and use the pinned operator and SSP sources. The current flow does not use an LDK payment. |
| Receive fails or `spark.needs_topup` is true | Use `cargo regtest fund a 1000` for Spark liquidity and inspect `cargo regtest ldk a list-channels` for inbound capacity. |
| A slow machine times out during payment checks | Set `BREEZ_E2E_TIMEOUT_SECS=600` for `cargo regtest test`. Service startup has separate timeouts. |

Use `--project NAME` before the command to manage another project. The
`REGTEST_PROJECT` environment variable also selects a project. `test` accepts
`BREEZ_E2E_PROJECT_NAME` as a fallback for CI compatibility. A custom project
passed to `test` will have its native data deleted, so keep it separate from your
development project.

For source development, `SPARK_REF` and `LDK_SERVER_REF` override the submodule
paths. Operator and signer builds use a cached `git archive` snapshot of the
Spark checkout's `HEAD`, excluding uncommitted changes. `SPARK_OPERATOR_COMMIT`
can select another commit available in that checkout. A Go build overlay binds
the operator's four TCP listeners to loopback, replacing Compose's loopback port
publishing. The overlay checks the expected source layout and fails if an update
requires review; it does not modify your checkout. LDK and SSP builds include
local source changes.

The fixture automatically mines a block about every ten seconds. Wait for
chain and operator synchronization after funding. Test BOLT11 first: the
pinned Breez SDK cannot parse the project's BOLT12 extension history, so the
acceptance client runs those extension checks last. See
[API coverage](SSP_API_COVERAGE.md) for other flow limits.

## Confirmed static deposits

Use Breez `receive_payment` with `ReceivePaymentMethod::BitcoinAddress {
new_address: Some(false) }`. Send regtest Bitcoin to the returned address.
After three confirmations, sync the wallet. The SSP verifies the unspent
output and its owner, then quotes its actual value minus the recovery miner
fee. At the local fallback rate of 1 sat/vB, a 10,000-sat deposit credits
9,901 sats. The address can receive more than one deposit.

The SSP needs enough Spark liquidity for the credit. For example, run
`cargo regtest fund a 20000` before a 10,000-sat deposit to SSP A. The
recovered Bitcoin does not automatically become new Spark leaves.

The claim plan, signing nonce, transfer ID, and signed recovery transaction
are saved before their dependent network calls. Pending claims resume after
an SSP restart. Keep both the SSP database and operator databases.

## Instant static deposits

The Rust runner sets `SSP_INSTANT_MAX_OUTSTANDING_SATS=100000` and
`SSP_INSTANT_MAX_DEPOSIT_SATS=10000` for its local fixture. You can override
these environment variables. Outside the runner, both limits default to zero
and new instant advances are disabled.

The acceptance test stops the miner, sends a 2,000-sat static deposit, and
claims 1,901 sats of Spark credit before any confirmation. It checks an
invalid signature, another owner's claim, repeated requests, and an SSP
restart. It then mines the deposit and checks the signed Bitcoin recovery.
The test repeats this flow with a fee-bumped replacement deposit.

The test uses upstream `BreezSdk::fetch_claim_deposit_quote` and
`BreezSdk::claim_deposit` for the normal instant-claim flow. Thin fork wrappers
around upstream's lower-level deposit service let the test change a quote and
replay a saved claim. Upstream owns authentication, quote validation, signing,
and deposit-key encryption. The SSP accepts that encrypted key share and
retains support for the older raw-key field.

The fixture disables automatic deposit claims and supplies an explicit fee cap
when it calls `claim_deposit`. This keeps background sync from racing the
negative checks. See [the acceptance test](../e2e/breez/src/instant.rs).

A pending advance remains counted against the limit until its recovery
transaction has three confirmations. Recovery supports a replacement with
the same address and value. A missing deposit or a different-value
replacement remains pending. Use admin status and settlements to inspect it.

## Settlement operations

`settlements` lists pending Lightning sends, deposit claims, and other pending
settlement records. `reconcile` performs the same checks as the background
Lightning worker. Missing BOLT11 submissions can retry with the pinned LDK
payment hash as the stable ID. A missing BOLT12 payment still needs backend
investigation; a timeout does not authorize a refund.

`bump` applies to an unconfirmed broadcast withdrawal with available SSP
change. It creates a child transaction that pays for the parent and child.
It preserves the payout and connector transaction IDs. One child is supported
per withdrawal; retry with the same rate to rebroadcast it. The SSP pays this
extra fee within `MAX_FEE`, in sats.

See [API coverage](SSP_API_COVERAGE.md) for receive quotes, history filters,
webhook signatures, and production operator authorization.
