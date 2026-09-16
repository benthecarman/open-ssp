# open-ssp

`open-ssp` is a self-hosted Spark Service Provider written in Rust. One
process provides the SSP GraphQL API, owns the Spark liquidity wallet, fills
Swap V3 requests, and settles Lightning payments through `ldk-server` or an embedded `ldk-node`.

The service uses the Breez Spark Rust SDK for its embedded wallet. Spark
Operators remain separate services and use the existing Spark protocol. The
operator build must expose the authenticated counter-swap RPC used by the SSP.

For a precompiled executable, see [binary releases](docs/BINARY_RELEASES.md).

## Supported flows

- Wallet challenge authentication and durable 24-hour sessions.
- Partial Spark transfers through atomic Swap V3 counter transfers.
- BOLT11 Lightning sends backed by a verified Spark preimage-swap transfer.
- BOLT11 Lightning receives with wallet-created preimage shares, an atomic
  operator swap, and a Spark payout before the Lightning claim.
- BOLT12 Lightning sends with a completed Spark prepayment and an idempotent
  refund after a final Lightning failure.
- BOLT12 Lightning receives with an SSP-created offer and a Spark payout after
  the Lightning payment completes.
- Durable payment state, event-stream reconnect, and payment reconciliation.
- Authenticated Spark liquidity deposits and leaf funding.
- Cooperative Bitcoin withdrawals through a dedicated Bitcoin Core wallet,
  with durable input reservations and recovery after restart.

- Confirmed static deposits with real UTXO quotes and durable recovery transactions.
- BOLT11 payments between wallets on the same SSP, without an LDK payment.
- Signed protobuf receive quotes, owner-scoped history, and signed webhook delivery.
- Private-wallet withdrawals and admin fee bumps that spend SSP change.

Instant deposits, fee-bearing receive quotes, withdrawal batching, and automatic
liquidity management remain unsupported. See
[SSP API coverage](docs/SSP_API_COVERAGE.md) for limits and configuration.

## HTTP endpoints

| Endpoint | Purpose |
|---|---|
| `GET /health` | Basic process health (`{ "status": "ok" }`) |
| `GET /identity` | Public SSP identity discovery |
| `GET /status` | Spark wallet, liquidity, and LDK status (admin bearer token required) |
| `POST /graphql/spark/rc` | Current Spark SDK GraphQL endpoint |
| `POST /graphql/spark/2025-03-19` | Dated Spark SDK endpoint |
| `POST /graphql` | GraphQL compatibility alias |
| `POST /admin/spark/deposit-address` | Create a Spark deposit address |
| `POST /admin/spark/claim-deposit` | Claim a confirmed deposit output |

The status and admin endpoints require
`Authorization: Bearer <SPARK_ADMIN_TOKEN>`.

## Client configuration

Read `identityPublicKey` from `/identity` and use it as the SSP identity:

```json
{
  "baseUrl": "https://ssp.example.com",
  "schemaEndpoint": "graphql/spark/rc",
  "identityPublicKey": "<identityPublicKey>"
}
```

The wallet network, operator set, and SSP must use the same Bitcoin network.
For MutinyNet, use `SIGNET`. The pinned JavaScript SDK fork fixes SIGNET
network mapping and accepts the shared TESTNET/SIGNET `lntb` invoice prefix.

### MutinyNet Spark configuration

Use the following wallet configuration to connect to the MutinyNet Spark
operators and SSP:

```json
{
  "network": "SIGNET",
  "signingOperators": {
    "0000000000000000000000000000000000000000000000000000000000000001": {
      "id": 0,
      "identifier": "0000000000000000000000000000000000000000000000000000000000000001",
      "address": "https://0.spark.mutinynet.com",
      "identityPublicKey": "02d446dcd16eef9814d6491f64898f96e70061ed06e01393e2801a2bae8d9582e5"
    },
    "0000000000000000000000000000000000000000000000000000000000000002": {
      "id": 1,
      "identifier": "0000000000000000000000000000000000000000000000000000000000000002",
      "address": "https://1.spark.mutinynet.com",
      "identityPublicKey": "026ee53806c9c8323d79f11b4980af3002e30040ced8c4adc34b684454121b5764"
    }
  },
  "electrsUrl": "https://mutinynet.com/api",
  "sspClientOptions": {
    "baseUrl": "https://ssp.mutinynet.com",
    "schemaEndpoint": "graphql/spark/rc",
    "identityPublicKey": "0306e597d556f83e3b6f4a524c7cd84630b14ce323252d9cc1f8444a9b00a46756"
  },
  "expectedWithdrawBondSats": 10000,
  "expectedWithdrawRelativeBlockLocktime": 1000,
  "optimizationOptions": {
    "auto": false,
    "multiplicity": 0
  }
}
```

## Local end-to-end test

For setup instructions and a client configuration example, see
[Run on regtest with the Breez SDK](docs/REGTEST_BREEZ.md).

The Lightning acceptance stack contains bitcoind, a local Electrs Esplora
service, PostgreSQL, three Spark Operators, two `ldk-server` nodes, two SSP
instances, and three Breez SDK wallets. Rust starts and manages the real service
binaries on Linux x86_64. Install Rust, Go, PostgreSQL server tools, and the build
dependencies in the guide; Docker is not required. The source revisions are
pinned by the Git submodules in `vendor/`. See
[the fixture README](e2e/upstream/README.md) for source and update details.

Clone with `git clone --recurse-submodules`, or initialize an existing
checkout, then run:

```sh
git submodule update --init --recursive
cargo regtest test
```

For a persistent development stack, use `cargo regtest up`. The `stop`,
`start`, `status`, `fund`, and `ldk` commands manage it from Rust.

The test runner creates a separate project with fresh native data and
two real LDK nodes. It funds each SSP with a coarse leaf, then receives exact amounts
through the Breez SDK. A restart between receives checks that the SSP can
split a previous change leaf again using persisted keys.

The wallets send BOLT11 payments in both directions. The runner checks Spark
balances, Breez records, Lightning payment records, and preimage hashes.
It also checks send replay, same-SSP settlement without an LDK payment, authentication,
malformed hashes, missing or unknown funding, invoice expiry, and a payment
that arrives while the SSP is stopped. Recovery must complete that payment
after restart without a second Spark payout.

The test then withdraws a wallet's balance to Bitcoin. It restarts the SSP
after broadcast and a fee bump, retries the same bump, mines the payout, and checks the Bitcoin amount, Breez
withdrawal record, and Spark leaves recovered by the SSP. Repeated completion
calls must keep the same payout. Separate BOLT12 send and receive checks run
last because the pinned Breez SDK cannot parse the extension's history.

`cargo regtest test` also checks single-use deposits, partial Spark transfers,
repeated swaps after restart, instant deposits, signed webhook retries, and
request-history pagination. All wallet actions use the pinned Breez SDK fork.
Bitcoin Core, LDK, and admin APIs only prepare the fixture and check settlement.
`./e2e/e2e.sh` forwards to the same Rust suite. Image publication waits for
that suite and the Rust checks. See [coverage and remaining gaps](docs/E2E_COVERAGE.md).

## Lightning backend

`LDK_BACKEND=server` (the default) uses the existing `LDK_GRPC_ADDR`, API key,
and TLS settings. Set `LDK_BACKEND=embedded` to run LDK Node inside the SSP:

```sh
LDK_BACKEND=embedded
LDK_NODE_CHAIN_SOURCE=esplora
LDK_NODE_ESPLORA_URL=http://127.0.0.1:30000
LDK_NODE_LISTEN_ADDR=0.0.0.0:9735
SSP_NETWORK=REGTEST
SSP_DATA_DIR=./data
```

Embedded mode needs no ldk-server process, API key, or gRPC certificate. Both
modes share the BOLT11/BOLT12 settlement and restart-recovery logic. The
embedded node uses the same pinned LDK Node revision as ldk-server.

To use Bitcoin Core directly, replace the Esplora settings with:

```sh
LDK_NODE_CHAIN_SOURCE=bitcoind
LDK_NODE_BITCOIND_RPC_HOST=127.0.0.1
LDK_NODE_BITCOIND_RPC_PORT=18443
LDK_NODE_BITCOIND_RPC_USER=ssp
LDK_NODE_BITCOIND_RPC_PASSWORD_FILE=/run/secrets/ldk-bitcoin-rpc-password
```

This example uses Core's usual regtest RPC port; set the port to your node's
actual RPC port (the SSP setting defaults to `8332`). The host has no URL
scheme, port, or wallet path; bracket IPv6 addresses, such as `[::1]`. LDK uses
HTTP RPC, so use a trusted private connection to Core. Core must have RPC
enabled (`server=1`). LDK manages its own wallet; no Core wallet endpoint or
Electrs service is needed for this chain source.

`LDK_NODE_BITCOIND_RPC_PASSWORD` can supply the password directly. A configured
password file takes precedence, with trailing line endings removed; an
unreadable or empty file prevents startup. Chain selection is explicit and
never falls back to another source when a connection fails. Esplora remains
the default for existing embedded deployments.

For a restored seed without wallet state, set
`LDK_NODE_BITCOIND_RESCAN_FROM_HEIGHT` to a height at or before the wallet's
first transaction (`0` scans from genesis). This only applies before wallet
state exists; an existing wallet is not rewound. If unset, a new bitcoind wallet
starts at the current tip. Core must retain the blocks needed for the scan.

These settings are independent of `COOP_BITCOIN_RPC_*`, which still configure
the SSP's dedicated Core wallet for deposits and withdrawals. Breez client
wallets may still need their own Esplora service.

The chain source must track the selected network. Node state and a separate
Lightning seed live in `<SSP_DATA_DIR>/ldk-node`, overridable with
`LDK_NODE_DATA_DIR`. Persist and back up that entire directory along with SSP
state. After first boot, set `LDK_NODE_SEED_REQUIRED=true`. A missing seed next
to existing node state always prevents startup. Only one SSP process may use
a node directory.

The node starts without channels. A funded peer can open a channel to the
configured Lightning listening address and push funds to provide outbound
capacity. Node/channel administration remains external in server mode;
embedded mode currently exposes payments through SSP, not the ldk-server
administration API. Allow inbound traffic on the Lightning port if peers
need to connect. `/status` reports `ldk_backend` and `ldk_node_id`.

Switching backend settings does not migrate a node or its channels. Use a
fresh SSP state directory for a new node; keep an existing deployment paired
with its original node until its pending payments have settled.

## Deployment

Use [the deployment runbook](docs/DEPLOY.md) for service requirements,
configuration, startup, liquidity, backups, and monitoring. Current Lightning
backend limits are in [ldk-server compatibility](docs/LDK_GAPS.md). The
`deploy/` directory contains a local Caddy edge example and an LDK command
helper. It is not a production topology. The production Compose definition is
in [MutinyWallet/mutiny-net](https://github.com/MutinyWallet/mutiny-net).
