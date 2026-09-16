# Deployment runbook

This runbook defines the service contract for a production SSP deployment.
Keep host names, secrets, volumes, and image policy in the deployment
repository.

## Required services

- A Bitcoin node for the selected network.
- At least two Spark Operators and their signers.
- PostgreSQL databases used by the Spark Operators.
- A Lightning backend with usable inbound and outbound channel capacity:
  `ldk-server` or [embedded LDK Node](../README.md#lightning-backend).
- `open-ssp` with persistent storage mounted at `/data`.

The Spark Operator build must include the authenticated Swap V3 counter RPC.
The SSP calls existing operator consensus code through this RPC.

## Configuration

| Variable | Requirement |
|---|---|
| `SSP_NETWORK` | `REGTEST`, `SIGNET`, `TESTNET`, or `MAINNET`; must match every dependency |
| `SSP_LISTEN_ADDR` | Listen address; use `0.0.0.0:5000` in a container |
| `SSP_PUBLIC_URL` | SSP URL reachable from the embedded wallet |
| `SSP_DATA_DIR` | SQLite directory; use persistent storage |
| `SPARK_MNEMONIC_FILE` | Persistent BIP39 mnemonic path |
| `SPARK_MNEMONIC_REQUIRED` | Set to `1` after the wallet file exists |
| `SSP_IDENTITY_PUBKEY` | Optional guard for the identity derived from the mnemonic |
| `SO_HOSTS` | Ordered, comma-separated operator gRPC addresses |
| `SO_IDENTITY_PUBKEYS` | Ordered operator identity keys; must match `SO_HOSTS` |
| `SO_CERT_FILES` | Empty for public trust, or one ordered certificate file per operator |
| `SSP_OPERATOR_HOSTS` | Optional ordered, comma-separated SSP-private operator gRPC addresses; enables just-in-time leaf splitting |
| `SSP_OPERATOR_CERT_FILES` | Empty for public trust, or one ordered certificate file per SSP-private operator endpoint |
| `SSP_MIN_SPLIT_CHILD_SATS` | Local minimum split-child value (default `330`, the standard P2TR relay dust floor) |
| `SSP_FROST_THRESHOLD` | Spark wallet signing threshold |
| `SPARK_ADMIN_TOKEN` | Bearer token for the liquidity endpoints |
| `LDK_BACKEND` | `server` (default) or `embedded`; see [embedded configuration](../README.md#lightning-backend) |
| `LDK_GRPC_ADDR` | `ldk-server` gRPC address without a URL scheme |
| `LDK_API_KEY` | Hex API key; use this or `LDK_API_KEY_FILE` |
| `LDK_API_KEY_FILE` | Mounted raw `ldk-server` API-key file |
| `LDK_TLS_CERT_FILE` | Mounted `ldk-server` TLS certificate |
| `SSP_SWAP_FEE_SATS` | Flat leaf-swap fee |
| `SSP_INSTANT_MAX_OUTSTANDING_SATS` | Maximum pending instant credit; default `0` disables new advances |
| `SSP_INSTANT_MAX_DEPOSIT_SATS` | Maximum Bitcoin value per instant deposit; default `0` disables new advances |
| `MAX_SWAP_TOTAL_SATS` | Maximum value accepted by one swap; `0` removes the cap |
| `SSP_CORS_ORIGINS` | Optional comma-separated browser origins |
| `RUST_LOG` | Optional tracing filter; use `info` unless more detail is needed |

Production must not set `SPARK_ADMIN_ALLOW_NO_AUTH=1`.
A numeric variable that is set must parse as an integer; a malformed value
stops the boot instead of silently falling back to its default.
`SSP_FROST_THRESHOLD` must be between 1 and the number of `SO_HOSTS` entries.
The service requires a live LDK backend at startup. In server mode, start LDK first and wait
for its health check before starting the SSP.

## Upgrade from the retired preimage extension

`mint_invoice_preimage`, `reveal_preimage`, and the old SSP-owned internal
settlement flow have been removed. The current same-SSP flow and standard
receives use preimages and shares created
by the wallet. Remove `SSP_FROST_OPERATORS` and `SSP_ALLOW_FAKE_LN` from old
deployment configuration. Keep `SSP_FROST_THRESHOLD` for wallet signing.

Before upgrading, let pending SSP-owned receives and internal sends finish
on the previous release. Back up the complete SSP data directory. Startup
refuses the migration while these requests remain pending. After they finish,
the migration creates explicit Lightning request relationships and removes
the old `preimages` table. Standard receive settlement checkpoints remain.

Send intent, funding, and idempotency records now commit before Lightning
submission. A lost reply is an uncertain outcome, not a final failure. See
[submission recovery](LDK_GAPS.md#submission-recovery) before investigating a
send that stays pending. Back up SSP and LDK data together; restoring only
one side can lose the link between a payment and its funding.

## Wallet initialization

For a new empty volume, start one SSP instance with
`SPARK_MNEMONIC_REQUIRED=0`. The process creates the mnemonic with mode `0600`.
At every start it also tightens the mnemonic, the SQLite database, and any
WAL/journal sidecars back to `0600` (Unix), and refuses to start when a
group- or world-readable secret file cannot be secured. Volume-level access
control and backup permissions remain the operator's responsibility. Back up
the file, stop the process, set `SPARK_MNEMONIC_REQUIRED=1`, and start
the service normally.

Never start two SSP instances against an empty shared mnemonic path. The
mnemonic controls the SSP identity and all Spark liquidity.

## TLS checks

Each private Spark Operator certificate must be a server certificate. It must
not be a CA certificate. Check every mounted certificate before SSP startup:

```sh
openssl x509 -in /path/to/server.crt -noout -ext basicConstraints
```

The result must contain `CA:FALSE`. `SO_CERT_FILES` must use the certificates
that belong to the currently running operator instances.

## Startup

1. Start Bitcoin, PostgreSQL, the Spark Operators, and their signers.
2. Wait for every operator to have signing keyshares and stable TLS files.
3. Start `ldk-server` and verify its node information and channels.
4. Start the SSP and wait for `/health`.
5. Query authenticated `/status` and verify that `spark_error` is `null` and
   `ldk_mode` is `live`.
6. Verify that `/identity`'s `identityPublicKey` equals `/status`'s
   `ssp_identity_pubkey`, `spark.identity_pubkey`, and the configured client
   identity.
7. Fund enough Spark leaves for the service.

Start or recreate the SSP only after operator certificates are ready. This
prevents the wallet from keeping connections to replaced certificates.

## Liquidity

Swap fills need exact SSP-owned Spark leaves. Lightning receives are exactly
value-conserving: the SSP only settles a receive when whole leaves totalling
exactly the invoice amount can be selected. Amounts the leaf ladder cannot
represent are rejected, the hold invoice is failed, and the payer is refunded.
Monitor these authenticated `/status` fields:

- `spark.available_sats`: spendable leaves visible on the operators.
- `spark.owned_sats`: all wallet-owned leaves, including temporarily missing
  leaves.
- `spark.needs_topup`: `true` after a leaf-selection or balance failure.

The funding helper obtains deposit addresses from the authenticated admin API,
funds them through Bitcoin RPC, waits for confirmation, and submits each raw
transaction to the SSP:

```sh
SPARK_ADMIN_TOKEN=<token> \
FUND_LADDER=1000,2000,4000,8000 \
FUND_MULTIPLICITY=12 \
node e2e/fund-ssp.mjs
```

Use `SSP_BASE_URL`, `BITCOIN_RPC_URL`, `BITCOIN_RPC_USER`, and
`BITCOIN_RPC_PASSWORD` when the services are not at the local defaults. Do not
put quotes inside values passed through Docker's `--env-file` option.

The SSP selects an exact combination of whole leaves when one exists and
rejects the receive otherwise, so the wallet is never paid more Spark value
than the Lightning invoice collected. Fund the ladder with denominations that
match the invoices the service is expected to settle (for example, add a
68-sat-compatible ladder or 1-sat leaves for small regtest invoices). A
ladder that starts at 1,000 sats cannot settle a 68-sat receive.

Keep `MAX_SWAP_TOTAL_SATS` below the amount of liquidity that the operator can
safely expose. Add small denominations before `needs_topup` becomes true.

## Lightning

Lightning receives need inbound channel capacity. Lightning sends need
outbound capacity. The SSP keeps the server-streaming event RPC open without a
unary deadline, reconnects with capped exponential backoff, and reconciles
payment state every 30 seconds.

Use `/health` for basic process liveness. Treat authenticated `/status` as
ready only when `ldk_mode` is `live` and `ldk_node_id` is the expected node.
Alert on repeated event-stream reconnects or reconciliation errors.

See [ldk-server compatibility](LDK_GAPS.md) for supported calls and current
backend gaps.

Run the local Lightning acceptance test before deployment:

```sh
cargo regtest test
```

It starts a fresh regtest network with local Electrs, three pinned Spark
Operators, two SSP instances, and two `ldk-server` nodes. Two Breez SDK wallets
receive initial Spark balances through Lightning and then pay BOLT11 invoices
in both directions. The test checks the wallet balances, Spark transfers,
Lightning settlement, and preimage hashes.

All wallet E2E actions run through the Breez SDK fork. The old JavaScript
receive test has been retired. See [E2E coverage](E2E_COVERAGE.md) for the
SDK entry points, fixture boundary, and remaining test gaps.

## Cooperative withdrawals

Create and fund a dedicated Bitcoin Core wallet on the SSP network. Keep this
wallet exclusive to one SSP and its SQLite database. Other processes must not
spend its coins: withdrawal input reservations are stored in the SSP database.
The Core wallet must have private keys, be unlocked for signing, and finish
scanning before the SSP starts. The native regtest fixture uses Bitcoin Core 29.

| Variable | Requirement |
|---|---|
| `COOP_BITCOIN_RPC_URL` | Wallet endpoint, such as `http://bitcoind:8332/wallet/ssp-withdrawals`; enables withdrawals |
| `COOP_BITCOIN_RPC_USER` | Bitcoin RPC user |
| `COOP_BITCOIN_RPC_PASSWORD_FILE` | File with the RPC password; preferred over the environment variable |
| `COOP_BITCOIN_RPC_PASSWORD` | RPC password when no password file is configured |
| `COOP_EXIT_FEE_SATS` | Flat SSP fee in addition to the miner fee; defaults to `0` |

The SSP rejects a node on the wrong network. Fee quotes use Core's conservative
estimates for 2, 6, and 12 blocks. Only regtest permits a 1 sat/vB fallback when
estimates are unavailable. Confirmation speed is an estimate, not a guarantee.

Each withdrawal needs one unreserved, confirmed P2WPKH or P2TR coin that covers
the payout, miner fee, connector funding, and a change output. The connector
reserves 330 sats per Spark leaf plus one extra output. This connector funding
stays reserved until the SSP recovers the Spark leaves. Keep several suitable
coins available for concurrent users;
this implementation does not combine inputs or batch withdrawals.

Wallets can first request a Spark swap to create separate payout and fee
leaves. Keep enough Spark liquidity for that swap. The total quoted fee must
also be representable by the available leaves or permitted split sizes. The
regtest fixture permits 1-sat split children for its small fee amounts; the
default deployment split minimum remains 330 sats.

The SSP signs only after the operators report the matching conditional Spark
transfer. A background task retries pending payouts and Spark recovery after
restart. Do not remove reservations or replace payout transactions manually.
A Bitcoin conflict keeps the reservation in place. An admin can use
`POST /admin/withdrawals/bump-fee` with `request_id`, `fee_rate` in sat/vB,
and `max_fee_sats`. It spends SSP change in one child transaction and leaves
the payout and connector IDs intact. Repeated calls must use the same rate.
Automatic fee selection and repeated child replacements are not implemented. The operator's confirmation rules control when the SSP can
recover the Spark leaves.

Back up the Core wallet, Spark mnemonic, and complete SSP SQLite data. Keep the
RPC credentials with the deployment secrets. Restoring only the Core wallet
does not restore withdrawal commitments. Run `cargo regtest test` to check a real
Breez withdrawal, exact Bitcoin payout, and Spark recovery after an SSP restart.

## Upgrade and rollback

1. Back up the mnemonic file and the complete `SSP_DATA_DIR` volume.
2. Pull the selected SSP image.
3. Recreate only the SSP service and wait for its health check.
4. Check identity, Spark balance, and LDK mode before client traffic resumes.

The process handles `SIGTERM` and closes cleanly. Do not delete or replace the
data volume during an image rollback.

## Secrets and backups

- Do not commit the Spark mnemonic, `SPARK_ADMIN_TOKEN`, or LDK API key.
- Restrict `/status` and the admin endpoints at the network edge as well as
  with the bearer token.
- Stop the SSP or use a SQLite-aware backup before copying its data directory.
- Back up `spark.mnemonic` and the complete SQLite data together.
- Test restore procedures with a wallet identity check before adding funds.

## Private wallets and confirmed deposits

Use the pinned operator source and configure `SSP_OPERATOR_HOSTS` plus its
certificates. On each operator with authorization enabled, set
`SSP_INTERNAL_ALLOWED_IDENTITIES` to the comma-separated compressed identity
keys of trusted SSPs. An ordinary wallet session cannot use the private read
methods. Keep the SSP listener restricted to the SSP network.

The dedicated Bitcoin Core wallet also receives static-deposit recovery
outputs. Confirmed deposits require three confirmations, an unspent output,
a matching operator-owned static address, the wallet's authorization, and
enough SSP Spark liquidity for the quote. Quotes deduct the recovery miner
fee from the Bitcoin value. The claim uses one durable transfer ID and saves
its signing plan and transaction for retries. Back up all of these records.

Instant deposits require both advance limits to be nonzero and enough Spark
liquidity to pay the credit. Set limits according to the Bitcoin loss the SSP
can accept from unconfirmed deposits. There is one pending advance per owner.
The fee is deducted from the deposit, and the full quoted credit is sent
before confirmation. The server reserves the budget until the Bitcoin
recovery transaction has three confirmations.

The worker resumes saved claims after a restart. A same-amount replacement
deposit can satisfy the reservation. A lost deposit, a replacement with a
different amount, or a reorganization after recovery signing can need manual
investigation. The server does not refund, resend, or release the budget on
a timeout. Setting the limits to zero stops new advances while recovery
continues. Keep the SSP and operator databases with their saved signing data.

`GET /status` reports instant credit exposure and configured limits.
`GET /admin/settlements` lists unresolved work, including instant recovery
phases and the last error. Deposit rows report their recovery phase, so a
payout that is broadcast but not yet confirmed stays listed until its recovery
transaction has three confirmations.
`POST /admin/settlements/reconcile` accepts a Lightning `request_id` and runs
the same guarded recovery as the background worker. A pending BOLT12 send
with no backend record still needs investigation; do not infer failure from
elapsed time.

Webhook delivery uses a persistent queue in the SSP database. Configure
HTTPS URLs that resolve to public addresses. Keep `SSP_WEBHOOK_ALLOW_LOCAL`
unset in deployments with real value. See [API coverage](SSP_API_COVERAGE.md)
for event types, signature verification, and retry behavior.
