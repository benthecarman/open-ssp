# ldk-server compatibility

The SSP uses `ldk-server-client` for Lightning and keeps all Spark settlement
state in the SSP. This document lists the current integration boundary and
verified limitations.

## Supported production path

### BOLT11 send

The SSP verifies a matching Spark preimage-swap transfer. It stores the send
intent, funding transfer, and wallet idempotency key in one SQLite transaction
before it calls `Bolt11Send`. Events and reconciliation then update the send.
On success, the SSP gives the Lightning preimage to the Spark transfer.

Retries with the same wallet, invoice, transfer ID, and idempotency key return
the stored request. A funding transfer can belong to only one send intent.

BOLT11 payments between wallets on the same SSP use the wallet-held operator
preimage shares. The SSP reserves the receive, commits its Spark payout, and
claims the sender transfer with the recovered preimage. No LDK payment is
submitted. A concurrent external HTLC is rejected after that reservation.
The retired SSP-owned preimage extension remains removed.

### Submission recovery

A send starts in `PREPARED`. The SSP rechecks its Spark funding and durably
changes it to `SUBMITTING` before the network call. A returned LDK payment ID
changes it to `PENDING`. An RPC error leaves it in `SUBMITTING`, with the error
saved in `lightning_sends.last_error`. It is not a final payment failure.

After a lost reply or restart, reconciliation finds BOLT11 payments by hash.
For new BOLT12 sends, the SSP puts `open-ssp:<request UUID>` in the payer note.
It finds that note in LDK payment records and checks the offer ID, outbound
direction, and amount before it attaches the payment to the intent. Events
that arrived before the attachment are recovered from LDK payment state.
Only a confirmed final Lightning failure can start a BOLT12 refund.

LDK does not accept a caller-defined submission key. A crash after the
`SUBMITTING` checkpoint but before the RPC is indistinguishable from an
accepted payment whose record is unavailable. For BOLT11, the pinned LDK implementation uses the invoice hash as its
payment ID and rejects a duplicate pending or successful payment. After an
authoritative lookup finds no payment, the SSP can retry that same invoice.
For BOLT12, the SSP does not resubmit a `SUBMITTING` intent or refund it on a timeout. The public status
stays `LIGHTNING_PAYMENT_INITIATED`. Keep the SSP and LDK data intact and
restore their connection so reconciliation can find the original payment.
If no record appears, operator investigation is required; do not delete the
intent or create replacement funding on the assumption that payment failed.
A legacy BOLT12 send with a lost submission reply has no correlation note and
also needs investigation. An idempotent submission API in LDK is needed to
remove this uncertainty.

### BOLT11 receive

The wallet creates the preimage and stores encrypted threshold shares with the
Spark Operators through the existing Spark SDK receive flow. The SSP calls
`Bolt11ReceiveForHash` for that hash and waits for `PaymentClaimable`. It then
prepares an SSP-to-wallet transfer and calls `InitiatePreimageSwapV3` with
`REASON_RECEIVE`. The operators commit the transfer and return the reconstructed
preimage. The SSP verifies the preimage hash before it calls
`Bolt11ClaimForHash`.

The Spark commit and returned preimage are stored before the Lightning claim.
Retries and process restarts therefore do not create a second Spark transfer.
The SSP calls `Bolt11FailForHash` when an unfunded swap cannot complete or the
hold invoice expires.

### BOLT12 send

The SSP verifies a completed standard Spark transfer from the wallet, stores
the durable intent, and then calls `Bolt12Send`. It verifies the final payment hash and preimage. A final
Lightning failure starts a deterministic Spark refund. Reconciliation can
repeat the refund without creating a second transfer.

This is a prepaid flow, not an atomic swap. BOLT12 offers do not expose the
payment hash before the invoice-request exchange, so the wallet cannot create
the BOLT11 hash-locked funding transfer.

### BOLT12 receive

The SSP calls `Bolt12Receive` to create a fixed-value offer. After
`PaymentReceived`, it sends the offer amount to the wallet with a deterministic
transfer ID. Reconciliation can repeat the payout safely after a restart.

`ldk-server` claims the payment before the SSP makes the Spark transfer. This
is not an atomic receive. The limitation comes from the missing BOLT12 hold
API described below.

### Event recovery

The `SubscribeEvents` server stream has no per-request deadline. Only stream
connection setup has a 15-second timeout. If the stream ends, the SSP
reconnects with jittered exponential backoff capped at approximately 30
seconds. It also reconciles durable payment state through `ListPayments` every
30 seconds.

## ldk-server API gaps

### Route-fee estimation

`ldk-server` can decode an invoice but does not expose the route-fee estimate
needed by the SSP API. `lightning_send_fee_estimate` therefore returns zero.
Callers must not interpret this value as a routing guarantee.

Needed capability: an estimate RPC that uses the same routing data and limits
as the payment call.

### BOLT12 hold invoices

`ldk-server` does not expose a BOLT12 equivalent of
`Bolt11ReceiveForHash`, `Bolt11ClaimForHash`, and `Bolt11FailForHash`.
The SSP can receive BOLT12 payments, but it cannot hold them until the Spark
payout completes.

Needed capability: create, claim, and fail APIs for a BOLT12 payment that uses
a caller-supplied hash.

### Outbound cancellation

The client API does not expose an abandon or cancel operation for an outbound
payment. The SSP can observe a final success or failure, but it cannot force a
stuck payment into a terminal state.

Needed capability: an idempotent payment-cancel RPC with a clear result when
the payment is already final.

### Channel liquidity automation

`ldk-server` exposes peer and channel operations, but the deployment has no
automatic inbound-liquidity, rebalance, or channel-replacement policy. An
operator must monitor and maintain channel capacity.

Needed capability: deployment automation or an external liquidity controller;
this does not require a change to the SSP protocol.

## SSP gaps that are not ldk-server gaps

These limitations are in the SSP and must not be attributed to `ldk-server`:

- Fee-bearing receive quotes and automatic Spark liquidity replenishment
  are not implemented.
- Instant static deposits require explicit advance limits and sufficient
  Spark liquidity. Recovery does not replenish Spark liquidity automatically.

Cooperative withdrawals use a dedicated Bitcoin Core wallet. The pinned
`ldk-server` API can send Bitcoin, but it cannot prepare and sign a transaction
in separate steps. Cooperative exits need the payout transaction ID before
the wallet commits its conditional Spark transfer.

See [SSP API coverage](SSP_API_COVERAGE.md) for the operation-level status.

## Deployment requirements

Production must provide `LDK_GRPC_ADDR`, either `LDK_API_KEY` or
`LDK_API_KEY_FILE`, and `LDK_TLS_CERT_FILE`. Authenticated `/status` must report
`ldk_mode: "live"` and the expected `ldk_node_id`; `/health` reports only basic
process liveness.

The service has one live LDK backend. Startup fails if LDK is unavailable or
its credentials cannot be read. Event reconnection and payment reconciliation
continue to handle interruptions after startup.
