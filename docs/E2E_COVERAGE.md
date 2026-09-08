# E2E coverage

Run `cargo regtest test`. CI runs this suite before image publication.
The runner uses the Breez SDK fork pinned by `vendor/breez-sdk`. Wallet
requests use its public API or authenticated, typed `ServiceProvider` API.
Tests do not construct GraphQL documents, mint sessions, derive signing keys,
or call the JavaScript Spark SDK.

| Feature | SDK entry point | E2E checks |
|---|---|---|
| Authentication | SDK session provider | Real challenge and signature exchange during wallet operations |
| Single-use deposits | `generate_single_use_deposit_address`, `claim_single_use_deposit` | Confirmed deposit becomes spendable Spark balance |
| Leaf swaps | `prepare_send_payment`, `send_payment` | Partial sends, split after restart, preserved balances, actual refund transactions and signatures |
| BOLT11 | `receive_payment`, `send_payment` | Both SSP directions, same-SSP settlement, preimages, exact credit, send replay, invalid and unfunded requests, invoice expiry |
| Missed receive | SDK invoice and sync | Pay while SSP is stopped; recover after restart without another payout |
| Confirmed static deposits | `receive_payment`, SDK request history | Credit, Bitcoin recovery, real fees, phase changes, stable timestamps |
| Instant static deposits | `get_instant_deposit_quote`, `claim_instant_deposit` | Zero-confirmation credit, wrong owner, changed quote, restart replay, same-amount replacement recovery |
| Cooperative withdrawals | `prepare_send_payment`, `send_payment`, SDK completion | Real payout, recovery, restart, completion replay |
| Withdrawal fee bump | SDK withdrawal plus admin fee bump | One CPFP child, bounded fee, retry keeps child ID, original payout preserved |
| BOLT12 | SDK Spark payment plus `ServiceProvider` offer methods | Prepayment, send, receive, request status, Spark balances, LDK settlement |
| Request history | `ServiceProvider::list_request_history`, `get_request_record` | Pagination, type/status/network filters, owner-bound cursors, stable reads |
| Webhooks | `register_webhook`, `list_webhooks`, `unregister_webhook` | Owner isolation, HMAC verification, repeated delivery of identical bytes, deletion |

Bitcoin Core, LDK, Docker, and SSP admin calls are fixture controls and
independent settlement checks. They mine blocks, provide liquidity, act as
external payers, restart services, and inspect real Bitcoin and Lightning
state. Wallet operations under test go through Breez.

## Remaining gaps

This is not complete E2E coverage of every failure path. In particular:

- Signed Lightning receive quote manifests and quote attestations have server
  tests, but the Breez SDK does not yet create invoices with committed quotes.
- Instant exposure limits have local tests. Missing or different-value
  replacements and recovery after a reorganization are not tested live;
  the live suite covers ordinary recovery and same-value replacements.
- Webhook registration and delivery run live for Lightning and withdrawals.
  Static-deposit event delivery has server integration coverage.
- Lost operator replies during signing, deep reorganizations, and BOLT12
  refund after a final backend failure do not have live fault-injection tests.
- BOLT12 uses the SDK's typed SSP methods. Its high-level payment-history
  adapter still expects BOLT11 invoices; these tests do not claim otherwise.
- Partner attribution and route fee estimation remain unsupported. There is
  no successful E2E operation to test for either feature.

Server unit and integration tests remain responsible for malformed protocol
inputs that the public wallet API cannot produce, signature domains,
reservation constraints, and deterministic reconciliation failure cases.
