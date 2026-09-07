# End-to-end dependencies

The `operator_*.key` files in this directory are public regtest fixtures.
Never reuse these keys on a network that carries value.

## Pinned source submodules

Both acceptance suites use the Git submodules recorded by this repository:

| Path | Source | Use |
|---|---|---|
| `vendor/spark` | [Spark fork](https://github.com/benthecarman/spark) | Three Spark Operators and the supplemental JavaScript SDK |
| `vendor/breez-sdk` | [SSP SDK fork](https://github.com/benthecarman/spark-sdk) | Embedded SSP wallet and private operator RPC client |
| `vendor/ldk-server` | [ldk-server](https://github.com/lightningdevkit/ldk-server) | Two Lightning nodes and their CLI |

The gitlinks store the exact commits. Inspect them with `git submodule status`.
Initialize the sources from the repository root:

```sh
git submodule update --init --recursive
```

New clones can use `git clone --recurse-submodules`. Run the update command
again after pulling changes to the parent repository. Normal setup must not
use `git submodule update --remote`, which follows upstream branches.

The Compose file, test runners, and CI use these same source paths. Set
`SPARK_REF` and `LDK_SERVER_REF` to use other local checkouts. Set `SDK_REF`
only when the JavaScript SDK checkout differs from `SPARK_REF`.

## Run the suites

The Lightning acceptance test runs `cargo regtest test`. It uses the Spark
submodule only to build three local operators. The Rust Breez SDK wallet dependency is
pinned in `e2e/breez/Cargo.toml` and `e2e/breez/Cargo.lock`; Cargo fetches it.
The SSP's Rust Spark dependencies use `vendor/breez-sdk`. The end client
continues to use its separate upstream Cargo pin.

The runner builds the operators from a clean detached worktree at the Spark
checkout's `HEAD`. It does not include uncommitted operator changes. Set
`SPARK_OPERATOR_COMMIT` to test another commit present in that checkout.
It starts a pinned Electrs image and uses its local Esplora API for Breez chain
data. See [the regtest guide](../../docs/REGTEST_BREEZ.md) for the full setup.

For the supplemental JavaScript suite, build the SDK first (Node.js 22 and
Corepack are required):

```sh
(cd vendor/spark/sdks/js && corepack enable && yarn install --no-immutable && yarn build:sdk)
./e2e/e2e.sh
```

If you set `SDK_REF`, build the SDK in that checkout instead.

## Update a source pin

Fetch and check out the intended commit inside the submodule, then record its
new gitlink in the parent repository. For example, from the repository root:

```sh
git -C vendor/spark fetch origin <commit>
git -C vendor/spark checkout --detach <commit>
git add vendor/spark
```

Use `vendor/ldk-server` for an LDK update. Run both acceptance suites and the
Rust checks before committing the pin update with the related changes. Update
Cargo dependency pins separately when the change requires it.
