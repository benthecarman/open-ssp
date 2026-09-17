# End-to-end dependencies

The `operator_*.key` files in this directory are public regtest fixtures.
Never reuse these keys on a network that carries value.

## Pinned source submodules

The acceptance suite uses the Git submodules recorded by this repository:

| Path | Source | Use |
|---|---|---|
| `vendor/spark` | [Spark fork](https://github.com/benthecarman/spark) | Three Spark Operators and their Rust signers |
| `vendor/breez-sdk` | [SSP SDK fork](https://github.com/benthecarman/spark-sdk) | SSP wallet, private operator RPC client, and Breez E2E client |
| `vendor/ldk-server` | [ldk-server](https://github.com/lightningdevkit/ldk-server) | Two Lightning nodes and their CLI |

The gitlinks store the exact commits. Inspect them with `git submodule status`.
Initialize the sources from the repository root:

```sh
git submodule update --init --recursive
```

New clones can use `git clone --recurse-submodules`. Run the update command
again after pulling changes to the parent repository. Normal setup must not
use `git submodule update --remote`, which follows upstream branches.

The native runner and CI use these source paths. Set `SPARK_REF` and
`LDK_SERVER_REF` to build from other local checkouts.

## Run the suite

Run `cargo regtest test`. Rust starts Bitcoin Core, PostgreSQL, an
Esplora-compatible Electrs indexer, three operator/signer pairs, two LDK nodes,
and two SSPs as native processes. Docker is not required. Both SSP wallets and
the acceptance client use the pinned `vendor/breez-sdk` fork through Cargo path
dependencies.

The operator and signer build uses a cached archive of the Spark checkout's
`HEAD`, excluding uncommitted changes. `SPARK_OPERATOR_COMMIT` selects another
commit present in that checkout. The runner passes `-listen-address 127.0.0.1`
to restrict the operator's HTTP and gRPC listeners to loopback. Alternate Spark
revisions must support this flag. LDK and SSP builds
include local edits.

`cargo regtest build` provisions checksum-verified tools and builds the service
binaries. Later tests can use `--no-build`. Build outputs survive fixture
resets. See [the regtest guide](../../docs/REGTEST_BREEZ.md) for prerequisites,
commands, artifact pins, and cache locations.

## Update a source pin

Fetch and check out the intended commit inside the submodule, then record its
new gitlink in the parent repository. For example, from the repository root:

```sh
git -C vendor/spark fetch origin <commit>
git -C vendor/spark checkout --detach <commit>
git add vendor/spark
```

Use `vendor/ldk-server` for an LDK update. Run the acceptance suite and the
Rust checks before committing the pin update with the related changes. Update
Cargo dependency pins separately when the change requires it.
