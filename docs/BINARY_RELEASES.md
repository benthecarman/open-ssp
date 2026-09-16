# Standalone open-ssp binary

The binary release contains the SSP executable and its SHA-256 checksum:

- `open-ssp-vMAJOR.MINOR.PATCH[-prerelease]-linux-x86_64`
- `open-ssp-vMAJOR.MINOR.PATCH[-prerelease]-linux-x86_64.sha256`

The executable runs the SSP service. It connects to separately managed Spark
operators and an LDK server. Bitcoin RPC is also needed for configured
on-chain recovery and withdrawal features. Configure the same environment
variables used for source or container deployments; see
[deployment configuration](DEPLOY.md) and [the config fields](../src/config.rs).

Verify the download, make it executable, and run it from any directory:

```sh
sha256sum -c open-ssp-v0.1.0-linux-x86_64.sha256
chmod +x open-ssp-v0.1.0-linux-x86_64
./open-ssp-v0.1.0-linux-x86_64
```

Set `SSP_DATA_DIR` and `SPARK_MNEMONIC_FILE` to persistent writable paths.
`LDK_GRPC_ADDR`, the LDK credentials/certificate, and the Spark operator
configuration must point at your existing services. `SPARK_ADMIN_TOKEN`
is required by default. `open-ssp healthcheck` checks the running service's
`/health` endpoint using `SSP_LISTEN_ADDR`.

The executable does not need a source checkout, Git, Cargo, Go, PostgreSQL,
or a compiler. SQLite is compiled in. Ubuntu 24.04 workflow builds require
Linux x86_64, glibc 2.39 or newer, and libgcc-s1. Local builds can require
newer libraries; inspect them with `ldd ./open-ssp`. Spark and LDK have their
own runtime requirements.

Build locally with `cargo build --release --locked --bin open-ssp`.
The result is `target/release/open-ssp`.

The `binary-release` workflow builds the release profile on Ubuntu 24.04,
checks dynamic linkage and configuration parsing, and uploads the executable
and checksum. It does not start or package a regtest stack. Publishing is
opt-in: select an existing `vVERSION` tag, give the matching version input,
and explicitly enable `publish`. The workflow never creates tags.
