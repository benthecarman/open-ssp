# Native regtest binary bundle

A Linux x86_64 bundle runs the fixture without a checkout, Git, Cargo, Go,
compiler, Docker, downloads, or vendor sources. It contains the runner, SSP,
Spark operator and signer, LDK server and CLI, Bitcoin Core 29.0 (`bitcoind`
and `bitcoin-cli`), package-broadcasting Electrs, Atlas Community 1.0.0,
operator configuration, regtest identity keys, and both Spark migration trees.
All credentials and keys are public test credentials; use only for regtest.

## Artifacts and integrity

Version `vMAJOR.MINOR.PATCH[-prerelease]` produces:

- `open-ssp-regtest-VERSION-linux-x86_64.tar.gz`
- `open-ssp-regtest-VERSION-linux-x86_64.manifest.json`
- `open-ssp-regtest-VERSION-linux-x86_64.sha256`

The `.sha256` file uses the GNU `sha256sum` format: lowercase SHA-256, two
spaces, basename, newline. Verify before extraction with `sha256sum -c FILE`.
The identical `manifest.json` inside the archive has `schema_version: 1`,
component Git revisions (open-ssp, Spark, LDK, Breez SDK, Electrs), tool
versions and upstream download hashes, build profiles/toolchains/OS, shared
library reports, and SHA-256 hashes of every payload file. `source_dirty`
identifies local development builds. Official builds use a clean checkout.
These checksums detect corruption; obtain artifacts/checksums from a trusted
release. The runner does not rehash the bundle at each command.

Build with `python3 scripts/regtest/package.py --version v0.1.0` from a
checkout with the build prerequisites in REGTEST_BREEZ.md. Builds reuse the
native fixture's tested profiles and strip debug information; “release
bundle” does not imply every service uses Cargo's release profile.
The release workflow builds on Ubuntu 24.04, validates the extracted bundle,
and uploads versioned artifacts. Manual runs do not publish by default.

## Runtime prerequisites

Use Linux x86_64 as a regular non-root user. Official Ubuntu 24.04 builds
require glibc 2.39 or newer, libstdc++6, libgcc-s1, libzmq5 and OpenSSL CLI, plus
PostgreSQL 16 server tools. On Ubuntu 24.04:

```sh
sudo apt-get install postgresql-16 openssl libstdc++6 libgcc-s1 libzmq5
export PGBIN=/usr/lib/postgresql/16/bin
```

`PGBIN` must contain `initdb`, `postgres`, and `psql`. If omitted, the runner
uses `pg_config --bindir` (install `libpq-dev` to supply it). PostgreSQL's own
shared libraries come from its distribution packages. No system PostgreSQL
service is needed: the runner creates a private cluster on port 54329.
Use the same PostgreSQL major version when resuming data; reset to upgrade.
The `libzmq5` package supplies its transitive dependencies (including sodium,
PGM, NORM and Kerberos libraries). Local builds may require newer glibc/libstdc++; consult `manifest.json` and
`ldd bin/*`. Services need several GB of free memory and disk.

## Downstream CLI

Extract anywhere and keep the entire directory together. The runner discovers
`../manifest.json` relative to its executable, independently of the working
directory. Use an absolute path for the executable and data directory:

```sh
export REGTEST_DATA_DIR=/absolute/writable/regtest
export REGTEST_PROJECT=orange-fixture
R=/absolute/extracted/open-ssp-regtest-v0.1.0-linux-x86_64/bin/open-ssp-regtest
"$R" up
"$R" fund a 5000000
"$R" miner stop
"$R" ldk a get-node-info
"$R" ldk b get-node-info
"$R" ldk a list-channels
"$R" ldk b bolt11-receive 500sat -d integration-test
"$R" info
"$R" certs
"$R" logs ssp ldk-server spark-operator-0 electrs
"$R" stop
"$R" start
"$R" reset
```

Global `--project NAME` and `--data-dir DIRECTORY` flags before the command
override the environment. Data defaults to
`${XDG_DATA_HOME:-$HOME/.local/share}/open-ssp/regtest`. `up` initializes,
starts, opens/funds the A/B Lightning channel, and gives each SSP at least
10,000 sats of Spark liquidity. `fund a 5000000` adds one 5,000,000-sat leaf
on top of that liquidity and mines the funding confirmations itself. Stop
automatic mining after funding; it normally mines every ten seconds.
`miner start` resumes it. `start` resumes services and enables the miner;
it does not provision the Lightning channel or SSP funding. `up` also
restarts services on an existing project. `stop`/`down` preserve data;
`reset` stops owned processes and deletes this project's `native/` data.

`ldk <a|b> COMMAND...` forwards arguments to the bundled `ldk-server-cli`
with the correct API key and TLS certificate. `ldk a --help` lists commands.
Failures return a nonzero exit code. `info` prints schema-versioned JSON
with absolute data/log/certificate paths, operator identities and endpoints.
`status` performs live readiness checks. `certs DIRECTORY` copies the three
operator certificates. `logs [SERVICE...]` prints the last 60 lines; archive
`info.log_dir/*.log` for complete logs **before reset**. `test --keep` also
runs the Breez acceptance suite, resetting the selected project first.
`init` and `build` reject bundle mode.

| Service | Endpoint |
|---|---|
| SSP A / B | http://127.0.0.1:5000 / :5001 |
| LDK A / B gRPC | https://localhost:3536 / :3537 |
| LDK A / B peer | 127.0.0.1:19735 / :19736 |
| Spark operators | https://localhost:8535, :8536, :8537 |
| Operator SSP gRPC | https://localhost:18535, :18536, :18537 |
| Esplora / Electrum | http://127.0.0.1:30000 / 127.0.0.1:60401 |
| Bitcoin RPC | http://127.0.0.1:8332 |

Bitcoin RPC credentials are `testutil:testutilpassword`; wallets are `default`
and `ssp-withdrawals`. `BITCOIN_RPC_PORT` overrides port 8332; keep it consistent
across all commands. Additional reserved ports: 18444, 28332, 28333, 24224,
54329. The SSP admin bearer token defaults to `regtest-spark-admin-token`;
override with `SPARK_ADMIN_TOKEN` consistently across invocations.

Projects isolate **data**, not ports. Run only one fixture per host/network
namespace. Lifecycle/funding/admin mutations take a nonblocking exclusive
lock at `DATA/native.lock`, and fail if another such command holds it. LDK
commands (including reads), `info`, `status`, `logs`, and `certs` do not take
that lock, so LDK reads can run during funding. Do not race lifecycle changes
or reset with these commands. Separate data roots do not share a lock; port
checks reject an active conflicting stack. After Electrs teardown the CLI
waits up to 75 seconds for port 30000's TIME_WAIT sockets to expire; an active
listener is rejected immediately. Consecutive fixtures need no manual delay.

## Asset and data layout

```text
BUNDLE/                         immutable; can be mounted read-only
  bin/                          all ten compiled executables
  share/upstream/                configuration and public regtest keys
  share/spark/spark/so/{ent,entephemeral}/migrate/migrations/
  share/licenses/               component license notices
  manifest.json
  README.md
DATA/                           writable; must be outside BUNDLE
  native.lock
  native-sockets/<project-path-hash>/   signer Unix sockets
  PROJECT/native/
    *.json                      process specifications
    *.pid                       PID and Linux start-time ownership
    *.log                       full service logs
    tls/                        generated operator certs and keys
    postgres/ bitcoin/ electrs/  private databases
    ldk-server/ ldk-server-2/    LDK TLS, API keys, wallets
    ssp/ ssp-2/                 SSP databases and wallets
```

Keep DATA paths short enough for Linux's 108-byte Unix socket path limit.
`reset` preserves the shared lock, other projects and immutable bundle.
Stop services before moving the bundle or data; `start` regenerates process
specifications and configuration for the new absolute paths. Never edit
assets in place while a fixture is running. For new versions, reset fixture
data unless compatibility with the prior component schemas is established.

Bitcoin Core is MIT licensed; [Atlas Community is Apache-2.0 licensed](https://github.com/ariga/atlas/releases/tag/v1.0.0).
Their license files and the other component notices are included under
`share/licenses`. The manifest records exact source revisions for the other services.

## Validate a build

The validation helper requires Python 3.12+ and bubblewrap on the build host:

```sh
python3 scripts/regtest/validate.py \
  dist/open-ssp-regtest-v0.1.0-linux-x86_64.tar.gz \
  --work-dir .regtest/bundle-validation
```

It mounts the extracted bundle read-only at `/opt/fixture`, exposes writable
state at `/state`, hides the checkout and build tools, and disables external
network access. It checks payload hashes, startup, 5,000,000-sat funding,
concurrent LDK A/B reads, invoice creation, mining stop/resume, certificates,
logs, stop/start, and consecutive reset/up. Full daemon logs are retained
under `--work-dir/state/logs`. The namespace is destroyed on exit, including
any processes left after a failed test. Host writes remain in `--work-dir`.
This smoke test does not run the full Breez payment acceptance suite.
