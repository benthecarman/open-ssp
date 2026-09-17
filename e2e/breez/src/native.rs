//! Docker-free regtest service lifecycle. Each daemon has its own process group.
pub mod tools;

use crate::{optional_env, poll};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::os::fd::AsRawFd;
use std::os::unix::process::CommandExt;
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::process::Command;

const SERVICES: &[&str] = &[
    "postgres",
    "bitcoind",
    "electrs",
    "signer-0",
    "signer-1",
    "signer-2",
    "spark-operator-0",
    "spark-operator-1",
    "spark-operator-2",
    "ldk-server",
    "ldk-server-2",
    "ssp",
    "ssp-2",
    "bitcoin-miner",
];

#[derive(Clone, Debug)]
pub struct Runtime {
    pub root: PathBuf,
    pub directory: PathBuf,
    pub project: String,
}

#[derive(Serialize, Deserialize)]
struct Service {
    executable: PathBuf,
    args: Vec<String>,
    env: BTreeMap<String, String>,
}

#[derive(Serialize, Deserialize)]
struct Process {
    pid: u32,
    start_time: String,
}

pub struct Lock(fs::File);

impl Drop for Lock {
    fn drop(&mut self) {
        // Explicit unlock also handles a concurrent fork briefly holding an
        // inherited descriptor before exec closes it.
        unsafe {
            libc::flock(self.0.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

fn process_identity(pid: u32) -> Option<String> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let fields: Vec<_> = stat.rsplit_once(") ")?.1.split_whitespace().collect();
    if fields.first() == Some(&"Z") {
        return None;
    }
    fields.get(19).map(|s| s.to_string())
}

impl Runtime {
    /// Serialize mutations and builds across projects sharing ports and caches.
    /// The lock lives outside resettable data and closes automatically on exit.
    pub fn lock(&self) -> Result<Lock> {
        fs::create_dir_all(self.root.join(".regtest"))?;
        let file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(self.root.join(".regtest/native.lock"))?;
        ensure!(
            unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
            "another native regtest command is running in this repository"
        );
        Ok(Lock(file))
    }

    fn check_ports(&self) -> Result<()> {
        let bitcoin_port = optional_env("BITCOIN_RPC_PORT", "8332").parse::<u16>()?;
        for (name, ports) in [
            ("postgres", vec![54329]),
            ("bitcoind", vec![bitcoin_port, 18444, 28332, 28333]),
            ("electrs", vec![30000, 60401, 24224]),
            ("spark-operator-0", vec![8535, 18535]),
            ("spark-operator-1", vec![8536, 18536]),
            ("spark-operator-2", vec![8537, 18537]),
            ("ldk-server", vec![3536, 19735]),
            ("ldk-server-2", vec![3537, 19736]),
            ("ssp", vec![5000]),
            ("ssp-2", vec![5001]),
        ] {
            if self.running(name)? {
                continue;
            }
            for port in ports {
                let socket = tokio::net::TcpSocket::new_v4()?;
                socket.set_reuseaddr(true)?;
                socket
                    .bind((std::net::Ipv4Addr::LOCALHOST, port).into())
                    .with_context(|| {
                        format!("{name} needs port {port}; stop the conflicting stack first")
                    })?;
            }
        }
        Ok(())
    }
    pub fn new(root: PathBuf, project: String) -> Self {
        let directory = root.join(".regtest").join(&project).join("native");
        Self {
            root,
            directory,
            project,
        }
    }

    pub fn cert_dir(&self) -> PathBuf {
        self.directory.join("tls")
    }
    pub fn data(&self, name: &str) -> PathBuf {
        self.directory.join(name)
    }
    pub fn ldk_cli(&self) -> PathBuf {
        self.root
            .join(".regtest/native-build/ldk/debug/ldk-server-cli")
    }

    fn service_file(&self, name: &str, suffix: &str) -> Result<PathBuf> {
        ensure!(SERVICES.contains(&name), "unknown service {name}");
        Ok(self.directory.join(format!("{name}.{suffix}")))
    }

    fn process(&self, name: &str) -> Result<Option<Process>> {
        let path = self.service_file(name, "pid")?;
        if !path.exists() {
            return Ok(None);
        }
        let process: Process = serde_json::from_slice(&fs::read(path)?)?;
        Ok(
            (process_identity(process.pid).as_deref() == Some(&process.start_time))
                .then_some(process),
        )
    }

    pub fn running(&self, name: &str) -> Result<bool> {
        Ok(self.process(name)?.is_some())
    }

    pub async fn watch_startup(&self) -> Result<()> {
        loop {
            for name in SERVICES {
                if self.service_file(name, "pid")?.is_file() {
                    ensure!(
                        self.running(name)?,
                        "{name} exited during startup; inspect its log"
                    );
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn start(&self, name: &str) -> Result<()> {
        if self.running(name)? {
            return Ok(());
        }
        let spec: Service = serde_json::from_slice(
            &fs::read(self.service_file(name, "json")?)
                .with_context(|| format!("{name} is not configured; run cargo regtest up"))?,
        )?;
        let log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.service_file(name, "log")?)?;
        let mut command = Command::new(&spec.executable);
        command
            .args(&spec.args)
            .envs(&spec.env)
            .current_dir(&self.directory)
            .stdin(std::process::Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log);
        command.as_std_mut().process_group(0);
        let mut child = command
            .spawn()
            .with_context(|| format!("start {name}: {}", spec.executable.display()))?;
        let pid = child.id().context("process has no PID")?;
        let save = (|| -> Result<()> {
            let start_time =
                process_identity(pid).with_context(|| format!("{name} exited during startup"))?;
            let temporary = self.service_file(name, "pid.partial")?;
            fs::write(
                &temporary,
                serde_json::to_vec(&Process { pid, start_time })?,
            )?;
            fs::rename(temporary, self.service_file(name, "pid")?)?;
            Ok(())
        })();
        if let Err(error) = save {
            unsafe {
                libc::kill(-(pid as i32), libc::SIGKILL);
            }
            let _ = child.wait().await;
            return Err(error.context(format!("record {name} process ownership")));
        }
        // Tokio reaps exited children. Persistent `up` intentionally survives the CLI.
        drop(child);
        println!("Started {name} (pid {pid})");
        Ok(())
    }

    pub async fn stop(&self, name: &str) -> Result<()> {
        if let Some(process) = self.process(name)? {
            // The saved Linux start time prevents signalling a reused PID.
            unsafe {
                libc::kill(-(process.pid as i32), libc::SIGTERM);
            }
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            while self.running(name)? && tokio::time::Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            if self.running(name)? {
                unsafe {
                    libc::kill(-(process.pid as i32), libc::SIGKILL);
                }
                poll(&format!("stop {name}"), Duration::from_secs(5), || async {
                    ensure!(!self.running(name)?, "{name} is still running");
                    Ok(())
                })
                .await?;
            }
            println!("Stopped {name}");
        }
        let pid = self.service_file(name, "pid")?;
        if pid.exists() {
            fs::remove_file(pid)?;
        }
        Ok(())
    }

    pub async fn restart(&self, name: &str) -> Result<()> {
        self.stop(name).await?;
        self.start(name).await
    }

    pub async fn stop_all(&self) -> Result<()> {
        let mut errors = Vec::new();
        for name in SERVICES.iter().rev() {
            if let Err(error) = self.stop(name).await {
                errors.push(format!("{name}: {error:#}"));
            }
        }
        ensure!(
            errors.is_empty(),
            "could not stop services: {}",
            errors.join("; ")
        );
        Ok(())
    }

    pub async fn reset(&self) -> Result<()> {
        self.stop_all().await?;
        if self.directory.exists() {
            fs::remove_dir_all(&self.directory)?;
        }
        Ok(())
    }

    pub fn status(&self) -> Result<()> {
        for name in SERVICES {
            println!(
                "{name}: {}",
                if self.running(name)? {
                    "running"
                } else {
                    "stopped"
                }
            );
        }
        Ok(())
    }

    pub fn logs(&self, names: &[String]) -> Result<()> {
        let names: Vec<&str> = if names.is_empty() {
            SERVICES.to_vec()
        } else {
            names.iter().map(String::as_str).collect()
        };
        for name in names {
            let path = self.service_file(name, "log")?;
            if let Ok(log) = fs::read_to_string(path) {
                println!("--- {name} ---");
                let lines: Vec<_> = log.lines().collect();
                for line in lines.iter().skip(lines.len().saturating_sub(60)) {
                    println!("{line}");
                }
            }
        }
        Ok(())
    }

    fn configure(
        &self,
        name: &str,
        executable: PathBuf,
        args: Vec<String>,
        env: BTreeMap<String, String>,
    ) -> Result<()> {
        fs::create_dir_all(&self.directory)?;
        fs::write(
            self.service_file(name, "json")?,
            serde_json::to_vec_pretty(&Service {
                executable,
                args,
                env,
            })?,
        )?;
        Ok(())
    }

    pub async fn sql(&self, database: &str, sql: &str) -> Result<String> {
        tools::output(Command::new(tools::pg_bin().await?.join("psql")).args([
            "-h",
            "127.0.0.1",
            "-p",
            "54329",
            "-U",
            "postgres",
            "-d",
            database,
            "-v",
            "ON_ERROR_STOP=1",
            "-tAc",
            sql,
        ]))
        .await
    }

    async fn rpc(&self, wallet: Option<&str>, method: &str, params: Value) -> Result<Value> {
        let base = format!(
            "http://127.0.0.1:{}",
            optional_env("BITCOIN_RPC_PORT", "8332")
        );
        let url = wallet.map_or(base.clone(), |wallet| format!("{base}/wallet/{wallet}"));
        let value: Value = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .build()?
            .post(url)
            .basic_auth("testutil", Some("testutilpassword"))
            .json(&json!({"jsonrpc":"1.0", "id":method, "method":method, "params":params}))
            .send()
            .await?
            .json()
            .await?;
        ensure!(
            value["error"].is_null(),
            "Bitcoin {method}: {}",
            value["error"]
        );
        Ok(value["result"].clone())
    }

    pub async fn miner(&self) -> Result<()> {
        loop {
            let address = self
                .rpc(Some("default"), "getnewaddress", json!([]))
                .await?;
            self.rpc(Some("default"), "generatetoaddress", json!([1, address]))
                .await?;
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    }

    pub async fn setup(&self, spark: &Path, admin_token: &str) -> Result<()> {
        self.check_ports()?;
        fs::create_dir_all(&self.directory)?;
        let pg = tools::pg_bin().await?;
        let bin = self.root.join(".regtest/native-tools");
        let builds = self.root.join(".regtest/native-build");
        let empty = BTreeMap::new();
        if !self.data("postgres/PG_VERSION").exists() {
            tools::output(
                Command::new(pg.join("initdb"))
                    .arg("-D")
                    .arg(self.data("postgres"))
                    .args([
                        "-U",
                        "postgres",
                        "--auth=trust",
                        "--no-locale",
                        "--encoding=UTF8",
                    ]),
            )
            .await?;
        }
        self.configure(
            "postgres",
            pg.join("postgres"),
            vec![
                "-D".into(),
                self.data("postgres").display().to_string(),
                "-h".into(),
                "127.0.0.1".into(),
                "-p".into(),
                "54329".into(),
                "-k".into(),
                "".into(),
            ],
            empty.clone(),
        )?;
        self.start("postgres").await?;
        poll("PostgreSQL", Duration::from_secs(30), || {
            self.sql("postgres", "SELECT 1")
        })
        .await?;

        fs::create_dir_all(self.data("bitcoin"))?;
        let bitcoin_config = fs::read_to_string(self.root.join("e2e/upstream/bitcoin.conf"))?
            .replace("0.0.0.0", "127.0.0.1")
            .replace("rpcallowip=127.0.0.1/0", "rpcallowip=127.0.0.1")
            .replace(
                "rpcport=8332",
                &format!("rpcport={}", optional_env("BITCOIN_RPC_PORT", "8332")),
            )
            .replace(
                "\nport=8333",
                "\nport=18444\nbind=127.0.0.1:18444\nlistenonion=0",
            );
        fs::write(self.data("bitcoin.conf"), bitcoin_config)?;
        self.configure(
            "bitcoind",
            bin.join("bitcoin-29.0/bin/bitcoind"),
            vec![
                format!("-datadir={}", self.data("bitcoin").display()),
                format!("-conf={}", self.data("bitcoin.conf").display()),
            ],
            empty.clone(),
        )?;
        self.start("bitcoind").await?;
        poll("Bitcoin RPC", Duration::from_secs(30), || {
            self.rpc(None, "getblockchaininfo", json!([]))
        })
        .await?;
        let loaded = self.rpc(None, "listwallets", json!([])).await?;
        let available = self.rpc(None, "listwalletdir", json!([])).await?;
        for name in ["default", "ssp-withdrawals"] {
            if !loaded
                .as_array()
                .context("wallet list missing")?
                .iter()
                .any(|w| w == name)
            {
                let exists = available["wallets"]
                    .as_array()
                    .is_some_and(|wallets| wallets.iter().any(|w| w["name"] == name));
                self.rpc(
                    None,
                    if exists { "loadwallet" } else { "createwallet" },
                    json!([name]),
                )
                .await?;
            }
        }
        if self
            .rpc(Some("default"), "getbalance", json!([]))
            .await?
            .as_f64()
            .unwrap_or(0.0)
            == 0.0
        {
            let address = self
                .rpc(Some("default"), "getnewaddress", json!([]))
                .await?;
            self.rpc(Some("default"), "generatetoaddress", json!([101, address]))
                .await?;
        }
        if self
            .rpc(Some("ssp-withdrawals"), "getbalance", json!([]))
            .await?
            .as_f64()
            .unwrap_or(0.0)
            == 0.0
        {
            let address = self
                .rpc(
                    Some("ssp-withdrawals"),
                    "getnewaddress",
                    json!(["", "bech32m"]),
                )
                .await?;
            self.rpc(Some("default"), "sendtoaddress", json!([address, 1]))
                .await?;
            let address = self
                .rpc(Some("default"), "getnewaddress", json!([]))
                .await?;
            self.rpc(Some("default"), "generatetoaddress", json!([3, address]))
                .await?;
        }

        fs::create_dir_all(self.cert_dir())?;
        for i in 0..3 {
            let key = self.cert_dir().join(format!("server_{i}.key"));
            let cert = self.cert_dir().join(format!("server_{i}.crt"));
            if !cert.is_file() {
                tools::output(
                    Command::new("openssl")
                        .args([
                            "req",
                            "-new",
                            "-x509",
                            "-newkey",
                            "rsa:2048",
                            "-nodes",
                            "-days",
                            "3650",
                            "-subj",
                            "/CN=localhost",
                            "-addext",
                            "subjectAltName=DNS:localhost,IP:127.0.0.1",
                            "-addext",
                            "basicConstraints=critical,CA:FALSE",
                        ])
                        .arg("-keyout")
                        .arg(&key)
                        .arg("-out")
                        .arg(&cert),
                )
                .await?;
            }
        }
        let mut operators: Value = serde_json::from_str(&fs::read_to_string(
            self.root.join("e2e/upstream/config.json"),
        )?)?;
        for (i, operator) in operators
            .as_array_mut()
            .context("operator config missing")?
            .iter_mut()
            .enumerate()
        {
            operator["address"] = json!(format!("localhost:{}", 8535 + i));
            operator["external_address"] = operator["address"].clone();
            operator["cert_path"] = json!(self.cert_dir().join(format!("server_{i}.crt")));
        }
        fs::write(
            self.data("operators.json"),
            serde_json::to_vec_pretty(&operators)?,
        )?;
        let config = fs::read_to_string(self.root.join("e2e/upstream/operator.config.yaml"))?
            .replace(
                "bitcoind:8332",
                &format!("127.0.0.1:{}", optional_env("BITCOIN_RPC_PORT", "8332")),
            )
            .replace("bitcoind:28332", "127.0.0.1:28332");
        fs::write(self.data("operator.yaml"), config)?;
        let socket_dir = self.root.join(".regtest/native-sockets").join(
            &hex::encode(Sha256::digest(
                self.directory.as_os_str().as_encoded_bytes(),
            ))[..12],
        );
        fs::create_dir_all(&socket_dir)?;
        for i in 0..3 {
            for (db, migrations) in [
                (
                    format!("sparkoperator_{i}"),
                    "spark/so/ent/migrate/migrations",
                ),
                (
                    format!("spark_ephemeral_{i}"),
                    "spark/so/entephemeral/migrate/migrations",
                ),
            ] {
                if self
                    .sql(
                        "postgres",
                        &format!("SELECT 1 FROM pg_database WHERE datname = '{db}'"),
                    )
                    .await?
                    .is_empty()
                {
                    self.sql("postgres", &format!("CREATE DATABASE {db}"))
                        .await?;
                }
                tools::output(
                    Command::new(bin.join("atlas"))
                        .args(["migrate", "apply", "--dir"])
                        .arg(format!("file://{}", spark.join(migrations).display()))
                        .arg("--url")
                        .arg(format!(
                            "postgresql://postgres@127.0.0.1:54329/{db}?sslmode=disable"
                        )),
                )
                .await?;
            }
            let signer = format!("signer-{i}");
            let socket = socket_dir.join(format!("{i}.sock"));
            if !self.running(&signer)? && socket.exists() {
                fs::remove_file(&socket)?;
            }
            self.configure(
                &signer,
                builds.join("signer/debug/spark-frost-signer"),
                vec!["-u".into(), socket.display().to_string()],
                BTreeMap::from([("RUST_LOG".into(), "warn".into())]),
            )?;
            self.start(&signer).await?;
            poll(&signer, Duration::from_secs(30), || async {
                ensure!(socket.exists(), "signer socket missing");
                Ok(())
            })
            .await?;
            let name = format!("spark-operator-{i}");
            self.configure(
                &name,
                bin.join("spark-operator"),
                vec![
                    "-config".into(),
                    self.data("operator.yaml").display().to_string(),
                    "-index".into(),
                    i.to_string(),
                    "-key".into(),
                    self.root
                        .join(format!("e2e/upstream/operator_{i}.key"))
                        .display()
                        .to_string(),
                    "-operators".into(),
                    self.data("operators.json").display().to_string(),
                    "-threshold".into(),
                    "2".into(),
                    "-signer".into(),
                    format!("unix://{}", socket.display()),
                    "-port".into(),
                    (8535 + i).to_string(),
                    "-listen-address".into(),
                    "127.0.0.1".into(),
                    "-database".into(),
                    format!(
                        "postgresql://postgres@127.0.0.1:54329/sparkoperator_{i}?sslmode=disable"
                    ),
                    "-ephemeral-database".into(),
                    format!(
                        "postgresql://postgres@127.0.0.1:54329/spark_ephemeral_{i}?sslmode=disable"
                    ),
                    "-server-cert".into(),
                    self.cert_dir()
                        .join(format!("server_{i}.crt"))
                        .display()
                        .to_string(),
                    "-server-key".into(),
                    self.cert_dir()
                        .join(format!("server_{i}.key"))
                        .display()
                        .to_string(),
                    "-ssp-grpc-port".into(),
                    (18535 + i).to_string(),
                    "-local".into(),
                    "true".into(),
                ],
                empty.clone(),
            )?;
            self.start(&name).await?;
        }

        fs::create_dir_all(self.data("electrs"))?;
        self.configure(
            "electrs",
            tools::electrs_binary(&self.root),
            vec![
                "--network=regtest".into(),
                "--jsonrpc-import".into(),
                "--cookie=testutil:testutilpassword".into(),
                format!(
                    "--daemon-rpc-addr=127.0.0.1:{}",
                    optional_env("BITCOIN_RPC_PORT", "8332")
                ),
                format!("--db-dir={}", self.data("electrs").display()),
                format!("--daemon-dir={}", self.data("bitcoin").display()),
                "--electrum-rpc-addr=127.0.0.1:60401".into(),
                "--http-addr=127.0.0.1:30000".into(),
                "--monitoring-addr=127.0.0.1:24224".into(),
                "--cors=*".into(),
                "--address-search".into(),
            ],
            empty.clone(),
        )?;
        self.start("electrs").await?;
        for (name, grpc, peer, ssp_port, ssp_name, min_split) in [
            ("ldk-server", 3536, 19735, 5000, "ssp", 1),
            ("ldk-server-2", 3537, 19736, 5001, "ssp-2", 330),
        ] {
            let data = self.data(name);
            fs::create_dir_all(&data)?;
            let config = format!(
                "[node]\ngrpc_service_address = '127.0.0.1:{grpc}'\nnetwork = 'regtest'\nlistening_addresses = ['127.0.0.1:{peer}']\n[storage.disk]\ndir_path = '{}'\n[log]\nlevel = 'Info'\n[tls]\nhosts = ['localhost', '127.0.0.1']\n[bitcoind]\nrpc_address = '127.0.0.1:{}'\nrpc_user = 'testutil'\nrpc_password = 'testutilpassword'\n",
                data.display(),
                optional_env("BITCOIN_RPC_PORT", "8332")
            );
            fs::write(self.data(&format!("{name}.toml")), config)?;
            self.configure(
                name,
                builds.join("ldk/debug/ldk-server"),
                vec![self.data(&format!("{name}.toml")).display().to_string()],
                empty.clone(),
            )?;
            self.start(name).await?;
            fs::create_dir_all(self.data(ssp_name))?;
            let certs = (0..3)
                .map(|i| {
                    self.cert_dir()
                        .join(format!("server_{i}.crt"))
                        .display()
                        .to_string()
                })
                .collect::<Vec<_>>()
                .join(",");
            let mut env = BTreeMap::from([
                ("SSP_LISTEN_ADDR".into(), format!("127.0.0.1:{ssp_port}")),
                ("SSP_NETWORK".into(), "REGTEST".into()),
                (
                    "SSP_DATA_DIR".into(),
                    self.data(ssp_name).display().to_string(),
                ),
                (
                    "SSP_PUBLIC_URL".into(),
                    format!("http://127.0.0.1:{ssp_port}"),
                ),
                (
                    "SPARK_MNEMONIC_FILE".into(),
                    self.data(ssp_name)
                        .join("spark.mnemonic")
                        .display()
                        .to_string(),
                ),
                ("SPARK_ADMIN_TOKEN".into(), admin_token.into()),
                ("SSP_WEBHOOK_ALLOW_LOCAL".into(), "1".into()),
                (
                    "SO_HOSTS".into(),
                    "localhost:8535,localhost:8536,localhost:8537".into(),
                ),
                (
                    "SO_IDENTITY_PUBKEYS".into(),
                    crate::OPERATOR_IDENTITIES.join(","),
                ),
                ("SO_CERT_FILES".into(), certs.clone()),
                (
                    "SSP_OPERATOR_HOSTS".into(),
                    "localhost:18535,localhost:18536,localhost:18537".into(),
                ),
                ("SSP_OPERATOR_CERT_FILES".into(), certs),
                ("SSP_MIN_SPLIT_CHILD_SATS".into(), min_split.to_string()),
                ("LDK_GRPC_ADDR".into(), format!("localhost:{grpc}")),
                (
                    "LDK_API_KEY_FILE".into(),
                    data.join("regtest/api_key").display().to_string(),
                ),
                (
                    "LDK_TLS_CERT_FILE".into(),
                    data.join("tls.crt").display().to_string(),
                ),
                ("SSP_SWAP_FEE_SATS".into(), "0".into()),
                ("SSP_FROST_THRESHOLD".into(), "2".into()),
                ("RUST_LOG".into(), "info".into()),
                (
                    "COOP_BITCOIN_RPC_URL".into(),
                    format!(
                        "http://127.0.0.1:{}/wallet/ssp-withdrawals",
                        optional_env("BITCOIN_RPC_PORT", "8332")
                    ),
                ),
                ("COOP_BITCOIN_RPC_USER".into(), "testutil".into()),
                (
                    "COOP_BITCOIN_RPC_PASSWORD".into(),
                    "testutilpassword".into(),
                ),
            ]);
            for (key, default) in [
                ("SSP_INSTANT_MAX_OUTSTANDING_SATS", "100000"),
                ("SSP_INSTANT_MAX_DEPOSIT_SATS", "10000"),
            ] {
                env.insert(key.into(), optional_env(key, default));
            }
            self.configure(
                ssp_name,
                self.root.join("target/debug/open-ssp"),
                vec![],
                env,
            )?;
        }
        self.configure(
            "bitcoin-miner",
            std::env::current_exe()?,
            vec!["--project".into(), self.project.clone(), "_mine".into()],
            empty,
        )?;
        self.start("bitcoin-miner").await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn detect_daemon_exit_during_startup() {
        let temp = tempfile::tempdir().unwrap();
        let runtime = Runtime::new(temp.path().into(), "test".into());
        runtime
            .configure(
                "ssp",
                "/bin/sleep".into(),
                vec!["60".into()],
                BTreeMap::new(),
            )
            .unwrap();
        runtime.start("ssp").await.unwrap();
        let pid = runtime.process("ssp").unwrap().unwrap().pid;
        unsafe {
            libc::kill(pid as i32, libc::SIGTERM);
        }
        let result = tokio::time::timeout(Duration::from_secs(2), runtime.watch_startup())
            .await
            .unwrap();
        assert!(result.unwrap_err().to_string().contains("ssp exited"));
        runtime.stop_all().await.unwrap();
    }

    #[tokio::test]
    async fn serialize_projects_and_preserve_files_outside_native_data() {
        let temp = tempfile::tempdir().unwrap();
        let a = Runtime::new(temp.path().into(), "a".into());
        let b = Runtime::new(temp.path().into(), "b".into());
        let lock = a.lock().unwrap();
        assert!(b.lock().is_err());
        fs::create_dir_all(&a.directory).unwrap();
        let legacy = a.directory.parent().unwrap().join("operator-certs");
        fs::write(&legacy, "legacy certificate").unwrap();
        fs::write(a.directory.join("fixture-data"), "reset me").unwrap();
        a.reset().await.unwrap();
        assert!(!a.directory.exists());
        assert!(legacy.is_file());
        assert!(b.lock().is_err(), "reset must not remove the shared lock");
        drop(lock);
        assert!(b.lock().is_ok());
    }

    #[tokio::test]
    async fn owns_restart_and_cleanup_without_signalling_reused_pids() {
        let temp = tempfile::tempdir().unwrap();
        let runtime = Runtime::new(temp.path().into(), "test".into());
        runtime
            .configure(
                "ssp",
                "/bin/sleep".into(),
                vec!["60".into()],
                BTreeMap::new(),
            )
            .unwrap();
        runtime.start("ssp").await.unwrap();
        let first = runtime.process("ssp").unwrap().unwrap().pid;
        runtime.restart("ssp").await.unwrap();
        assert_ne!(first, runtime.process("ssp").unwrap().unwrap().pid);
        runtime.stop_all().await.unwrap();
        assert!(!runtime.running("ssp").unwrap());
        let stale = Process {
            pid: std::process::id(),
            start_time: "wrong-start-time".into(),
        };
        fs::write(
            runtime.service_file("ssp", "pid").unwrap(),
            serde_json::to_vec(&stale).unwrap(),
        )
        .unwrap();
        runtime.stop("ssp").await.unwrap();
        assert!(process_identity(std::process::id()).is_some());
        assert!(runtime.logs(&["../outside".into()]).is_err());
    }
}
