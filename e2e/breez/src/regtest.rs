//! Local regtest lifecycle and acceptance runner. All host orchestration is Rust.

use std::{env, future::Future, path::PathBuf, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use reqwest::Client;
use tempfile::TempDir;
use tokio::process::Command;

use super::{LdkClient, TestConfig, acceptance, fund_ssp, optional_env, poll, setup_lightning};

const HELP: &str = "Usage: cargo regtest [--project NAME] COMMAND

  init                  Fetch the pinned Git submodules
  up                    Build, start, and fund a persistent development stack
  status                Show containers and check both SSPs and the chain service
  stop                  Stop containers and preserve all data
  start                 Resume stopped containers and wait for readiness
  down                  Remove containers and preserve volumes
  reset                 Remove containers AND DELETE this project's volumes
  logs [SERVICE...]     Show the last 100 log lines
  certs [DIRECTORY]     Copy operator certificates (prints the destination)
  fund <a|b> SATS       Add one Spark liquidity leaf to the selected SSP
  settlements <a|b>    List unresolved payment and deposit intents
  reconcile <a|b> ID   Recheck one Lightning send against the backend
  bump <a|b> ID RATE MAX_FEE  Fund a withdrawal CPFP at RATE sat/vB
  ldk <a|b> COMMAND...  Run ldk-server-cli with the node's local credentials
  test [--keep]         Reset a separate test project and run Breez acceptance

Development defaults to open-ssp-regtest; test defaults to open-ssp-breez-e2e.
Use --project or REGTEST_PROJECT to select a project. Test deletes that project's
volumes before every run, even with --keep. Other projects can still conflict
with its host ports. Stop them first.

Sources default to vendor/spark and vendor/ldk-server. SPARK_REF and LDK_SERVER_REF
override those paths. SPARK_OPERATOR_COMMIT selects a committed operator revision.
SPARK_ADMIN_TOKEN defaults to regtest-spark-admin-token (local regtest only).
";

#[derive(Debug, PartialEq)]
enum Action {
    Help,
    Init,
    Up,
    Status,
    Stop,
    Start,
    Down,
    Reset,
    Logs(Vec<String>),
    Certs(Option<PathBuf>),
    Fund {
        url: &'static str,
        sats: u64,
    },
    Admin {
        side: String,
        path: &'static str,
        body: Option<serde_json::Value>,
    },
    Ldk {
        service: &'static str,
        args: Vec<String>,
    },
    Test {
        keep: bool,
    },
}

#[derive(Debug)]
struct Options {
    project: Option<String>,
    action: Action,
}

impl Options {
    fn parse(args: Vec<String>) -> Result<Self> {
        let mut args = args.as_slice();
        let project = if args.first().map(String::as_str) == Some("--project") {
            let name = args.get(1).context("--project needs a name")?;
            validate_project(name)?;
            args = &args[2..];
            Some(name.clone())
        } else {
            None
        };
        let words: Vec<&str> = args.iter().map(String::as_str).collect();
        let action = match words.as_slice() {
            [] | ["help" | "--help" | "-h"] => Action::Help,
            ["init"] => Action::Init,
            ["up"] => Action::Up,
            ["status"] => Action::Status,
            ["stop"] => Action::Stop,
            ["start"] => Action::Start,
            ["down"] => Action::Down,
            ["reset"] => Action::Reset,
            ["logs", ..] => Action::Logs(args[1..].to_vec()),
            ["certs"] => Action::Certs(None),
            ["certs", directory] => Action::Certs(Some(PathBuf::from(directory))),
            ["fund", side, sats] => {
                let url = match *side {
                    "a" => "http://127.0.0.1:5000",
                    "b" => "http://127.0.0.1:5001",
                    _ => bail!("select SSP a or b"),
                };
                let sats = sats.parse().context("SATS must be a positive integer")?;
                ensure!(sats > 0, "SATS must be positive");
                Action::Fund { url, sats }
            }
            ["settlements", side] => {
                ensure!(matches!(*side, "a" | "b"), "select SSP a or b");
                Action::Admin {
                    side: side.to_string(),
                    path: "/admin/settlements",
                    body: None,
                }
            }
            ["reconcile", side, id] => {
                ensure!(matches!(*side, "a" | "b"), "select SSP a or b");
                Action::Admin {
                    side: side.to_string(),
                    path: "/admin/settlements/reconcile",
                    body: Some(serde_json::json!({"request_id":id})),
                }
            }
            ["bump", side, id, rate, max_fee] => {
                ensure!(matches!(*side, "a" | "b"), "select SSP a or b");
                let rate: u64 = rate.parse().context("RATE must be a positive integer")?;
                let max_fee: u64 = max_fee
                    .parse()
                    .context("MAX_FEE must be a positive integer")?;
                ensure!(
                    (1..=10_000).contains(&rate) && max_fee > 0,
                    "invalid fee rate or budget"
                );
                Action::Admin {
                    side: side.to_string(),
                    path: "/admin/withdrawals/bump-fee",
                    body: Some(
                        serde_json::json!({"request_id":id,"fee_rate":rate,"max_fee_sats":max_fee}),
                    ),
                }
            }
            ["ldk", side, _, ..] => {
                let service = match *side {
                    "a" => "ldk-server",
                    "b" => "ldk-server-2",
                    _ => bail!("select LDK node a or b"),
                };
                Action::Ldk {
                    service,
                    args: args[2..].to_vec(),
                }
            }
            ["test"] => Action::Test { keep: false },
            ["test", "--keep"] => Action::Test { keep: true },
            _ => bail!("invalid command; run cargo regtest --help"),
        };
        Ok(Self { project, action })
    }
}

fn validate_project(name: &str) -> Result<()> {
    ensure!(
        name.starts_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit())
            && name
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_'),
        "project names must start with a lowercase letter or digit and use only a-z, 0-9, - and _"
    );
    Ok(())
}

#[derive(Clone)]
struct Stack {
    root: PathBuf,
    project: String,
    spark: PathBuf,
    ldk: PathBuf,
    admin_token: String,
    client: Client,
}

impl Stack {
    fn new(options: &Options) -> Result<Self> {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()?;
        let fallback = if matches!(options.action, Action::Test { .. }) {
            optional_env("BREEZ_E2E_PROJECT_NAME", "open-ssp-breez-e2e")
        } else {
            "open-ssp-regtest".to_owned()
        };
        let project = options
            .project
            .clone()
            .unwrap_or_else(|| optional_env("REGTEST_PROJECT", &fallback));
        validate_project(&project)?;
        Ok(Self {
            spark: root.join(optional_env("SPARK_REF", "vendor/spark")),
            ldk: root.join(optional_env("LDK_SERVER_REF", "vendor/ldk-server")),
            root,
            project,
            admin_token: optional_env("SPARK_ADMIN_TOKEN", "regtest-spark-admin-token"),
            client: Client::builder().timeout(Duration::from_secs(20)).build()?,
        })
    }

    fn compose(&self, args: &[&str]) -> Command {
        let mut command = Command::new("docker");
        command
            .kill_on_drop(true)
            .current_dir(&self.root)
            .args([
                "compose",
                "-p",
                &self.project,
                "-f",
                "docker-compose.regtest.yml",
            ])
            .args(args)
            .env("SPARK_REF", &self.spark)
            .env("LDK_SERVER_REF", &self.ldk)
            .env("SPARK_ADMIN_TOKEN", &self.admin_token)
            .env("SSP_NETWORK", "REGTEST")
            .env(
                "SSP_INSTANT_MAX_OUTSTANDING_SATS",
                optional_env("SSP_INSTANT_MAX_OUTSTANDING_SATS", "100000"),
            )
            .env(
                "SSP_INSTANT_MAX_DEPOSIT_SATS",
                optional_env("SSP_INSTANT_MAX_DEPOSIT_SATS", "10000"),
            )
            .env(
                "COMPOSE_PROGRESS",
                optional_env("COMPOSE_PROGRESS", "plain"),
            )
            .env("COMPOSE_BAKE", optional_env("COMPOSE_BAKE", "false"));
        command
    }

    async fn run_compose(&self, args: &[&str]) -> Result<()> {
        let status = self
            .compose(args)
            .status()
            .await
            .context("could not run Docker Compose")?;
        ensure!(status.success(), "Docker Compose failed ({status})");
        Ok(())
    }

    async fn output(&self, args: &[&str]) -> Result<String> {
        let output = self
            .compose(args)
            .output()
            .await
            .context("could not run Docker Compose")?;
        ensure!(
            output.status.success(),
            "Docker Compose failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
        Ok(String::from_utf8(output.stdout)?.trim().to_owned())
    }

    fn check_sources(&self) -> Result<()> {
        for source in [&self.spark, &self.ldk] {
            ensure!(
                source.join("Dockerfile").is_file(),
                "missing source checkout: {}. Run cargo regtest init, or check SPARK_REF and LDK_SERVER_REF",
                source.display()
            );
        }
        Ok(())
    }

    async fn container(&self, service: &str) -> Result<String> {
        let id = self.output(&["ps", "-q", service]).await?;
        ensure!(
            !id.is_empty(),
            "{service} is not running; run cargo regtest up"
        );
        Ok(id)
    }

    fn cert_dir(&self) -> PathBuf {
        self.root
            .join(".regtest")
            .join(&self.project)
            .join("operator-certs")
    }

    async fn certificates(&self, directory: Option<PathBuf>) -> Result<PathBuf> {
        let directory = directory.unwrap_or_else(|| self.cert_dir());
        std::fs::create_dir_all(&directory)?;
        let directory = directory.canonicalize()?;
        for index in 0..3 {
            self.run_compose(&[
                "cp",
                &format!("cert-init:/tls/server_{index}.crt"),
                directory
                    .join(format!("server_{index}.crt"))
                    .to_str()
                    .context("certificate path is not UTF-8")?,
            ])
            .await?;
        }
        println!("Operator certificates: {}", directory.display());
        Ok(directory)
    }

    async fn config(&self) -> Result<TestConfig> {
        TestConfig::from_env(
            self.admin_token.clone(),
            self.cert_dir(),
            self.container("ssp").await?,
        )
    }

    async fn node(&self, service: &str) -> Result<LdkClient> {
        LdkClient::connect(self.container(service).await?).await
    }

    async fn status_json(&self, port: u16) -> Result<serde_json::Value> {
        let value: serde_json::Value = self
            .client
            .get(format!("http://127.0.0.1:{port}/status"))
            .bearer_auth(&self.admin_token)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        ensure!(
            value["ldk_mode"] == "live",
            "SSP on port {port} has no live LDK backend"
        );
        ensure!(
            value["spark_error"].is_null() && value["spark"].is_object(),
            "SSP on port {port} has no ready Spark wallet: {value}"
        );
        Ok(value)
    }

    async fn ready(&self) -> Result<()> {
        for port in [5000, 5001] {
            poll(
                &format!("SSP on port {port}"),
                Duration::from_secs(360),
                || self.status_json(port),
            )
            .await?;
        }
        Ok(())
    }

    async fn start_services(&self) -> Result<()> {
        println!(
            "Start Bitcoin, operators, and Lightning nodes ({})",
            self.project
        );
        self.run_compose(&[
            "up",
            "--build",
            "-d",
            "postgres",
            "bitcoind",
            "bitcoin-init",
            "bitcoin-miner",
            "electrs",
            "cert-init",
            "spark-operator-0",
            "spark-operator-1",
            "spark-operator-2",
            "ldk-server",
            "ldk-server-2",
        ])
        .await?;
        let chain_url = format!(
            "{}/blocks/tip/height",
            optional_env("BREEZ_CHAIN_SERVICE_URL", "http://127.0.0.1:30000").trim_end_matches('/')
        );
        println!("Wait for Esplora and operator signing keyshares");
        poll("Esplora", Duration::from_secs(240), || async {
            self.client
                .get(&chain_url)
                .send()
                .await?
                .error_for_status()?;
            Ok(())
        })
        .await?;
        poll(
            "Spark signing keyshares",
            Duration::from_secs(600),
            || async {
                for index in 0..3 {
                    let count = self
                        .output(&[
                            "exec",
                            "-T",
                            "postgres",
                            "psql",
                            "-U",
                            "postgres",
                            "-d",
                            &format!("sparkoperator_{index}"),
                            "-tAc",
                            "SELECT count(*) FROM signing_keyshares WHERE status = 'AVAILABLE';",
                        ])
                        .await?;
                    ensure!(
                        count.parse::<u64>().unwrap_or(0) > 0,
                        "operator {index} has no available keyshares"
                    );
                }
                Ok(())
            },
        )
        .await?;
        for service in ["ldk-server", "ldk-server-2"] {
            poll(service, Duration::from_secs(180), || async {
                self.node(service).await?.json(&["get-node-info"]).await
            })
            .await?;
        }
        println!("Build and start both SSP instances");
        self.run_compose(&["build", "ssp"]).await?;
        self.run_compose(&["up", "--no-build", "--no-deps", "-d", "ssp", "ssp-2"])
            .await?;
        self.ready().await?;
        self.certificates(None).await?;
        Ok(())
    }

    async fn up(&self) -> Result<()> {
        self.check_sources()?;
        let source = CleanSource::create(self).await?;
        let build_stack = Self {
            spark: source.path.clone(),
            ..self.clone()
        };
        let result = interruptible(async {
            build_stack.start_services().await?;
            let config = self.config().await?;
            setup_lightning(&self.client, &config, &self.node("ldk-server").await?, &self.node("ldk-server-2").await?).await?;
            for (port, url) in [(5000, "http://127.0.0.1:5000"), (5001, "http://127.0.0.1:5001")] {
                let balance = self.status_json(port).await?["spark"]["available_sats"].as_u64().unwrap_or(0);
                if balance < 10_000 {
                    fund_ssp(&self.client, &config, url, 10_000).await?;
                }
            }
            println!("Regtest is ready. Each SSP has at least 10000 Spark sats. Use cargo regtest status.");
            Ok(())
        }).await;
        if result.is_err() {
            self.failure_logs().await;
            eprintln!("Development data was kept. Use cargo regtest logs to inspect the stack.");
        }
        result
    }

    async fn test(&self, keep: bool) -> Result<()> {
        self.check_sources()?;
        let source = CleanSource::create(self).await?;
        let stack = Self {
            spark: source.path.clone(),
            ..self.clone()
        };
        println!(
            "Reset test project {}: its existing volumes will be deleted",
            self.project
        );
        let result = interruptible(async {
            stack.run_compose(&["down", "--volumes", "--remove-orphans"]).await?;
            stack.start_services().await?;
            acceptance(stack.config().await?, stack.node("ldk-server").await?, stack.node("ldk-server-2").await?).await?;
            println!("Verify repeated split trees on every operator");
            for index in 0..3 {
                let depth = stack.output(&[
                    "exec", "-T", "postgres", "psql", "-U", "postgres", "-d",
                    &format!("sparkoperator_{index}"), "-tAc",
                    "WITH RECURSIVE depths AS (
                       SELECT id, tree_node_parent, 0 AS depth FROM tree_nodes WHERE tree_node_parent IS NULL
                       UNION ALL
                       SELECT child.id, child.tree_node_parent, parent.depth + 1
                       FROM tree_nodes child JOIN depths parent ON child.tree_node_parent = parent.id
                     ) SELECT COALESCE(MAX(depth), 0) FROM depths;",
                ]).await?;
                ensure!(depth.parse::<u64>()? >= 2, "operator {index} has split depth {depth}; expected at least 2");
            }
            println!("PASS Breez regtest acceptance and operator split checks");
            Ok(())
        }).await;
        if result.is_err() {
            stack.failure_logs().await;
        }
        // Always attempt teardown after a failed or interrupted test. Development
        // data uses a different project unless the caller explicitly overrides it.
        let cleanup = if keep {
            println!("Kept test project {} and its certificates", self.project);
            Ok(())
        } else {
            let cleanup = stack
                .run_compose(&["down", "--volumes", "--remove-orphans"])
                .await;
            if cleanup.is_ok() && self.cert_dir().exists() {
                std::fs::remove_dir_all(self.cert_dir())?;
            }
            cleanup
        };
        if let Err(error) = &cleanup {
            eprintln!("Test cleanup failed: {error:#}");
        }
        result.and(cleanup)
    }

    async fn failure_logs(&self) {
        let _ = self.run_compose(&["ps", "-a"]).await;
        let _ = self
            .run_compose(&[
                "logs",
                "--tail=100",
                "spark-operator-0",
                "spark-operator-1",
                "spark-operator-2",
                "ldk-server",
                "ldk-server-2",
                "ssp",
                "ssp-2",
            ])
            .await;
    }
}

/// Keep temporary Git worktree registration cleanup independent of async task
/// cancellation. Only this runner's temporary worktree can be removed here.
struct CleanSource {
    source: PathBuf,
    path: PathBuf,
    _directory: TempDir,
}

impl CleanSource {
    async fn create(stack: &Stack) -> Result<Self> {
        let directory = tempfile::Builder::new()
            .prefix("open-ssp-operators-")
            .tempdir()?;
        let path = directory.path().join("spark");
        let status = Command::new("git")
            .kill_on_drop(true)
            .arg("-C")
            .arg(&stack.spark)
            .args(["worktree", "add", "--detach"])
            .arg(&path)
            .arg(optional_env("SPARK_OPERATOR_COMMIT", "HEAD"))
            .status()
            .await?;
        ensure!(status.success(), "could not create clean operator worktree");
        Ok(Self {
            source: stack.spark.clone(),
            path,
            _directory: directory,
        })
    }
}

impl Drop for CleanSource {
    fn drop(&mut self) {
        match std::process::Command::new("git")
            .arg("-C")
            .arg(&self.source)
            .args(["worktree", "remove", "--force"])
            .arg(&self.path)
            .output()
        {
            Ok(output) if output.status.success() => {}
            _ => eprintln!(
                "Could not remove temporary worktree registration for {}",
                self.path.display()
            ),
        }
    }
}

async fn interruptible<F: Future<Output = Result<()>>>(work: F) -> Result<()> {
    tokio::select! {
        result = work => result,
        signal = shutdown_signal() => {
            signal?;
            bail!("interrupted");
        }
    }
}

async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    Ok(())
}

pub(super) async fn run() -> Result<()> {
    let options = Options::parse(env::args().skip(1).collect())?;
    if options.action == Action::Help {
        print!("{HELP}");
        return Ok(());
    }
    let stack = Stack::new(&options)?;
    match options.action {
        Action::Help => unreachable!(),
        Action::Init => {
            let status = Command::new("git")
                .kill_on_drop(true)
                .current_dir(&stack.root)
                .args(["submodule", "update", "--init", "--recursive"])
                .status()
                .await?;
            ensure!(status.success(), "submodule initialization failed");
            stack.check_sources()?;
        }
        Action::Up => stack.up().await?,
        Action::Test { keep } => {
            stack
                .test(keep || optional_env("KEEP_BREEZ_E2E_STACK", "0") == "1")
                .await?
        }
        Action::Stop => stack.run_compose(&["stop"]).await?,
        Action::Start => {
            stack.run_compose(&["start"]).await?;
            interruptible(stack.ready()).await?;
        }
        Action::Down => stack.run_compose(&["down", "--remove-orphans"]).await?,
        Action::Reset => {
            stack
                .run_compose(&["down", "--volumes", "--remove-orphans"])
                .await?;
            if stack.cert_dir().exists() {
                std::fs::remove_dir_all(stack.cert_dir())?;
            }
        }
        Action::Logs(services) => {
            let mut args = vec!["logs", "--tail=100"];
            args.extend(services.iter().map(String::as_str));
            stack.run_compose(&args).await?;
        }
        Action::Certs(directory) => {
            stack.certificates(directory).await?;
        }
        Action::Status => {
            stack.run_compose(&["ps", "-a"]).await?;
            stack.container("ssp").await?;
            stack.container("ssp-2").await?;
            for port in [5000, 5001] {
                println!(
                    "SSP {port}: {}",
                    serde_json::to_string_pretty(&stack.status_json(port).await?)?
                );
            }
            let url = format!(
                "{}/blocks/tip/height",
                optional_env("BREEZ_CHAIN_SERVICE_URL", "http://127.0.0.1:30000")
                    .trim_end_matches('/')
            );
            let height = stack
                .client
                .get(url)
                .send()
                .await?
                .error_for_status()?
                .text()
                .await?;
            println!("Esplora block height: {height}");
        }
        Action::Fund { url, sats } => {
            interruptible(async {
                fund_ssp(&stack.client, &stack.config().await?, url, sats).await?;
                println!("Added a {sats}-sat Spark leaf to {url}");
                Ok(())
            })
            .await?;
        }
        Action::Admin { side, path, body } => {
            stack
                .container(if side == "a" { "ssp" } else { "ssp-2" })
                .await?;
            let url = format!(
                "http://127.0.0.1:{}{path}",
                if side == "a" { 5000 } else { 5001 }
            );
            let request = if let Some(body) = body {
                stack.client.post(url).json(&body)
            } else {
                stack.client.get(url)
            };
            let response = request.bearer_auth(&stack.admin_token).send().await?;
            let status = response.status();
            let value: serde_json::Value = response.json().await?;
            ensure!(status.is_success(), "SSP {status}: {value}");
            println!("{}", serde_json::to_string_pretty(&value)?);
        }
        Action::Ldk { service, args } => {
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            print!("{}", stack.node(service).await?.output(&args).await?);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Options> {
        Options::parse(args.iter().map(|s| s.to_string()).collect())
    }

    #[test]
    fn fee_bumps_require_explicit_rate_and_budget() {
        assert!(matches!(
            parse(&["settlements", "a"]).unwrap().action,
            Action::Admin { body: None, .. }
        ));
        let Action::Admin {
            body: Some(body), ..
        } = parse(&["bump", "b", "request", "5", "5000"])
            .unwrap()
            .action
        else {
            panic!("expected admin command")
        };
        assert_eq!(
            body,
            serde_json::json!({"request_id":"request","fee_rate":5,"max_fee_sats":5000})
        );
        for args in [
            vec!["bump", "a", "request", "5"],
            vec!["bump", "a", "request", "0", "5000"],
            vec!["bump", "a", "request", "5", "0"],
            vec!["bump", "c", "request", "5", "5000"],
        ] {
            assert!(parse(&args).is_err());
        }
    }

    #[test]
    fn destructive_commands_must_be_explicit() {
        assert_eq!(parse(&[]).unwrap().action, Action::Help);
        assert_eq!(parse(&["up"]).unwrap().action, Action::Up);
        assert_eq!(parse(&["down"]).unwrap().action, Action::Down);
        assert_eq!(parse(&["reset"]).unwrap().action, Action::Reset);
        assert!(parse(&["up", "--reset"]).is_err());
        assert!(parse(&["down", "--volumes"]).is_err());
        assert_eq!(
            parse(&["test", "--keep"]).unwrap().action,
            Action::Test { keep: true }
        );
    }

    #[test]
    fn validate_project_before_using_it_as_a_path() {
        for name in ["", "../outside", "/tmp/other", "UPPERCASE", "-option"] {
            assert!(validate_project(name).is_err(), "{name}");
        }
        assert_eq!(
            parse(&["--project", "local-ssp_2", "up"])
                .unwrap()
                .project
                .as_deref(),
            Some("local-ssp_2")
        );
    }

    #[test]
    fn validate_funding_and_keep_ldk_arguments_literal() {
        for args in [["fund", "a", "0"], ["fund", "a", "-1"], ["fund", "c", "10"]] {
            assert!(parse(&args).is_err());
        }
        assert_eq!(
            parse(&["fund", "b", "1000"]).unwrap().action,
            Action::Fund {
                url: "http://127.0.0.1:5001",
                sats: 1000
            }
        );
        assert_eq!(
            parse(&[
                "ldk",
                "b",
                "bolt11-receive",
                "500sat",
                "-d",
                "a description; $(literal)"
            ])
            .unwrap()
            .action,
            Action::Ldk {
                service: "ldk-server-2",
                args: vec![
                    "bolt11-receive".into(),
                    "500sat".into(),
                    "-d".into(),
                    "a description; $(literal)".into()
                ]
            }
        );
    }
}
