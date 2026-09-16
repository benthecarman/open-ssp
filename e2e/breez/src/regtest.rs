//! Local regtest lifecycle and acceptance runner. All host orchestration is Rust.

use std::{env, future::Future, path::PathBuf, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use reqwest::Client;
use tokio::process::Command;

use super::{
    LdkClient, TestConfig, acceptance, fund_ssp, optional_env, poll, setup_lightning, timed,
};

const HELP: &str = "Usage: cargo regtest [--project NAME] COMMAND

  init                  Fetch the pinned Git submodules
  build                 Provision tools and build native service binaries
  up                    Build, start, and fund a persistent development stack
  status                Show processes and check both SSPs and the chain service
  stop                  Stop processes and preserve all data
  start                 Resume stopped processes and wait for readiness
  down                  Stop processes and preserve all data (same as stop)
  reset                 Stop processes AND DELETE this project's native data
  logs [SERVICE...]     Show the last 60 log lines per service
  certs [DIRECTORY]     Copy operator certificates (prints the destination)
  fund <a|b> SATS       Add one Spark liquidity leaf to the selected SSP
  settlements <a|b>    List unresolved payment and deposit intents
  reconcile <a|b> ID   Recheck one Lightning send against the backend
  bump <a|b> ID RATE MAX_FEE  Fund a withdrawal CPFP at RATE sat/vB
  ldk <a|b> COMMAND...  Run ldk-server-cli with the node's local credentials
  miner <start|stop>     Control the background regtest miner
  test [--keep] [--no-build]  Reset a separate project and run Breez acceptance

Development defaults to open-ssp-regtest; test defaults to open-ssp-breez-e2e.
Use --project or REGTEST_PROJECT to select a project. Test deletes that project's
data directories before every run, even with --keep. Other projects can still conflict
with its host ports. Stop them first.
--no-build uses existing native binaries; run cargo regtest build first.

Sources default to vendor/spark and vendor/ldk-server. SPARK_REF and LDK_SERVER_REF
override those paths. SPARK_OPERATOR_COMMIT selects a committed operator revision.
SPARK_ADMIN_TOKEN defaults to regtest-spark-admin-token (local regtest only).
";

#[derive(Debug, PartialEq)]
enum Action {
    Help,
    Init,
    Build,
    Mine,
    Miner(bool),
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
        build: bool,
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
            ["build"] => Action::Build,
            ["_mine"] => Action::Mine,
            ["miner", "start"] => Action::Miner(true),
            ["miner", "stop"] => Action::Miner(false),
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
            ["test", flags @ ..] => {
                ensure!(
                    flags
                        .iter()
                        .all(|flag| matches!(*flag, "--keep" | "--no-build")),
                    "invalid test option; run cargo regtest --help"
                );
                Action::Test {
                    keep: flags.contains(&"--keep"),
                    build: !flags.contains(&"--no-build"),
                }
            }
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
    runtime: crate::native::Runtime,
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
            runtime: crate::native::Runtime::new(root.clone(), project.clone()),
            spark: root.join(optional_env("SPARK_REF", "vendor/spark")),
            ldk: root.join(optional_env("LDK_SERVER_REF", "vendor/ldk-server")),
            root,
            project,
            admin_token: optional_env("SPARK_ADMIN_TOKEN", "regtest-spark-admin-token"),
            client: Client::builder().timeout(Duration::from_secs(20)).build()?,
        })
    }

    fn check_sources(&self) -> Result<()> {
        ensure!(
            self.spark.join("spark/go.mod").is_file(),
            "missing Spark sources; run cargo regtest init"
        );
        ensure!(
            self.ldk.join("Cargo.toml").is_file(),
            "missing LDK sources; run cargo regtest init"
        );
        Ok(())
    }

    async fn build(&self) -> Result<PathBuf> {
        self.check_sources()?;
        let source = crate::native::tools::spark_source(&self.root, &self.spark).await?;
        timed(
            "native builds",
            crate::native::tools::build(&self.root, &source, &self.ldk),
        )
        .await?;
        Ok(source)
    }

    async fn certificates(&self, directory: Option<PathBuf>) -> Result<PathBuf> {
        let source = self.runtime.cert_dir();
        let destination = directory.unwrap_or_else(|| source.clone());
        if destination != source {
            std::fs::create_dir_all(&destination)?;
            for i in 0..3 {
                std::fs::copy(
                    source.join(format!("server_{i}.crt")),
                    destination.join(format!("server_{i}.crt")),
                )?;
            }
        }
        ensure!(
            destination.join("server_0.crt").is_file(),
            "certificates missing; run cargo regtest up"
        );
        println!("Operator certificates: {}", destination.display());
        Ok(destination)
    }

    async fn config(&self) -> Result<TestConfig> {
        TestConfig::from_env(
            self.admin_token.clone(),
            self.runtime.cert_dir(),
            self.runtime.clone(),
        )
    }

    async fn node(&self, service: &str) -> Result<LdkClient> {
        LdkClient::connect(&self.runtime, service).await
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
            value["ldk_mode"] == "live"
                && value["spark_error"].is_null()
                && value["spark"].is_object(),
            "SSP on port {port} is not ready: {value}"
        );
        Ok(value)
    }

    async fn ready(&self) -> Result<()> {
        for port in [5000, 5001] {
            poll(
                &format!("SSP on port {port}"),
                Duration::from_secs(180),
                || self.status_json(port),
            )
            .await?;
        }
        Ok(())
    }

    async fn start_services(&self, source: &std::path::Path) -> Result<()> {
        tokio::select! {
            result = self.start_services_inner(source) => result,
            result = self.runtime.watch_startup() => result,
        }
    }

    async fn start_services_inner(&self, source: &std::path::Path) -> Result<()> {
        self.runtime.setup(source, &self.admin_token).await?;
        let chain_url = format!(
            "{}/blocks/tip/height",
            optional_env("BREEZ_CHAIN_SERVICE_URL", "http://127.0.0.1:30000").trim_end_matches('/')
        );
        poll("Esplora", Duration::from_secs(120), || async {
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
            Duration::from_secs(180),
            || async {
                for i in 0..3 {
                    ensure!(
                        self.runtime.running(&format!("spark-operator-{i}"))?,
                        "operator {i} exited; inspect its log"
                    );
                    let count = self
                        .runtime
                        .sql(
                            &format!("sparkoperator_{i}"),
                            "SELECT count(*) FROM signing_keyshares WHERE status = 'AVAILABLE'",
                        )
                        .await?;
                    ensure!(
                        count.parse::<u64>().unwrap_or(0) > 0,
                        "operator {i} has no keyshares"
                    );
                }
                Ok(())
            },
        )
        .await?;
        for service in ["ldk-server", "ldk-server-2"] {
            poll(service, Duration::from_secs(60), || async {
                self.node(service).await?.json(&["get-node-info"]).await
            })
            .await?;
        }
        self.runtime.start("ssp").await?;
        self.runtime.start("ssp-2").await?;
        self.ready().await
    }

    async fn up(&self, build: bool) -> Result<()> {
        let source = if build {
            self.build().await?
        } else {
            crate::native::tools::spark_source(&self.root, &self.spark).await?
        };
        let result = interruptible(async {
            // Load rebuilt binaries while preserving wallets and channels.
            self.runtime.stop_all().await?;
            self.start_services(&source).await?;
            let config = self.config().await?;
            setup_lightning(
                &self.client,
                &config,
                &self.node("ldk-server").await?,
                &self.node("ldk-server-2").await?,
            )
            .await?;
            for (port, url) in [
                (5000, "http://127.0.0.1:5000"),
                (5001, "http://127.0.0.1:5001"),
            ] {
                if self.status_json(port).await?["spark"]["available_sats"]
                    .as_u64()
                    .unwrap_or(0)
                    < 10_000
                {
                    fund_ssp(&self.client, &config, url, 10_000).await?;
                }
            }
            println!(
                "Regtest is ready. Native service data: {}",
                self.runtime.directory.display()
            );
            Ok(())
        })
        .await;
        if result.is_err() {
            self.runtime.logs(&[])?;
            self.runtime.stop_all().await?;
        }
        result
    }

    async fn test(&self, keep: bool, build: bool) -> Result<()> {
        let started = std::time::Instant::now();
        self.check_sources()?;
        let source = if build {
            self.build().await?
        } else {
            crate::native::tools::spark_source(&self.root, &self.spark).await?
        };
        println!("Reset native test project {}", self.project);
        let result = interruptible(async {
            self.runtime.reset().await?;
            timed("stack setup", self.start_services(&source)).await?;
            timed("acceptance", acceptance(self.config().await?, self.node("ldk-server").await?, self.node("ldk-server-2").await?)).await?;
            for i in 0..3 {
                let depth = self.runtime.sql(&format!("sparkoperator_{i}"),
                    "WITH RECURSIVE depths AS (
                       SELECT id, tree_node_parent, 0 AS depth FROM tree_nodes WHERE tree_node_parent IS NULL
                       UNION ALL
                       SELECT child.id, child.tree_node_parent, parent.depth + 1
                       FROM tree_nodes child JOIN depths parent ON child.tree_node_parent = parent.id
                     ) SELECT COALESCE(MAX(depth), 0) FROM depths;").await?;
                ensure!(depth.parse::<u64>()? >= 2, "operator {i} did not exercise repeated splits");
            }
            println!("PASS Breez regtest acceptance and operator split checks");
            Ok(())
        }).await;
        if result.is_err() {
            let _ = self.runtime.logs(&[]);
        }
        let cleanup = if keep {
            Ok(())
        } else {
            timed("teardown", self.runtime.reset()).await
        };
        println!("TIMING total: {:.1}s", started.elapsed().as_secs_f64());
        result.and(cleanup)
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
    let _lock = if matches!(
        options.action,
        Action::Mine | Action::Status | Action::Logs(_)
    ) {
        None
    } else {
        Some(stack.runtime.lock()?)
    };
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
        Action::Build => {
            stack.build().await?;
        }
        Action::Mine => return stack.runtime.miner().await,
        Action::Miner(start) => {
            if start {
                stack.runtime.start("bitcoin-miner").await?;
            } else {
                stack.runtime.stop("bitcoin-miner").await?;
            }
        }
        Action::Up => stack.up(true).await?,
        Action::Test { keep, build } => {
            stack
                .test(
                    keep || optional_env("KEEP_BREEZ_E2E_STACK", "0") == "1",
                    build,
                )
                .await?
        }
        Action::Stop => stack.runtime.stop_all().await?,
        Action::Start => {
            let source = crate::native::tools::spark_source(&stack.root, &stack.spark).await?;
            let result = interruptible(stack.start_services(&source)).await;
            if result.is_err() {
                let _ = stack.runtime.logs(&[]);
                stack.runtime.stop_all().await?;
            }
            result?;
        }
        Action::Down => stack.runtime.stop_all().await?,
        Action::Reset => stack.runtime.reset().await?,
        Action::Logs(services) => stack.runtime.logs(&services)?,
        Action::Certs(directory) => {
            stack.certificates(directory).await?;
        }
        Action::Status => {
            stack.runtime.status()?;
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
            Action::Test {
                keep: true,
                build: true
            }
        );
    }

    #[test]
    fn skipping_builds_does_not_imply_keeping_test_data() {
        assert_eq!(
            parse(&["test", "--no-build"]).unwrap().action,
            Action::Test {
                keep: false,
                build: false
            }
        );
        for flags in [
            ["test", "--keep", "--no-build"],
            ["test", "--no-build", "--keep"],
        ] {
            assert_eq!(
                parse(&flags).unwrap().action,
                Action::Test {
                    keep: true,
                    build: false
                }
            );
        }
        assert!(parse(&["test", "--no-buid"]).is_err());
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
