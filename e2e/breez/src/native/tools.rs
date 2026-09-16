//! Native tools and builds. Downloaded artifacts are pinned and checksum verified.
use anyhow::{Context, Result, ensure};
use sha2::{Digest, Sha256};
use std::{
    env,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};
use tokio::process::Command;

pub async fn output(command: &mut Command) -> Result<String> {
    let label = format!("{:?}", command.as_std().get_program());
    let result = command
        .kill_on_drop(true)
        .output()
        .await
        .with_context(|| format!("start {label}"))?;
    ensure!(
        result.status.success(),
        "{label} failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    Ok(String::from_utf8(result.stdout)?.trim().to_owned())
}

async fn run(command: &mut Command) -> Result<()> {
    let label = format!("{:?}", command.as_std().get_program());
    let status = command
        .kill_on_drop(true)
        .status()
        .await
        .with_context(|| format!("start {label}"))?;
    ensure!(status.success(), "{label} failed ({status})");
    Ok(())
}

pub fn executable(name: &str) -> Result<PathBuf> {
    let found = env::var_os("PATH").and_then(|path| {
        env::split_paths(&path)
            .map(|dir| dir.join(name))
            .find(|path| path.is_file())
    });
    if let Some(path) = found {
        return Ok(path);
    }
    let go = PathBuf::from("/usr/local/go/bin/go");
    if name == "go" && go.is_file() {
        return Ok(go);
    }
    anyhow::bail!("missing native tool {name}; see docs/REGTEST_BREEZ.md for prerequisites")
}

pub async fn pg_bin() -> Result<PathBuf> {
    if let Some(path) = env::var_os("PGBIN") {
        return Ok(path.into());
    }
    let path = output(Command::new(executable("pg_config")?).arg("--bindir")).await?;
    ensure!(
        Path::new(&path).join("postgres").is_file(),
        "install the PostgreSQL server tools or set PGBIN"
    );
    Ok(path.into())
}

async fn download(path: &Path, url: &str, hash: &str) -> Result<()> {
    if path.is_file() && hex::encode(Sha256::digest(std::fs::read(path)?)) == hash {
        return Ok(());
    }
    println!("Download {url}");
    let bytes = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(600))
        .build()?
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    ensure!(
        hex::encode(Sha256::digest(&bytes)) == hash,
        "checksum mismatch for {url}"
    );
    let temporary = path.with_extension("partial");
    std::fs::write(&temporary, &bytes)?;
    std::fs::rename(temporary, path)?;
    Ok(())
}

pub async fn provision(root: &Path) -> Result<()> {
    ensure!(
        cfg!(all(target_os = "linux", target_arch = "x86_64")),
        "native tool downloads currently support Linux x86_64"
    );
    let dir = root.join(".regtest/native-tools");
    std::fs::create_dir_all(&dir)?;
    download(
        &dir.join("bitcoin.tar.gz"),
        "https://bitcoincore.org/bin/bitcoin-core-29.0/bitcoin-29.0-x86_64-linux-gnu.tar.gz",
        "a681e4f6ce524c338a105f214613605bac6c33d58c31dc5135bbc02bc458bb6c",
    )
    .await?;
    if !dir.join("bitcoin-29.0/bin/bitcoind").is_file() {
        run(Command::new("tar")
            .arg("-xzf")
            .arg(dir.join("bitcoin.tar.gz"))
            .arg("-C")
            .arg(&dir))
        .await?;
    }
    download(&dir.join("electrs.zip"),
        "https://github.com/RCasatta/electrsd/releases/download/electrs_releases/electrs_linux_esplora_a33e97e1a1fc63fa9c20a116bb92579bbf43b254.zip",
        "865e26a96e8df77df01d96f2f569dcf9622fc87a8d99a9b8fe30861a4db9ddf1").await?;
    if !dir.join("electrs").is_file() {
        run(Command::new("unzip")
            .arg("-o")
            .arg(dir.join("electrs.zip"))
            .arg("-d")
            .arg(&dir))
        .await?;
    }
    download(
        &dir.join("atlas"),
        "https://release.ariga.io/atlas/atlas-community-linux-amd64-v1.0.0",
        "9933f9a75cad6962ba0cf39813ecc2b1454aa35e952e4bcc36ee714c921ac860",
    )
    .await?;
    for name in ["electrs", "atlas"] {
        std::fs::set_permissions(dir.join(name), std::fs::Permissions::from_mode(0o755))?;
    }
    pg_bin().await?;
    Ok(())
}

pub async fn spark_source(root: &Path, source: &Path) -> Result<PathBuf> {
    let revision = output(
        Command::new("git")
            .arg("-C")
            .arg(source)
            .args(["rev-parse", "--verify"])
            .arg(format!(
                "{}^{{commit}}",
                super::super::optional_env("SPARK_OPERATOR_COMMIT", "HEAD")
            )),
    )
    .await?;
    ensure!(
        revision.len() == 40 && revision.bytes().all(|b| b.is_ascii_hexdigit()),
        "invalid Spark revision"
    );
    let snapshots = root.join(".regtest/native-sources");
    std::fs::create_dir_all(&snapshots)?;
    let destination = snapshots.join(format!("spark-{revision}"));
    if !destination.is_dir() {
        let temp = tempfile::tempdir_in(&snapshots)?;
        let archive = temp.path().join("source.tar");
        run(Command::new("git")
            .arg("-C")
            .arg(source)
            .args(["archive", "--format=tar"])
            .arg(format!("--output={}", archive.display()))
            .arg(&revision))
        .await?;
        let unpacked = temp.path().join("source");
        std::fs::create_dir(&unpacked)?;
        run(Command::new("tar")
            .arg("-xf")
            .arg(archive)
            .arg("-C")
            .arg(&unpacked))
        .await?;
        std::fs::rename(unpacked, &destination)?;
    }
    Ok(destination)
}

pub async fn build(root: &Path, spark: &Path, ldk: &Path) -> Result<()> {
    provision(root).await?;
    let build = root.join(".regtest/native-build");
    let bins = root.join(".regtest/native-tools");
    println!("Build native Spark operator and signer");
    // Compose used loopback port publishing. Preserve that boundary without
    // changing the pinned source checkout or the operator's protocol behavior.
    let main = spark.join("spark/bin/operator/main.go");
    let original = std::fs::read_to_string(&main)?;
    let listener = "fmt.Sprintf(\":%d\", args.";
    ensure!(
        original.matches(listener).count() == 4,
        "Spark listener layout changed; review the native loopback overlay"
    );
    let patched = bins.join("operator-loopback.go");
    std::fs::write(
        &patched,
        original.replace(listener, "fmt.Sprintf(\"127.0.0.1:%d\", args."),
    )?;
    let overlay = bins.join("operator-overlay.json");
    std::fs::write(
        &overlay,
        serde_json::to_vec(&serde_json::json!({"Replace": {main.display().to_string(): patched}}))?,
    )?;
    run(Command::new(executable("go")?)
        .current_dir(spark.join("spark"))
        .args(["build", "-buildvcs=false", "-overlay"])
        .arg(overlay)
        .arg("-o")
        .arg(bins.join("spark-operator"))
        .arg("./bin/operator"))
    .await?;
    run(Command::new("cargo")
        .args(["build", "--locked", "--manifest-path"])
        .arg(spark.join("signer/Cargo.toml"))
        .args(["-p", "spark-frost-signer", "--target-dir"])
        .arg(build.join("signer"))
        // Pure-Rust curve arithmetic otherwise exceeds DKG RPC deadlines on
        // small CI runners. Keep the other service builds unoptimized.
        .env("CARGO_PROFILE_DEV_OPT_LEVEL", "1")
        .env("CARGO_PROFILE_DEV_DEBUG", "0"))
    .await?;
    println!("Build native LDK server and client");
    run(Command::new("cargo")
        .args(["build", "--locked", "--manifest-path"])
        .arg(ldk.join("Cargo.toml"))
        .args(["-p", "ldk-server", "-p", "ldk-server-cli", "--target-dir"])
        .arg(build.join("ldk"))
        .env("CARGO_PROFILE_DEV_DEBUG", "0"))
    .await?;
    println!("Build native SSP");
    run(Command::new("cargo")
        .current_dir(root)
        .args(["build", "--locked", "--bin", "open-ssp"]))
    .await?;
    Ok(())
}
