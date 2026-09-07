use std::{net::SocketAddr, sync::Arc};

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tracing::info;

mod auth;
mod config;
mod coop_exit;
mod db;
mod fs;
mod graphql;
mod history;
mod internal_payments;
mod ldk;
mod lightning_store;
mod quotes;
mod request_updates;
mod spark;
mod static_deposits;
mod webhooks;

use config::Config;
use db::Db;
use ldk::LdkGrpcBackend;
use spark::SparkService;

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub db: Arc<Db>,
    pub ldk: Arc<LdkGrpcBackend>,
    pub spark: Arc<SparkService>,
    pub coop_exit: Option<Arc<coop_exit::CoopExitService>>,
    pub static_deposit: Option<Arc<static_deposits::StaticDepositService>>,
    /// Serializes the check-and-pay section for idempotent Lightning sends.
    pub send_lock: Arc<tokio::sync::Mutex<()>>,
}

pub async fn ssp_identity(state: &AppState) -> Result<String, String> {
    Ok(state.spark.identity())
}

#[derive(Debug, Deserialize)]
pub struct GraphqlRequest {
    pub query: String,
    #[serde(default)]
    pub variables: serde_json::Value,
    #[serde(default, rename = "operationName")]
    pub operation_name: Option<String>,
}

#[derive(Debug, Serialize)]
struct GraphqlResponse {
    data: serde_json::Value,
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

async fn identity_handler(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "identityPublicKey": state.spark.identity(),
    }))
}

async fn status(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    if !admin_authorized(&state.config, &headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "unauthorized"})),
        );
    }
    let backend = &state.ldk;
    let spark = state.spark.health().await;
    (
        StatusCode::OK,
        Json(serde_json::json!({
        "network": state.config.network,
        "ssp_identity_pubkey": state.spark.identity(),
        "identity_source": "spark-wallet",
        "spark": spark.as_ref().ok(),
        "spark_error": spark.as_ref().err(),
        "ldk_mode": "live",
        "ldk_node_id": backend.node_id,
        })),
    )
}

fn admin_authorized(config: &Config, headers: &HeaderMap) -> bool {
    if config.spark_admin_token.is_empty() {
        return std::env::var("SPARK_ADMIN_ALLOW_NO_AUTH").unwrap_or_default() == "1";
    }
    let supplied = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .unwrap_or_default();
    Sha256::digest(supplied.as_bytes())
        .ct_eq(&Sha256::digest(config.spark_admin_token.as_bytes()))
        .into()
}

async fn spark_deposit_address(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if !admin_authorized(&state.config, &headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "unauthorized"})),
        );
    }
    match state.spark.generate_deposit_address().await {
        Ok(address) => (
            StatusCode::OK,
            Json(serde_json::json!({"address": address})),
        ),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": error})),
        ),
    }
}

#[derive(Deserialize)]
struct ClaimDepositRequest {
    transaction_hex: String,
    vout: u32,
}

async fn spark_claim_deposit(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ClaimDepositRequest>,
) -> impl IntoResponse {
    if !admin_authorized(&state.config, &headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "unauthorized"})),
        );
    }
    match state
        .spark
        .claim_deposit(&request.transaction_hex, request.vout)
        .await
    {
        Ok(values) => (
            StatusCode::OK,
            Json(serde_json::json!({"ok": true, "leaf_values": values})),
        ),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": error})),
        ),
    }
}

async fn settlements(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    if !admin_authorized(&state.config, &headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error":"unauthorized"})),
        );
    }
    let result=state.db.with(|c| {
        let mut stmt=c.prepare("SELECT request_id,kind,status,payment_id,last_error FROM lightning_sends WHERE status NOT IN ('SUCCEEDED','FAILED') UNION ALL SELECT id,'STATIC_DEPOSIT',status,NULL,last_error FROM deposit_claims WHERE status!='SUCCEEDED' UNION ALL SELECT l.request_id,'LIGHTNING_RECEIVE',COALESCE(p.status,'INVOICE_CREATED'),NULL,NULL FROM lightning_receives l LEFT JOIN receive_payments p ON p.hash=l.hash WHERE COALESCE(p.status,'INVOICE_CREATED') NOT IN ('TRANSFER_COMPLETED','HTLC_FAILED') UNION ALL SELECT id,'COOP_EXIT',status,NULL,NULL FROM coop_exits WHERE status NOT IN ('SUCCEEDED','EXPIRED') LIMIT 1000")?;
        let rows=stmt.query_map([],|r|Ok(serde_json::json!({"request_id":r.get::<_,String>(0)?,"kind":r.get::<_,String>(1)?,"state":r.get::<_,String>(2)?,"backend_payment_id":r.get::<_,Option<String>>(3)?,"last_error":r.get::<_,Option<String>>(4)?})))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
    }).await;
    match result {
        Ok(rows) => (StatusCode::OK, Json(serde_json::json!({"unresolved":rows}))),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error":error})),
        ),
    }
}
#[derive(Deserialize)]
struct ReconcileRequest {
    request_id: String,
}
async fn reconcile_settlement(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ReconcileRequest>,
) -> impl IntoResponse {
    if !admin_authorized(&state.config, &headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error":"unauthorized"})),
        );
    }
    match state.ldk.reconcile_request(&request.request_id).await {
        Ok(status) => (StatusCode::OK, Json(serde_json::json!({"state":status}))),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error":error})),
        ),
    }
}

#[derive(Deserialize)]
struct BumpWithdrawalRequest {
    request_id: String,
    fee_rate: u64,
    max_fee_sats: u64,
}

async fn bump_withdrawal(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<BumpWithdrawalRequest>,
) -> impl IntoResponse {
    if !admin_authorized(&state.config, &headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error":"unauthorized"})),
        );
    }
    let result = match &state.coop_exit {
        Some(service) => {
            service
                .bump_fee(&request.request_id, request.fee_rate, request.max_fee_sats)
                .await
        }
        None => Err("cooperative withdrawals are not configured".into()),
    };
    match result {
        Ok(value) => (StatusCode::OK, Json(value)),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error":error})),
        ),
    }
}

async fn graphql_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    // The SDK's Requester deflate-compresses bodies >1024 bytes
    // (CompressionStream exists in Node 18+), so decode first.
    let raw: Vec<u8> = if headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .contains("deflate")
    {
        match inflate_raw_deflate(&body) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!("deflate decode failed: {e}");
                let err = serde_json::json!({ "errors": [{ "message": "bad deflate body" }] });
                return (StatusCode::OK, Json(err)).into_response();
            }
        }
    } else {
        body.to_vec()
    };
    let body = match String::from_utf8(raw) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("non-utf8 graphql body: {e}");
            let err = serde_json::json!({ "errors": [{ "message": "bad graphql body" }] });
            return (StatusCode::OK, Json(err)).into_response();
        }
    };
    let req: GraphqlRequest = match serde_json::from_str(&body) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("unparseable graphql body: {e}");
            let err =
                serde_json::json!({ "errors": [{ "message": format!("bad graphql body: {e}") }] });
            return (StatusCode::OK, Json(err)).into_response();
        }
    };
    let op = detect_operation(&req);
    let logged_op: String = op.chars().take(100).collect();
    info!(op = %logged_op, "ssp graphql call");
    match graphql::dispatch(state, &headers, &op, &req).await {
        Ok(mut data) => {
            // The SDK's generated documents alias every field
            // (`foo_bar: foo`). Real GraphQL servers echo the alias names;
            // our canonical resolver output uses raw schema names, so rewrite
            // keys per the query's own `alias: field` pairs.
            graphql::apply_query_aliases(&mut data, &req.query);
            (StatusCode::OK, Json(GraphqlResponse { data })).into_response()
        }
        Err(e) => {
            let body = serde_json::json!({ "errors": [{ "message": e.to_string() }] });
            (StatusCode::OK, Json(body)).into_response()
        }
    }
}

/// Inflate what CompressionStream("deflate") emits (zlib-wrapped deflate).
/// Falls back to raw deflate for other producers. Capped: a 2 MB compressed
/// body must never expand past MAX_INFLATED_BYTES (M3).
const MAX_INFLATED_BYTES: u64 = 8 * 1024 * 1024;

fn read_capped<R: std::io::Read>(r: R) -> Result<Vec<u8>, String> {
    use std::io::Read;
    let mut out = Vec::new();
    r.take(MAX_INFLATED_BYTES + 1)
        .read_to_end(&mut out)
        .map_err(|e| e.to_string())?;
    if out.len() as u64 > MAX_INFLATED_BYTES {
        return Err("deflate body exceeds limit".to_string());
    }
    Ok(out)
}

fn inflate_raw_deflate(input: &[u8]) -> Result<Vec<u8>, String> {
    match read_capped(flate2::read::ZlibDecoder::new(input)) {
        Ok(out) => Ok(out),
        Err(_) => read_capped(flate2::read::DeflateDecoder::new(input)),
    }
}

/// Detect the GraphQL operation from operationName or query text.
/// The JS SDK sends raw documents like `mutation RequestLightningSend(...)`.
fn detect_operation(req: &GraphqlRequest) -> String {
    if let Some(name) = &req.operation_name {
        if !name.is_empty() {
            return name.clone();
        }
    }
    // Fallback: first `query Foo` / `mutation Foo` token in the document.
    let q = req.query.replace(['\n', '\r', '{', '(', ')'], " ");
    let mut prev = "";
    for tok in q.split_whitespace() {
        if prev == "query" || prev == "mutation" {
            return tok.to_string();
        }
        prev = tok;
    }
    // Last resort substring match (covers aliased/minified docs).
    for known in [
        "GetChallenge",
        "VerifyChallenge",
        "LightningReceiveQuote",
        "RequestLightningReceive",
        "RequestLightningSend",
        "RequestSwap",
        "LeavesSwapFeeEstimate",
        "LightningSendFeeEstimate",
        "StaticDepositQuote",
        "ClaimStaticDeposit",
        "CreateInstantStaticDepositQuote",
        "CreateClaimInstantStaticDeposit",
        "RequestCoopExit",
        "CompleteCoopExit",
        "CoopExitFeeEstimates",
        "CoopExitFeeEstimate",
        "CoopExitFeeQuote",
        "Transfers",
        "UserRequest",
        "FetchCurrentUserToUserRequestsConnection",
        "RegisterWalletWebhook",
        "DeleteWalletWebhook",
        "ListSparkWalletWebhooks",
    ] {
        if req.query.contains(known) {
            return known.to_string();
        }
    }
    "Unknown".to_string()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if std::env::args().nth(1).as_deref() == Some("healthcheck") {
        return binary_healthcheck().await;
    }
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let config = Config::from_env();
    if config.fee_flat_sats_swap != 0 {
        return Err(
            "SSP_SWAP_FEE_SATS must be zero: fee-bearing Swap V3 fills are not supported".into(),
        );
    }
    if config.spark_admin_token.is_empty()
        && std::env::var("SPARK_ADMIN_ALLOW_NO_AUTH").unwrap_or_default() != "1"
    {
        return Err("SPARK_ADMIN_TOKEN is required unless SPARK_ADMIN_ALLOW_NO_AUTH=1".into());
    }
    let db = Arc::new(Db::open(&config.data_dir).map_err(|e| format!("db: {e}"))?);
    let spark = SparkService::connect(&config, db.clone())
        .await
        .map_err(|e| format!("spark: {e}"))?;
    info!(identity = %spark.identity(), "embedded Spark wallet connected");
    let coop_exit =
        coop_exit::CoopExitService::from_env(db.clone(), spark.clone(), &config.network).await?;
    let static_deposit = coop_exit.as_ref().map(|bitcoin| {
        static_deposits::StaticDepositService::new(
            db.clone(),
            spark.clone(),
            bitcoin.clone(),
            config.network.clone(),
        )
    });
    if let Some(service) = &static_deposit {
        tokio::spawn(service.clone().run());
    }
    let backend = Arc::new(LdkGrpcBackend::connect(&config, db.clone(), spark.clone()).await?);
    tokio::spawn(LdkGrpcBackend::run_event_pump(backend.clone()));
    tokio::spawn(LdkGrpcBackend::run_reconciler(backend.clone()));
    // Prune expired sessions, old challenges, and compatibility requests.
    {
        let db = db.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(600)).await;
                let now = chrono::Utc::now();
                let _ = db.prune_expired_sessions(&now.to_rfc3339()).await;
                let _ = db
                    .prune_challenges(&(now - chrono::Duration::hours(1)).to_rfc3339())
                    .await;
                let _ = db
                    .prune_compat_requests(&(now - chrono::Duration::hours(24)).to_rfc3339())
                    .await;
            }
        });
    }
    tokio::spawn(spark.clone().run_swap_history());
    tokio::spawn(webhooks::run(
        db.clone(),
        webhooks::allow_local(&config.network),
    ));
    let addr: SocketAddr = config.listen_addr.parse()?;
    if let Some(service) = coop_exit.clone() {
        tokio::spawn(service.run());
    }
    let state = AppState {
        config: config.clone(),
        db,
        ldk: backend,
        spark: spark.clone(),
        coop_exit,
        static_deposit,
        send_lock: Arc::new(tokio::sync::Mutex::new(())),
    };
    info!("SSP listening on {} (network={})", addr, config.network);
    let app = Router::new()
        .route("/health", get(health))
        .route("/", get(health))
        .route("/identity", get(identity_handler))
        .route("/status", get(status))
        .route("/admin/settlements", get(settlements))
        .route("/admin/settlements/reconcile", post(reconcile_settlement))
        .route("/admin/spark/deposit-address", post(spark_deposit_address))
        .route("/admin/spark/claim-deposit", post(spark_claim_deposit))
        .route("/admin/withdrawals/bump-fee", post(bump_withdrawal))
        // Both schema endpoints the SDK uses:
        // default "graphql/spark/2025-03-19", LOCAL override "graphql/spark/rc".
        .route("/graphql/spark/2025-03-19", post(graphql_handler))
        .route("/graphql/spark/rc", post(graphql_handler))
        .route("/graphql", post(graphql_handler))
        .with_state(state);
    let app = if config.cors_origins.trim().is_empty() {
        app
    } else {
        let origins = config
            .cors_origins
            .split(',')
            .map(str::trim)
            .filter(|origin| !origin.is_empty())
            .map(|origin| origin.parse::<axum::http::HeaderValue>())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("invalid SSP_CORS_ORIGINS: {e}"))?;
        app.layer(
            tower_http::cors::CorsLayer::new()
                .allow_origin(origins)
                .allow_methods([axum::http::Method::GET, axum::http::Method::POST])
                .allow_headers(tower_http::cors::Any),
        )
    };
    let listener = tokio::net::TcpListener::bind(addr).await?;
    spark.start_background_processing().await;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "failed to install Ctrl-C handler");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => tracing::error!(%error, "failed to install SIGTERM handler"),
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
    tracing::info!("shutdown signal received");
}

async fn binary_healthcheck() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listen = std::env::var("SSP_LISTEN_ADDR").unwrap_or_else(|_| "127.0.0.1:5000".into());
    let port = listen
        .rsplit_once(':')
        .ok_or("SSP_LISTEN_ADDR has no port")?
        .1
        .parse::<u16>()?;
    let mut stream = tokio::time::timeout(
        std::time::Duration::from_secs(4),
        tokio::net::TcpStream::connect(("127.0.0.1", port)),
    )
    .await??;
    stream
        .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await?;
    let mut response = [0u8; 64];
    let count = stream.read(&mut response).await?;
    if response[..count].starts_with(b"HTTP/1.1 200") {
        Ok(())
    } else {
        Err("SSP health endpoint did not return HTTP 200".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[tokio::test]
    async fn health_only_reports_status() {
        let Json(body) = health().await;

        assert_eq!(body, serde_json::json!({ "status": "ok" }));
    }

    #[test]
    fn inflate_accepts_zlib_and_raw_deflate() {
        let input = b"graphql request";
        let mut zlib = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        zlib.write_all(input).unwrap();
        assert_eq!(inflate_raw_deflate(&zlib.finish().unwrap()).unwrap(), input);

        let mut raw =
            flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
        raw.write_all(input).unwrap();
        assert_eq!(inflate_raw_deflate(&raw.finish().unwrap()).unwrap(), input);
    }

    #[test]
    fn inflate_rejects_expansion_past_limit() {
        let input = vec![0u8; MAX_INFLATED_BYTES as usize + 1];
        let mut zlib = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::best());
        zlib.write_all(&input).unwrap();
        assert!(inflate_raw_deflate(&zlib.finish().unwrap()).is_err());
    }
}
