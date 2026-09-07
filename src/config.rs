use serde::{Deserialize, Serialize};

/// Runtime config. All values have sane regtest defaults.
/// Set via env; works for any network (regtest/signet/testnet/mainnet/custom MutinyNet).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub listen_addr: String,
    /// Comma-separated browser origins. Empty disables cross-origin access.
    pub cors_origins: String,
    pub network: String,
    /// Optional deployment guard. If set, the embedded wallet must derive this
    /// compressed identity public key from its mnemonic.
    pub ssp_identity_pubkey: String,
    /// Directory for sqlite state (volume-mount in compose).
    pub data_dir: String,
    /// Live LDK backend (host:port WITHOUT scheme, e.g. "ldk-server:3536").
    pub ldk_grpc_addr: String,
    pub ldk_api_key: String,
    pub ldk_api_key_file: String,
    pub ldk_tls_cert_file: String,
    pub fee_flat_sats_swap: u64,
    /// Public URL used by the embedded wallet for SSP GraphQL calls.
    pub ssp_public_url: String,
    /// BIP39 mnemonic storage for the embedded Spark wallet.
    pub spark_mnemonic_file: String,
    /// Refuse to create a new mnemonic when the configured file is absent.
    /// Production enables this to prevent an accidental identity change.
    pub spark_mnemonic_required: bool,
    /// Custom operator endpoints and identities, in the same order.
    pub so_hosts: String,
    pub so_identity_pubkeys: String,
    /// Optional comma-separated CA certificate files for custom operators.
    pub so_cert_files: String,
    /// Private SSP-facing operator endpoints, in the same order as SO_HOSTS.
    /// Empty disables just-in-time leaf splitting.
    pub ssp_operator_hosts: String,
    /// Optional comma-separated CA certificate files for SSP operator endpoints.
    pub ssp_operator_cert_files: String,
    /// Smallest child the local liquidity policy will create. Spark accepts
    /// positive sub-dust leaves off chain; set this to the deployment's relay
    /// dust floor when every child must be independently exit-relayable.
    pub ssp_min_split_child_sats: u64,
    /// Token for the integrated funding endpoints. A missing token fails
    /// closed unless SPARK_ADMIN_ALLOW_NO_AUTH=1 is explicit.
    pub spark_admin_token: String,
    /// FROST threshold (must match the SO signing threshold).
    pub frost_threshold: usize,
    /// Max total per swap (sats). Bounds operator exposure: user swap
    /// primaries settle only via SO expiry-return, so a restored user wallet
    /// could resurrect spent leaves inside the return window. 0 = no cap.
    pub max_swap_total_sats: u64,
}

impl Config {
    pub fn from_env() -> Self {
        let get = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
        Self {
            listen_addr: get("SSP_LISTEN_ADDR", "127.0.0.1:5000"),
            cors_origins: get("SSP_CORS_ORIGINS", ""),
            network: get("SSP_NETWORK", "REGTEST"),
            // Empty = use the resolved signing key's pubkey (first boot
            // generates its mnemonic and publishes the key via /identity).
            ssp_identity_pubkey: get("SSP_IDENTITY_PUBKEY", ""),
            data_dir: get("SSP_DATA_DIR", "./data"),
            ldk_grpc_addr: get("LDK_GRPC_ADDR", ""),
            ldk_api_key: get("LDK_API_KEY", ""),
            ldk_api_key_file: get("LDK_API_KEY_FILE", ""),
            ldk_tls_cert_file: get("LDK_TLS_CERT_FILE", ""),
            fee_flat_sats_swap: get("SSP_SWAP_FEE_SATS", "0").parse().unwrap_or(0),
            ssp_public_url: get("SSP_PUBLIC_URL", "http://127.0.0.1:5000")
                .trim_end_matches('/')
                .to_string(),
            spark_mnemonic_file: get("SPARK_MNEMONIC_FILE", "./data/spark.mnemonic"),
            spark_mnemonic_required: matches!(
                get("SPARK_MNEMONIC_REQUIRED", "0")
                    .to_ascii_lowercase()
                    .as_str(),
                "1" | "true" | "yes"
            ),
            so_hosts: get("SO_HOSTS", ""),
            so_identity_pubkeys: get("SO_IDENTITY_PUBKEYS", ""),
            so_cert_files: get("SO_CERT_FILES", ""),
            ssp_operator_hosts: get("SSP_OPERATOR_HOSTS", ""),
            ssp_operator_cert_files: get("SSP_OPERATOR_CERT_FILES", ""),
            ssp_min_split_child_sats: get("SSP_MIN_SPLIT_CHILD_SATS", "330")
                .parse()
                .unwrap_or(330),
            spark_admin_token: std::env::var("SPARK_ADMIN_TOKEN")
                .or_else(|_| std::env::var("SIDECAR_TOKEN"))
                .unwrap_or_default(),
            frost_threshold: get("SSP_FROST_THRESHOLD", "2").parse().unwrap_or(2),
            max_swap_total_sats: get("MAX_SWAP_TOTAL_SATS", "1000000")
                .parse()
                .unwrap_or(1000000),
        }
    }
}
