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
    pub ldk_backend: LdkBackendMode,
    /// Empty selects <SSP_DATA_DIR>/ldk-node.
    pub ldk_node_data_dir: String,
    pub ldk_node_esplora_url: String,
    pub ldk_node_listen_addr: String,
    /// Refuse to generate a seed on an existing deployment.
    pub ldk_node_seed_required: bool,
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
    /// Zero disables new advances; pending recoveries still run.
    pub instant_max_outstanding_sats: u64,
    pub instant_max_deposit_sats: u64,
}

fn parsed<T: std::str::FromStr>(key: &str, default: T) -> Result<T, String>
where
    T::Err: std::fmt::Display,
{
    match std::env::var(key) {
        Ok(value) if !value.trim().is_empty() => {
            value.trim().parse::<T>().map_err(|e| format!("{key}: {e}"))
        }
        _ => Ok(default),
    }
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let get = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
        Ok(Self {
            listen_addr: get("SSP_LISTEN_ADDR", "127.0.0.1:5000"),
            cors_origins: get("SSP_CORS_ORIGINS", ""),
            network: get("SSP_NETWORK", "REGTEST"),
            // Empty = use the resolved signing key's pubkey (first boot
            // generates its mnemonic and publishes the key via /identity).
            ssp_identity_pubkey: get("SSP_IDENTITY_PUBKEY", ""),
            data_dir: get("SSP_DATA_DIR", "./data"),
            ldk_backend: parsed("LDK_BACKEND", LdkBackendMode::Server)?,
            ldk_node_data_dir: get("LDK_NODE_DATA_DIR", ""),
            ldk_node_esplora_url: get("LDK_NODE_ESPLORA_URL", ""),
            ldk_node_listen_addr: get("LDK_NODE_LISTEN_ADDR", "0.0.0.0:9735"),
            ldk_node_seed_required: parsed("LDK_NODE_SEED_REQUIRED", false)?,
            ldk_grpc_addr: get("LDK_GRPC_ADDR", ""),
            ldk_api_key: get("LDK_API_KEY", ""),
            ldk_api_key_file: get("LDK_API_KEY_FILE", ""),
            ldk_tls_cert_file: get("LDK_TLS_CERT_FILE", ""),
            fee_flat_sats_swap: parsed("SSP_SWAP_FEE_SATS", 0)?,
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
            ssp_min_split_child_sats: parsed("SSP_MIN_SPLIT_CHILD_SATS", 330)?,
            // A missing token fails closed unless SPARK_ADMIN_ALLOW_NO_AUTH=1
            // is explicit.
            spark_admin_token: std::env::var("SPARK_ADMIN_TOKEN")
                .or_else(|_| std::env::var("SIDECAR_TOKEN"))
                .unwrap_or_default(),
            frost_threshold: parsed("SSP_FROST_THRESHOLD", 2)?,
            max_swap_total_sats: parsed("MAX_SWAP_TOTAL_SATS", 1_000_000)?,
            instant_max_outstanding_sats: parsed("SSP_INSTANT_MAX_OUTSTANDING_SATS", 0)?,
            instant_max_deposit_sats: parsed("SSP_INSTANT_MAX_DEPOSIT_SATS", 0)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{parsed, LdkBackendMode};

    #[test]
    fn lightning_backend_selection_is_explicit() {
        assert_eq!(LdkBackendMode::default(), LdkBackendMode::Server);
        assert_eq!(
            "server".parse::<LdkBackendMode>().unwrap(),
            LdkBackendMode::Server
        );
        assert_eq!(
            "embedded".parse::<LdkBackendMode>().unwrap(),
            LdkBackendMode::Embedded
        );
        assert!("embeded".parse::<LdkBackendMode>().is_err());
        assert!("".parse::<LdkBackendMode>().is_err());
    }

    #[test]
    fn numeric_settings_use_defaults_and_reject_malformed_values() {
        std::env::set_var("SSP_TEST_LIMIT", "21");
        assert_eq!(parsed::<u64>("SSP_TEST_LIMIT", 0).unwrap(), 21);
        std::env::set_var("SSP_TEST_LIMIT", " 7 ");
        assert_eq!(parsed::<u64>("SSP_TEST_LIMIT", 0).unwrap(), 7);
        std::env::set_var("SSP_TEST_LIMIT", "");
        assert_eq!(parsed::<u64>("SSP_TEST_LIMIT", 330).unwrap(), 330);
        std::env::set_var("SSP_TEST_LIMIT", "21,5");
        assert!(parsed::<u64>("SSP_TEST_LIMIT", 0)
            .unwrap_err()
            .contains("SSP_TEST_LIMIT"));
    }
}

/// Explicit selection; never fall back to a different node after a connection error.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LdkBackendMode {
    #[default]
    Server,
    Embedded,
}

impl std::str::FromStr for LdkBackendMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "server" => Ok(Self::Server),
            "embedded" => Ok(Self::Embedded),
            _ => Err("expected server or embedded".into()),
        }
    }
}
