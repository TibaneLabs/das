use crate::error::DasApiError;
use {
    figment::{providers::Env, Figment},
    serde::Deserialize,
};

#[derive(Deserialize, Default)]
pub struct Config {
    pub database_url: String,
    pub metrics_port: Option<u16>,
    pub metrics_host: Option<String>,
    pub server_port: u16,
    /// TibaneLabs fork: bind address. Defaults to 127.0.0.1 when unset.
    pub server_host: Option<String>,
    pub env: Option<String>,

    /// TibaneLabs fork: upstream DAS endpoint used for assets this node has not indexed.
    /// Carries an API key, so it is never logged. Unset disables the fallback.
    pub fallback_das_url: Option<String>,
    /// Seconds before an upstream request is abandoned (default 10).
    pub fallback_das_timeout_secs: Option<u64>,
    /// Seconds a cached upstream answer stays usable (default 3600).
    pub fallback_das_cache_ttl_secs: Option<u64>,

    /// TibaneLabs fork: RPC used for `showNativeBalance` (default http://127.0.0.1:8899).
    pub rpc_url: Option<String>,
}

pub fn load_config() -> Result<Config, DasApiError> {
    Figment::new()
        .join(Env::prefixed("APP_"))
        .extract()
        .map_err(|config_error| DasApiError::ConfigurationError(config_error.to_string()))
}
