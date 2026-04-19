use serde::Deserialize;
use std::collections::HashMap;
use crate::types::Chain;

#[derive(Debug, Deserialize)]
pub struct CaptureConfig {
    pub chains: HashMap<String, ChainConfig>,
    pub mevshare: MevShareConfig,
    pub ground_truth: GroundTruthConfig,
    pub storage: StorageConfig,
}

#[derive(Debug, Deserialize)]
pub struct ChainConfig {
    pub enabled: bool,
    pub rpc_http: String,
    pub rpc_ws: Option<String>,
    pub chain_id: u64,
    pub block_time_ms: u64,
    pub requests_per_second: u32,
    #[serde(default)]
    pub dex_protocols: Vec<DexProtocolConfig>,
    #[serde(default)]
    pub lending_protocols: Vec<LendingProtocolConfig>,
}

#[derive(Debug, Deserialize)]
pub struct DexProtocolConfig {
    pub name: String,
    pub factory: Option<String>,
    pub router: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct LendingProtocolConfig {
    pub name: String,
    pub pool: String,
}

#[derive(Debug, Deserialize)]
pub struct MevShareConfig {
    pub enabled: bool,
    pub sse_url: String,
}

#[derive(Debug, Deserialize)]
pub struct GroundTruthConfig {
    pub eigenphi_enabled: bool,
    pub eigenphi_api_key: String,
    pub zeromev_enabled: bool,
}

#[derive(Debug, Deserialize)]
pub struct StorageConfig {
    pub data_dir: String,
    pub duckdb_path: String,
    pub flush_interval_secs: u64,
    pub buffer_size: usize,
}

impl CaptureConfig {
    pub fn load(path: &std::path::Path) -> eyre::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let resolved = resolve_env_vars(&content);
        Ok(toml::from_str(&resolved)?)
    }

    pub fn enabled_chains(&self) -> Vec<(Chain, &ChainConfig)> {
        self.chains
            .iter()
            .filter(|(_, c)| c.enabled)
            .filter_map(|(name, config)| {
                let chain = match name.as_str() {
                    "ethereum" => Some(Chain::Ethereum),
                    "base" => Some(Chain::Base),
                    "arbitrum" => Some(Chain::Arbitrum),
                    "polygon" => Some(Chain::Polygon),
                    "scroll" => Some(Chain::Scroll),
                    "blast" => Some(Chain::Blast),
                    _ => None,
                };
                chain.map(|c| (c, config))
            })
            .collect()
    }
}

/// Resolve `${VAR_NAME}` patterns against environment variables.
fn resolve_env_vars(input: &str) -> String {
    let mut result = input.to_string();
    // Find all ${...} patterns and replace with env var values
    loop {
        let start = match result.find("${") {
            Some(i) => i,
            None => break,
        };
        let end = match result[start..].find('}') {
            Some(i) => start + i,
            None => break,
        };
        let var_name = &result[start + 2..end];
        let value = std::env::var(var_name).unwrap_or_default();
        result = format!("{}{}{}", &result[..start], value, &result[end + 1..]);
    }
    result
}
