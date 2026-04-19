//! Bot fingerprinting and cross-chain competition analysis.
//! Answers: are the same bots active across chains? What's their win rate?

use eyre::Result;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct BotProfile {
    pub address: String,
    pub chains_active: Vec<String>,
    pub mev_types: Vec<String>,
    pub total_extractions: usize,
    pub total_profit_eth: f64,
    pub win_rate: f64,
    pub avg_gas_bid_eth: f64,
    pub first_seen_block: u64,
    pub last_seen_block: u64,
}

#[derive(Debug, Serialize)]
pub struct CrossChainOverlap {
    pub bot_address: String,
    pub chain_a: String,
    pub chain_b: String,
    pub active_on_both: bool,
}

pub fn profile_bots(duckdb_path: &str) -> Result<Vec<BotProfile>> {
    // TODO: query ground truth data, group by searcher address
    // Join across chains to detect cross-chain operators
    todo!("implement bot profiling")
}
