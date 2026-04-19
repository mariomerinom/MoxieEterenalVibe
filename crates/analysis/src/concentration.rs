//! Market concentration analysis per chain × MEV type.
//! Computes Herfindahl-Hirschman Index, top-N share, active bot count.
//!
//! This is the automated, multi-chain version of the Polygon/Arbitrum
//! liquidation analysis that killed Track A.

use eyre::Result;
use mev_capture::types::{Chain, MevType};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct ConcentrationReport {
    pub chain: String,
    pub mev_type: String,
    pub herfindahl_index: f64,     // 0.0 = perfect competition, 1.0 = monopoly
    pub top1_share: f64,           // % of profit captured by top bot
    pub top5_share: f64,           // % of profit captured by top 5
    pub active_bots: usize,        // distinct addresses with >=1 extraction
    pub total_extractions: usize,
    pub total_profit_eth: f64,
    pub avg_profit_per_extraction: f64,
    pub period_days: u32,
}

/// Compute concentration for all chain × MEV type combinations.
/// Queries DuckDB views over the Parquet data lake.
pub fn compute_all(duckdb_path: &str) -> Result<Vec<ConcentrationReport>> {
    // TODO:
    // 1. Connect to DuckDB
    // 2. For each (chain, mev_type) pair with data:
    //    SELECT searcher, COUNT(*) as extractions, SUM(profit_eth) as total_profit
    //    FROM ground_truth
    //    GROUP BY chain, mev_type, searcher
    // 3. Compute Herfindahl: sum of squared market shares
    // 4. Compute top-1, top-5 shares
    // 5. Return ConcentrationReport per combination
    todo!("implement concentration analysis over DuckDB")
}

/// Herfindahl-Hirschman Index from a list of market shares (0.0 to 1.0 each).
pub fn herfindahl(shares: &[f64]) -> f64 {
    shares.iter().map(|s| s * s).sum()
}
