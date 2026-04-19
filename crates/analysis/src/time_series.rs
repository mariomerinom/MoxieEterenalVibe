//! Trend analysis: are opportunities growing or shrinking over the capture window?
//! Detects opportunity decay (competition saturating a market).

use eyre::Result;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct TrendReport {
    pub chain: String,
    pub mev_type: String,
    pub weekly_profits: Vec<f64>,     // profit per week over capture window
    pub weekly_bot_counts: Vec<usize>, // active bots per week
    pub profit_slope: f64,            // linear regression slope (positive = growing)
    pub competition_slope: f64,       // bot count trend
    pub is_growing: bool,
    pub is_saturating: bool,          // profit flat/down while competition rises
}

pub fn compute_trends(duckdb_path: &str) -> Result<Vec<TrendReport>> {
    // TODO:
    // 1. Query weekly aggregates per chain × mev_type
    // 2. Fit linear trend to profit and bot count series
    // 3. Flag saturating markets (competition up, profit flat/down)
    todo!("implement trend analysis")
}
