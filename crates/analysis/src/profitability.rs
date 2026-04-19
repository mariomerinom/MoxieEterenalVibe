//! Profit distribution analysis. Pareto analysis, percentile breakdowns,
//! total addressable market per chain × MEV type.

use eyre::Result;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct ProfitabilityReport {
    pub chain: String,
    pub mev_type: String,
    pub total_profit_eth: f64,
    pub monthly_profit_eth: f64,      // annualized from period
    pub median_profit_per_tx: f64,
    pub p90_profit_per_tx: f64,
    pub p10_profit_per_tx: f64,       // bottom 10% (often negative = gas losses)
    pub pct_profitable_txs: f64,      // % of extractions that were net positive
    pub avg_gas_cost_eth: f64,
    pub pareto_80_20: bool,           // does top 20% of bots capture 80%+ of profit?
}

pub fn compute_all(duckdb_path: &str) -> Result<Vec<ProfitabilityReport>> {
    // TODO: query DuckDB, compute stats per chain × mev_type
    todo!("implement profitability analysis")
}
