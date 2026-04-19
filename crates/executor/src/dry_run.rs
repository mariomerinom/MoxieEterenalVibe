//! Paper trading mode. Logs detailed theoretical P&L without submitting real bundles.
//!
//! Each opportunity is logged as a JSONL line with full details for post-analysis.

use eyre::Result;
use serde::Serialize;
use tracing::info;

/// A detailed dry-run opportunity log entry.
#[derive(Debug, Clone, Serialize)]
pub struct DryRunEntry {
    pub timestamp: String,
    pub block_number: u64,
    pub chain: String,
    pub strategy: String,
    pub cycle_label: String,
    pub pools: Vec<String>,
    pub input_amount_eth: f64,
    pub expected_output_eth: f64,
    pub gross_profit_eth: f64,
    pub gas_cost_eth: f64,
    pub bribe_eth: f64,
    pub net_profit_eth: f64,
    pub estimated_gas: u64,
    pub base_fee_gwei: f64,
    pub block_process_time_ms: u64,
    /// EVM simulation result (None = not simulated)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sim_success: Option<bool>,
    /// Gas used in EVM simulation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sim_gas_used: Option<u64>,
}

pub struct DryRunExecutor {
    log_path: std::path::PathBuf,
    /// Running totals for summary.
    total_opportunities: u64,
    total_gross_profit: f64,
    total_net_profit: f64,
    total_gas_cost: f64,
    total_bribes: f64,
    blocks_processed: u64,
}

impl DryRunExecutor {
    pub fn new(log_path: std::path::PathBuf) -> Self {
        Self {
            log_path,
            total_opportunities: 0,
            total_gross_profit: 0.0,
            total_net_profit: 0.0,
            total_gas_cost: 0.0,
            total_bribes: 0.0,
            blocks_processed: 0,
        }
    }

    /// Log a detected opportunity with full details.
    pub fn log_opportunity(&mut self, entry: &DryRunEntry) -> Result<()> {
        // Update running totals
        self.total_opportunities += 1;
        self.total_gross_profit += entry.gross_profit_eth;
        self.total_net_profit += entry.net_profit_eth;
        self.total_gas_cost += entry.gas_cost_eth;
        self.total_bribes += entry.bribe_eth;

        // Write JSONL line
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)?;
        use std::io::Write;
        writeln!(file, "{}", serde_json::to_string(&entry)?)?;

        // Structured log for real-time monitoring
        info!(
            block = entry.block_number,
            strategy = %entry.strategy,
            cycle = %entry.cycle_label,
            net_profit_eth = format!("{:.6}", entry.net_profit_eth),
            gross_profit_eth = format!("{:.6}", entry.gross_profit_eth),
            input_eth = format!("{:.4}", entry.input_amount_eth),
            gas = entry.estimated_gas,
            "DRY-RUN opportunity"
        );

        Ok(())
    }

    /// Record that a block was processed (even if no opportunities found).
    pub fn record_block(&mut self) {
        self.blocks_processed += 1;
    }

    /// Log the simple theoretical format (backward compat).
    pub async fn log_theoretical(
        &mut self,
        strategy: &str,
        _chain: &str,
        profit_eth: f64,
        gas_eth: f64,
    ) -> Result<()> {
        let entry = DryRunEntry {
            timestamp: chrono::Utc::now().to_rfc3339(),
            block_number: 0,
            chain: String::new(),
            strategy: strategy.to_string(),
            cycle_label: String::new(),
            pools: vec![],
            input_amount_eth: 0.0,
            expected_output_eth: profit_eth + gas_eth,
            gross_profit_eth: profit_eth,
            gas_cost_eth: gas_eth,
            bribe_eth: 0.0,
            net_profit_eth: profit_eth - gas_eth,
            estimated_gas: 0,
            base_fee_gwei: 0.0,
            block_process_time_ms: 0,
            sim_success: None,
            sim_gas_used: None,
        };
        self.log_opportunity(&entry)
    }

    /// Print a summary of all recorded opportunities.
    pub fn summary(&self) {
        info!(
            blocks = self.blocks_processed,
            opportunities = self.total_opportunities,
            total_gross_profit_eth = format!("{:.6}", self.total_gross_profit),
            total_net_profit_eth = format!("{:.6}", self.total_net_profit),
            total_gas_eth = format!("{:.6}", self.total_gas_cost),
            total_bribes_eth = format!("{:.6}", self.total_bribes),
            avg_profit_per_opp = format!("{:.6}",
                if self.total_opportunities > 0 {
                    self.total_net_profit / self.total_opportunities as f64
                } else { 0.0 }
            ),
            opp_rate = format!("{:.1}%",
                if self.blocks_processed > 0 {
                    self.total_opportunities as f64 / self.blocks_processed as f64 * 100.0
                } else { 0.0 }
            ),
            "DRY-RUN SUMMARY"
        );
    }
}
