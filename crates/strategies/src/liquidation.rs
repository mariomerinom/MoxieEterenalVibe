//! Liquidation strategy template. Chain-agnostic — activated only if Phase 2
//! finds a chain where liquidation competition is thin enough.

use eyre::Result;
use mev_capture::types::Chain;
use crate::traits::{Action, Event, Strategy};

pub struct LiquidationStrategy {
    pub chain: Chain,
    pub min_profit_eth: f64,
    pub max_gas_gwei: f64,
    pub bribe_pct: f64,
    pub hf_liquidate_threshold: f64,
}

#[async_trait::async_trait]
impl Strategy for LiquidationStrategy {
    fn name(&self) -> &str { "liquidation" }
    fn chain(&self) -> Chain { self.chain }

    async fn process_event(&self, event: &Event) -> Result<Vec<Action>> {
        match event {
            Event::NewBlock(_block) => {
                // TODO: check health factors, simulate flash loan liquidation
                Ok(vec![])
            }
            _ => Ok(vec![]),
        }
    }

    fn params(&self) -> serde_json::Value {
        serde_json::json!({
            "min_profit_eth": self.min_profit_eth,
            "max_gas_gwei": self.max_gas_gwei,
            "bribe_pct": self.bribe_pct,
            "hf_liquidate_threshold": self.hf_liquidate_threshold,
        })
    }

    fn set_params(&mut self, p: serde_json::Value) -> Result<()> {
        if let Some(v) = p.get("min_profit_eth").and_then(|v| v.as_f64()) { self.min_profit_eth = v; }
        if let Some(v) = p.get("max_gas_gwei").and_then(|v| v.as_f64()) { self.max_gas_gwei = v; }
        if let Some(v) = p.get("bribe_pct").and_then(|v| v.as_f64()) { self.bribe_pct = v; }
        if let Some(v) = p.get("hf_liquidate_threshold").and_then(|v| v.as_f64()) { self.hf_liquidate_threshold = v; }
        Ok(())
    }
}
