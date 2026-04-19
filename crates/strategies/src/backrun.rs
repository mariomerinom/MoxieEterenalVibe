//! MEV-Share backrunning. Activated only if Phase 2 scores L1 backrunning as viable.

use eyre::Result;
use mev_capture::types::Chain;
use crate::traits::{Action, Event, Strategy};

pub struct BackrunStrategy {
    pub min_profit_eth: f64,
    pub max_gas_fraction: f64,
    pub bribe_pct: f64,
}

impl Default for BackrunStrategy {
    fn default() -> Self {
        Self { min_profit_eth: 0.001, max_gas_fraction: 0.80, bribe_pct: 0.50 }
    }
}

#[async_trait::async_trait]
impl Strategy for BackrunStrategy {
    fn name(&self) -> &str { "backrun" }
    fn chain(&self) -> Chain { Chain::Ethereum }

    async fn process_event(&self, event: &Event) -> Result<Vec<Action>> {
        match event {
            Event::MevShareHint(_hint) => {
                // TODO: parse hint, simulate backrun, check profitability
                Ok(vec![])
            }
            _ => Ok(vec![]),
        }
    }

    fn params(&self) -> serde_json::Value {
        serde_json::json!({ "min_profit_eth": self.min_profit_eth, "max_gas_fraction": self.max_gas_fraction, "bribe_pct": self.bribe_pct })
    }

    fn set_params(&mut self, p: serde_json::Value) -> Result<()> {
        if let Some(v) = p.get("min_profit_eth").and_then(|v| v.as_f64()) { self.min_profit_eth = v; }
        if let Some(v) = p.get("max_gas_fraction").and_then(|v| v.as_f64()) { self.max_gas_fraction = v; }
        if let Some(v) = p.get("bribe_pct").and_then(|v| v.as_f64()) { self.bribe_pct = v; }
        Ok(())
    }
}
