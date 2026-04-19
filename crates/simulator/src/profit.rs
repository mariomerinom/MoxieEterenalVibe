//! Net P&L: gross profit - gas - builder bribe.

use crate::gas_model::GasModel;

#[derive(Debug, Clone)]
pub struct PnL {
    pub gross_profit_eth: f64,
    pub gas_cost_eth: f64,
    pub bribe_eth: f64,
    pub net_profit_eth: f64,
}

impl PnL {
    pub fn compute(gross: f64, gas_model: &GasModel, gas_used: u64, bribe_pct: f64) -> Self {
        let gas = gas_model.cost_eth(gas_used);
        let bribe = gross * bribe_pct;
        Self {
            gross_profit_eth: gross,
            gas_cost_eth: gas,
            bribe_eth: bribe,
            net_profit_eth: gross - gas - bribe,
        }
    }

    pub fn is_profitable(&self) -> bool { self.net_profit_eth > 0.0 }
}
