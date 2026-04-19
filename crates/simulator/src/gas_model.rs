//! Per-block gas cost modeling. Uses actual basefee + priority fee, not averages.

use alloy_primitives::U256;

pub struct GasModel {
    pub base_fee: U256,
    pub priority_fee: U256,
}

impl GasModel {
    pub fn from_block(base_fee: U256, priority_fee: U256) -> Self {
        Self { base_fee, priority_fee }
    }

    pub fn cost_eth(&self, gas_used: u64) -> f64 {
        let gas_price = self.base_fee + self.priority_fee;
        let cost_wei = gas_price * U256::from(gas_used);
        cost_wei.to_string().parse::<f64>().unwrap_or(0.0) / 1e18
    }
}
