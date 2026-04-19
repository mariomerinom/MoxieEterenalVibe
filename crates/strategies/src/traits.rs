//! Shared Strategy trait. All strategy implementations satisfy this interface.

use alloy_primitives::{Address, U256, B256};
use eyre::Result;
use mev_capture::types::{BlockData, Chain, MevShareHint};

#[derive(Debug, Clone)]
pub struct Action {
    pub chain: Chain,
    pub strategy: String,
    pub target_tx: Option<B256>,
    pub to: Address,
    pub calldata: Vec<u8>,
    pub value: U256,
    pub estimated_profit_eth: f64,
    pub estimated_gas: u64,
    pub bribe_pct: f64,
    /// Human-readable label for the arb cycle (e.g. "WETH->USDC via V2 -> USDC->WETH via V3").
    pub cycle_label: String,
    /// Optimal input amount in ETH.
    pub input_amount_eth: f64,
    /// Pool addresses involved in this action.
    pub pool_addresses: Vec<Address>,
}

#[derive(Debug, Clone)]
pub enum Event {
    NewBlock(BlockData),
    MevShareHint(MevShareHint),
}

#[async_trait::async_trait]
pub trait Strategy: Send + Sync {
    fn name(&self) -> &str;
    fn chain(&self) -> Chain;
    async fn process_event(&self, event: &Event) -> Result<Vec<Action>>;
    fn params(&self) -> serde_json::Value;
    fn set_params(&mut self, params: serde_json::Value) -> Result<()>;
}
