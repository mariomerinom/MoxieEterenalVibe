pub mod swaps;
pub mod liquidations;
pub mod solana_swaps;

use eyre::Result;
use crate::types::{BlockData, LiquidationEvent, SwapEvent};

/// Parse all MEV-relevant events from a block's transaction logs.
pub fn extract_all_events(block: &BlockData) -> Result<ExtractedEvents> {
    let mut swap_events = Vec::new();
    let mut liquidation_events = Vec::new();

    for tx in &block.transactions {
        swap_events.extend(swaps::parse_swaps(block.chain, block.number, tx)?);
        liquidation_events.extend(liquidations::parse_liquidations(block.chain, block.number, tx)?);
    }

    Ok(ExtractedEvents {
        swaps: swap_events,
        liquidations: liquidation_events,
    })
}

#[derive(Debug)]
pub struct ExtractedEvents {
    pub swaps: Vec<SwapEvent>,
    pub liquidations: Vec<LiquidationEvent>,
}
