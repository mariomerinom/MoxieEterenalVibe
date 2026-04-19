//! Multi-protocol lending liquidation event parser.
//! Supports Aave V3 (Phase 1.0). Compound V3, Seamless, Moonwell, Radiant, Orbit deferred to 1.1.

use alloy_primitives::{Address, B256, U256};
use eyre::Result;

use crate::types::{Chain, LendingProtocol, LiquidationEvent, TransactionData};

// Aave V3: LiquidationCall(address indexed collateralAsset, address indexed debtAsset, address indexed user,
//                           uint256 debtToCover, uint256 liquidatedCollateralAmount, address liquidator, bool receiveAToken)
const AAVE_V3_LIQUIDATION_TOPIC: B256 = B256::new(hex_literal::hex!(
    "e413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286"
));

pub fn parse_liquidations(
    chain: Chain,
    block_number: u64,
    tx: &TransactionData,
) -> Result<Vec<LiquidationEvent>> {
    let mut events = Vec::new();

    for log in &tx.logs {
        if log.topics.is_empty() {
            continue;
        }

        if log.topics[0] == AAVE_V3_LIQUIDATION_TOPIC {
            if let Some(event) = decode_aave_v3_liquidation(chain, block_number, tx, log) {
                events.push(event);
            }
        }
    }

    Ok(events)
}

fn decode_aave_v3_liquidation(
    chain: Chain,
    block_number: u64,
    tx: &TransactionData,
    log: &crate::types::LogData,
) -> Option<LiquidationEvent> {
    // Need at least 4 topics and 96 bytes of data
    if log.topics.len() < 4 || log.data.len() < 96 {
        return None;
    }

    // Indexed params from topics (addresses are right-aligned in 32-byte topic)
    let collateral_asset = Address::from_slice(&log.topics[1].as_slice()[12..32]);
    let debt_asset = Address::from_slice(&log.topics[2].as_slice()[12..32]);
    let borrower = Address::from_slice(&log.topics[3].as_slice()[12..32]);

    // Non-indexed params from data:
    // [0..32]   uint256 debtToCover
    // [32..64]  uint256 liquidatedCollateralAmount
    // [64..96]  address liquidator (right-aligned in 32 bytes)
    // [96..128] bool receiveAToken (if present)
    let debt_to_cover = U256::from_be_slice(&log.data[0..32]);
    let liquidated_collateral = U256::from_be_slice(&log.data[32..64]);
    let liquidator = Address::from_slice(&log.data[76..96]);

    Some(LiquidationEvent {
        chain,
        block_number,
        tx_hash: tx.hash,
        tx_index: tx.tx_index,
        protocol: LendingProtocol::AaveV3,
        liquidator,
        borrower,
        collateral_asset,
        debt_asset,
        debt_to_cover,
        liquidated_collateral,
        gas_used: tx.gas_used,
        gas_price: tx.gas_price,
    })
}
