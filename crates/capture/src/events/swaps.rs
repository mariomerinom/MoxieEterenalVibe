//! Multi-protocol DEX swap event parser.
//! Decodes swap events from Uniswap V2/V3 (and forks: SushiSwap, QuickSwap, Aerodrome, etc).
//!
//! For Phase 1.0: token_in/token_out are set to Address::ZERO since resolving them
//! requires factory contract lookups. The pool address identifies the pair.

use alloy_primitives::{Address, B256, U256};
use eyre::Result;

use crate::types::{Chain, DexProtocol, SwapEvent, TransactionData};

// Uniswap V2 Swap(address indexed sender, uint amount0In, uint amount1In, uint amount0Out, uint amount1Out, address indexed to)
const UNIV2_SWAP_TOPIC: B256 = B256::new(hex_literal::hex!(
    "d78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
));

// Uniswap V3 Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)
const UNIV3_SWAP_TOPIC: B256 = B256::new(hex_literal::hex!(
    "c42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
));

pub fn parse_swaps(
    chain: Chain,
    block_number: u64,
    tx: &TransactionData,
) -> Result<Vec<SwapEvent>> {
    let mut swaps = Vec::new();

    for log in &tx.logs {
        if log.topics.is_empty() {
            continue;
        }
        let topic0 = log.topics[0];

        if topic0 == UNIV2_SWAP_TOPIC {
            if let Some(swap) = decode_v2_swap(chain, block_number, tx, log) {
                swaps.push(swap);
            }
        } else if topic0 == UNIV3_SWAP_TOPIC {
            if let Some(swap) = decode_v3_swap(chain, block_number, tx, log) {
                swaps.push(swap);
            }
        }
    }

    Ok(swaps)
}

fn decode_v2_swap(
    chain: Chain,
    block_number: u64,
    tx: &TransactionData,
    log: &crate::types::LogData,
) -> Option<SwapEvent> {
    // Data: amount0In (32) + amount1In (32) + amount0Out (32) + amount1Out (32) = 128 bytes
    if log.data.len() < 128 {
        return None;
    }

    let amount0_in = U256::from_be_slice(&log.data[0..32]);
    let amount1_in = U256::from_be_slice(&log.data[32..64]);
    let amount0_out = U256::from_be_slice(&log.data[64..96]);
    let amount1_out = U256::from_be_slice(&log.data[96..128]);

    // Determine swap direction
    let (amount_in, amount_out) = if !amount0_in.is_zero() {
        (amount0_in, amount1_out)
    } else {
        (amount1_in, amount0_out)
    };

    // Sender from topics[1]
    let sender = if log.topics.len() >= 2 {
        Address::from_slice(&log.topics[1].as_slice()[12..32])
    } else {
        Address::ZERO
    };

    Some(SwapEvent {
        chain,
        block_number,
        tx_hash: tx.hash,
        tx_index: tx.tx_index,
        log_index: log.log_index,
        pool: log.address,
        protocol: DexProtocol::UniswapV2, // SushiSwap/QuickSwap use same sig
        token_in: Address::ZERO,  // Requires factory lookup (Phase 1.1)
        token_out: Address::ZERO,
        amount_in,
        amount_out,
        sender,
        tx_from: tx.from,
    })
}

fn decode_v3_swap(
    chain: Chain,
    block_number: u64,
    tx: &TransactionData,
    log: &crate::types::LogData,
) -> Option<SwapEvent> {
    // Data: int256 amount0 (32) + int256 amount1 (32) + uint160 sqrtPriceX96 (32) + uint128 liquidity (32) + int24 tick (32) = 160 bytes
    if log.data.len() < 64 {
        return None;
    }

    // amount0 and amount1 are signed int256
    // Positive = pool received (token in), negative = pool paid out (token out)
    let amount0_raw = U256::from_be_slice(&log.data[0..32]);
    let amount1_raw = U256::from_be_slice(&log.data[32..64]);

    let amount0_negative = is_negative_i256(amount0_raw);
    let _amount1_negative = is_negative_i256(amount1_raw);

    let (amount_in, amount_out) = if !amount0_negative {
        // amount0 is positive (pool received token0), amount1 is negative (pool paid token1)
        (amount0_raw, negate_i256(amount1_raw))
    } else {
        // amount1 is positive (pool received token1), amount0 is negative (pool paid token0)
        (amount1_raw, negate_i256(amount0_raw))
    };

    // Sender from topics[1]
    let sender = if log.topics.len() >= 2 {
        Address::from_slice(&log.topics[1].as_slice()[12..32])
    } else {
        Address::ZERO
    };

    Some(SwapEvent {
        chain,
        block_number,
        tx_hash: tx.hash,
        tx_index: tx.tx_index,
        log_index: log.log_index,
        pool: log.address,
        protocol: DexProtocol::UniswapV3,
        token_in: Address::ZERO,
        token_out: Address::ZERO,
        amount_in,
        amount_out,
        sender,
        tx_from: tx.from,
    })
}

/// Check if a U256 represents a negative int256 (top bit set).
fn is_negative_i256(val: U256) -> bool {
    val.bit(255)
}

/// Negate a two's complement int256 stored as U256 (compute absolute value).
fn negate_i256(val: U256) -> U256 {
    if is_negative_i256(val) {
        // Two's complement negation: !val + 1
        (!val).wrapping_add(U256::from(1))
    } else {
        val
    }
}
