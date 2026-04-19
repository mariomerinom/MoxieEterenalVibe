//! Solidity ABI bindings for MevBot contract.
//! Uses alloy sol! macro to generate type-safe calldata encoders.

use alloy_primitives::{Address, I256, U256};
use alloy_sol_types::sol;

// Generate Rust types from the MevBot Solidity interface.
sol! {
    /// MevBot contract ABI — sandwich + arb execution.
    #[derive(Debug)]
    interface IMevBot {
        // V2 sandwich leg
        function swapV2(
            address pair,
            address tokenIn,
            uint256 amountIn,
            uint256 amountOutMin,
            bool zeroForOne
        ) external;

        // V3 sandwich leg
        function swapV3(
            address pool,
            bool zeroForOne,
            int256 amountIn
        ) external;

        // Multi-hop arb
        function executeArb(
            bytes calldata hops,
            address tokenIn,
            uint256 amountIn,
            uint256 minProfit
        ) external;

        // Builder tip
        function payBuilder(uint256 amount) external;
        function payBuilderPercent(uint256 profitWei, uint256 bribePercent) external;

        // WETH helpers
        function wrapETH(uint256 amount) external;
        function unwrapWETH(uint256 amount) external;

        // Admin
        function withdrawETH() external;
        function withdrawToken(address token) external;
        function approveToken(address token, address spender, uint256 amount) external;
    }
}

/// Encode a V2 swap calldata.
pub fn encode_swap_v2(
    pair: Address,
    token_in: Address,
    amount_in: U256,
    amount_out_min: U256,
    zero_for_one: bool,
) -> Vec<u8> {
    let call = IMevBot::swapV2Call {
        pair,
        tokenIn: token_in,
        amountIn: amount_in,
        amountOutMin: amount_out_min,
        zeroForOne: zero_for_one,
    };
    <IMevBot::swapV2Call as alloy_sol_types::SolCall>::abi_encode(&call)
}

/// Encode a V3 swap calldata.
pub fn encode_swap_v3(pool: Address, zero_for_one: bool, amount_in: I256) -> Vec<u8> {
    let call = IMevBot::swapV3Call {
        pool,
        zeroForOne: zero_for_one,
        amountIn: amount_in,
    };
    <IMevBot::swapV3Call as alloy_sol_types::SolCall>::abi_encode(&call)
}

/// Encode a multi-hop arb calldata.
///
/// Each hop is encoded as: 20 bytes (pool address) + 1 byte (isV3) + 1 byte (zeroForOne).
pub fn encode_arb(
    hops: &[(Address, bool, bool)], // (pool, is_v3, zero_for_one)
    token_in: Address,
    amount_in: U256,
    min_profit: U256,
) -> Vec<u8> {
    let mut hop_bytes = Vec::with_capacity(hops.len() * 22);
    for (pool, is_v3, zero_for_one) in hops {
        hop_bytes.extend_from_slice(pool.as_slice());
        hop_bytes.push(if *is_v3 { 1 } else { 0 });
        hop_bytes.push(if *zero_for_one { 1 } else { 0 });
    }

    let call = IMevBot::executeArbCall {
        hops: hop_bytes.into(),
        tokenIn: token_in,
        amountIn: amount_in,
        minProfit: min_profit,
    };
    <IMevBot::executeArbCall as alloy_sol_types::SolCall>::abi_encode(&call)
}

/// Encode payBuilder calldata.
pub fn encode_pay_builder(amount: U256) -> Vec<u8> {
    let call = IMevBot::payBuilderCall { amount };
    <IMevBot::payBuilderCall as alloy_sol_types::SolCall>::abi_encode(&call)
}

/// Encode payBuilderPercent calldata.
pub fn encode_pay_builder_percent(profit_wei: U256, bribe_percent: U256) -> Vec<u8> {
    let call = IMevBot::payBuilderPercentCall {
        profitWei: profit_wei,
        bribePercent: bribe_percent,
    };
    <IMevBot::payBuilderPercentCall as alloy_sol_types::SolCall>::abi_encode(&call)
}

/// Encode wrapETH calldata.
pub fn encode_wrap_eth(amount: U256) -> Vec<u8> {
    let call = IMevBot::wrapETHCall { amount };
    <IMevBot::wrapETHCall as alloy_sol_types::SolCall>::abi_encode(&call)
}

/// Encode approveToken calldata.
pub fn encode_approve_token(token: Address, spender: Address, amount: U256) -> Vec<u8> {
    let call = IMevBot::approveTokenCall {
        token,
        spender,
        amount,
    };
    <IMevBot::approveTokenCall as alloy_sol_types::SolCall>::abi_encode(&call)
}
