//! Universal Router decoder — handles the dominant swap commands only.
//!
//! Universal Router's `execute(commands, inputs[], deadline)` packs a
//! sequence of 1-byte command codes into `commands` and the matching
//! ABI-encoded args into `inputs[]`. We decode only the V2/V3 swap
//! commands; other ops (permits, transfers, sweeps) are skipped.
//!
//! This is good enough to surface most sandwich-able activity: the
//! Universal Router was introduced specifically as a wrapper for V2/V3
//! swaps, and aggregator routes don't typically use UR.

use alloy_primitives::{Address, Bytes, U256};
use alloy_sol_types::{sol, SolCall, SolValue};

use super::selectors::*;
use super::{DecodedSwap, RouterKind};

sol! {
    function execute(bytes commands, bytes[] inputs, uint256 deadline) external payable;
    function execute(bytes commands, bytes[] inputs) external payable;
}

pub fn decode(selector: [u8; 4], args: &[u8]) -> Option<DecodedSwap> {
    let mut full = Vec::with_capacity(4 + args.len());
    full.extend_from_slice(&selector);
    full.extend_from_slice(args);

    let (commands, inputs) = match selector {
        SEL_UR_EXECUTE => {
            let c = execute_0Call::abi_decode(&full).ok()?;
            (c.commands, c.inputs)
        }
        SEL_UR_EXECUTE_NO_DEADLINE => {
            let c = execute_1Call::abi_decode(&full).ok()?;
            (c.commands, c.inputs)
        }
        _ => return None,
    };

    // Find the FIRST swap command in the sequence; if there are multiple,
    // we'll only decode the first one (the relevant one for sandwich-side
    // analysis is usually the first swap).
    for (i, cmd_byte) in commands.iter().enumerate() {
        let cmd = cmd_byte & 0x3f; // strip flag bits
        let input = inputs.get(i)?;

        match cmd {
            UR_CMD_V2_SWAP_EXACT_IN => return decode_v2_exact_in(input),
            UR_CMD_V2_SWAP_EXACT_OUT => return decode_v2_exact_out(input),
            UR_CMD_V3_SWAP_EXACT_IN => return decode_v3_exact_in(input),
            UR_CMD_V3_SWAP_EXACT_OUT => return decode_v3_exact_out(input),
            _ => continue, // skip non-swap commands
        }
    }
    None
}

// V2 swap inputs: (recipient, amountIn, amountOutMin, path, payerIsUser)
fn decode_v2_exact_in(input: &Bytes) -> Option<DecodedSwap> {
    type Tuple = (Address, U256, U256, Vec<Address>, bool);
    let (recipient, amount_in, amount_out_min, path, _payer_is_user) =
        Tuple::abi_decode_params(input).ok()?;
    if path.len() < 2 {
        return None;
    }
    Some(DecodedSwap {
        router: RouterKind::UniversalRouter,
        token_in: path[0],
        token_out: path[1],
        amount_in: Some(amount_in),
        max_amount_in: amount_in,
        min_amount_out: amount_out_min,
        recipient,
        fee_bps: None,
        additional_hops: path
            .windows(2)
            .skip(1)
            .map(|w| (w[0], w[1]))
            .collect(),
    })
}

// V2 swap-exact-out inputs: (recipient, amountOut, amountInMax, path, payerIsUser)
fn decode_v2_exact_out(input: &Bytes) -> Option<DecodedSwap> {
    type Tuple = (Address, U256, U256, Vec<Address>, bool);
    let (recipient, amount_out, amount_in_max, path, _) =
        Tuple::abi_decode_params(input).ok()?;
    if path.len() < 2 {
        return None;
    }
    Some(DecodedSwap {
        router: RouterKind::UniversalRouter,
        token_in: path[0],
        token_out: path[1],
        amount_in: None,
        max_amount_in: amount_in_max,
        min_amount_out: amount_out,
        recipient,
        fee_bps: None,
        additional_hops: path
            .windows(2)
            .skip(1)
            .map(|w| (w[0], w[1]))
            .collect(),
    })
}

// V3 swap-exact-in inputs: (recipient, amountIn, amountOutMin, path, payerIsUser)
// The path is the V3 packed format (same as V3 router).
fn decode_v3_exact_in(input: &Bytes) -> Option<DecodedSwap> {
    type Tuple = (Address, U256, U256, Bytes, bool);
    let (recipient, amount_in, amount_out_min, path, _) =
        Tuple::abi_decode_params(input).ok()?;
    let hops = super::uniswap_v3::parse_v3_path_pub(&path)?;
    let (token_in, token_out, fee_bps) = hops.first().copied()?;
    Some(DecodedSwap {
        router: RouterKind::UniversalRouter,
        token_in,
        token_out,
        amount_in: Some(amount_in),
        max_amount_in: amount_in,
        min_amount_out: amount_out_min,
        recipient,
        fee_bps: Some(fee_bps),
        additional_hops: hops.iter().skip(1).map(|(a, b, _)| (*a, *b)).collect(),
    })
}

// V3 swap-exact-out: (recipient, amountOut, amountInMax, path, payerIsUser).
// V3 exactOut paths are reversed.
fn decode_v3_exact_out(input: &Bytes) -> Option<DecodedSwap> {
    type Tuple = (Address, U256, U256, Bytes, bool);
    let (recipient, amount_out, amount_in_max, path, _) =
        Tuple::abi_decode_params(input).ok()?;
    let mut hops = super::uniswap_v3::parse_v3_path_pub(&path)?;
    hops.reverse();
    let (token_in, _, fee_bps) = *hops.first()?;
    let token_out = hops.last()?.1;
    Some(DecodedSwap {
        router: RouterKind::UniversalRouter,
        token_in,
        token_out,
        amount_in: None,
        max_amount_in: amount_in_max,
        min_amount_out: amount_out,
        recipient,
        fee_bps: Some(fee_bps),
        additional_hops: hops.iter().skip(1).map(|(a, b, _)| (*a, *b)).collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::address;

    #[test]
    fn decode_ur_v2_exact_in() {
        let weth = address!("c02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
        let usdc = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
        let recipient = address!("4444444444444444444444444444444444444444");

        let v2_input = (
            recipient,
            U256::from(1_000_000_000_000_000_000u128), // 1 ETH
            U256::from(2_900_000_000u64),               // 2900 USDC
            vec![weth, usdc],
            true,
        );
        let encoded = v2_input.abi_encode_params();

        let commands = Bytes::from(vec![UR_CMD_V2_SWAP_EXACT_IN]);
        let inputs = vec![Bytes::from(encoded)];
        let call = execute_1Call { commands, inputs };
        let cd = call.abi_encode();

        let decoded = decode(SEL_UR_EXECUTE_NO_DEADLINE, &cd[4..]).expect("should decode");
        assert_eq!(decoded.router, RouterKind::UniversalRouter);
        assert_eq!(decoded.token_in, weth);
        assert_eq!(decoded.token_out, usdc);
        assert_eq!(
            decoded.amount_in,
            Some(U256::from(1_000_000_000_000_000_000u128))
        );
        assert_eq!(decoded.recipient, recipient);
    }

    #[test]
    fn skips_non_swap_commands() {
        // Universal Router with a single PERMIT2_PERMIT command (not a swap)
        let commands = Bytes::from(vec![UR_CMD_PERMIT2_PERMIT]);
        let inputs = vec![Bytes::from(vec![0u8; 32])];
        let call = execute_1Call { commands, inputs };
        let cd = call.abi_encode();
        assert_eq!(decode(SEL_UR_EXECUTE_NO_DEADLINE, &cd[4..]), None);
    }
}
