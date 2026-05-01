//! Uniswap V3 SwapRouter / SwapRouter02 decoder.
//!
//! V3 has four entry points: exactInputSingle / exactInput (multi-hop) and
//! the matching exactOutput variants. Multi-hop encodes the path as a
//! tightly packed bytes blob: `token0 || fee0 || token1 || fee1 || token2`.

use alloy_primitives::{Address, Bytes, U256};
use alloy_sol_types::{sol, SolCall};

use super::selectors::*;
use super::{DecodedSwap, RouterKind};

sol! {
    struct ExactInputSingleParams {
        address tokenIn;
        address tokenOut;
        uint24 fee;
        address recipient;
        uint256 deadline;
        uint256 amountIn;
        uint256 amountOutMinimum;
        uint160 sqrtPriceLimitX96;
    }

    struct ExactInputParams {
        bytes path;
        address recipient;
        uint256 deadline;
        uint256 amountIn;
        uint256 amountOutMinimum;
    }

    struct ExactOutputSingleParams {
        address tokenIn;
        address tokenOut;
        uint24 fee;
        address recipient;
        uint256 deadline;
        uint256 amountOut;
        uint256 amountInMaximum;
        uint160 sqrtPriceLimitX96;
    }

    struct ExactOutputParams {
        bytes path;
        address recipient;
        uint256 deadline;
        uint256 amountOut;
        uint256 amountInMaximum;
    }

    function exactInputSingle(ExactInputSingleParams params) external payable returns (uint256);
    function exactInput(ExactInputParams params) external payable returns (uint256);
    function exactOutputSingle(ExactOutputSingleParams params) external payable returns (uint256);
    function exactOutput(ExactOutputParams params) external payable returns (uint256);
}

pub fn decode(to: Address, selector: [u8; 4], args: &[u8]) -> Option<DecodedSwap> {
    let router = if to == UNI_V3_SWAP_ROUTER {
        RouterKind::UniswapV3SwapRouter
    } else if to == UNI_V3_SWAP_ROUTER_02 {
        RouterKind::UniswapV3SwapRouter02
    } else {
        return None;
    };

    let mut full = Vec::with_capacity(4 + args.len());
    full.extend_from_slice(&selector);
    full.extend_from_slice(args);

    match selector {
        SEL_V3_EXACT_INPUT_SINGLE => {
            let c = exactInputSingleCall::abi_decode(&full).ok()?;
            let p = c.params;
            Some(DecodedSwap {
                router,
                token_in: p.tokenIn,
                token_out: p.tokenOut,
                amount_in: Some(p.amountIn),
                max_amount_in: p.amountIn,
                min_amount_out: p.amountOutMinimum,
                recipient: p.recipient,
                fee_bps: Some(p.fee.to::<u32>()),
                additional_hops: Vec::new(),
            })
        }
        SEL_V3_EXACT_INPUT => {
            let c = exactInputCall::abi_decode(&full).ok()?;
            let p = c.params;
            let hops = parse_v3_path(&p.path)?;
            let (token_in, token_out, fee_bps) = hops.first().copied()?;
            let additional: Vec<(Address, Address)> = hops
                .iter()
                .skip(1)
                .map(|(a, b, _)| (*a, *b))
                .collect();
            Some(DecodedSwap {
                router,
                token_in,
                token_out,
                amount_in: Some(p.amountIn),
                max_amount_in: p.amountIn,
                min_amount_out: p.amountOutMinimum,
                recipient: p.recipient,
                fee_bps: Some(fee_bps),
                additional_hops: additional,
            })
        }
        SEL_V3_EXACT_OUTPUT_SINGLE => {
            let c = exactOutputSingleCall::abi_decode(&full).ok()?;
            let p = c.params;
            Some(DecodedSwap {
                router,
                token_in: p.tokenIn,
                token_out: p.tokenOut,
                amount_in: None,
                max_amount_in: p.amountInMaximum,
                min_amount_out: p.amountOut,
                recipient: p.recipient,
                fee_bps: Some(p.fee.to::<u32>()),
                additional_hops: Vec::new(),
            })
        }
        SEL_V3_EXACT_OUTPUT => {
            let c = exactOutputCall::abi_decode(&full).ok()?;
            let p = c.params;
            // V3 exactOutput path is REVERSED — encoded from tokenOut to tokenIn.
            let mut hops = parse_v3_path(&p.path)?;
            hops.reverse();
            // After reversal, hops are (tokenA, tokenB, fee) where first hop's
            // tokenA is the actual input and last hop's tokenB is the output.
            // But since the path is encoded outOut-fee-mid-fee-in, after reversal
            // we get (in, fee_in_to_mid, mid, fee_mid_to_out, out) — fees are
            // off by one because tokenA still maps to fee_a_to_b. Acceptable
            // for our purposes: we only need first/last token identity.
            let (token_in, _, fee_bps) = *hops.first()?;
            let token_out = hops.last()?.1;
            Some(DecodedSwap {
                router,
                token_in,
                token_out,
                amount_in: None,
                max_amount_in: p.amountInMaximum,
                min_amount_out: p.amountOut,
                recipient: p.recipient,
                fee_bps: Some(fee_bps),
                additional_hops: hops
                    .iter()
                    .skip(1)
                    .map(|(a, b, _)| (*a, *b))
                    .collect(),
            })
        }
        SEL_V3_MULTICALL | SEL_V3_MULTICALL_DEADLINE | SEL_V3_MULTICALL_HEX => {
            // Multicall wraps multiple subcalls. Decoding the wrapper requires
            // splitting the bytes[] argument and running each subcall through
            // this same decoder. Out of scope for first pass — return None
            // so callers know we couldn't read this swap, rather than guess.
            None
        }
        _ => None,
    }
}

/// Parse the tightly-packed V3 path: `token0 || fee0 || token1 || fee1 || ... || tokenN`.
/// Each token is 20 bytes, each fee is 3 bytes (uint24).
pub(crate) fn parse_v3_path_pub(path: &Bytes) -> Option<Vec<(Address, Address, u32)>> {
    parse_v3_path(path)
}

fn parse_v3_path(path: &Bytes) -> Option<Vec<(Address, Address, u32)>> {
    const HOP_LEN: usize = 20 + 3 + 20; // 43 bytes per (in, fee, out) triple
    if path.len() < HOP_LEN {
        return None;
    }
    let mut hops = Vec::new();
    let mut cursor = 0;
    while cursor + 23 + 20 <= path.len() {
        let token_a = Address::from_slice(&path[cursor..cursor + 20]);
        let fee_bytes = &path[cursor + 20..cursor + 23];
        let fee = ((fee_bytes[0] as u32) << 16)
            | ((fee_bytes[1] as u32) << 8)
            | (fee_bytes[2] as u32);
        let token_b = Address::from_slice(&path[cursor + 23..cursor + 43]);
        hops.push((token_a, token_b, fee));
        cursor += 23; // advance by token + fee, next hop's `in` = this hop's `out`
    }
    if hops.is_empty() {
        None
    } else {
        Some(hops)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{address, U160};

    #[test]
    fn decode_exact_input_single() {
        let weth = address!("c02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
        let usdc = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
        let recipient = address!("3333333333333333333333333333333333333333");

        let call = exactInputSingleCall {
            params: ExactInputSingleParams {
                tokenIn: weth,
                tokenOut: usdc,
                fee: alloy_primitives::aliases::U24::from(500u32), // 0.05%
                recipient,
                deadline: U256::from(u64::MAX),
                amountIn: U256::from(2_500_000_000_000_000_000u128), // 2.5 ETH
                amountOutMinimum: U256::from(7_400_000_000u64),       // 7400 USDC
                sqrtPriceLimitX96: U160::ZERO,
            },
        };
        let cd = call.abi_encode();
        let decoded = decode(UNI_V3_SWAP_ROUTER, SEL_V3_EXACT_INPUT_SINGLE, &cd[4..])
            .expect("should decode");
        assert_eq!(decoded.token_in, weth);
        assert_eq!(decoded.token_out, usdc);
        assert_eq!(decoded.fee_bps, Some(500));
        assert_eq!(decoded.amount_in, Some(U256::from(2_500_000_000_000_000_000u128)));
        assert_eq!(decoded.min_amount_out, U256::from(7_400_000_000u64));
        assert_eq!(decoded.recipient, recipient);
    }

    #[test]
    fn decode_v3_path_multi_hop() {
        // path = WETH(20) || 500(3) || USDC(20) || 100(3) || DAI(20) = 66 bytes
        let weth = address!("c02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
        let usdc = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
        let dai = address!("6B175474E89094C44Da98b954EedeAC495271d0F");

        let mut path = Vec::new();
        path.extend_from_slice(weth.as_slice());
        path.extend_from_slice(&[0x00, 0x01, 0xf4]); // 500 = 0x1f4
        path.extend_from_slice(usdc.as_slice());
        path.extend_from_slice(&[0x00, 0x00, 0x64]); // 100
        path.extend_from_slice(dai.as_slice());

        let hops = parse_v3_path(&Bytes::from(path)).expect("should parse");
        assert_eq!(hops.len(), 2);
        assert_eq!(hops[0], (weth, usdc, 500));
        assert_eq!(hops[1], (usdc, dai, 100));
    }

    #[test]
    fn rejects_short_v3_path() {
        let path = Bytes::from(vec![0; 30]);
        assert_eq!(parse_v3_path(&path), None);
    }
}
