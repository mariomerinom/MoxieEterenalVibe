//! Uniswap V2 / SushiSwap router decoder.
//!
//! All V2 swap fns share the same shape: (amount_a, amount_b, path, to,
//! deadline). We just pick which of `amount_a`/`amount_b` is the input
//! vs output bound based on the selector.

use alloy_primitives::{Address, U256};
use alloy_sol_types::{sol, SolCall};

use super::selectors::*;
use super::{DecodedSwap, RouterKind};

sol! {
    function swapExactETHForTokens(uint256 amountOutMin, address[] path, address to, uint256 deadline)
        external payable returns (uint256[] memory amounts);
    function swapExactTokensForETH(uint256 amountIn, uint256 amountOutMin, address[] path, address to, uint256 deadline)
        external returns (uint256[] memory amounts);
    function swapExactTokensForTokens(uint256 amountIn, uint256 amountOutMin, address[] path, address to, uint256 deadline)
        external returns (uint256[] memory amounts);
    function swapETHForExactTokens(uint256 amountOut, address[] path, address to, uint256 deadline)
        external payable returns (uint256[] memory amounts);
    function swapTokensForExactETH(uint256 amountOut, uint256 amountInMax, address[] path, address to, uint256 deadline)
        external returns (uint256[] memory amounts);
    function swapTokensForExactTokens(uint256 amountOut, uint256 amountInMax, address[] path, address to, uint256 deadline)
        external returns (uint256[] memory amounts);
}

pub fn decode(to: Address, selector: [u8; 4], args: &[u8]) -> Option<DecodedSwap> {
    let router = match to {
        x if x == UNI_V2_ROUTER_02 => RouterKind::UniswapV2Router02,
        x if x == SUSHISWAP_ROUTER => RouterKind::SushiSwapRouter,
        _ => return None,
    };

    // Reconstruct full calldata (selector + args) for SolCall::abi_decode.
    let mut full = Vec::with_capacity(4 + args.len());
    full.extend_from_slice(&selector);
    full.extend_from_slice(args);

    match selector {
        SEL_V2_SWAP_EXACT_ETH_FOR_TOKENS | SEL_V2_SWAP_EXACT_ETH_FOR_TOKENS_FEE => {
            let c = swapExactETHForTokensCall::abi_decode(&full).ok()?;
            from_path(
                router,
                None,                     // amount_in not in calldata (it's tx.value)
                U256::ZERO,                // max_amount_in: also tx.value
                c.amountOutMin,
                c.to,
                &c.path,
            )
        }
        SEL_V2_SWAP_EXACT_TOKENS_FOR_ETH | SEL_V2_SWAP_EXACT_TOKENS_FOR_ETH_FEE => {
            let c = swapExactTokensForETHCall::abi_decode(&full).ok()?;
            from_path(router, Some(c.amountIn), c.amountIn, c.amountOutMin, c.to, &c.path)
        }
        SEL_V2_SWAP_EXACT_TOKENS_FOR_TOKENS | SEL_V2_SWAP_EXACT_TOKENS_FOR_TOKENS_FEE => {
            let c = swapExactTokensForTokensCall::abi_decode(&full).ok()?;
            from_path(router, Some(c.amountIn), c.amountIn, c.amountOutMin, c.to, &c.path)
        }
        SEL_V2_SWAP_ETH_FOR_EXACT_TOKENS => {
            let c = swapETHForExactTokensCall::abi_decode(&full).ok()?;
            // exactOutput: input is tx.value (unknown from calldata)
            from_path(router, None, U256::ZERO, c.amountOut, c.to, &c.path)
        }
        SEL_V2_SWAP_TOKENS_FOR_EXACT_ETH => {
            let c = swapTokensForExactETHCall::abi_decode(&full).ok()?;
            from_path(router, None, c.amountInMax, c.amountOut, c.to, &c.path)
        }
        SEL_V2_SWAP_TOKENS_FOR_EXACT_TOKENS => {
            let c = swapTokensForExactTokensCall::abi_decode(&full).ok()?;
            from_path(router, None, c.amountInMax, c.amountOut, c.to, &c.path)
        }
        _ => None,
    }
}

fn from_path(
    router: RouterKind,
    amount_in: Option<U256>,
    max_amount_in: U256,
    min_amount_out: U256,
    recipient: Address,
    path: &[Address],
) -> Option<DecodedSwap> {
    if path.len() < 2 {
        return None;
    }
    let token_in = path[0];
    let token_out = path[1];
    let additional_hops: Vec<(Address, Address)> = path
        .windows(2)
        .skip(1)
        .map(|w| (w[0], w[1]))
        .collect();

    Some(DecodedSwap {
        router,
        token_in,
        token_out,
        amount_in,
        max_amount_in,
        min_amount_out,
        recipient,
        fee_bps: None,
        additional_hops,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{address, hex};

    /// Real swap: swapExactETHForTokens (router 0x7a25, victim sends ETH to buy USDC)
    /// 1 ETH in, 3000 USDC min out, path = [WETH, USDC], deadline far future.
    #[test]
    fn decode_swap_exact_eth_for_tokens() {
        // Hand-rolled calldata: selector + amountOutMin + path_offset + to + deadline + path_len + path[0] + path[1]
        // amountOutMin = 3000 * 1e6
        let weth = address!("c02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
        let usdc = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
        let recipient = address!("1111111111111111111111111111111111111111");

        let call = swapExactETHForTokensCall {
            amountOutMin: U256::from(3_000_000_000u64),
            path: vec![weth, usdc],
            to: recipient,
            deadline: U256::from(u64::MAX),
        };
        let cd = call.abi_encode();
        assert_eq!(cd[..4], SEL_V2_SWAP_EXACT_ETH_FOR_TOKENS);

        let decoded = decode(UNI_V2_ROUTER_02, SEL_V2_SWAP_EXACT_ETH_FOR_TOKENS, &cd[4..])
            .expect("should decode");
        assert_eq!(decoded.router, RouterKind::UniswapV2Router02);
        assert_eq!(decoded.token_in, weth);
        assert_eq!(decoded.token_out, usdc);
        assert_eq!(decoded.amount_in, None); // exactETH: input is tx.value
        assert_eq!(decoded.min_amount_out, U256::from(3_000_000_000u64));
        assert_eq!(decoded.recipient, recipient);
        assert!(decoded.additional_hops.is_empty());

        // Sanity: hex check selector
        assert_eq!(hex::encode(&cd[..4]), "7ff36ab5");
    }

    #[test]
    fn decode_swap_exact_tokens_for_tokens() {
        let usdc = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
        let weth = address!("c02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
        let dai = address!("6B175474E89094C44Da98b954EedeAC495271d0F");
        let recipient = address!("2222222222222222222222222222222222222222");

        let call = swapExactTokensForTokensCall {
            amountIn: U256::from(5_000_000_000u64), // 5000 USDC
            amountOutMin: U256::from(2_900_000_000u64),
            path: vec![usdc, weth, dai], // multi-hop
            to: recipient,
            deadline: U256::from(u64::MAX),
        };
        let cd = call.abi_encode();
        let decoded = decode(UNI_V2_ROUTER_02, SEL_V2_SWAP_EXACT_TOKENS_FOR_TOKENS, &cd[4..])
            .expect("should decode");
        assert_eq!(decoded.token_in, usdc);
        assert_eq!(decoded.token_out, weth); // first hop
        assert_eq!(decoded.amount_in, Some(U256::from(5_000_000_000u64)));
        assert_eq!(decoded.additional_hops.len(), 1);
        assert_eq!(decoded.additional_hops[0], (weth, dai));
    }
}
