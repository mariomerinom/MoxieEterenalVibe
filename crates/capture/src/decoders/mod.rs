//! Strategy-agnostic calldata decoders for common swap routers.
//!
//! Used by mempool consumers (sandwich/JIT/oracle MEV) to extract swap
//! parameters from pending transactions. Recognizes the dominant routers
//! on Ethereum mainnet:
//!   * Uniswap V2 Router02
//!   * Uniswap V3 SwapRouter / SwapRouter02
//!   * Uniswap Universal Router (basic V2_SWAP_EXACT_IN command)
//!   * SushiSwap (same selectors as V2 — handled by V2 path)
//!
//! Not yet decoded: 1inch, 0x, CowSwap aggregators (their calldata is
//! solver-specific and harder to interpret without their order data).
//! These show up as `RouterKind::AggregatorOpaque` so callers can skip.
//!
//! Returns enough structure to:
//!   - identify victim swap direction (in/out mints)
//!   - compute slippage tolerance (amountOutMin)
//!   - measure swap size in token-of-origin units
//! Without re-implementing the full router contract logic.

use alloy_primitives::{Address, Bytes, U256};

mod selectors;
mod uniswap_v2;
mod uniswap_v3;
mod universal_router;

pub use selectors::*;

/// A recognized swap-bearing transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedSwap {
    pub router: RouterKind,
    /// Source token (sold by victim).
    pub token_in: Address,
    /// Destination token (bought by victim).
    pub token_out: Address,
    /// Input amount, when fixed. None for `exactOutput`-style calls (we'd
    /// have to invert through pool state to know the input — out of scope).
    pub amount_in: Option<U256>,
    /// Maximum input the victim is willing to pay (for exactOutput) or the
    /// fixed input (for exactInput). Always present so callers can size
    /// front-runs.
    pub max_amount_in: U256,
    /// Minimum output the victim accepts (slippage floor). Front-runner
    /// must keep the pool inside this tolerance or the victim tx reverts.
    pub min_amount_out: U256,
    /// Recipient of the output token (typically the victim themselves;
    /// some flows route to a contract).
    pub recipient: Address,
    /// V3 fee tier in 1e6 units (e.g., 500 = 0.05%, 3000 = 0.3%). None for V2.
    pub fee_bps: Option<u32>,
    /// Multi-hop path beyond the first hop, if any. The first hop is
    /// `(token_in, token_out)` — deeper hops are listed here in order.
    pub additional_hops: Vec<(Address, Address)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouterKind {
    UniswapV2Router02,
    SushiSwapRouter,
    UniswapV3SwapRouter,
    UniswapV3SwapRouter02,
    UniversalRouter,
    /// Unsupported router we recognize but don't decode (1inch, 0x, etc.).
    AggregatorOpaque,
}

/// Top-level entrypoint. Identifies the router by `to` address and
/// dispatches to the appropriate decoder. Returns None when:
///   - `to` is not a recognized router
///   - calldata is too short to decode
///   - selector is unknown to us (e.g., a router upgrade we haven't tracked)
pub fn decode_swap(to: Address, calldata: &Bytes) -> Option<DecodedSwap> {
    if calldata.len() < 4 {
        return None;
    }
    let selector: [u8; 4] = calldata[0..4].try_into().ok()?;

    match identify_router(to)? {
        RouterKind::UniswapV2Router02 | RouterKind::SushiSwapRouter => {
            uniswap_v2::decode(to, selector, &calldata[4..])
        }
        RouterKind::UniswapV3SwapRouter | RouterKind::UniswapV3SwapRouter02 => {
            uniswap_v3::decode(to, selector, &calldata[4..])
        }
        RouterKind::UniversalRouter => {
            universal_router::decode(selector, &calldata[4..])
        }
        RouterKind::AggregatorOpaque => Some(DecodedSwap {
            router: RouterKind::AggregatorOpaque,
            token_in: Address::ZERO,
            token_out: Address::ZERO,
            amount_in: None,
            max_amount_in: U256::ZERO,
            min_amount_out: U256::ZERO,
            recipient: Address::ZERO,
            fee_bps: None,
            additional_hops: Vec::new(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::address;

    #[test]
    fn rejects_short_calldata() {
        let r = address!("7a250d5630B4cF539739dF2C5dAcb4c659F2488D");
        assert_eq!(decode_swap(r, &Bytes::from_static(&[0x01, 0x02])), None);
    }

    #[test]
    fn rejects_unknown_router() {
        let unknown = address!("0000000000000000000000000000000000000000");
        let cd = Bytes::from_static(&[0x7f, 0xf3, 0x6a, 0xb5, 0, 0, 0, 0]);
        assert_eq!(decode_swap(unknown, &cd), None);
    }

    #[test]
    fn classifies_aggregators_opaque() {
        let oneinch = address!("1111111254EEB25477B68fb85Ed929f73A960582");
        let cd = Bytes::from_static(&[0x12, 0xaa, 0x3c, 0xaf, 0, 0, 0, 0]);
        let d = decode_swap(oneinch, &cd).expect("should classify");
        assert_eq!(d.router, RouterKind::AggregatorOpaque);
    }
}
