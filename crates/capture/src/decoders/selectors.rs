//! Router address registry and selector identification.

use alloy_primitives::{address, Address};

use super::RouterKind;

// ── Router addresses (Ethereum mainnet) ────────────────────────────────────

pub const UNI_V2_ROUTER_02: Address = address!("7a250d5630B4cF539739dF2C5dAcb4c659F2488D");
pub const UNI_V3_SWAP_ROUTER: Address = address!("E592427A0AEce92De3Edee1F18E0157C05861564");
pub const UNI_V3_SWAP_ROUTER_02: Address = address!("68b3465833fb72A70ecDF485E0e4C7bD8665Fc45");
pub const UNI_UNIVERSAL_ROUTER_OLD: Address =
    address!("Ef1c6E67703c7BD7107eed8303Fbe6EC2554BF6B");
pub const UNI_UNIVERSAL_ROUTER: Address =
    address!("3fC91A3afd70395Cd496C647d5a6CC9D4B2b7FAD");
pub const SUSHISWAP_ROUTER: Address = address!("d9e1cE17f2641f24aE83637ab66a2cca9C378B9F");
pub const ONE_INCH_V5: Address = address!("1111111254EEB25477B68fb85Ed929f73A960582");
pub const ONE_INCH_V6: Address = address!("111111125421cA6dc452d289314280a0f8842A65");
pub const ZRX_PROXY: Address = address!("Def1C0ded9bec7F1a1670819833240f027b25EfF");
pub const COW_PROTOCOL: Address = address!("9008D19f58AAbD9eD0D60971565AA8510560ab41");

/// Map a `to` address to its router kind.
pub fn identify_router(to: Address) -> Option<RouterKind> {
    if to == UNI_V2_ROUTER_02 {
        Some(RouterKind::UniswapV2Router02)
    } else if to == SUSHISWAP_ROUTER {
        Some(RouterKind::SushiSwapRouter)
    } else if to == UNI_V3_SWAP_ROUTER {
        Some(RouterKind::UniswapV3SwapRouter)
    } else if to == UNI_V3_SWAP_ROUTER_02 {
        Some(RouterKind::UniswapV3SwapRouter02)
    } else if to == UNI_UNIVERSAL_ROUTER || to == UNI_UNIVERSAL_ROUTER_OLD {
        Some(RouterKind::UniversalRouter)
    } else if to == ONE_INCH_V5
        || to == ONE_INCH_V6
        || to == ZRX_PROXY
        || to == COW_PROTOCOL
    {
        Some(RouterKind::AggregatorOpaque)
    } else {
        None
    }
}

// ── Selector constants (first 4 bytes of keccak256("name(args)")) ──────────

// Uniswap V2 — supportingFeeOnTransfer variants behave the same for our purposes.
pub const SEL_V2_SWAP_EXACT_ETH_FOR_TOKENS: [u8; 4] = [0x7f, 0xf3, 0x6a, 0xb5];
pub const SEL_V2_SWAP_EXACT_TOKENS_FOR_ETH: [u8; 4] = [0x18, 0xcb, 0xaf, 0xe5];
pub const SEL_V2_SWAP_EXACT_TOKENS_FOR_TOKENS: [u8; 4] = [0x38, 0xed, 0x17, 0x39];
pub const SEL_V2_SWAP_ETH_FOR_EXACT_TOKENS: [u8; 4] = [0xfb, 0x3b, 0xdb, 0x41];
pub const SEL_V2_SWAP_TOKENS_FOR_EXACT_ETH: [u8; 4] = [0x4a, 0x25, 0xd9, 0x4a];
pub const SEL_V2_SWAP_TOKENS_FOR_EXACT_TOKENS: [u8; 4] = [0x88, 0x03, 0xdb, 0xee];
pub const SEL_V2_SWAP_EXACT_TOKENS_FOR_TOKENS_FEE: [u8; 4] = [0x5c, 0x11, 0xd7, 0x95];
pub const SEL_V2_SWAP_EXACT_ETH_FOR_TOKENS_FEE: [u8; 4] = [0xb6, 0xf9, 0xde, 0x95];
pub const SEL_V2_SWAP_EXACT_TOKENS_FOR_ETH_FEE: [u8; 4] = [0x79, 0x1a, 0xc9, 0x47];

// Uniswap V3
pub const SEL_V3_EXACT_INPUT_SINGLE: [u8; 4] = [0x41, 0x4b, 0xf3, 0x89];
pub const SEL_V3_EXACT_INPUT: [u8; 4] = [0xc0, 0x4b, 0x8d, 0x59];
pub const SEL_V3_EXACT_OUTPUT_SINGLE: [u8; 4] = [0xdb, 0x3e, 0x21, 0x98];
pub const SEL_V3_EXACT_OUTPUT: [u8; 4] = [0xf2, 0x8c, 0x04, 0x98];
pub const SEL_V3_MULTICALL: [u8; 4] = [0xac, 0x96, 0x50, 0xd8];
pub const SEL_V3_MULTICALL_DEADLINE: [u8; 4] = [0x5a, 0xe4, 0x01, 0xdc];
pub const SEL_V3_MULTICALL_HEX: [u8; 4] = [0x1f, 0x04, 0x64, 0xd1];

// Universal Router
pub const SEL_UR_EXECUTE: [u8; 4] = [0x35, 0x93, 0x56, 0x4c];
pub const SEL_UR_EXECUTE_NO_DEADLINE: [u8; 4] = [0x24, 0x85, 0x6b, 0xc3];

// Universal Router commands (1 byte each, used inside execute() command bytes)
pub const UR_CMD_V3_SWAP_EXACT_IN: u8 = 0x00;
pub const UR_CMD_V3_SWAP_EXACT_OUT: u8 = 0x01;
pub const UR_CMD_PERMIT2_TRANSFER_FROM: u8 = 0x02;
pub const UR_CMD_PERMIT2_PERMIT_BATCH: u8 = 0x03;
pub const UR_CMD_SWEEP: u8 = 0x04;
pub const UR_CMD_TRANSFER: u8 = 0x05;
pub const UR_CMD_PAY_PORTION: u8 = 0x06;
pub const UR_CMD_V2_SWAP_EXACT_IN: u8 = 0x08;
pub const UR_CMD_V2_SWAP_EXACT_OUT: u8 = 0x09;
pub const UR_CMD_PERMIT2_PERMIT: u8 = 0x0a;
pub const UR_CMD_WRAP_ETH: u8 = 0x0b;
pub const UR_CMD_UNWRAP_WETH: u8 = 0x0c;
