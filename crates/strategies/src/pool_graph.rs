//! Pool universe loading and arb cycle detection.
//!
//! Loads resolved pool token pairs from JSON and precomputes 2-hop arb
//! cycles through WETH (or any base token). Each cycle represents a
//! potential arbitrage path: base -> intermediate -> base via two pools.

use alloy_primitives::Address;
use eyre::Result;
use mev_capture::types::DexProtocol;
use serde::Deserialize;
use std::collections::HashMap;
use std::str::FromStr;

/// WETH address on Ethereum mainnet.
pub const WETH: &str = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2";

/// WETH address on Base.
pub const WETH_BASE: &str = "0x4200000000000000000000000000000000000006";

/// WETH address on Arbitrum.
pub const WETH_ARBITRUM: &str = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1";

/// Get the WETH address for a given chain.
pub fn weth_for_chain(chain: mev_capture::types::Chain) -> &'static str {
    match chain {
        mev_capture::types::Chain::Base => WETH_BASE,
        mev_capture::types::Chain::Arbitrum => WETH_ARBITRUM,
        _ => WETH,
    }
}

/// A resolved pool with its token pair and protocol.
#[derive(Debug, Clone)]
pub struct PoolInfo {
    pub pool: Address,
    pub protocol: DexProtocol,
    pub token0: Address,
    pub token1: Address,
    pub decimals0: u8,
    pub decimals1: u8,
    pub symbol0: String,
    pub symbol1: String,
    pub fee: Option<u32>,
}

impl PoolInfo {
    /// True if this pool is a V3 pool.
    pub fn is_v3(&self) -> bool {
        matches!(self.protocol, DexProtocol::UniswapV3)
    }

    /// True if this pool is from a Solidly-fork DEX (Aerodrome, Velodrome, Camelot, etc.)
    /// that uses non-standard swap math (stableswap invariant for correlated pairs,
    /// different fee structure). These pools CANNOT use constant-product x*y=k math.
    pub fn is_solidly_fork(&self) -> bool {
        matches!(self.protocol, DexProtocol::Aerodrome | DexProtocol::Camelot)
    }

    /// True if `token` is token0.
    pub fn has_token0(&self, token: Address) -> bool {
        self.token0 == token
    }

    /// The other token given one side.
    pub fn other_token(&self, token: Address) -> Address {
        if self.token0 == token {
            self.token1
        } else {
            self.token0
        }
    }

    /// Whether a swap of `token` into this pool is zero_for_one.
    pub fn zero_for_one(&self, token_in: Address) -> bool {
        self.token0 == token_in
    }

    /// Symbol of the other token given one side.
    pub fn other_symbol(&self, token: Address) -> &str {
        if self.token0 == token {
            &self.symbol1
        } else {
            &self.symbol0
        }
    }
}

/// A precomputed arb cycle: base -> pool1 -> ... -> poolN -> base.
///
/// Supports 2-hop (base → X → base) and 3-hop (base → X → Y → base).
#[derive(Debug, Clone)]
pub struct ArbCycle {
    /// (pool_address, is_v3, zero_for_one) for each hop.
    pub hops: Vec<(Address, bool, bool)>,
    /// Starting/ending token (usually WETH).
    pub base_token: Address,
    /// First intermediate token.
    pub intermediate_token: Address,
    /// Second intermediate token (only for 3-hop cycles).
    pub intermediate_token2: Option<Address>,
    /// Pool info for the first hop.
    pub pool1: PoolInfo,
    /// Pool info for the second hop.
    pub pool2: PoolInfo,
    /// Pool info for the third hop (only for 3-hop cycles).
    pub pool3: Option<PoolInfo>,
    /// Human-readable label.
    pub label: String,
}

/// JSON format from pool_tokens.json — legacy flat format (Ethereum).
#[derive(Debug, Deserialize)]
struct PoolTokensEntryLegacy {
    token0: String,
    token1: String,
    symbol0: Option<String>,
    symbol1: Option<String>,
    decimals0: Option<u8>,
    decimals1: Option<u8>,
    swaps: Option<u64>,
    protocol: Option<String>,
    fee: Option<u32>,
}

/// JSON format from pool_tokens_*.json — new structured format (multichain).
#[derive(Debug, Deserialize)]
struct PoolTokensFileNew {
    chain: Option<String>,
    weth: Option<String>,
    pools: Vec<PoolEntryNew>,
    tokens: Option<HashMap<String, TokenMeta>>,
}

#[derive(Debug, Deserialize)]
struct PoolEntryNew {
    address: String,
    token0: String,
    token1: String,
    protocol: Option<String>,
    fee: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct TokenMeta {
    symbol: Option<String>,
    decimals: Option<u8>,
    #[allow(dead_code)]
    address: Option<String>,
}

fn protocol_from_str(s: Option<&str>) -> DexProtocol {
    match s {
        Some("uniswapv2") | Some("sushiswap") => DexProtocol::UniswapV2,
        Some("uniswapv3") => DexProtocol::UniswapV3,
        Some("camelot") => DexProtocol::Camelot,
        Some("aerodrome") | Some("velodrome") => DexProtocol::Aerodrome,
        _ => DexProtocol::UniswapV2, // default assumption
    }
}

/// Default V2 fee in basis points for a given protocol string.
/// Returns None for V3 (fee comes from pool data).
fn default_v2_fee_bps(protocol: Option<&str>) -> Option<u32> {
    match protocol {
        Some("camelot") => Some(16),       // Camelot default 0.16%
        Some("aerodrome") => Some(30),     // Aerodrome varies, default 0.30%
        Some("uniswapv2") | Some("sushiswap") => Some(30), // 0.30%
        _ => Some(30),
    }
}

/// Load pool universe from a pool_tokens JSON file.
///
/// Supports two formats:
/// - Legacy flat: `{ "0xpool": { token0, token1, ... }, ... }`
/// - New structured: `{ chain, weth, pools: [{ address, token0, token1, ... }], tokens: { ... } }`
pub fn load_pool_universe(path: &str) -> Result<Vec<PoolInfo>> {
    let data = std::fs::read_to_string(path)?;

    // Try new format first (has "pools" key)
    if let Ok(file) = serde_json::from_str::<PoolTokensFileNew>(&data) {
        let token_meta = file.tokens.unwrap_or_default();
        let weth_lower = file.weth.as_deref().unwrap_or("").to_lowercase();
        let mut pools = Vec::new();

        for entry in &file.pools {
            let protocol = protocol_from_str(entry.protocol.as_deref());
            let pool = Address::from_str(&entry.address)?;
            let token0 = Address::from_str(&entry.token0)?;
            let token1 = Address::from_str(&entry.token1)?;

            // Look up token metadata
            let t0_lower = entry.token0.to_lowercase();
            let t1_lower = entry.token1.to_lowercase();

            let (sym0, dec0) = if t0_lower == weth_lower {
                ("WETH".to_string(), 18)
            } else {
                lookup_meta(&token_meta, &entry.token0)
            };
            let (sym1, dec1) = if t1_lower == weth_lower {
                ("WETH".to_string(), 18)
            } else {
                lookup_meta(&token_meta, &entry.token1)
            };

            // For V2 pools, use protocol-specific fee if pool doesn't specify one
            let fee = entry.fee.or_else(|| {
                if protocol == DexProtocol::UniswapV2 {
                    default_v2_fee_bps(entry.protocol.as_deref())
                } else {
                    None
                }
            });

            pools.push(PoolInfo {
                pool,
                protocol,
                token0,
                token1,
                decimals0: dec0,
                decimals1: dec1,
                symbol0: sym0,
                symbol1: sym1,
                fee,
            });
        }

        tracing::info!(
            pools = pools.len(),
            chain = file.chain.as_deref().unwrap_or("unknown"),
            "loaded pool universe (new format)"
        );
        return Ok(pools);
    }

    // Fall back to legacy flat format
    let raw: HashMap<String, PoolTokensEntryLegacy> = serde_json::from_str(&data)?;
    let mut pools = Vec::new();
    for (pool_addr, entry) in &raw {
        let protocol = protocol_from_str(entry.protocol.as_deref());
        let pool = Address::from_str(pool_addr)?;
        let token0 = Address::from_str(&entry.token0)?;
        let token1 = Address::from_str(&entry.token1)?;

        pools.push(PoolInfo {
            pool,
            protocol,
            token0,
            token1,
            decimals0: entry.decimals0.unwrap_or(18),
            decimals1: entry.decimals1.unwrap_or(18),
            symbol0: entry.symbol0.clone().unwrap_or_default(),
            symbol1: entry.symbol1.clone().unwrap_or_default(),
            fee: entry.fee,
        });
    }

    tracing::info!(pools = pools.len(), "loaded pool universe (legacy format)");
    Ok(pools)
}

fn lookup_meta(meta: &HashMap<String, TokenMeta>, addr: &str) -> (String, u8) {
    // Try exact match first
    if let Some(m) = meta.get(addr) {
        return (
            m.symbol.clone().unwrap_or_else(|| "UNKNOWN".to_string()),
            m.decimals.unwrap_or(18),
        );
    }
    // Try lowercase match
    let lower = addr.to_lowercase();
    if let Some(m) = meta.get(&lower) {
        return (
            m.symbol.clone().unwrap_or_else(|| "UNKNOWN".to_string()),
            m.decimals.unwrap_or(18),
        );
    }
    // Try EIP-55 checksummed variant
    if let Ok(parsed) = Address::from_str(addr) {
        let checksummed = format!("{parsed:#?}"); // Debug format gives 0x... checksum
        // alloy Address Display is checksummed; try to_checksum
        let cs_str = format!("{parsed}");
        if let Some(m) = meta.get(&cs_str) {
            return (
                m.symbol.clone().unwrap_or_else(|| "UNKNOWN".to_string()),
                m.decimals.unwrap_or(18),
            );
        }
        // Also try with 0x prefix lowercase (alloy default)
        let lc_str = format!("{parsed:#x}");
        if &lc_str != &lower {
            if let Some(m) = meta.get(&lc_str) {
                return (
                    m.symbol.clone().unwrap_or_else(|| "UNKNOWN".to_string()),
                    m.decimals.unwrap_or(18),
                );
            }
        }
        // Brute force: iterate keys case-insensitively (only for small metadata maps)
        let _ = checksummed; // suppress unused warning
        for (key, m) in meta.iter() {
            if key.to_lowercase() == lower {
                return (
                    m.symbol.clone().unwrap_or_else(|| "UNKNOWN".to_string()),
                    m.decimals.unwrap_or(18),
                );
            }
        }
    }
    ("UNKNOWN".to_string(), 18)
}

/// Find all 2-hop arb cycles through a base token (typically WETH).
///
/// A 2-hop cycle: base -> intermediate via pool1, intermediate -> base via pool2.
/// Both pools must contain the base token on one side and share the same
/// intermediate token on the other.
pub fn find_arb_cycles(pools: &[PoolInfo], base_token: Address) -> Vec<ArbCycle> {
    // Index: intermediate_token -> Vec<(pool_index, zero_for_one_from_base)>
    // For each pool containing `base_token`, record which intermediate token
    // is on the other side.
    let mut by_intermediate: HashMap<Address, Vec<usize>> = HashMap::new();

    for (i, pool) in pools.iter().enumerate() {
        if pool.token0 == base_token || pool.token1 == base_token {
            let intermediate = pool.other_token(base_token);
            by_intermediate.entry(intermediate).or_default().push(i);
        }
    }

    let mut cycles = Vec::new();

    // For each intermediate token with 2+ pools connecting to base,
    // generate all pairs as arb cycles.
    for (intermediate, pool_indices) in &by_intermediate {
        if pool_indices.len() < 2 {
            continue;
        }

        for i in 0..pool_indices.len() {
            for j in (i + 1)..pool_indices.len() {
                let p1 = &pools[pool_indices[i]];
                let p2 = &pools[pool_indices[j]];

                // Direction 1: base -> p1 -> intermediate -> p2 -> base
                let hop1_zfo = p1.zero_for_one(base_token);
                let hop2_zfo = p2.zero_for_one(*intermediate);

                let label = format!(
                    "WETH->{} via {:?} -> {}->WETH via {:?}",
                    p1.symbol0.clone() + "/" + &p1.symbol1,
                    if p1.is_v3() { "V3" } else { "V2" },
                    p2.symbol0.clone() + "/" + &p2.symbol1,
                    if p2.is_v3() { "V3" } else { "V2" },
                );

                cycles.push(ArbCycle {
                    hops: vec![
                        (p1.pool, p1.is_v3(), hop1_zfo),
                        (p2.pool, p2.is_v3(), hop2_zfo),
                    ],
                    base_token,
                    intermediate_token: *intermediate,
                    intermediate_token2: None,
                    pool1: p1.clone(),
                    pool2: p2.clone(),
                    pool3: None,
                    label,
                });

                // Direction 2: base -> p2 -> intermediate -> p1 -> base (reverse)
                let hop1_zfo_rev = p2.zero_for_one(base_token);
                let hop2_zfo_rev = p1.zero_for_one(*intermediate);

                let label_rev = format!(
                    "WETH->{} via {:?} -> {}->WETH via {:?} (rev)",
                    p2.symbol0.clone() + "/" + &p2.symbol1,
                    if p2.is_v3() { "V3" } else { "V2" },
                    p1.symbol0.clone() + "/" + &p1.symbol1,
                    if p1.is_v3() { "V3" } else { "V2" },
                );

                cycles.push(ArbCycle {
                    hops: vec![
                        (p2.pool, p2.is_v3(), hop1_zfo_rev),
                        (p1.pool, p1.is_v3(), hop2_zfo_rev),
                    ],
                    base_token,
                    intermediate_token: *intermediate,
                    intermediate_token2: None,
                    pool1: p2.clone(),
                    pool2: p1.clone(),
                    pool3: None,
                    label: label_rev,
                });
            }
        }
    }

    tracing::info!(
        cycles = cycles.len(),
        intermediates = by_intermediate.len(),
        "found arb cycles through base token"
    );
    cycles
}

/// Find 3-hop arb cycles: base → X → Y → base.
///
/// Pool 1 connects base↔X, pool 2 connects X↔Y, pool 3 connects Y↔base.
/// Cap total cycles to avoid blowing up scan time.
pub fn find_arb_cycles_3hop(
    pools: &[PoolInfo],
    base_token: Address,
    max_cycles: usize,
) -> Vec<ArbCycle> {
    // Index pools by each token they contain
    let mut by_token: HashMap<Address, Vec<usize>> = HashMap::new();
    for (i, pool) in pools.iter().enumerate() {
        by_token.entry(pool.token0).or_default().push(i);
        by_token.entry(pool.token1).or_default().push(i);
    }

    // Pools containing base token (for hops 1 and 3)
    let base_pools: Vec<usize> = by_token.get(&base_token).cloned().unwrap_or_default();

    // For each base pool, find the intermediate token X
    // Then for each pool containing X (but not base), find token Y
    // Then check if Y has a pool back to base
    let mut cycles = Vec::new();

    for &pi1 in &base_pools {
        let p1 = &pools[pi1];
        let token_x = p1.other_token(base_token);

        // Pools containing X (for hop 2: X→Y)
        let x_pools = match by_token.get(&token_x) {
            Some(v) => v,
            None => continue,
        };

        for &pi2 in x_pools {
            if pi2 == pi1 {
                continue;
            }
            let p2 = &pools[pi2];
            let token_y = p2.other_token(token_x);

            // Skip if Y == base (that's a 2-hop, already covered)
            if token_y == base_token {
                continue;
            }
            // Skip if Y == X (degenerate)
            if token_y == token_x {
                continue;
            }

            // Pools connecting Y back to base (hop 3)
            let y_pools = match by_token.get(&token_y) {
                Some(v) => v,
                None => continue,
            };

            for &pi3 in y_pools {
                if pi3 == pi1 || pi3 == pi2 {
                    continue;
                }
                let p3 = &pools[pi3];
                // p3 must connect Y ↔ base
                if p3.other_token(token_y) != base_token {
                    continue;
                }

                let hop1_zfo = p1.zero_for_one(base_token);
                let hop2_zfo = p2.zero_for_one(token_x);
                let hop3_zfo = p3.zero_for_one(token_y);

                let label = format!(
                    "WETH->{}->{}->{} [{}/{}/{}]",
                    p1.other_symbol(base_token),
                    p2.other_symbol(token_x),
                    "WETH",
                    if p1.is_v3() { "V3" } else { "V2" },
                    if p2.is_v3() { "V3" } else { "V2" },
                    if p3.is_v3() { "V3" } else { "V2" },
                );

                cycles.push(ArbCycle {
                    hops: vec![
                        (p1.pool, p1.is_v3(), hop1_zfo),
                        (p2.pool, p2.is_v3(), hop2_zfo),
                        (p3.pool, p3.is_v3(), hop3_zfo),
                    ],
                    base_token,
                    intermediate_token: token_x,
                    intermediate_token2: Some(token_y),
                    pool1: p1.clone(),
                    pool2: p2.clone(),
                    pool3: Some(p3.clone()),
                    label,
                });

                if cycles.len() >= max_cycles {
                    tracing::info!(
                        cycles = cycles.len(),
                        "3-hop cycle limit reached"
                    );
                    return cycles;
                }
            }
        }
    }

    tracing::info!(
        cycles = cycles.len(),
        "found 3-hop arb cycles through base token"
    );
    cycles
}

/// Compute optimal input amount for a 2-hop V2-V2 arb.
///
/// For pools with reserves (rA1, rB1) and (rB2, rA2):
///   output(x) = (y * 997 * rA2) / (rB2 * 1000 + y * 997)
///   where y = (x * 997 * rB1) / (rA1 * 1000 + x * 997)
///
/// Optimal x = sqrt(rA1 * rB1 * rB2 * rA2 * (997/1000)^2) * (1000/997) - rA1 * 1000 / 997
/// Simplified: x_opt = sqrt(rA1 * rB1 * rA2 * rB2) * 997 / 1000 - rA1 * 1000 / 997
pub fn optimal_v2_v2_amount(
    reserve_a1: f64, // base token reserve in pool1
    reserve_b1: f64, // intermediate reserve in pool1
    reserve_b2: f64, // intermediate reserve in pool2
    reserve_a2: f64, // base token reserve in pool2
) -> Option<(f64, f64)> {
    // Check if arb exists: product of cross-reserves must exceed threshold
    let product = reserve_a1 * reserve_b1 * reserve_b2 * reserve_a2;
    if product <= 0.0 {
        return None;
    }

    let fee = 0.997; // 0.3% fee per swap
    let sqrt_product = product.sqrt();

    let x_opt = sqrt_product * fee - reserve_a1 / fee;

    if x_opt <= 0.0 {
        return None; // No profitable arb
    }

    // Compute output for x_opt
    let y = (x_opt * fee * reserve_b1) / (reserve_a1 + x_opt * fee);
    let z = (y * fee * reserve_a2) / (reserve_b2 + y * fee);
    let profit = z - x_opt;

    if profit <= 0.0 {
        return None;
    }

    Some((x_opt, profit))
}

// ===== Uniswap V3 swap math =====
//
// V3 uses concentrated liquidity with sqrtPriceX96 encoding.
// sqrtPriceX96 = sqrt(token1/token0) * 2^96
//
// For a swap within a single tick range (no tick crossing):
//   Virtual reserves: x = L / sqrtP, y = L * sqrtP
//
// zeroForOne (sell token0 → buy token1):
//   dx_effective = dx * (1 - fee/1e6)
//   sqrtP_new = L * sqrtP / (L + dx_effective * sqrtP)
//   dy = L * (sqrtP - sqrtP_new)
//
// oneForZero (sell token1 → buy token0):
//   dy_effective = dy * (1 - fee/1e6)
//   sqrtP_new = sqrtP + dy_effective / L
//   dx = L * (1/sqrtP - 1/sqrtP_new) = L * (sqrtP_new - sqrtP) / (sqrtP * sqrtP_new)

const Q96: f64 = (1u128 << 96) as f64;

/// V3 pool state for swap computation.
#[derive(Debug, Clone, Copy)]
pub struct V3PoolState {
    /// sqrt(token1/token0) * 2^96
    pub sqrt_price_x96: f64,
    /// Current in-range liquidity
    pub liquidity: f64,
    /// Fee tier in hundredths of a bip (e.g., 3000 = 0.30%)
    pub fee: u32,
}

impl V3PoolState {
    /// Compute swap output for a V3 pool (single tick range approximation).
    ///
    /// Returns the output amount (as raw f64 in token units, not human units).
    /// `zero_for_one`: true = selling token0 for token1, false = reverse.
    ///
    /// NOTE: This is a single-tick-range approximation. Large swaps that would
    /// cross tick boundaries in reality are capped here to avoid phantom profits.
    /// The virtual reserve on the output side bounds how much can come out without
    /// crossing a tick boundary.
    pub fn compute_swap_output(&self, amount_in: f64, zero_for_one: bool) -> Option<f64> {
        if self.liquidity <= 0.0 || self.sqrt_price_x96 <= 0.0 || amount_in <= 0.0 {
            return None;
        }

        let fee_factor = 1.0 - (self.fee as f64) / 1_000_000.0;
        let effective_in = amount_in * fee_factor;
        let sqrt_p = self.sqrt_price_x96 / Q96; // normalized sqrtPrice
        let l = self.liquidity;

        if zero_for_one {
            // Selling token0 → buying token1
            // Virtual reserve of token0 in range: x_virtual = L / sqrtP
            // Cap input to 10% of virtual reserve to stay within tick range
            let x_virtual = l / sqrt_p;
            if x_virtual <= 0.0 {
                return None;
            }
            let capped_in = effective_in.min(x_virtual * 0.10);
            if capped_in <= 0.0 {
                return None;
            }

            let denom = l + capped_in * sqrt_p;
            if denom <= 0.0 {
                return None;
            }
            let sqrt_p_new = l * sqrt_p / denom;
            if sqrt_p_new <= 0.0 {
                return None;
            }
            let amount_out = l * (sqrt_p - sqrt_p_new);
            if amount_out > 0.0 {
                Some(amount_out)
            } else {
                None
            }
        } else {
            // Selling token1 → buying token0
            // Virtual reserve of token1 in range: y_virtual = L * sqrtP
            // Cap input to 10% of virtual reserve to stay within tick range
            let y_virtual = l * sqrt_p;
            if y_virtual <= 0.0 {
                return None;
            }
            let capped_in = effective_in.min(y_virtual * 0.10);
            if capped_in <= 0.0 {
                return None;
            }

            let sqrt_p_new = sqrt_p + capped_in / l;
            // amount_out_0 = L * (sqrtP_new - sqrtP) / (sqrtP * sqrtP_new)
            let amount_out = l * (sqrt_p_new - sqrt_p) / (sqrt_p * sqrt_p_new);
            if amount_out > 0.0 {
                Some(amount_out)
            } else {
                None
            }
        }
    }

    /// Compute the implied price of token1 per token0.
    pub fn price(&self) -> f64 {
        let sqrt_p = self.sqrt_price_x96 / Q96;
        sqrt_p * sqrt_p
    }
}

/// Compute V2 swap output given reserves, input amount, and fee (in basis points).
///
/// `fee_bps`: fee in basis points. Uniswap V2 = 30 (0.30%), SushiSwap = 30,
/// Camelot = 16 (0.16%) or 1 (0.01%), Aerodrome = varies.
/// Pass `None` for default 30 bps (0.30%).
pub fn v2_swap_output(amount_in: f64, reserve_in: f64, reserve_out: f64) -> Option<f64> {
    v2_swap_output_with_fee(amount_in, reserve_in, reserve_out, 30)
}

/// V2 swap output with explicit fee in basis points.
pub fn v2_swap_output_with_fee(
    amount_in: f64,
    reserve_in: f64,
    reserve_out: f64,
    fee_bps: u32,
) -> Option<f64> {
    if reserve_in <= 0.0 || reserve_out <= 0.0 || amount_in <= 0.0 {
        return None;
    }
    let fee_factor = 1.0 - (fee_bps as f64) / 10_000.0;
    let effective = amount_in * fee_factor;
    let out = (effective * reserve_out) / (reserve_in + effective);
    if out > 0.0 && out < reserve_out {
        Some(out)
    } else {
        None
    }
}

/// Compute optimal input for V2→V3 arb using binary search.
///
/// Hop 1: V2 pool — sell base_token for intermediate.
/// Hop 2: V3 pool — sell intermediate for base_token.
///
/// Returns (optimal_input, profit) in base_token raw units, or None.
pub fn optimal_v2_v3_amount(
    v2_reserve_base: f64,
    v2_reserve_inter: f64,
    v3_state: &V3PoolState,
    v3_zero_for_one: bool, // direction of hop2 in the V3 pool
) -> Option<(f64, f64)> {
    // Binary search for optimal input amount
    let mut lo = 0.0_f64;
    let mut hi = v2_reserve_base * 0.5; // Don't try to swap more than half the reserve
    let mut best_input = 0.0;
    let mut best_profit = 0.0;

    for _ in 0..80 {
        let mid = (lo + hi) / 2.0;
        let mid_r = mid + (hi - lo) * 0.001;

        let profit_mid = compute_2hop_profit_v2_v3(
            mid,
            v2_reserve_base,
            v2_reserve_inter,
            v3_state,
            v3_zero_for_one,
        );
        let profit_r = compute_2hop_profit_v2_v3(
            mid_r,
            v2_reserve_base,
            v2_reserve_inter,
            v3_state,
            v3_zero_for_one,
        );

        if profit_mid > best_profit {
            best_profit = profit_mid;
            best_input = mid;
        }

        // Follow the gradient
        if profit_r > profit_mid {
            lo = mid;
        } else {
            hi = mid;
        }
    }

    if best_profit > 0.0 {
        Some((best_input, best_profit))
    } else {
        None
    }
}

/// Compute optimal input for V3→V2 arb using binary search.
pub fn optimal_v3_v2_amount(
    v3_state: &V3PoolState,
    v3_zero_for_one: bool, // direction of hop1 in the V3 pool
    v2_reserve_inter: f64,
    v2_reserve_base: f64,
) -> Option<(f64, f64)> {
    let mut lo = 0.0_f64;
    // For V3, limit input to avoid unreasonable amounts
    let hi_estimate = v3_state.liquidity / (v3_state.sqrt_price_x96 / Q96) * 0.1;
    let mut hi = if hi_estimate > 0.0 { hi_estimate } else { 1e24 };
    let mut best_input = 0.0;
    let mut best_profit = 0.0;

    for _ in 0..80 {
        let mid = (lo + hi) / 2.0;
        let mid_r = mid + (hi - lo) * 0.001;

        let profit_mid =
            compute_2hop_profit_v3_v2(mid, v3_state, v3_zero_for_one, v2_reserve_inter, v2_reserve_base);
        let profit_r =
            compute_2hop_profit_v3_v2(mid_r, v3_state, v3_zero_for_one, v2_reserve_inter, v2_reserve_base);

        if profit_mid > best_profit {
            best_profit = profit_mid;
            best_input = mid;
        }

        if profit_r > profit_mid {
            lo = mid;
        } else {
            hi = mid;
        }
    }

    if best_profit > 0.0 {
        Some((best_input, best_profit))
    } else {
        None
    }
}

/// Compute optimal input for V3→V3 arb using binary search.
pub fn optimal_v3_v3_amount(
    v3_state1: &V3PoolState,
    v3_zfo1: bool,
    v3_state2: &V3PoolState,
    v3_zfo2: bool,
) -> Option<(f64, f64)> {
    let hi_estimate = v3_state1.liquidity / (v3_state1.sqrt_price_x96 / Q96).max(1.0) * 0.1;
    let mut lo = 0.0_f64;
    let mut hi = if hi_estimate > 0.0 { hi_estimate } else { 1e24 };
    let mut best_input = 0.0;
    let mut best_profit = 0.0;

    for _ in 0..80 {
        let mid = (lo + hi) / 2.0;
        let mid_r = mid + (hi - lo) * 0.001;

        let profit = |x: f64| -> f64 {
            let out1 = v3_state1.compute_swap_output(x, v3_zfo1);
            match out1 {
                Some(inter) => {
                    let out2 = v3_state2.compute_swap_output(inter, v3_zfo2);
                    out2.map_or(-x, |base_out| base_out - x)
                }
                None => f64::NEG_INFINITY,
            }
        };

        let p_mid = profit(mid);
        let p_r = profit(mid_r);

        if p_mid > best_profit {
            best_profit = p_mid;
            best_input = mid;
        }

        if p_r > p_mid {
            lo = mid;
        } else {
            hi = mid;
        }
    }

    if best_profit > 0.0 {
        Some((best_input, best_profit))
    } else {
        None
    }
}

fn compute_2hop_profit_v2_v3(
    input: f64,
    v2_reserve_base: f64,
    v2_reserve_inter: f64,
    v3_state: &V3PoolState,
    v3_zero_for_one: bool,
) -> f64 {
    // Hop 1: V2 swap base -> intermediate
    let inter_out = match v2_swap_output(input, v2_reserve_base, v2_reserve_inter) {
        Some(v) => v,
        None => return f64::NEG_INFINITY,
    };
    // Hop 2: V3 swap intermediate -> base
    let base_out = match v3_state.compute_swap_output(inter_out, v3_zero_for_one) {
        Some(v) => v,
        None => return f64::NEG_INFINITY,
    };
    base_out - input
}

fn compute_2hop_profit_v3_v2(
    input: f64,
    v3_state: &V3PoolState,
    v3_zero_for_one: bool,
    v2_reserve_inter: f64,
    v2_reserve_base: f64,
) -> f64 {
    // Hop 1: V3 swap base -> intermediate
    let inter_out = match v3_state.compute_swap_output(input, v3_zero_for_one) {
        Some(v) => v,
        None => return f64::NEG_INFINITY,
    };
    // Hop 2: V2 swap intermediate -> base
    let base_out = match v2_swap_output(inter_out, v2_reserve_inter, v2_reserve_base) {
        Some(v) => v,
        None => return f64::NEG_INFINITY,
    };
    base_out - input
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_optimal_v2_v2_amount() {
        // Pool1: 1000 WETH, 2000000 USDC (price $2000/ETH)
        // Pool2: 900 WETH, 1900000 USDC (price $2111/ETH - 5.5% higher)
        // Arb: buy USDC cheap on pool1, sell USDC for WETH on pool2
        let result = optimal_v2_v2_amount(
            1000.0 * 1e18,   // rA1: WETH in pool1
            2000000.0 * 1e6, // rB1: USDC in pool1
            1900000.0 * 1e6, // rB2: USDC in pool2
            900.0 * 1e18,    // rA2: WETH in pool2
        );

        assert!(result.is_some());
        let (x_opt, profit) = result.unwrap();
        assert!(x_opt > 0.0, "optimal amount should be positive");
        assert!(profit > 0.0, "profit should be positive");
    }

    #[test]
    fn test_no_arb_equal_prices() {
        // Both pools at same price -> no arb
        let result = optimal_v2_v2_amount(
            1000.0 * 1e18,
            2000000.0 * 1e6,
            2000000.0 * 1e6,
            1000.0 * 1e18,
        );
        // Should be None or very small profit (fees eat it)
        assert!(result.is_none() || result.unwrap().1 < 1e15); // < 0.001 ETH
    }
}
