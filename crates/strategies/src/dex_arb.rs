//! Cross-pool DEX arbitrage strategy.
//!
//! On each new block:
//! 1. Batch-fetch all pool states via Multicall3 (single RPC call)
//! 2. For each precomputed arb cycle, evaluate using cached state
//! 3. Compute optimal input amount and expected profit
//! 4. If profitable after gas+bribe, emit an Action

use alloy_primitives::{Address, Bytes, U256};
use alloy_sol_types::{SolCall, SolValue};
use eyre::Result;
use mev_capture::types::Chain;
use std::collections::{HashMap, HashSet};
use tracing::{debug, info};

use crate::pool_graph::{
    ArbCycle, PoolInfo, V3PoolState,
    optimal_v2_v2_amount, optimal_v2_v3_amount, optimal_v3_v2_amount, optimal_v3_v3_amount,
    v2_swap_output_with_fee,
};
use crate::traits::{Action, Event, Strategy};
use mev_executor::contracts::encode_arb;

// ---- Multicall3 ABI ----

alloy::sol! {
    /// Multicall3 aggregate3 interface.
    #[derive(Debug)]
    struct Call3 {
        address target;
        bool allowFailure;
        bytes callData;
    }

    #[derive(Debug)]
    struct MulticallResult {
        bool success;
        bytes returnData;
    }

    #[derive(Debug)]
    function aggregate3(Call3[] calls) external payable returns (MulticallResult[] returnData);
}

/// Multicall3 contract address (same on all EVM chains).
const MULTICALL3: &str = "0xcA11bde05977b3631167028862bE2a173976CA11";

// ---- Pool state selectors ----
const GET_RESERVES_SELECTOR: [u8; 4] = [0x09, 0x02, 0xf1, 0xac];
const SLOT0_SELECTOR: [u8; 4] = [0x38, 0x50, 0xc7, 0xbd];
const LIQUIDITY_SELECTOR: [u8; 4] = [0x1a, 0x68, 0x65, 0x02];
/// Solidly getAmountOut(uint256,address) selector = 0xf140a35a
const GET_AMOUNT_OUT_SELECTOR: [u8; 4] = [0xf1, 0x40, 0xa3, 0x5a];

/// Reference amounts (in raw units) for Solidly getAmountOut batching.
/// We query at multiple amounts to build an interpolation function.
const SOLIDLY_REF_AMOUNTS: [f64; 5] = [
    1e15,   // 0.001 ETH
    1e16,   // 0.01 ETH
    1e17,   // 0.1 ETH
    1e18,   // 1.0 ETH
    1e19,   // 10.0 ETH
];

/// Cached pool states fetched via Multicall3.
struct PoolStateCache {
    v2_reserves: HashMap<Address, (U256, U256)>,
    v3_states: HashMap<Address, V3PoolState>,
    /// Solidly pool output curves: pool -> token_in -> Vec<(input_amount, output_amount)>
    solidly_outputs: HashMap<(Address, Address), Vec<(f64, f64)>>,
}

/// DexArb strategy: detects and executes cross-pool arbitrage.
pub struct DexArbStrategy {
    pub chain: Chain,
    pub min_profit_eth: f64,
    pub max_gas_gwei: f64,
    pub bribe_pct: f64,
    /// Precomputed arb cycles from pool graph.
    pub arb_cycles: Vec<ArbCycle>,
    /// Deployed MevBot contract address.
    pub contract_address: Address,
    /// RPC URL for state forking.
    pub rpc_url: String,
    /// Chain-specific WETH address.
    pub weth: Address,
}

impl DexArbStrategy {
    pub fn new(
        chain: Chain,
        arb_cycles: Vec<ArbCycle>,
        contract_address: Address,
        rpc_url: String,
        min_profit_eth: f64,
        bribe_pct: f64,
        weth: Address,
    ) -> Self {
        Self {
            chain,
            min_profit_eth,
            max_gas_gwei: 100.0,
            bribe_pct,
            arb_cycles,
            contract_address,
            rpc_url,
            weth,
        }
    }

    /// Batch-fetch all pool states needed by arb cycles via Multicall3.
    ///
    /// Makes a single `eth_call` to Multicall3's aggregate3, which calls
    /// getReserves/slot0/liquidity on every unique pool. Returns a cache
    /// that evaluate_cycle_cached can read from with zero RPC overhead.
    async fn prefetch_pool_states(
        &self,
        block_number: u64,
    ) -> Result<PoolStateCache> {
        // Collect unique pools and their types
        let mut v2_pools: Vec<Address> = Vec::new();
        let mut v3_pools: Vec<Address> = Vec::new();
        let mut v2_set: HashSet<Address> = HashSet::new();
        let mut v3_set: HashSet<Address> = HashSet::new();
        // Track V3 fee per pool address
        let mut v3_fees: HashMap<Address, u32> = HashMap::new();
        // Solidly pools: (pool_address, token_in_for_each_direction)
        let mut solidly_pools: Vec<(Address, Address)> = Vec::new();
        let mut solidly_set: HashSet<(Address, Address)> = HashSet::new();

        for cycle in &self.arb_cycles {
            let pools_to_check: Vec<&PoolInfo> = if let Some(ref p3) = cycle.pool3 {
                vec![&cycle.pool1, &cycle.pool2, p3]
            } else {
                vec![&cycle.pool1, &cycle.pool2]
            };

            // Determine token_in for each pool in this cycle
            let tokens_in = match cycle.hops.len() {
                2 => vec![cycle.base_token, cycle.intermediate_token],
                3 => vec![
                    cycle.base_token,
                    cycle.intermediate_token,
                    cycle.intermediate_token2.unwrap_or(cycle.base_token),
                ],
                _ => continue,
            };

            for (pool, &token_in) in pools_to_check.iter().zip(tokens_in.iter()) {
                if pool.is_v3() {
                    if v3_set.insert(pool.pool) {
                        v3_pools.push(pool.pool);
                        v3_fees.insert(pool.pool, pool.fee.unwrap_or(3000));
                    }
                } else if pool.is_solidly_fork() {
                    let key = (pool.pool, token_in);
                    if solidly_set.insert(key) {
                        solidly_pools.push(key);
                    }
                } else {
                    if v2_set.insert(pool.pool) {
                        v2_pools.push(pool.pool);
                    }
                }
            }
        }

        debug!(
            v2 = v2_pools.len(),
            v3 = v3_pools.len(),
            solidly = solidly_pools.len(),
            "prefetching pool states via multicall3"
        );

        // Build multicall3 calls array:
        // - V2 pools: getReserves()
        // - V3 pools: slot0() and liquidity() (2 calls per pool)
        let mut calls: Vec<Call3> = Vec::with_capacity(
            v2_pools.len() + v3_pools.len() * 2
        );

        // Track which index maps to which pool and call type
        #[derive(Debug)]
        enum CallType {
            V2Reserves(Address),
            V3Slot0(Address),
            V3Liquidity(Address),
            SolidlyAmountOut(Address, Address, f64), // (pool, token_in, amount_in)
        }
        let mut call_map: Vec<CallType> = Vec::with_capacity(calls.capacity());

        for &pool in &v2_pools {
            calls.push(Call3 {
                target: pool,
                allowFailure: true,
                callData: Bytes::from(GET_RESERVES_SELECTOR.to_vec()),
            });
            call_map.push(CallType::V2Reserves(pool));
        }

        for &pool in &v3_pools {
            calls.push(Call3 {
                target: pool,
                allowFailure: true,
                callData: Bytes::from(SLOT0_SELECTOR.to_vec()),
            });
            call_map.push(CallType::V3Slot0(pool));

            calls.push(Call3 {
                target: pool,
                allowFailure: true,
                callData: Bytes::from(LIQUIDITY_SELECTOR.to_vec()),
            });
            call_map.push(CallType::V3Liquidity(pool));
        }

        // Solidly pools: getAmountOut(uint256 amountIn, address tokenIn) at reference amounts
        for &(pool, token_in) in &solidly_pools {
            for &ref_amount in &SOLIDLY_REF_AMOUNTS {
                // Encode: selector + uint256(amountIn) + address(tokenIn)
                let amount_u256 = U256::from(ref_amount as u128);
                let mut calldata = Vec::with_capacity(68);
                calldata.extend_from_slice(&GET_AMOUNT_OUT_SELECTOR);
                calldata.extend_from_slice(&amount_u256.to_be_bytes::<32>());
                // Address is left-padded to 32 bytes
                let mut addr_bytes = [0u8; 32];
                addr_bytes[12..32].copy_from_slice(token_in.as_slice());
                calldata.extend_from_slice(&addr_bytes);

                calls.push(Call3 {
                    target: pool,
                    allowFailure: true,
                    callData: Bytes::from(calldata),
                });
                call_map.push(CallType::SolidlyAmountOut(pool, token_in, ref_amount));
            }
        }

        if calls.is_empty() {
            return Ok(PoolStateCache {
                v2_reserves: HashMap::new(),
                v3_states: HashMap::new(),
                solidly_outputs: HashMap::new(),
            });
        }

        // Encode the aggregate3 call
        let multicall_calldata = aggregate3Call { calls }.abi_encode();

        // Make direct eth_call via alloy provider
        let url: alloy::transports::http::reqwest::Url = self.rpc_url.parse()
            .map_err(|e| eyre::eyre!("bad RPC URL: {e}"))?;
        let provider = alloy::providers::ProviderBuilder::new()
            .connect_http(url);

        use alloy::providers::Provider;
        let multicall3_addr: Address = MULTICALL3.parse()
            .map_err(|e| eyre::eyre!("bad multicall3 address: {e}"))?;

        let tx = alloy::rpc::types::TransactionRequest::default()
            .to(multicall3_addr)
            .input(alloy::rpc::types::TransactionInput::new(
                Bytes::from(multicall_calldata),
            ));

        let output = provider
            .call(tx)
            .block(alloy::eips::BlockId::number(block_number))
            .await
            .map_err(|e| eyre::eyre!("multicall3 eth_call failed: {e}"))?;

        // Decode aggregate3 return: MulticallResult[]
        let results = <Vec<MulticallResult>>::abi_decode(&output)
            .map_err(|e| eyre::eyre!("multicall3 decode failed: {e}"))?;

        if results.len() != call_map.len() {
            eyre::bail!(
                "multicall3 returned {} results, expected {}",
                results.len(),
                call_map.len()
            );
        }

        // Parse results into cache
        let mut v2_reserves: HashMap<Address, (U256, U256)> = HashMap::new();
        let mut v3_slot0: HashMap<Address, (f64, i32)> = HashMap::new(); // sqrtPrice, tick
        let mut v3_liq: HashMap<Address, f64> = HashMap::new();
        let mut solidly_raw: HashMap<(Address, Address), Vec<(f64, f64)>> = HashMap::new();

        for (i, result) in results.iter().enumerate() {
            if !result.success {
                continue;
            }
            let data = &result.returnData;

            match &call_map[i] {
                CallType::V2Reserves(pool) => {
                    if data.len() >= 64 {
                        let r0 = U256::from_be_slice(&data[0..32]);
                        let r1 = U256::from_be_slice(&data[32..64]);
                        v2_reserves.insert(*pool, (r0, r1));
                    }
                }
                CallType::V3Slot0(pool) => {
                    if data.len() >= 64 {
                        let sqrt_price_x96 = U256::from_be_slice(&data[0..32]);
                        let tick_raw = U256::from_be_slice(&data[32..64]);
                        let tick = {
                            let low24 = tick_raw.as_limbs()[0] as i32;
                            if low24 & 0x800000 != 0 {
                                low24 | !0xFFFFFF_i32
                            } else {
                                low24
                            }
                        };
                        v3_slot0.insert(*pool, (u256_to_f64(sqrt_price_x96), tick));
                    }
                }
                CallType::V3Liquidity(pool) => {
                    if data.len() >= 32 {
                        let liq = U256::from_be_slice(&data[0..32]);
                        v3_liq.insert(*pool, u256_to_f64(liq));
                    }
                }
                CallType::SolidlyAmountOut(pool, token_in, amount_in) => {
                    if data.len() >= 32 {
                        let amount_out = u256_to_f64(U256::from_be_slice(&data[0..32]));
                        if amount_out > 0.0 {
                            solidly_raw
                                .entry((*pool, *token_in))
                                .or_default()
                                .push((*amount_in, amount_out));
                        }
                    }
                }
            }
        }

        // Combine V3 slot0 + liquidity into V3PoolState
        let mut v3_states: HashMap<Address, V3PoolState> = HashMap::new();
        for &pool in &v3_pools {
            if let (Some(&(sqrt_price, _tick)), Some(&liq)) =
                (v3_slot0.get(&pool), v3_liq.get(&pool))
            {
                if sqrt_price > 0.0 && liq > 0.0 {
                    let fee = v3_fees.get(&pool).copied().unwrap_or(3000);
                    v3_states.insert(pool, V3PoolState {
                        sqrt_price_x96: sqrt_price,
                        liquidity: liq,
                        fee,
                    });
                }
            }
        }

        // Sort Solidly output curves by input amount
        for curve in solidly_raw.values_mut() {
            curve.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        }

        debug!(
            v2_ok = v2_reserves.len(),
            v3_ok = v3_states.len(),
            solidly_ok = solidly_raw.len(),
            v2_total = v2_pools.len(),
            v3_total = v3_pools.len(),
            solidly_total = solidly_pools.len(),
            "pool states fetched"
        );

        Ok(PoolStateCache {
            v2_reserves,
            v3_states,
            solidly_outputs: solidly_raw,
        })
    }

    /// Scan all arb cycles using pre-fetched pool states, return profitable Actions.
    async fn scan_cycles(
        &self,
        block_number: u64,
        base_fee_gwei: f64,
    ) -> Result<Vec<Action>> {
        // Single RPC call to fetch all pool states
        let cache = self.prefetch_pool_states(block_number).await?;

        let weth = self.weth;
        let mut actions = Vec::new();
        let base_fee_wei = (base_fee_gwei * 1e9) as u128;

        for cycle in &self.arb_cycles {
            let result = self.evaluate_cycle_cached(cycle, &cache, weth);
            let (x_opt, profit) = match result {
                Some((x, p)) if p > 0.0 => (x, p),
                _ => continue,
            };

            // Sanity: skip if optimal input is < 0.001 ETH (likely phantom from V3 math)
            let input_eth = x_opt / 1e18;
            if input_eth < 0.001 {
                debug!(
                    cycle = %cycle.label,
                    input_eth = format!("{:.6}", input_eth),
                    "skipping phantom (input too small)"
                );
                continue;
            }

            // Sanity: skip if profit/input ratio > 50% (unrealistic for DEX arb)
            let profit_ratio = profit / x_opt;
            if profit_ratio > 0.5 {
                debug!(
                    cycle = %cycle.label,
                    ratio = format!("{:.2}", profit_ratio),
                    "skipping phantom (profit/input ratio too high)"
                );
                continue;
            }

            // Convert profit to ETH for threshold check
            let profit_eth = profit / 1e18;

            // Estimate gas cost: V3 hops cost more gas, 3-hop costs more
            let has_v3 = cycle.pool1.is_v3() || cycle.pool2.is_v3()
                || cycle.pool3.as_ref().map_or(false, |p| p.is_v3());
            let is_3hop = cycle.hops.len() == 3;
            let estimated_gas: u64 = match (is_3hop, has_v3) {
                (true, true) => 500_000,
                (true, false) => 400_000,
                (false, true) => 350_000,
                (false, false) => 250_000,
            };
            let gas_cost_wei = estimated_gas as u128 * base_fee_wei;
            let gas_cost_eth = gas_cost_wei as f64 / 1e18;

            // Bribe cost
            let bribe_eth = profit_eth * self.bribe_pct;
            let net_profit = profit_eth - gas_cost_eth - bribe_eth;

            if net_profit < self.min_profit_eth {
                continue;
            }

            // Build the Action
            let amount_in = U256::from(x_opt as u128);
            let min_profit_wei = U256::from((net_profit * 0.5 * 1e18) as u128);

            let calldata = encode_arb(&cycle.hops, weth, amount_in, min_profit_wei);

            info!(
                cycle = %cycle.label,
                profit_eth = format!("{:.6}", profit_eth),
                gas_cost_eth = format!("{:.6}", gas_cost_eth),
                bribe_eth = format!("{:.6}", bribe_eth),
                net_profit = format!("{:.6}", net_profit),
                input_eth = format!("{:.4}", x_opt / 1e18),
                "arb opportunity found"
            );

            // Collect pool addresses for this cycle
            let mut pool_addrs = vec![cycle.pool1.pool, cycle.pool2.pool];
            if let Some(ref p3) = cycle.pool3 {
                pool_addrs.push(p3.pool);
            }

            actions.push(Action {
                chain: self.chain,
                strategy: "dex_arb".to_string(),
                target_tx: None,
                to: self.contract_address,
                calldata,
                value: U256::ZERO,
                estimated_profit_eth: net_profit,
                estimated_gas,
                bribe_pct: self.bribe_pct,
                cycle_label: cycle.label.clone(),
                input_amount_eth: x_opt / 1e18,
                pool_addresses: pool_addrs,
            });
        }

        if actions.is_empty() {
            debug!(
                block = block_number,
                cycles_scanned = self.arb_cycles.len(),
                "no profitable arbs this block"
            );
        }

        Ok(actions)
    }

    /// Evaluate a single arb cycle using pre-fetched pool states.
    /// Returns (optimal_input, profit) in raw token units, or None.
    fn evaluate_cycle_cached(
        &self,
        cycle: &ArbCycle,
        cache: &PoolStateCache,
        weth: Address,
    ) -> Option<(f64, f64)> {
        // 3-hop cycles use generic chain evaluation
        if cycle.hops.len() == 3 {
            return self.evaluate_cycle_3hop_cached(cycle, cache, weth);
        }

        // If either pool is a Solidly fork, use the generic chain evaluation
        // (which uses build_swap_fn_cached with interpolated getAmountOut data)
        if cycle.pool1.is_solidly_fork() || cycle.pool2.is_solidly_fork() {
            // Re-use 3-hop evaluation logic (binary search over chained swap fns)
            // for 2-hop Solidly cycles — it handles any pool type via build_swap_fn_cached
            let hop1_swap = self.build_swap_fn_cached(cache, &cycle.pool1, weth)?;
            let hop2_swap = self.build_swap_fn_cached(cache, &cycle.pool2, cycle.intermediate_token)?;

            let mut lo = 0.0_f64;
            let mut hi = 10.0 * 1e18;
            let mut best_input = 0.0;
            let mut best_profit = 0.0;

            for _ in 0..60 {
                let mid = (lo + hi) / 2.0;
                let profit = match hop1_swap(mid).and_then(|out1| hop2_swap(out1)) {
                    Some(out2) => out2 - mid,
                    None => { hi = mid; continue; }
                };
                let profit_up = hop1_swap(mid * 1.001)
                    .and_then(|o1| hop2_swap(o1))
                    .map(|o2| o2 - mid * 1.001)
                    .unwrap_or(f64::NEG_INFINITY);

                if profit_up > profit { lo = mid; } else { hi = mid; }
                if profit > best_profit { best_profit = profit; best_input = mid; }
            }

            return if best_profit > 0.0 && best_input > 0.0 {
                Some((best_input, best_profit))
            } else {
                None
            };
        }

        let p1_v3 = cycle.pool1.is_v3();
        let p2_v3 = cycle.pool2.is_v3();

        match (p1_v3, p2_v3) {
            // V2-V2: analytical solution
            (false, false) => {
                let &(r0_1, r1_1) = cache.v2_reserves.get(&cycle.pool1.pool)?;
                let &(r0_2, r1_2) = cache.v2_reserves.get(&cycle.pool2.pool)?;

                let (ra1, rb1) = if cycle.pool1.has_token0(weth) {
                    (u256_to_f64(r0_1), u256_to_f64(r1_1))
                } else {
                    (u256_to_f64(r1_1), u256_to_f64(r0_1))
                };
                let (rb2, ra2) = if cycle.pool2.has_token0(cycle.intermediate_token) {
                    (u256_to_f64(r0_2), u256_to_f64(r1_2))
                } else {
                    (u256_to_f64(r1_2), u256_to_f64(r0_2))
                };

                optimal_v2_v2_amount(ra1, rb1, rb2, ra2)
            }

            // V2-V3
            (false, true) => {
                let &(r0_1, r1_1) = cache.v2_reserves.get(&cycle.pool1.pool)?;
                let v3 = cache.v3_states.get(&cycle.pool2.pool)?;

                let (ra1, rb1) = if cycle.pool1.has_token0(weth) {
                    (u256_to_f64(r0_1), u256_to_f64(r1_1))
                } else {
                    (u256_to_f64(r1_1), u256_to_f64(r0_1))
                };

                let hop2_zfo = cycle.pool2.zero_for_one(cycle.intermediate_token);
                optimal_v2_v3_amount(ra1, rb1, v3, hop2_zfo)
            }

            // V3-V2
            (true, false) => {
                let v3 = cache.v3_states.get(&cycle.pool1.pool)?;
                let &(r0_2, r1_2) = cache.v2_reserves.get(&cycle.pool2.pool)?;

                let hop1_zfo = cycle.pool1.zero_for_one(weth);

                let (rb2, ra2) = if cycle.pool2.has_token0(cycle.intermediate_token) {
                    (u256_to_f64(r0_2), u256_to_f64(r1_2))
                } else {
                    (u256_to_f64(r1_2), u256_to_f64(r0_2))
                };

                optimal_v3_v2_amount(v3, hop1_zfo, rb2, ra2)
            }

            // V3-V3
            (true, true) => {
                let v3_1 = cache.v3_states.get(&cycle.pool1.pool)?;
                let v3_2 = cache.v3_states.get(&cycle.pool2.pool)?;

                let hop1_zfo = cycle.pool1.zero_for_one(weth);
                let hop2_zfo = cycle.pool2.zero_for_one(cycle.intermediate_token);

                optimal_v3_v3_amount(v3_1, hop1_zfo, v3_2, hop2_zfo)
            }
        }
    }

    /// Evaluate a 3-hop cycle using binary search over chained swaps (cached version).
    fn evaluate_cycle_3hop_cached(
        &self,
        cycle: &ArbCycle,
        cache: &PoolStateCache,
        weth: Address,
    ) -> Option<(f64, f64)> {
        let p3 = cycle.pool3.as_ref()?;
        let inter2 = cycle.intermediate_token2?;

        // Build swap closures from cached state
        let hop1_swap = self.build_swap_fn_cached(cache, &cycle.pool1, weth)?;
        let hop2_swap = self.build_swap_fn_cached(cache, &cycle.pool2, cycle.intermediate_token)?;
        let hop3_swap = self.build_swap_fn_cached(cache, p3, inter2)?;

        // Binary search for optimal input
        let mut lo = 0.0_f64;
        let mut hi = 10.0 * 1e18; // Max 10 WETH input
        let mut best_input = 0.0;
        let mut best_profit = 0.0;

        let chain_profit = |x: f64| -> Option<f64> {
            let out1 = hop1_swap(x)?;
            let out2 = hop2_swap(out1)?;
            let out3 = hop3_swap(out2)?;
            Some(out3 - x)
        };

        for _ in 0..60 {
            let mid = (lo + hi) / 2.0;
            let profit = match chain_profit(mid) {
                Some(p) => p,
                None => {
                    hi = mid;
                    continue;
                }
            };

            let mid_up = mid * 1.001;
            let profit_up = chain_profit(mid_up).unwrap_or(f64::NEG_INFINITY);

            if profit_up > profit {
                lo = mid;
            } else {
                hi = mid;
            }

            if profit > best_profit {
                best_profit = profit;
                best_input = mid;
            }
        }

        if best_profit > 0.0 && best_input > 0.0 {
            Some((best_input, best_profit))
        } else {
            None
        }
    }

    /// Build a swap function for a single pool hop using cached state.
    fn build_swap_fn_cached(
        &self,
        cache: &PoolStateCache,
        pool: &PoolInfo,
        token_in: Address,
    ) -> Option<Box<dyn Fn(f64) -> Option<f64>>> {
        let zfo = pool.zero_for_one(token_in);

        if pool.is_v3() {
            let state = *cache.v3_states.get(&pool.pool)?;
            Some(Box::new(move |amount_in: f64| {
                state.compute_swap_output(amount_in, zfo)
            }))
        } else if pool.is_solidly_fork() {
            // Use interpolated getAmountOut data from Multicall3 batch
            let key = (pool.pool, token_in);
            let curve = cache.solidly_outputs.get(&key)?.clone();
            if curve.is_empty() {
                return None;
            }
            Some(Box::new(move |amount_in: f64| {
                solidly_interpolate(&curve, amount_in)
            }))
        } else {
            let &(r0, r1) = cache.v2_reserves.get(&pool.pool)?;
            let (reserve_in, reserve_out) = if zfo {
                (u256_to_f64(r0), u256_to_f64(r1))
            } else {
                (u256_to_f64(r1), u256_to_f64(r0))
            };
            let fee_bps = pool.fee.unwrap_or(30);
            Some(Box::new(move |amount_in: f64| {
                v2_swap_output_with_fee(amount_in, reserve_in, reserve_out, fee_bps)
            }))
        }
    }
}

#[async_trait::async_trait]
impl Strategy for DexArbStrategy {
    fn name(&self) -> &str {
        "dex_arb"
    }

    fn chain(&self) -> Chain {
        self.chain
    }

    async fn process_event(&self, event: &Event) -> Result<Vec<Action>> {
        match event {
            Event::NewBlock(block) => {
                let base_fee_gwei = block
                    .base_fee
                    .map(|bf| u256_to_f64(bf) / 1e9)
                    .unwrap_or(30.0);

                self.scan_cycles(block.number, base_fee_gwei).await
            }
            _ => Ok(vec![]),
        }
    }

    fn params(&self) -> serde_json::Value {
        serde_json::json!({
            "min_profit_eth": self.min_profit_eth,
            "max_gas_gwei": self.max_gas_gwei,
            "bribe_pct": self.bribe_pct,
            "cycles": self.arb_cycles.len(),
        })
    }

    fn set_params(&mut self, p: serde_json::Value) -> Result<()> {
        if let Some(v) = p.get("min_profit_eth").and_then(|v| v.as_f64()) {
            self.min_profit_eth = v;
        }
        if let Some(v) = p.get("max_gas_gwei").and_then(|v| v.as_f64()) {
            self.max_gas_gwei = v;
        }
        if let Some(v) = p.get("bribe_pct").and_then(|v| v.as_f64()) {
            self.bribe_pct = v;
        }
        Ok(())
    }
}

/// Interpolate Solidly getAmountOut from reference points.
///
/// Given a sorted curve of (input, output) pairs from batched getAmountOut calls,
/// estimate the output for an arbitrary input via linear interpolation.
/// Extrapolates linearly beyond the sampled range.
fn solidly_interpolate(curve: &[(f64, f64)], amount_in: f64) -> Option<f64> {
    if curve.is_empty() || amount_in <= 0.0 {
        return None;
    }

    // Below smallest sample: linear scale from origin
    if amount_in <= curve[0].0 {
        let ratio = curve[0].1 / curve[0].0;
        return Some(amount_in * ratio);
    }

    // Above largest sample: linear extrapolation from last two points
    if amount_in >= curve[curve.len() - 1].0 {
        if curve.len() >= 2 {
            let (x1, y1) = curve[curve.len() - 2];
            let (x2, y2) = curve[curve.len() - 1];
            let slope = (y2 - y1) / (x2 - x1);
            let out = y2 + slope * (amount_in - x2);
            return if out > 0.0 { Some(out) } else { None };
        }
        // Only one point: linear scale
        let ratio = curve[0].1 / curve[0].0;
        return Some(amount_in * ratio);
    }

    // Between samples: linear interpolation
    for i in 0..curve.len() - 1 {
        if amount_in >= curve[i].0 && amount_in <= curve[i + 1].0 {
            let (x1, y1) = curve[i];
            let (x2, y2) = curve[i + 1];
            let t = (amount_in - x1) / (x2 - x1);
            let out = y1 + t * (y2 - y1);
            return if out > 0.0 { Some(out) } else { None };
        }
    }

    None
}

/// Convert U256 to f64 (lossy but fine for reserve/amount comparisons).
fn u256_to_f64(v: U256) -> f64 {
    let limbs = v.as_limbs();
    if limbs[2] == 0 && limbs[3] == 0 {
        let low = limbs[0] as f64;
        let high = limbs[1] as f64 * (u64::MAX as f64 + 1.0);
        low + high
    } else {
        v.to_string().parse::<f64>().unwrap_or(f64::MAX)
    }
}
