//! Solana DEX swap event parser.
//!
//! Detects swaps from Raydium AMM, Orca Whirlpool, and Jupiter v6.
//!
//! Strategy:
//! - Scan inner instructions (CPI calls) for known DEX program invocations
//! - Extract the pool account from each DEX instruction's account list
//! - Use signer's pre/post token balance diffs to determine swap amounts
//! - For Jupiter multi-hop routes, emit one swap per underlying DEX CPI call

use std::collections::{HashMap, HashSet};

use crate::chain::solana::constants::*;
use crate::solana_types::*;

/// Parse all swap events from a Solana block.
pub fn parse_solana_swaps(block: &SolanaBlockData) -> Vec<SolanaSwapEvent> {
    let mut swaps = Vec::new();

    for tx in &block.transactions {
        if !tx.success {
            continue;
        }

        parse_transaction_swaps(block.slot, tx, &mut swaps);
    }

    swaps
}

/// Parse swaps from a single transaction.
///
/// Looks at both top-level instructions and inner (CPI) instructions.
/// For Jupiter: the top-level instruction is Jupiter, but the actual swaps
/// are inner CPI calls to Raydium/Orca. We emit one event per CPI swap.
fn parse_transaction_swaps(
    slot: u64,
    tx: &SolanaTransactionData,
    swaps: &mut Vec<SolanaSwapEvent>,
) {
    let accounts = &tx.all_accounts;
    if accounts.is_empty() {
        return;
    }

    // Collect all DEX instruction hits: (protocol, pool_address, instruction_index)
    let mut dex_hits: Vec<(SolanaDexProtocol, String, u32)> = Vec::new();
    let mut jupiter_top_level: Vec<(SolanaDexProtocol, String, u32)> = Vec::new();

    // Check top-level instructions — track Jupiter separately
    for (ix_idx, ix) in tx.instructions.iter().enumerate() {
        let program_id = match accounts.get(ix.program_id_index as usize) {
            Some(id) => id.as_str(),
            None => continue,
        };

        if let Some(protocol) = match_dex_program(program_id) {
            let pool = extract_pool_address(protocol, &ix.accounts, accounts);
            if protocol == SolanaDexProtocol::JupiterV6 {
                // Hold Jupiter top-level hits — only use if no inner CPI DEX hits found
                jupiter_top_level.push((protocol, pool, ix_idx as u32));
            } else {
                if !is_system_program(&pool) {
                    dex_hits.push((protocol, pool, ix_idx as u32));
                }
            }
        }
    }

    // Check inner instructions (CPI calls) — this is where Jupiter's
    // sub-swaps to Raydium/Orca/etc. appear
    for group in &tx.inner_instructions {
        for inner_ix in &group.instructions {
            let program_id = match accounts.get(inner_ix.program_id_index as usize) {
                Some(id) => id.as_str(),
                None => continue,
            };

            if let Some(protocol) = match_dex_program(program_id) {
                // Skip Jupiter inner CPI — we only want the underlying DEX calls
                if protocol == SolanaDexProtocol::JupiterV6 {
                    continue;
                }
                let pool = extract_pool_address(protocol, &inner_ix.accounts, accounts);
                if !is_system_program(&pool) {
                    // Use the parent instruction index for grouping
                    dex_hits.push((protocol, pool, group.index as u32));
                }
            }
        }
    }

    // If no inner DEX CPI hits were found but Jupiter top-level exists,
    // emit a single aggregated swap event
    if dex_hits.is_empty() && !jupiter_top_level.is_empty() {
        for (protocol, _pool, ix_idx) in jupiter_top_level {
            dex_hits.push((protocol, "jupiter_aggregated".to_string(), ix_idx));
        }
    }

    if dex_hits.is_empty() {
        return;
    }

    // Deduplicate: same pool in same tx = same swap
    let mut seen_pools: HashSet<String> = HashSet::new();

    // Compute the signer's net token balance diffs for the whole transaction
    let balance_diffs = compute_signer_balance_diffs(tx);

    if dex_hits.len() == 1 {
        // Simple case: single swap — use balance diffs directly
        let (protocol, pool, ix_idx) = &dex_hits[0];

        let mut decreases: Vec<&BalanceDiff> =
            balance_diffs.iter().filter(|d| d.delta < 0).collect();
        let mut increases: Vec<&BalanceDiff> =
            balance_diffs.iter().filter(|d| d.delta > 0).collect();
        decreases.sort_by_key(|d| d.delta);
        increases.sort_by_key(|d| std::cmp::Reverse(d.delta));

        if let (Some(token_in), Some(token_out)) = (decreases.first(), increases.first()) {
            swaps.push(SolanaSwapEvent {
                slot,
                signature: tx.signature.clone(),
                tx_index: tx.tx_index,
                instruction_index: *ix_idx,
                pool: pool.clone(),
                protocol: *protocol,
                token_in_mint: token_in.mint.clone(),
                token_out_mint: token_out.mint.clone(),
                amount_in: (-token_in.delta) as u64,
                amount_out: token_out.delta as u64,
                signer: tx.signer.clone(),
            });
        }
    } else {
        // Multi-swap transaction (Jupiter route or multi-DEX).
        // Emit one swap per unique pool, using per-pool token balance diffs
        // from the inner instruction's token account indices.
        //
        // Fallback: if we can't isolate per-pool diffs, emit per-pool events
        // with the pool address but use the overall balance diffs for the
        // first/last swap in the route.

        // Try to find per-pool token movements from pre/post balances
        // by matching token accounts used in each instruction
        for (protocol, pool, ix_idx) in &dex_hits {
            if !seen_pools.insert(pool.clone()) {
                continue; // Already emitted for this pool
            }

            // Find token balance changes associated with this pool's instruction
            // by looking at the instruction's account indices
            let pool_diffs = compute_pool_balance_diffs(tx, &dex_hits, pool, accounts);

            let diffs = if pool_diffs.len() >= 2 {
                pool_diffs
            } else {
                // Fallback to signer's overall diffs
                balance_diffs.clone()
            };

            let mut decreases: Vec<&BalanceDiff> =
                diffs.iter().filter(|d| d.delta < 0).collect();
            let mut increases: Vec<&BalanceDiff> =
                diffs.iter().filter(|d| d.delta > 0).collect();
            decreases.sort_by_key(|d| d.delta);
            increases.sort_by_key(|d| std::cmp::Reverse(d.delta));

            if let (Some(token_in), Some(token_out)) = (decreases.first(), increases.first()) {
                swaps.push(SolanaSwapEvent {
                    slot,
                    signature: tx.signature.clone(),
                    tx_index: tx.tx_index,
                    instruction_index: *ix_idx,
                    pool: pool.clone(),
                    protocol: *protocol,
                    token_in_mint: token_in.mint.clone(),
                    token_out_mint: token_out.mint.clone(),
                    amount_in: (-token_in.delta) as u64,
                    amount_out: token_out.delta as u64,
                    signer: tx.signer.clone(),
                });
            }
        }
    }
}

/// Extract the pool/AMM account address from a DEX instruction's account list.
///
/// Each DEX has a known account layout:
/// - Raydium AMM: accounts[1] = AMM ID (pool)
/// - Orca Whirlpool: accounts[2] = Whirlpool account (pool)
/// - Jupiter v6: accounts[0] = not a pool (Jupiter is an aggregator)
fn extract_pool_address(
    protocol: SolanaDexProtocol,
    account_indices: &[u8],
    all_accounts: &[String],
) -> String {
    let pool_idx = match protocol {
        SolanaDexProtocol::RaydiumAmm => 1,  // AMM ID
        SolanaDexProtocol::OrcaWhirlpool => 2, // Whirlpool account
        SolanaDexProtocol::JupiterV6 => {
            // Jupiter is an aggregator, not a pool. Use a hash of involved accounts
            // as a pseudo-pool identifier, or just return the first account.
            0
        }
    };

    account_indices
        .get(pool_idx)
        .and_then(|&idx| all_accounts.get(idx as usize))
        .cloned()
        .unwrap_or_else(|| "unknown".to_string())
}

/// Match a program ID to a known DEX protocol.
fn match_dex_program(program_id: &str) -> Option<SolanaDexProtocol> {
    match program_id {
        RAYDIUM_AMM_PROGRAM => Some(SolanaDexProtocol::RaydiumAmm),
        ORCA_WHIRLPOOL_PROGRAM => Some(SolanaDexProtocol::OrcaWhirlpool),
        JUPITER_V6_PROGRAM => Some(SolanaDexProtocol::JupiterV6),
        _ => None,
    }
}

/// Returns true if the address is a known system/infrastructure program
/// (not a real pool). These should never appear as pool addresses.
fn is_system_program(addr: &str) -> bool {
    matches!(
        addr,
        TOKEN_PROGRAM
            | TOKEN_2022_PROGRAM
            | SYSTEM_PROGRAM
            | JUPITER_V6_PROGRAM
            | "SysvarRent111111111111111111111111111111111"
            | "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr"
            | "Memo1UhkJBfCVE8sVGKKGqseYAopLFEUJBBFjjSpa1"
            | "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"
            | "ComputeBudget111111111111111111111111111111"
    )
}

#[derive(Clone)]
struct BalanceDiff {
    mint: String,
    delta: i128,
}

/// Compute net token balance changes for the transaction signer.
fn compute_signer_balance_diffs(tx: &SolanaTransactionData) -> Vec<BalanceDiff> {
    let signer = &tx.signer;

    let mut pre_map: HashMap<String, u64> = HashMap::new();
    for bal in &tx.pre_token_balances {
        if &bal.owner == signer {
            pre_map.insert(bal.mint.clone(), bal.amount);
        }
    }

    let mut post_map: HashMap<String, u64> = HashMap::new();
    for bal in &tx.post_token_balances {
        if &bal.owner == signer {
            post_map.insert(bal.mint.clone(), bal.amount);
        }
    }

    let mut all_mints: HashSet<&String> = HashSet::new();
    all_mints.extend(pre_map.keys());
    all_mints.extend(post_map.keys());

    let mut diffs = Vec::new();
    for mint in all_mints {
        let pre = *pre_map.get(mint).unwrap_or(&0) as i128;
        let post = *post_map.get(mint).unwrap_or(&0) as i128;
        let delta = post - pre;
        if delta != 0 {
            diffs.push(BalanceDiff {
                mint: mint.clone(),
                delta,
            });
        }
    }

    diffs
}

/// Attempt to compute per-pool token balance diffs by looking at which
/// token accounts the pool's instruction touches.
///
/// For each token balance entry, if the account_index is in the pool instruction's
/// account list, we attribute that balance change to this pool.
fn compute_pool_balance_diffs(
    tx: &SolanaTransactionData,
    _dex_hits: &[(SolanaDexProtocol, String, u32)],
    pool: &str,
    all_accounts: &[String],
) -> Vec<BalanceDiff> {
    // Find all account indices used by instructions targeting this pool
    let mut pool_account_indices: HashSet<u8> = HashSet::new();

    // Check inner instructions for this pool
    for group in &tx.inner_instructions {
        for inner_ix in &group.instructions {
            let program_id = all_accounts
                .get(inner_ix.program_id_index as usize)
                .map(|s| s.as_str())
                .unwrap_or("");

            if match_dex_program(program_id).is_some() {
                let ix_pool = extract_pool_address(
                    match_dex_program(program_id).unwrap(),
                    &inner_ix.accounts,
                    all_accounts,
                );
                if ix_pool == pool {
                    pool_account_indices.extend(&inner_ix.accounts);
                }
            }
        }
    }

    // Also check top-level instructions
    for ix in &tx.instructions {
        let program_id = all_accounts
            .get(ix.program_id_index as usize)
            .map(|s| s.as_str())
            .unwrap_or("");

        if match_dex_program(program_id).is_some() {
            let ix_pool = extract_pool_address(
                match_dex_program(program_id).unwrap(),
                &ix.accounts,
                all_accounts,
            );
            if ix_pool == pool {
                pool_account_indices.extend(&ix.accounts);
            }
        }
    }

    if pool_account_indices.is_empty() {
        return Vec::new();
    }

    // Now find token balance changes on accounts used by this pool
    let mut pre_map: HashMap<String, u64> = HashMap::new();
    for bal in &tx.pre_token_balances {
        if pool_account_indices.contains(&bal.account_index) {
            // Use signer-owned accounts only
            if bal.owner == tx.signer {
                pre_map.insert(bal.mint.clone(), bal.amount);
            }
        }
    }

    let mut post_map: HashMap<String, u64> = HashMap::new();
    for bal in &tx.post_token_balances {
        if pool_account_indices.contains(&bal.account_index) {
            if bal.owner == tx.signer {
                post_map.insert(bal.mint.clone(), bal.amount);
            }
        }
    }

    let mut all_mints: HashSet<&String> = HashSet::new();
    all_mints.extend(pre_map.keys());
    all_mints.extend(post_map.keys());

    let mut diffs = Vec::new();
    for mint in all_mints {
        let pre = *pre_map.get(mint).unwrap_or(&0) as i128;
        let post = *post_map.get(mint).unwrap_or(&0) as i128;
        let delta = post - pre;
        if delta != 0 {
            diffs.push(BalanceDiff {
                mint: mint.clone(),
                delta,
            });
        }
    }

    diffs
}
