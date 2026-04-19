//! Solana block fetcher.
//!
//! Mirrors the EvmFetcher pattern (rate limiting, concurrent fetch, retry)
//! but returns Solana-specific types instead of EVM `BlockData`.

use chrono::{DateTime, TimeZone, Utc};
use eyre::Result;
use futures::{stream, StreamExt};
use tracing::{debug, info};

use crate::solana_types::*;
use crate::types::Chain;

use super::rpc::SolanaRpcClient;
use super::rpc_types::*;

pub struct SolanaFetcher {
    rpc: SolanaRpcClient,
    concurrency: usize,
}

impl SolanaFetcher {
    pub fn new(rpc_http: String, rate_limit_rps: u32, concurrency: usize) -> Self {
        Self {
            rpc: SolanaRpcClient::new(rpc_http, rate_limit_rps),
            concurrency,
        }
    }

    pub fn chain(&self) -> Chain {
        Chain::Solana
    }

    /// Get the latest confirmed slot.
    pub async fn latest_slot(&self) -> Result<u64> {
        self.rpc.get_slot().await
    }

    /// Fetch a single block by slot. Returns None for skipped/empty slots.
    pub async fn fetch_block(&self, slot: u64) -> Result<Option<SolanaBlockData>> {
        let raw = self.rpc.get_block_with_retry(slot).await?;
        match raw {
            None => Ok(None),
            Some(block) => Ok(Some(convert_block(slot, block))),
        }
    }

    /// Fetch a range of slots concurrently. Skipped slots are filtered out.
    /// Returns only actual blocks, sorted by slot.
    pub async fn fetch_range(&self, from: u64, to: u64) -> Result<(Vec<SolanaBlockData>, u64)> {
        let slots: Vec<u64> = (from..=to).collect();
        let total_slots = slots.len() as u64;

        let results: Vec<Option<SolanaBlockData>> = stream::iter(slots)
            .map(|slot| async move {
                match self.fetch_block(slot).await {
                    Ok(block) => block,
                    Err(e) => {
                        tracing::error!(slot, error = %e, "failed to fetch slot");
                        None
                    }
                }
            })
            .buffer_unordered(self.concurrency)
            .collect()
            .await;

        let mut blocks: Vec<SolanaBlockData> = results.into_iter().flatten().collect();
        blocks.sort_by_key(|b| b.slot);

        debug!(
            from,
            to,
            slots_scanned = total_slots,
            blocks_found = blocks.len(),
            "fetch_range complete"
        );

        Ok((blocks, total_slots))
    }
}

/// Convert raw RPC block data to domain types.
fn convert_block(slot: u64, raw: RawSolanaBlock) -> SolanaBlockData {
    let timestamp = raw
        .block_time
        .and_then(|ts| Utc.timestamp_opt(ts, 0).single())
        .unwrap_or_else(Utc::now);

    let mut total_cu: u64 = 0;
    let mut total_fees: u64 = 0;
    let mut successful_count: usize = 0;
    let mut transactions = Vec::with_capacity(raw.transactions.len());

    for (idx, raw_tx) in raw.transactions.into_iter().enumerate() {
        let tx = convert_transaction(idx as u32, raw_tx, &mut total_cu, &mut total_fees, &mut successful_count);
        transactions.push(tx);
    }

    SolanaBlockData {
        slot,
        block_height: raw.block_height,
        blockhash: raw.blockhash,
        parent_slot: raw.parent_slot,
        timestamp,
        tx_count: transactions.len(),
        successful_tx_count: successful_count,
        total_compute_units: total_cu,
        total_fees_lamports: total_fees,
        transactions,
    }
}

fn convert_transaction(
    tx_index: u32,
    raw: RawBlockTransaction,
    total_cu: &mut u64,
    total_fees: &mut u64,
    successful_count: &mut usize,
) -> SolanaTransactionData {
    let signature = raw.transaction.signatures.first().cloned().unwrap_or_default();
    let account_keys = &raw.transaction.message.account_keys;
    let signer = account_keys.first().cloned().unwrap_or_default();

    let meta = raw.meta.as_ref();
    let success = meta.map_or(true, |m| m.err.is_none());
    let fee = meta.map_or(0, |m| m.fee);
    let cu = meta.and_then(|m| m.compute_units_consumed).unwrap_or(0);

    *total_cu += cu;
    *total_fees += fee;
    if success {
        *successful_count += 1;
    }

    // Collect unique program IDs from instructions
    let mut program_ids: Vec<String> = raw
        .transaction
        .message
        .instructions
        .iter()
        .filter_map(|ix| account_keys.get(ix.program_id_index as usize).cloned())
        .collect();
    program_ids.sort();
    program_ids.dedup();

    let num_instructions = raw.transaction.message.instructions.len() as u32;

    let log_messages = meta
        .and_then(|m| m.log_messages.clone())
        .unwrap_or_default();

    // Build full account list including loaded addresses (for v0 transactions)
    let mut all_accounts = account_keys.clone();
    if let Some(meta) = meta {
        if let Some(loaded) = &meta.loaded_addresses {
            all_accounts.extend(loaded.writable.iter().cloned());
            all_accounts.extend(loaded.readonly.iter().cloned());
        }
    }

    let pre_token_balances = meta
        .and_then(|m| m.pre_token_balances.as_ref())
        .map(|balances| convert_token_balances(balances))
        .unwrap_or_default();

    let post_token_balances = meta
        .and_then(|m| m.post_token_balances.as_ref())
        .map(|balances| convert_token_balances(balances))
        .unwrap_or_default();

    // Convert instructions
    let instructions: Vec<SolanaInstruction> = raw
        .transaction
        .message
        .instructions
        .iter()
        .map(|ix| SolanaInstruction {
            program_id_index: ix.program_id_index,
            accounts: ix.accounts.clone(),
        })
        .collect();

    // Convert inner instructions (CPI calls)
    let inner_instructions: Vec<SolanaInnerInstructionGroup> = meta
        .and_then(|m| m.inner_instructions.as_ref())
        .map(|groups| {
            groups
                .iter()
                .map(|g| SolanaInnerInstructionGroup {
                    index: g.index,
                    instructions: g
                        .instructions
                        .iter()
                        .map(|ix| SolanaInnerInstruction {
                            program_id_index: ix.program_id_index,
                            accounts: ix.accounts.clone(),
                        })
                        .collect(),
                })
                .collect()
        })
        .unwrap_or_default();

    SolanaTransactionData {
        signature,
        tx_index,
        success,
        fee_lamports: fee,
        compute_units_consumed: cu,
        signer,
        num_instructions,
        program_ids,
        log_messages,
        pre_token_balances,
        post_token_balances,
        all_accounts,
        instructions,
        inner_instructions,
    }
}

fn convert_token_balances(raw: &[RawTokenBalance]) -> Vec<SolanaTokenBalance> {
    raw.iter()
        .map(|b| {
            let amount = b.ui_token_amount.amount.parse::<u64>().unwrap_or(0);
            SolanaTokenBalance {
                account_index: b.account_index,
                mint: b.mint.clone(),
                owner: b.owner.clone().unwrap_or_default(),
                amount,
                decimals: b.ui_token_amount.decimals,
            }
        })
        .collect()
}
