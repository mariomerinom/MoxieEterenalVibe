//! Solana-specific domain types.
//!
//! Parallel to the EVM `BlockData`/`TransactionData` types but with Solana's
//! native concepts: slots, compute units, base58 addresses, instruction-based events.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A confirmed Solana block (non-empty slot).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolanaBlockData {
    pub slot: u64,
    pub block_height: Option<u64>,
    pub blockhash: String,
    pub parent_slot: u64,
    pub timestamp: DateTime<Utc>,
    pub tx_count: usize,
    pub successful_tx_count: usize,
    pub total_compute_units: u64,
    pub total_fees_lamports: u64,
    pub transactions: Vec<SolanaTransactionData>,
}

/// A single Solana transaction within a block.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolanaTransactionData {
    /// First signature (base58).
    pub signature: String,
    pub tx_index: u32,
    pub success: bool,
    pub fee_lamports: u64,
    pub compute_units_consumed: u64,
    /// First account key = fee payer / signer (base58).
    pub signer: String,
    pub num_instructions: u32,
    /// Program IDs invoked (base58), deduplicated.
    pub program_ids: Vec<String>,
    pub log_messages: Vec<String>,
    pub pre_token_balances: Vec<SolanaTokenBalance>,
    pub post_token_balances: Vec<SolanaTokenBalance>,
    /// All account keys (static + loaded from address lookup tables).
    pub all_accounts: Vec<String>,
    /// Top-level instructions with program index and account indices.
    pub instructions: Vec<SolanaInstruction>,
    /// Inner (CPI) instructions grouped by top-level instruction index.
    pub inner_instructions: Vec<SolanaInnerInstructionGroup>,
}

/// A top-level instruction in a Solana transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolanaInstruction {
    pub program_id_index: u8,
    pub accounts: Vec<u8>,
}

/// A group of inner (CPI) instructions triggered by a top-level instruction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolanaInnerInstructionGroup {
    /// Index of the top-level instruction that spawned these.
    pub index: u8,
    pub instructions: Vec<SolanaInnerInstruction>,
}

/// A single inner (CPI) instruction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolanaInnerInstruction {
    pub program_id_index: u8,
    pub accounts: Vec<u8>,
}

/// SPL token balance snapshot (pre or post transaction).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolanaTokenBalance {
    pub account_index: u8,
    pub mint: String,
    pub owner: String,
    /// Raw amount (ui_amount * 10^decimals).
    pub amount: u64,
    pub decimals: u8,
}

/// A parsed swap event from a Solana DEX.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolanaSwapEvent {
    pub slot: u64,
    pub signature: String,
    pub tx_index: u32,
    pub instruction_index: u32,
    /// Pool / AMM account (base58).
    pub pool: String,
    pub protocol: SolanaDexProtocol,
    /// SPL token mint of the input token (base58).
    pub token_in_mint: String,
    /// SPL token mint of the output token (base58).
    pub token_out_mint: String,
    /// Raw amount in (token's smallest unit).
    pub amount_in: u64,
    /// Raw amount out (token's smallest unit).
    pub amount_out: u64,
    /// Transaction signer (base58).
    pub signer: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SolanaDexProtocol {
    RaydiumAmm,
    OrcaWhirlpool,
    JupiterV6,
}

impl SolanaDexProtocol {
    pub fn as_str(&self) -> &'static str {
        match self {
            SolanaDexProtocol::RaydiumAmm => "raydium_amm",
            SolanaDexProtocol::OrcaWhirlpool => "orca_whirlpool",
            SolanaDexProtocol::JupiterV6 => "jupiter_v6",
        }
    }
}
