//! Raw JSON-RPC response types for Solana's `getBlock` and `getSlot` methods.
//!
//! These are deserialization targets only — converted to domain types in the fetcher.

use serde::Deserialize;

/// Top-level JSON-RPC response wrapper.
#[derive(Debug, Deserialize)]
pub struct RpcResponse<T> {
    pub result: Option<T>,
    pub error: Option<RpcError>,
}

#[derive(Debug, Deserialize)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
}

/// Response from `getSlot`.
pub type SlotResponse = RpcResponse<u64>;

/// Response from `getBlock` with full transaction details.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawSolanaBlock {
    pub blockhash: String,
    pub block_height: Option<u64>,
    pub block_time: Option<i64>,
    pub parent_slot: u64,
    #[serde(default)]
    pub transactions: Vec<RawBlockTransaction>,
}

#[derive(Debug, Deserialize)]
pub struct RawBlockTransaction {
    pub transaction: RawTransactionContent,
    pub meta: Option<RawTransactionMeta>,
}

#[derive(Debug, Deserialize)]
pub struct RawTransactionContent {
    pub signatures: Vec<String>,
    pub message: RawTransactionMessage,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawTransactionMessage {
    pub account_keys: Vec<String>,
    pub instructions: Vec<RawInstruction>,
    /// Present in v0 transactions (address lookup tables).
    #[serde(default)]
    pub address_table_lookups: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawInstruction {
    pub program_id_index: u8,
    pub accounts: Vec<u8>,
    pub data: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawTransactionMeta {
    pub err: Option<serde_json::Value>,
    pub fee: u64,
    #[serde(default)]
    pub inner_instructions: Option<Vec<RawInnerInstructionGroup>>,
    #[serde(default)]
    pub log_messages: Option<Vec<String>>,
    #[serde(default)]
    pub pre_token_balances: Option<Vec<RawTokenBalance>>,
    #[serde(default)]
    pub post_token_balances: Option<Vec<RawTokenBalance>>,
    #[serde(default)]
    pub compute_units_consumed: Option<u64>,
    #[serde(default)]
    pub pre_balances: Vec<u64>,
    #[serde(default)]
    pub post_balances: Vec<u64>,
    /// Loaded addresses from address lookup tables (v0 transactions).
    #[serde(default)]
    pub loaded_addresses: Option<RawLoadedAddresses>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawLoadedAddresses {
    #[serde(default)]
    pub writable: Vec<String>,
    #[serde(default)]
    pub readonly: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct RawInnerInstructionGroup {
    pub index: u8,
    pub instructions: Vec<RawInnerInstruction>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawInnerInstruction {
    pub program_id_index: u8,
    pub accounts: Vec<u8>,
    pub data: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawTokenBalance {
    pub account_index: u8,
    pub mint: String,
    pub owner: Option<String>,
    pub ui_token_amount: RawUiTokenAmount,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawUiTokenAmount {
    pub amount: String,
    pub decimals: u8,
    pub ui_amount: Option<f64>,
}

/// RPC error codes for skipped/unavailable slots.
pub const SLOT_SKIPPED_CODE: i64 = -32007;
pub const SLOT_NOT_AVAILABLE_CODE: i64 = -32009;
/// Long-term storage cleanup can also return this.
pub const BLOCK_NOT_AVAILABLE_CODE: i64 = -32004;
