use alloy_primitives::{Address, B256, U256};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Identifies which chain data came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Chain {
    Ethereum,
    Base,
    Arbitrum,
    Polygon,
    Scroll,
    Blast,
    Solana,
}

impl Chain {
    pub fn as_str(&self) -> &'static str {
        match self {
            Chain::Ethereum => "ethereum",
            Chain::Base => "base",
            Chain::Arbitrum => "arbitrum",
            Chain::Polygon => "polygon",
            Chain::Scroll => "scroll",
            Chain::Blast => "blast",
            Chain::Solana => "solana",
        }
    }

    pub fn all() -> &'static [Chain] {
        &[
            Chain::Ethereum,
            Chain::Base,
            Chain::Arbitrum,
            Chain::Polygon,
            Chain::Scroll,
            Chain::Blast,
            Chain::Solana,
        ]
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockData {
    pub chain: Chain,
    pub number: u64,
    pub hash: B256,
    pub parent_hash: B256,
    pub timestamp: DateTime<Utc>,
    pub base_fee: Option<U256>,
    pub gas_used: u64,
    pub gas_limit: u64,
    pub tx_count: usize,
    pub transactions: Vec<TransactionData>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionData {
    pub hash: B256,
    pub from: Address,
    pub to: Option<Address>,
    pub value: U256,
    pub gas_price: U256,
    pub max_fee_per_gas: Option<U256>,
    pub max_priority_fee_per_gas: Option<U256>,
    pub gas_used: u64,
    pub tx_index: u64,
    pub input: Vec<u8>,
    pub logs: Vec<LogData>,
    pub success: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogData {
    pub address: Address,
    pub topics: Vec<B256>,
    pub data: Vec<u8>,
    pub log_index: u64,
}

// --- Event types ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwapEvent {
    pub chain: Chain,
    pub block_number: u64,
    pub tx_hash: B256,
    pub tx_index: u64,
    pub log_index: u64,
    pub pool: Address,
    pub protocol: DexProtocol,
    pub token_in: Address,
    pub token_out: Address,
    pub amount_in: U256,
    pub amount_out: U256,
    pub sender: Address,
    /// Transaction originator (actual user/bot). On Polygon/L2s, `sender`
    /// is often a router contract — `tx_from` is the real actor.
    pub tx_from: Address,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum DexProtocol {
    UniswapV2,
    UniswapV3,
    SushiSwap,
    Aerodrome,
    Camelot,
    QuickSwap,
    SyncSwap,
    Thruster,
    Curve,
    Balancer,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiquidationEvent {
    pub chain: Chain,
    pub block_number: u64,
    pub tx_hash: B256,
    pub tx_index: u64,
    pub protocol: LendingProtocol,
    pub liquidator: Address,
    pub borrower: Address,
    pub collateral_asset: Address,
    pub debt_asset: Address,
    pub debt_to_cover: U256,
    pub liquidated_collateral: U256,
    pub gas_used: u64,
    pub gas_price: U256,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum LendingProtocol {
    AaveV3,
    CompoundV3,
    Seamless,
    Moonwell,
    Radiant,
    Orbit,
}

// --- Ground truth types ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MevExtraction {
    pub chain: Chain,
    pub block_number: u64,
    pub tx_hash: B256,
    pub mev_type: MevType,
    pub profit_eth: f64,
    pub gas_cost_eth: f64,
    pub searcher: Address,
    pub source: GroundTruthSource,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum MevType {
    Arbitrage,
    Sandwich,
    Liquidation,
    Backrun,
    JitLiquidity,
    CexDex,
    Unknown,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum GroundTruthSource {
    EigenPhi,
    ZeroMEV,
}

// --- MEV-Share types ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MevShareHint {
    pub hash: B256,
    pub timestamp: DateTime<Utc>,
    pub logs: Vec<LogData>,
    pub to: Option<Address>,
    pub function_selector: Option<[u8; 4]>,
    pub gas_used: Option<u64>,
}
