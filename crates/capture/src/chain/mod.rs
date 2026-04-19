pub mod evm_fetcher;
pub mod ethereum;
pub mod base;
pub mod arbitrum;
pub mod polygon;
pub mod scroll;
pub mod blast;
pub mod solana;

use async_trait::async_trait;
use eyre::Result;
use crate::types::{BlockData, Chain};

/// Trait that all chain-specific fetchers implement.
/// Abstracts away chain differences (RPC quirks, block times, receipt formats).
#[async_trait]
pub trait ChainFetcher: Send + Sync {
    fn chain(&self) -> Chain;

    /// Fetch a single block with all transactions and receipts.
    async fn fetch_block(&self, number: u64) -> Result<BlockData>;

    /// Fetch a range of blocks. Implementations should handle rate limiting.
    async fn fetch_range(&self, from: u64, to: u64) -> Result<Vec<BlockData>> {
        let mut blocks = Vec::with_capacity((to - from + 1) as usize);
        for n in from..=to {
            blocks.push(self.fetch_block(n).await?);
        }
        Ok(blocks)
    }

    /// Get the latest block number on this chain.
    async fn latest_block(&self) -> Result<u64>;

    /// Subscribe to new blocks via WebSocket (for live streaming mode).
    /// Returns a receiver that yields new block numbers as they appear.
    async fn subscribe_new_blocks(
        &self,
    ) -> Result<tokio::sync::mpsc::Receiver<u64>>;
}

/// Create a chain fetcher from config.
pub fn create_fetcher(
    chain: Chain,
    rpc_http: &str,
    rpc_ws: Option<&str>,
    rate_limit_rps: u32,
) -> Box<dyn ChainFetcher> {
    match chain {
        Chain::Ethereum => Box::new(ethereum::EthereumFetcher::new(
            rpc_http.to_string(),
            rpc_ws.map(|s| s.to_string()),
            rate_limit_rps,
        )),
        Chain::Base => Box::new(base::BaseFetcher::new(
            rpc_http.to_string(),
            rpc_ws.map(|s| s.to_string()),
            rate_limit_rps,
        )),
        Chain::Arbitrum => Box::new(arbitrum::ArbitrumFetcher::new(
            rpc_http.to_string(),
            rpc_ws.map(|s| s.to_string()),
            rate_limit_rps,
        )),
        Chain::Polygon => Box::new(polygon::PolygonFetcher::new(
            rpc_http.to_string(),
            rpc_ws.map(|s| s.to_string()),
            rate_limit_rps,
        )),
        Chain::Scroll => Box::new(scroll::ScrollFetcher::new(
            rpc_http.to_string(),
            rpc_ws.map(|s| s.to_string()),
            rate_limit_rps,
        )),
        Chain::Blast => Box::new(blast::BlastFetcher::new(
            rpc_http.to_string(),
            rpc_ws.map(|s| s.to_string()),
            rate_limit_rps,
        )),
        Chain::Solana => panic!("Use create_solana_fetcher() for Solana — it does not implement ChainFetcher"),
    }
}

/// Create a Solana fetcher (separate from EVM fetcher — different return types).
pub fn create_solana_fetcher(
    rpc_http: &str,
    rate_limit_rps: u32,
) -> solana::SolanaFetcher {
    solana::SolanaFetcher::new(rpc_http.to_string(), rate_limit_rps, 8)
}
