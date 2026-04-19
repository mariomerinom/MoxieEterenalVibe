//! Arbitrum block fetcher.
//! 0.25s blocks, FCFS sequencer, extremely high throughput.
//! Needs aggressive concurrency for backfill — 4x more blocks per second than Ethereum.

use async_trait::async_trait;
use eyre::Result;

use super::evm_fetcher::EvmFetcher;
use super::ChainFetcher;
use crate::types::{BlockData, Chain};

pub struct ArbitrumFetcher {
    inner: EvmFetcher,
}

impl ArbitrumFetcher {
    pub fn new(rpc_http: String, rpc_ws: Option<String>, rate_limit_rps: u32) -> Self {
        Self {
            // Highest concurrency — 0.25s blocks means 345,600 blocks/day
            inner: EvmFetcher::new(Chain::Arbitrum, rpc_http, rpc_ws, rate_limit_rps, 20),
        }
    }
}

#[async_trait]
impl ChainFetcher for ArbitrumFetcher {
    fn chain(&self) -> Chain {
        Chain::Arbitrum
    }

    async fn fetch_block(&self, number: u64) -> Result<BlockData> {
        self.inner.fetch_block(number).await
    }

    async fn fetch_range(&self, from: u64, to: u64) -> Result<Vec<BlockData>> {
        self.inner.fetch_range(from, to).await
    }

    async fn latest_block(&self) -> Result<u64> {
        self.inner.latest_block().await
    }

    async fn subscribe_new_blocks(&self) -> Result<tokio::sync::mpsc::Receiver<u64>> {
        todo!("Phase 1.3: WebSocket block subscription")
    }
}
