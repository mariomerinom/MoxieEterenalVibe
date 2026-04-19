//! Base (OP Stack L2) block fetcher.
//! 2s blocks, growing MEV landscape, Aerodrome dominant DEX.

use async_trait::async_trait;
use eyre::Result;

use super::evm_fetcher::EvmFetcher;
use super::ChainFetcher;
use crate::types::{BlockData, Chain};

pub struct BaseFetcher {
    inner: EvmFetcher,
}

impl BaseFetcher {
    pub fn new(rpc_http: String, rpc_ws: Option<String>, rate_limit_rps: u32) -> Self {
        Self {
            // Higher concurrency for fast 2s blocks
            inner: EvmFetcher::new(Chain::Base, rpc_http, rpc_ws, rate_limit_rps, 15),
        }
    }
}

#[async_trait]
impl ChainFetcher for BaseFetcher {
    fn chain(&self) -> Chain {
        Chain::Base
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
