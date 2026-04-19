//! Polygon PoS block fetcher.
//! 2s blocks, Bor consensus, FastLane MEV marketplace.

use async_trait::async_trait;
use eyre::Result;

use super::evm_fetcher::EvmFetcher;
use super::ChainFetcher;
use crate::types::{BlockData, Chain};

pub struct PolygonFetcher {
    inner: EvmFetcher,
}

impl PolygonFetcher {
    pub fn new(rpc_http: String, rpc_ws: Option<String>, rate_limit_rps: u32) -> Self {
        Self {
            inner: EvmFetcher::new(Chain::Polygon, rpc_http, rpc_ws, rate_limit_rps, 15),
        }
    }
}

#[async_trait]
impl ChainFetcher for PolygonFetcher {
    fn chain(&self) -> Chain {
        Chain::Polygon
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
