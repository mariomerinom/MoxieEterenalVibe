//! Ethereum L1 block fetcher.
//! 12s blocks, Flashbots-dominated MEV, mature market.

use async_trait::async_trait;
use eyre::Result;

use super::evm_fetcher::EvmFetcher;
use super::ChainFetcher;
use crate::types::{BlockData, Chain};

pub struct EthereumFetcher {
    inner: EvmFetcher,
}

impl EthereumFetcher {
    pub fn new(rpc_http: String, rpc_ws: Option<String>, rate_limit_rps: u32) -> Self {
        Self {
            inner: EvmFetcher::new(Chain::Ethereum, rpc_http, rpc_ws, rate_limit_rps, 10),
        }
    }
}

#[async_trait]
impl ChainFetcher for EthereumFetcher {
    fn chain(&self) -> Chain {
        Chain::Ethereum
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
        self.inner.subscribe_new_blocks().await
    }
}

impl EthereumFetcher {
    /// Subscribe to pending (mempool) transactions — input feed for
    /// sandwich/JIT/oracle MEV strategies.
    ///
    /// Requires a WebSocket endpoint that exposes `eth_subscribe(
    /// "newPendingTransactions", true)`. Self-hosted Geth does this; many
    /// public RPCs strip pending-tx subscriptions.
    pub async fn subscribe_pending_transactions(
        &self,
    ) -> Result<tokio::sync::mpsc::Receiver<alloy_rpc_types::Transaction>> {
        self.inner.subscribe_pending_transactions().await
    }
}
