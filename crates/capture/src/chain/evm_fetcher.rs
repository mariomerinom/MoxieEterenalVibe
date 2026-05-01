//! Generic EVM-compatible block fetcher.
//!
//! Shared implementation for all EVM chains (Ethereum, Base, Arbitrum, Polygon, Scroll, Blast).
//! Each chain module creates an EvmFetcher with the appropriate Chain variant and concurrency settings.

use std::sync::Arc;
use std::time::Duration;

use alloy::consensus::Transaction as TxTrait;
use alloy::network::{AnyNetwork, ReceiptResponse, TransactionResponse};
use alloy::providers::{Provider, ProviderBuilder, RootProvider, WsConnect};
use alloy_primitives::{B256, U256};
use alloy_rpc_types::{BlockId, BlockNumberOrTag};
use chrono::{DateTime, Utc};
use eyre::{Result, WrapErr};
use futures::{stream, StreamExt};
use tokio::sync::Semaphore;
use tokio::sync::mpsc;

use crate::types::{BlockData, Chain, LogData, TransactionData};

pub struct EvmFetcher {
    chain: Chain,
    provider: RootProvider<AnyNetwork>,
    rate_limiter: Arc<Semaphore>,
    /// Max blocks fetched concurrently in a range request.
    concurrency: usize,
    _rpc_ws: Option<String>,
}

impl EvmFetcher {
    pub fn new(
        chain: Chain,
        rpc_http: String,
        rpc_ws: Option<String>,
        rate_limit_rps: u32,
        concurrency: usize,
    ) -> Self {
        let url = rpc_http.parse().expect("invalid RPC URL");
        let client = alloy::rpc::client::RpcClient::new_http(url);
        let provider = RootProvider::<AnyNetwork>::new(client);

        Self {
            chain,
            provider,
            rate_limiter: Arc::new(Semaphore::new(rate_limit_rps as usize)),
            concurrency,
            _rpc_ws: rpc_ws,
        }
    }

    pub fn chain(&self) -> Chain {
        self.chain
    }

    /// Acquire a rate limit permit that auto-releases after 1 second.
    async fn acquire_permit(&self) {
        let permit = self
            .rate_limiter
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore closed");
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            drop(permit);
        });
    }

    pub async fn fetch_block(&self, number: u64) -> Result<BlockData> {
        let mut last_err = None;
        for attempt in 0..3u32 {
            if attempt > 0 {
                let delay = Duration::from_millis(1000 * 2u64.pow(attempt - 1));
                tracing::warn!(
                    chain = %self.chain.as_str(),
                    block = number,
                    attempt,
                    "retrying after {:?}", delay
                );
                tokio::time::sleep(delay).await;
            }
            match self.try_fetch_block(number).await {
                Ok(block) => return Ok(block),
                Err(e) => {
                    tracing::warn!(
                        chain = %self.chain.as_str(),
                        block = number,
                        attempt,
                        error = %e,
                        "fetch failed"
                    );
                    last_err = Some(e);
                }
            }
        }
        Err(last_err.unwrap())
    }

    async fn try_fetch_block(&self, number: u64) -> Result<BlockData> {
        self.acquire_permit().await;

        let block = self
            .provider
            .get_block_by_number(BlockNumberOrTag::Number(number))
            .full()
            .await
            .wrap_err_with(|| format!("[{}] get_block_by_number({number})", self.chain.as_str()))?
            .ok_or_else(|| {
                eyre::eyre!("[{}] block {number} not found", self.chain.as_str())
            })?;

        self.acquire_permit().await;

        let receipts = self
            .provider
            .get_block_receipts(BlockId::Number(BlockNumberOrTag::Number(number)))
            .await
            .wrap_err_with(|| format!("[{}] get_block_receipts({number})", self.chain.as_str()))?
            .unwrap_or_default();

        // Receipt lookup by tx hash
        let receipt_map: std::collections::HashMap<B256, _> =
            receipts.iter().map(|r| (r.transaction_hash, r)).collect();

        let timestamp = DateTime::<Utc>::from_timestamp(block.header.timestamp as i64, 0)
            .unwrap_or_default();

        let full_txs: Vec<_> = block.transactions.clone().into_transactions().collect();
        let mut transactions = Vec::with_capacity(full_txs.len());

        for (idx, tx) in full_txs.iter().enumerate() {
            let tx_hash = tx.tx_hash();
            let receipt = receipt_map.get(&tx_hash);

            let gas_used = receipt.map(|r| r.gas_used as u64).unwrap_or(0);
            let success = receipt.map(|r| r.inner.status()).unwrap_or(false);

            let logs: Vec<LogData> = receipt
                .map(|r| {
                    r.inner
                        .logs()
                        .iter()
                        .enumerate()
                        .map(|(li, log)| LogData {
                            address: log.address(),
                            topics: log.topics().to_vec(),
                            data: log.data().data.to_vec(),
                            log_index: li as u64,
                        })
                        .collect()
                })
                .unwrap_or_default();

            transactions.push(TransactionData {
                hash: tx_hash,
                from: tx.from(),
                to: tx.to(),
                value: tx.value(),
                gas_price: U256::from(TransactionResponse::gas_price(tx).unwrap_or(0)),
                max_fee_per_gas: {
                    let v = TxTrait::max_fee_per_gas(tx);
                    if v > 0 {
                        Some(U256::from(v))
                    } else {
                        None
                    }
                },
                max_priority_fee_per_gas: TxTrait::max_priority_fee_per_gas(tx)
                    .map(|v| U256::from(v)),
                gas_used,
                tx_index: idx as u64,
                input: tx.input().to_vec(),
                logs,
                success,
            });
        }

        Ok(BlockData {
            chain: self.chain,
            number,
            hash: block.header.hash,
            parent_hash: block.header.parent_hash,
            timestamp,
            base_fee: block.header.base_fee_per_gas.map(U256::from),
            gas_used: block.header.gas_used as u64,
            gas_limit: block.header.gas_limit as u64,
            tx_count: transactions.len(),
            transactions,
        })
    }

    pub async fn fetch_range(&self, from: u64, to: u64) -> Result<Vec<BlockData>> {
        let results: Vec<Result<BlockData>> = stream::iter(from..=to)
            .map(|n| async move { self.fetch_block(n).await })
            .buffer_unordered(self.concurrency)
            .collect()
            .await;

        let mut blocks = Vec::with_capacity(results.len());
        for r in results {
            blocks.push(r?);
        }
        blocks.sort_by_key(|b| b.number);
        Ok(blocks)
    }

    pub async fn latest_block(&self) -> Result<u64> {
        Ok(self.provider.get_block_number().await?)
    }

    /// Subscribe to new block numbers via WebSocket.
    ///
    /// Returns a receiver yielding block numbers as they're produced.
    /// The internal task owns the WS provider so it stays connected
    /// for as long as the receiver is held.
    pub async fn subscribe_new_blocks(&self) -> Result<mpsc::Receiver<u64>> {
        let ws_url = self
            ._rpc_ws
            .clone()
            .ok_or_else(|| eyre::eyre!("[{}] WS URL not configured", self.chain.as_str()))?;

        let ws = WsConnect::new(ws_url);
        let provider = ProviderBuilder::new()
            .network::<AnyNetwork>()
            .connect_ws(ws)
            .await
            .wrap_err("failed to connect WS provider")?;

        let sub = provider
            .subscribe_blocks()
            .await
            .wrap_err("subscribe_blocks failed")?;

        let (tx, rx) = mpsc::channel::<u64>(256);
        let chain = self.chain;

        tokio::spawn(async move {
            // Hold provider alive: dropping it would close the WS transport.
            let _provider_guard = provider;
            let mut stream = sub.into_stream();
            while let Some(header) = stream.next().await {
                let n = header.number;
                if tx.send(n).await.is_err() {
                    tracing::debug!(chain = %chain.as_str(), "block subscriber dropped, exiting");
                    break;
                }
            }
            tracing::warn!(chain = %chain.as_str(), "WS block stream ended");
        });

        Ok(rx)
    }

    /// Subscribe to pending (mempool) transactions in **full** form.
    ///
    /// Each item is a full transaction object (hash, from, to, value, input,
    /// gas, gas price, nonce, etc.). This is the input to sandwich/JIT/oracle
    /// MEV strategies.
    ///
    /// Performance: pending tx volume on Ethereum mainnet is ~10/s (public mempool
    /// only — much higher with private order flow access). The 8192-deep channel
    /// gives ~13min of buffering before back-pressuring.
    ///
    /// Returns receiver yielding alloy `Transaction` (full RPC representation).
    pub async fn subscribe_pending_transactions(
        &self,
    ) -> Result<mpsc::Receiver<alloy_rpc_types::Transaction>> {
        let ws_url = self
            ._rpc_ws
            .clone()
            .ok_or_else(|| eyre::eyre!("[{}] WS URL not configured", self.chain.as_str()))?;

        let ws = WsConnect::new(ws_url);
        let provider = ProviderBuilder::new()
            .connect_ws(ws)
            .await
            .wrap_err("failed to connect WS provider")?;

        let sub = provider
            .subscribe_full_pending_transactions()
            .await
            .wrap_err("subscribe_full_pending_transactions failed")?;

        let (tx, rx) = mpsc::channel::<alloy_rpc_types::Transaction>(8192);
        let chain = self.chain;

        tokio::spawn(async move {
            // Hold provider alive: dropping it would close the WS transport.
            let _provider_guard = provider;
            let mut stream = sub.into_stream();
            let mut count = 0u64;
            let mut last_log = std::time::Instant::now();
            while let Some(pending_tx) = stream.next().await {
                count += 1;
                if last_log.elapsed() > Duration::from_secs(60) {
                    let rate = count as f64 / last_log.elapsed().as_secs_f64();
                    tracing::info!(
                        chain = %chain.as_str(),
                        count,
                        rate_per_sec = rate,
                        "pending tx stream"
                    );
                    last_log = std::time::Instant::now();
                    count = 0;
                }
                if tx.send(pending_tx).await.is_err() {
                    tracing::debug!(chain = %chain.as_str(), "pending-tx subscriber dropped");
                    break;
                }
            }
            tracing::warn!(chain = %chain.as_str(), "WS pending-tx stream ended");
        });

        Ok(rx)
    }
}
