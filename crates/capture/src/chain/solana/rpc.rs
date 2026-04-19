//! Solana JSON-RPC client.
//!
//! Lightweight HTTP client using reqwest — avoids pulling the heavy solana-sdk dependency.
//! Rate limiting via tokio Semaphore (same pattern as EvmFetcher).

use std::sync::Arc;
use std::time::Duration;

use eyre::{Result, WrapErr, bail};
use reqwest::Client;
use serde_json::{json, Value};
use tokio::sync::Semaphore;
use tracing::{debug, warn};

use super::rpc_types::*;

pub struct SolanaRpcClient {
    client: Client,
    rpc_url: String,
    rate_limiter: Arc<Semaphore>,
}

impl SolanaRpcClient {
    pub fn new(rpc_url: String, rate_limit_rps: u32) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .expect("failed to build HTTP client");

        Self {
            client,
            rpc_url,
            rate_limiter: Arc::new(Semaphore::new(rate_limit_rps as usize)),
        }
    }

    /// Acquire a rate-limit permit that auto-releases after 1 second.
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

    /// Send a JSON-RPC request and return the raw response value.
    async fn rpc_call(&self, method: &str, params: Value) -> Result<Value> {
        self.acquire_permit().await;

        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });

        let resp = self
            .client
            .post(&self.rpc_url)
            .json(&body)
            .send()
            .await
            .wrap_err_with(|| format!("RPC request to {} failed", method))?;

        let status = resp.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            bail!("rate limited (429)");
        }

        resp.json::<Value>()
            .await
            .wrap_err("failed to parse RPC response")
    }

    /// Get the latest confirmed slot number.
    pub async fn get_slot(&self) -> Result<u64> {
        let resp = self.rpc_call("getSlot", json!([{"commitment": "confirmed"}])).await?;

        if let Some(err) = resp.get("error") {
            bail!("getSlot error: {}", err);
        }

        resp["result"]
            .as_u64()
            .ok_or_else(|| eyre::eyre!("getSlot: unexpected result format"))
    }

    /// Fetch a full block by slot number.
    ///
    /// Returns `Ok(None)` for skipped/unavailable slots.
    /// Returns `Ok(Some(block))` for confirmed blocks.
    pub async fn get_block(&self, slot: u64) -> Result<Option<RawSolanaBlock>> {
        let params = json!([
            slot,
            {
                "encoding": "json",
                "transactionDetails": "full",
                "rewards": false,
                "maxSupportedTransactionVersion": 0
            }
        ]);

        let resp = self.rpc_call("getBlock", params).await?;

        // Check for skipped/unavailable slot errors
        if let Some(error) = resp.get("error") {
            let code = error.get("code").and_then(|c| c.as_i64()).unwrap_or(0);
            if code == SLOT_SKIPPED_CODE
                || code == SLOT_NOT_AVAILABLE_CODE
                || code == BLOCK_NOT_AVAILABLE_CODE
            {
                debug!(slot, "slot skipped or unavailable");
                return Ok(None);
            }
            let msg = error.get("message").and_then(|m| m.as_str()).unwrap_or("unknown");
            bail!("getBlock error for slot {}: code={} msg={}", slot, code, msg);
        }

        match resp.get("result") {
            Some(Value::Null) | None => Ok(None),
            Some(result) => {
                let block: RawSolanaBlock = serde_json::from_value(result.clone())
                    .wrap_err_with(|| format!("failed to parse block at slot {}", slot))?;
                Ok(Some(block))
            }
        }
    }

    /// Retry wrapper: attempts up to 3 times with exponential backoff.
    pub async fn get_block_with_retry(&self, slot: u64) -> Result<Option<RawSolanaBlock>> {
        let mut last_err = None;
        for attempt in 0..3u32 {
            match self.get_block(slot).await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    let delay = Duration::from_secs(1 << attempt);
                    warn!(slot, attempt, delay_secs = delay.as_secs(), error = %e, "retrying getBlock");
                    last_err = Some(e);
                    tokio::time::sleep(delay).await;
                }
            }
        }
        Err(last_err.unwrap())
    }
}
