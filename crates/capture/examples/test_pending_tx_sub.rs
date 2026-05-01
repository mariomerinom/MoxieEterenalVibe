//! Minimal smoke test for the pending-tx WebSocket subscription.
//!
//! Run with:
//!   cargo run -p mev-capture --example test_pending_tx_sub -- ws://127.0.0.1:8546 30
//!
//! Args: <ws_url> <duration_seconds>
//!
//! Subscribes to pending transactions for `duration_seconds`, prints summary
//! stats. Exit code 0 if at least one pending tx was seen, else 1.

use std::time::{Duration, Instant};

use alloy::consensus::Transaction as TxTrait;
use eyre::Result;
use mev_capture::chain::ethereum::EthereumFetcher;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let ws_url = args.get(1).cloned().unwrap_or_else(|| "ws://127.0.0.1:8546".to_string());
    let duration_sec: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(30);

    println!("connecting to {ws_url}, running for {duration_sec}s");

    // HTTP URL doesn't matter for this test; pass the same address with http://.
    let http = ws_url.replace("ws://", "http://").replace("wss://", "https://");
    let fetcher = EthereumFetcher::new(http, Some(ws_url), 25);

    let mut rx = fetcher.subscribe_pending_transactions().await?;

    let deadline = Instant::now() + Duration::from_secs(duration_sec);
    let mut count = 0u64;
    let mut first_seen: Option<Instant> = None;

    loop {
        let remaining = deadline.checked_duration_since(Instant::now());
        let Some(rem) = remaining else { break };

        match tokio::time::timeout(rem, rx.recv()).await {
            Ok(Some(tx)) => {
                count += 1;
                first_seen.get_or_insert_with(Instant::now);
                if count <= 3 {
                    println!(
                        "  tx #{}: hash={:?} to={:?} value={} input_len={}",
                        count,
                        tx.inner.hash(),
                        tx.inner.to(),
                        tx.inner.value(),
                        tx.inner.input().len(),
                    );
                }
            }
            Ok(None) => {
                println!("stream closed");
                break;
            }
            Err(_) => break,
        }
    }

    let elapsed = first_seen.map(|t| t.elapsed().as_secs_f64()).unwrap_or(0.0);
    let rate = if elapsed > 0.0 { count as f64 / elapsed } else { 0.0 };

    println!("\n=== Summary ===");
    println!("  Total pending txs: {count}");
    println!("  Active duration: {elapsed:.1}s");
    println!("  Rate: {rate:.2} txs/sec");

    if count == 0 {
        eprintln!("WARN: no pending txs received — node may not be fully synced or has no peers gossiping");
        std::process::exit(1);
    }
    Ok(())
}
