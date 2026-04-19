//! mev-ingest: CLI binary for block ingestion and backfill.
//!
//! Usage:
//!   mev-ingest backfill --chain ethereum --days 7
//!   mev-ingest backfill --chain ethereum --from 21500000 --to 21550400
//!   mev-ingest backfill --all --days 7        # All enabled chains in parallel
//!   mev-ingest status --chain ethereum
//!   mev-ingest status --all

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use eyre::Result;
use tracing_subscriber::EnvFilter;

use mev_capture::chain;
use mev_capture::checkpoint::Checkpoint;
use mev_capture::config::CaptureConfig;
use mev_capture::events;
use mev_capture::events::solana_swaps;
use mev_capture::solana_storage::SolanaStorage;
use mev_capture::storage::Storage;
use mev_capture::types::Chain;
use mev_capture::chain::solana::constants::SLOTS_PER_DAY;

const BATCH_SIZE: u64 = 100;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,mev_capture=debug")),
        )
        .init();

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        print_usage();
        return Ok(());
    }

    match args[1].as_str() {
        "backfill" => cmd_backfill(&args[2..]).await,
        "status" => cmd_status(&args[2..]),
        _ => {
            print_usage();
            Ok(())
        }
    }
}

fn print_usage() {
    eprintln!("Usage:");
    eprintln!("  mev-ingest backfill --chain ethereum --days 7");
    eprintln!("  mev-ingest backfill --chain ethereum --from <N> --to <M>");
    eprintln!("  mev-ingest backfill --all --days 7");
    eprintln!("  mev-ingest status --chain ethereum");
    eprintln!("  mev-ingest status --all");
}

// ================================================================
// Arg parsing
// ================================================================

struct BackfillArgs {
    chain_name: Option<String>,
    all_chains: bool,
    days: Option<u64>,
    from_block: Option<u64>,
    to_block: Option<u64>,
}

fn parse_backfill_args(args: &[String]) -> BackfillArgs {
    let mut result = BackfillArgs {
        chain_name: None,
        all_chains: false,
        days: None,
        from_block: None,
        to_block: None,
    };

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--chain" => {
                result.chain_name = args.get(i + 1).cloned();
                i += 2;
            }
            "--all" => {
                result.all_chains = true;
                i += 1;
            }
            "--days" => {
                result.days = args.get(i + 1).and_then(|s| s.parse().ok());
                i += 2;
            }
            "--from" => {
                result.from_block = args.get(i + 1).and_then(|s| s.parse().ok());
                i += 2;
            }
            "--to" => {
                result.to_block = args.get(i + 1).and_then(|s| s.parse().ok());
                i += 2;
            }
            _ => i += 1,
        }
    }

    result
}

// ================================================================
// Backfill command
// ================================================================

async fn cmd_backfill(args: &[String]) -> Result<()> {
    let parsed = parse_backfill_args(args);

    let config_path = PathBuf::from("config/chains.toml");
    let config = CaptureConfig::load(&config_path)?;

    if parsed.all_chains {
        cmd_backfill_all(&config, parsed.days.unwrap_or(7)).await
    } else {
        let chain_name = parsed.chain_name.unwrap_or_else(|| "ethereum".to_string());
        cmd_backfill_single(
            &config,
            &chain_name,
            parsed.days,
            parsed.from_block,
            parsed.to_block,
        )
        .await
    }
}

/// Backfill all enabled chains in parallel.
async fn cmd_backfill_all(config: &CaptureConfig, days: u64) -> Result<()> {
    let enabled = config.enabled_chains();
    if enabled.is_empty() {
        tracing::warn!("no chains enabled in config");
        return Ok(());
    }

    tracing::info!(
        chains = enabled.len(),
        days,
        "starting parallel backfill for all enabled chains"
    );

    let data_dir = PathBuf::from(&config.storage.data_dir);
    let duckdb_path = PathBuf::from(&config.storage.duckdb_path);
    let storage = Arc::new(Mutex::new(Storage::new(
        data_dir.clone(),
        duckdb_path,
        config.storage.buffer_size,
    )));

    let mut handles: Vec<(String, tokio::task::JoinHandle<Result<BackfillStats>>)> = Vec::new();

    for (chain, chain_config) in &enabled {
        let chain = *chain;
        let chain_name = chain.as_str().to_string();
        let rpc_http = chain_config.rpc_http.clone();
        let rpc_ws = chain_config.rpc_ws.clone();
        let rps = chain_config.requests_per_second;
        let block_time_ms = chain_config.block_time_ms;
        let data_dir = data_dir.clone();
        let buffer_size = config.storage.buffer_size;

        if chain == Chain::Solana {
            // Solana uses its own fetcher and storage — separate code path
            let chain_name_inner = chain_name.clone();
            let handle = tokio::spawn(async move {
                match backfill_solana_chain(
                    &chain_name_inner,
                    &rpc_http,
                    rps,
                    days,
                    None,
                    None,
                    &data_dir,
                    buffer_size,
                )
                .await
                {
                    Ok(stats) => {
                        tracing::info!(
                            chain = %chain_name_inner,
                            blocks = stats.total_blocks,
                            swaps = stats.total_swaps,
                            slots_scanned = stats.slots_scanned,
                            elapsed = format!("{:.1}s", stats.elapsed_secs),
                            "chain backfill complete"
                        );
                        Ok(BackfillStats {
                            total_blocks: stats.total_blocks,
                            total_swaps: stats.total_swaps,
                            total_liquidations: 0,
                            elapsed_secs: stats.elapsed_secs,
                        })
                    }
                    Err(e) => {
                        tracing::error!(chain = %chain_name_inner, error = %e, "chain backfill failed");
                        Err(e)
                    }
                }
            });
            handles.push((chain_name, handle));
            continue;
        }

        let storage = Arc::clone(&storage);
        let chain_name_inner = chain_name.clone();

        let handle = tokio::spawn(async move {
            match backfill_chain(
                chain,
                &chain_name_inner,
                &rpc_http,
                rpc_ws.as_deref(),
                rps,
                block_time_ms,
                days,
                None,
                None,
                storage,
                &data_dir,
                buffer_size,
            )
            .await
            {
                Ok(stats) => {
                    tracing::info!(
                        chain = %chain_name_inner,
                        blocks = stats.total_blocks,
                        swaps = stats.total_swaps,
                        liquidations = stats.total_liquidations,
                        elapsed = format!("{:.1}s", stats.elapsed_secs),
                        "chain backfill complete"
                    );
                    Ok(stats)
                }
                Err(e) => {
                    tracing::error!(chain = %chain_name_inner, error = %e, "chain backfill failed");
                    Err(e)
                }
            }
        });

        handles.push((chain_name, handle));
    }

    // Wait for all chains
    let mut total_blocks = 0u64;
    let mut total_swaps = 0u64;
    let mut total_liquidations = 0u64;
    let mut failures = Vec::new();

    for (name, handle) in handles {
        match handle.await {
            Ok(Ok(stats)) => {
                total_blocks += stats.total_blocks;
                total_swaps += stats.total_swaps;
                total_liquidations += stats.total_liquidations;
            }
            Ok(Err(e)) => failures.push((name, format!("{e}"))),
            Err(e) => failures.push((name, format!("task panic: {e}"))),
        }
    }

    // Final flush
    {
        let mut s = storage.lock().unwrap();
        s.flush_all()?;
        if let Err(e) = s.init_duckdb_views() {
            tracing::warn!(error = %e, "DuckDB view init failed");
        }
    }

    tracing::info!(
        total_blocks,
        total_swaps,
        total_liquidations,
        failed_chains = failures.len(),
        "all-chain backfill complete"
    );

    for (name, err) in &failures {
        tracing::error!(chain = %name, error = %err, "chain failed");
    }

    if !failures.is_empty() {
        eyre::bail!(
            "{} chain(s) failed: {}",
            failures.len(),
            failures
                .iter()
                .map(|(n, _)| n.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    Ok(())
}

/// Backfill a single chain.
async fn cmd_backfill_single(
    config: &CaptureConfig,
    chain_name: &str,
    days: Option<u64>,
    from_block: Option<u64>,
    to_block: Option<u64>,
) -> Result<()> {
    let chain_config = config
        .chains
        .get(chain_name)
        .ok_or_else(|| eyre::eyre!("chain '{}' not found in config", chain_name))?;

    let chain = parse_chain_name(chain_name)?;
    let data_dir = PathBuf::from(&config.storage.data_dir);
    let duckdb_path = PathBuf::from(&config.storage.duckdb_path);

    // Solana uses a separate code path (different types, no ChainFetcher trait)
    if chain == Chain::Solana {
        let stats = backfill_solana_chain(
            chain_name,
            &chain_config.rpc_http,
            chain_config.requests_per_second,
            days.unwrap_or(1),
            from_block,
            to_block,
            &data_dir,
            config.storage.buffer_size,
        )
        .await?;

        // Init DuckDB views for Solana
        let storage = Storage::new(data_dir, duckdb_path, config.storage.buffer_size);
        if let Err(e) = storage.init_duckdb_views() {
            tracing::warn!(error = %e, "DuckDB view init failed");
        }

        tracing::info!(
            chain = %chain_name,
            blocks = stats.total_blocks,
            swaps = stats.total_swaps,
            slots_scanned = stats.slots_scanned,
            elapsed = format!("{:.1}s", stats.elapsed_secs),
            "backfill complete"
        );
        return Ok(());
    }

    let storage = Arc::new(Mutex::new(Storage::new(
        data_dir.clone(),
        duckdb_path,
        config.storage.buffer_size,
    )));

    let stats = backfill_chain(
        chain,
        chain_name,
        &chain_config.rpc_http,
        chain_config.rpc_ws.as_deref(),
        chain_config.requests_per_second,
        chain_config.block_time_ms,
        days.unwrap_or(1),
        from_block,
        to_block,
        Arc::clone(&storage),
        &data_dir,
        config.storage.buffer_size,
    )
    .await?;

    // Final flush
    {
        let mut s = storage.lock().unwrap();
        s.flush_all()?;
        if let Err(e) = s.init_duckdb_views() {
            tracing::warn!(error = %e, "DuckDB view init failed");
        }
    }

    tracing::info!(
        chain = %chain_name,
        blocks = stats.total_blocks,
        swaps = stats.total_swaps,
        liquidations = stats.total_liquidations,
        elapsed = format!("{:.1}s", stats.elapsed_secs),
        blocks_per_sec = format!("{:.1}", stats.total_blocks as f64 / stats.elapsed_secs),
        "backfill complete"
    );

    Ok(())
}

struct BackfillStats {
    total_blocks: u64,
    total_swaps: u64,
    total_liquidations: u64,
    elapsed_secs: f64,
}

async fn backfill_chain(
    chain: Chain,
    chain_name: &str,
    rpc_http: &str,
    rpc_ws: Option<&str>,
    rps: u32,
    block_time_ms: u64,
    days: u64,
    from_block: Option<u64>,
    to_block: Option<u64>,
    storage: Arc<Mutex<Storage>>,
    data_dir: &PathBuf,
    _buffer_size: usize,
) -> Result<BackfillStats> {
    // Validate RPC URL is set (env var resolved)
    if rpc_http.is_empty() {
        eyre::bail!(
            "[{}] RPC URL is empty — set the env var in .env",
            chain_name
        );
    }

    let fetcher = chain::create_fetcher(chain, rpc_http, rpc_ws, rps);

    // Determine block range
    let latest = fetcher.latest_block().await?;
    tracing::info!(chain = %chain_name, latest_block = latest, "connected to RPC");

    let (range_from, range_to) = if let (Some(f), Some(t)) = (from_block, to_block) {
        (f, t)
    } else {
        let blocks_per_day = 86400 * 1000 / block_time_ms;
        let from = latest.saturating_sub(blocks_per_day * days);
        (from, latest)
    };

    tracing::info!(
        chain = %chain_name,
        from = range_from,
        to = range_to,
        blocks = range_to - range_from + 1,
        "backfill range"
    );

    // Checkpoint
    let checkpoint_dir = data_dir.join("checkpoints");
    let checkpoint_path = checkpoint_dir.join(format!("{chain_name}_backfill.json"));
    let mut checkpoint = match Checkpoint::load(&checkpoint_path)? {
        Some(cp)
            if cp.target_from == range_from
                && cp.target_to == range_to
                && !cp.is_complete() =>
        {
            tracing::info!(
                chain = %chain_name,
                resuming_from = cp.next_block(),
                remaining = cp.remaining(),
                "resuming from checkpoint"
            );
            cp
        }
        _ => Checkpoint::new(chain_name, range_from, range_to),
    };

    let start_time = Instant::now();
    let mut total_blocks = 0u64;
    let mut total_swaps = 0u64;
    let mut total_liquidations = 0u64;
    let start_block = checkpoint.next_block();

    let mut cursor = start_block;
    while cursor <= range_to {
        let batch_end = (cursor + BATCH_SIZE - 1).min(range_to);
        let batch_start = Instant::now();

        let blocks = fetcher.fetch_range(cursor, batch_end).await?;

        let mut batch_swaps = 0;
        let mut batch_liquidations = 0;

        for block in blocks {
            let extracted = events::extract_all_events(&block)?;
            batch_swaps += extracted.swaps.len();
            batch_liquidations += extracted.liquidations.len();

            // Lock storage briefly per block
            {
                let mut s = storage.lock().unwrap();
                s.buffer_swaps(extracted.swaps)?;
                s.buffer_liquidations(extracted.liquidations)?;
                s.buffer_block(block)?;
            }
        }

        let batch_count = batch_end - cursor + 1;
        total_blocks += batch_count;
        total_swaps += batch_swaps as u64;
        total_liquidations += batch_liquidations as u64;

        checkpoint.last_completed_block = batch_end;
        checkpoint.save(&checkpoint_path)?;

        let elapsed = start_time.elapsed().as_secs_f64();
        let blocks_per_sec = total_blocks as f64 / elapsed;
        let remaining = range_to.saturating_sub(batch_end);
        let eta_secs = if blocks_per_sec > 0.0 {
            remaining as f64 / blocks_per_sec
        } else {
            0.0
        };

        tracing::info!(
            chain = %chain_name,
            block_range = %format!("{cursor}..{batch_end}"),
            batch_ms = batch_start.elapsed().as_millis(),
            total_blocks,
            swaps = batch_swaps,
            liquidations = batch_liquidations,
            blocks_per_sec = format!("{blocks_per_sec:.1}"),
            remaining,
            eta = format!("{:.0}s", eta_secs),
            "batch complete"
        );

        cursor = batch_end + 1;
    }

    Ok(BackfillStats {
        total_blocks,
        total_swaps,
        total_liquidations,
        elapsed_secs: start_time.elapsed().as_secs_f64(),
    })
}

// ================================================================
// Solana backfill (separate pipeline — different types)
// ================================================================

struct SolanaBackfillStats {
    total_blocks: u64,
    total_swaps: u64,
    slots_scanned: u64,
    elapsed_secs: f64,
}

async fn backfill_solana_chain(
    chain_name: &str,
    rpc_http: &str,
    rps: u32,
    days: u64,
    from_slot: Option<u64>,
    to_slot: Option<u64>,
    data_dir: &PathBuf,
    buffer_size: usize,
) -> Result<SolanaBackfillStats> {
    if rpc_http.is_empty() {
        eyre::bail!("[{}] RPC URL is empty — set SOLANA_RPC_HTTP in .env", chain_name);
    }

    let fetcher = chain::create_solana_fetcher(rpc_http, rps);

    let latest = fetcher.latest_slot().await?;
    tracing::info!(chain = %chain_name, latest_slot = latest, "connected to Solana RPC");

    let (range_from, range_to) = if let (Some(f), Some(t)) = (from_slot, to_slot) {
        (f, t)
    } else {
        let from = latest.saturating_sub(SLOTS_PER_DAY * days);
        (from, latest)
    };

    tracing::info!(
        chain = %chain_name,
        from = range_from,
        to = range_to,
        slots = range_to - range_from + 1,
        "solana backfill range"
    );

    // Checkpoint
    let checkpoint_dir = data_dir.join("checkpoints");
    let checkpoint_path = checkpoint_dir.join(format!("{chain_name}_backfill.json"));
    let mut checkpoint = match Checkpoint::load(&checkpoint_path)? {
        Some(cp)
            if cp.target_from == range_from
                && cp.target_to == range_to
                && !cp.is_complete() =>
        {
            tracing::info!(
                chain = %chain_name,
                resuming_from = cp.next_block(),
                remaining = cp.remaining(),
                "resuming solana from checkpoint"
            );
            cp
        }
        _ => Checkpoint::new(chain_name, range_from, range_to),
    };

    let mut storage = SolanaStorage::new(data_dir.clone(), buffer_size);

    let start_time = Instant::now();
    let mut total_blocks = 0u64;
    let mut total_swaps = 0u64;
    let mut total_slots_scanned = 0u64;
    let start_slot = checkpoint.next_block();

    let mut cursor = start_slot;
    while cursor <= range_to {
        let batch_end = (cursor + BATCH_SIZE - 1).min(range_to);
        let batch_start = Instant::now();

        let (blocks, _total_slots) = fetcher.fetch_range(cursor, batch_end).await?;

        let batch_blocks = blocks.len() as u64;
        let mut batch_swaps = 0usize;

        for block in blocks {
            let swaps = solana_swaps::parse_solana_swaps(&block);
            batch_swaps += swaps.len();
            storage.buffer_swaps(swaps)?;
            storage.buffer_block(block)?;
        }

        let batch_slots = batch_end - cursor + 1;
        total_slots_scanned += batch_slots;
        total_blocks += batch_blocks;
        total_swaps += batch_swaps as u64;

        checkpoint.last_completed_block = batch_end;
        checkpoint.save(&checkpoint_path)?;

        let elapsed = start_time.elapsed().as_secs_f64();
        let slots_per_sec = total_slots_scanned as f64 / elapsed;
        let remaining = range_to.saturating_sub(batch_end);
        let eta_secs = if slots_per_sec > 0.0 {
            remaining as f64 / slots_per_sec
        } else {
            0.0
        };

        tracing::info!(
            chain = %chain_name,
            slot_range = %format!("{cursor}..{batch_end}"),
            batch_ms = batch_start.elapsed().as_millis(),
            blocks_found = batch_blocks,
            skipped = batch_slots - batch_blocks,
            swaps = batch_swaps,
            slots_per_sec = format!("{slots_per_sec:.1}"),
            remaining,
            eta = format!("{:.0}s", eta_secs),
            "batch complete"
        );

        cursor = batch_end + 1;
    }

    storage.flush_all()?;

    Ok(SolanaBackfillStats {
        total_blocks,
        total_swaps,
        slots_scanned: total_slots_scanned,
        elapsed_secs: start_time.elapsed().as_secs_f64(),
    })
}

fn parse_chain_name(name: &str) -> Result<Chain> {
    match name {
        "ethereum" => Ok(Chain::Ethereum),
        "base" => Ok(Chain::Base),
        "arbitrum" => Ok(Chain::Arbitrum),
        "polygon" => Ok(Chain::Polygon),
        "scroll" => Ok(Chain::Scroll),
        "blast" => Ok(Chain::Blast),
        "solana" => Ok(Chain::Solana),
        _ => eyre::bail!("unknown chain: {}", name),
    }
}

// ================================================================
// Status command
// ================================================================

fn cmd_status(args: &[String]) -> Result<()> {
    let config_path = PathBuf::from("config/chains.toml");
    let config = CaptureConfig::load(&config_path)?;

    let show_all = args.iter().any(|a| a == "--all");

    let chains: Vec<String> = if show_all {
        config
            .enabled_chains()
            .iter()
            .map(|(c, _)| c.as_str().to_string())
            .collect()
    } else {
        let mut chain_name = "ethereum".to_string();
        let mut i = 0;
        while i < args.len() {
            if args[i] == "--chain" {
                chain_name = args.get(i + 1).cloned().unwrap_or_default();
                i += 2;
            } else {
                i += 1;
            }
        }
        vec![chain_name]
    };

    for chain_name in &chains {
        let checkpoint_path = PathBuf::from(&config.storage.data_dir)
            .join("checkpoints")
            .join(format!("{chain_name}_backfill.json"));

        match Checkpoint::load(&checkpoint_path)? {
            Some(cp) => {
                let pct = if cp.target_to > cp.target_from {
                    let done = cp.last_completed_block.saturating_sub(cp.target_from);
                    let total = cp.target_to - cp.target_from;
                    (done as f64 / total as f64 * 100.0).min(100.0)
                } else {
                    100.0
                };
                println!(
                    "{:<12} {:>10} .. {:<10}  completed: {:<10}  remaining: {:<8}  {:.1}%  {}",
                    cp.chain,
                    cp.target_from,
                    cp.target_to,
                    cp.last_completed_block,
                    cp.remaining(),
                    pct,
                    if cp.is_complete() {
                        "✓ DONE"
                    } else {
                        "⏳ IN PROGRESS"
                    }
                );
            }
            None => {
                println!("{:<12} no checkpoint", chain_name);
            }
        }
    }

    Ok(())
}
