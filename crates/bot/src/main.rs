//! MEV Bot orchestrator.
//!
//! Subscribes to new blocks, runs strategies, simulates, and executes (or dry-runs) bundles.

use alloy_primitives::{Address, U256};
use eyre::{bail, Result};
use futures::StreamExt;
use std::str::FromStr;
use std::time::Instant;
use tracing::{error, info, warn};

use mev_executor::bundle_builder::BundleBuilder;
use mev_executor::dry_run::{DryRunEntry, DryRunExecutor};
use mev_executor::flashbots::{FlashbotsExecutor, SigningKey};
use mev_simulator::replay::BlockReplay;
use mev_strategies::dex_arb::DexArbStrategy;
use mev_strategies::pool_graph::{self, weth_for_chain};
use mev_strategies::traits::{Event, Strategy};

#[derive(Debug, Clone, PartialEq)]
enum Mode {
    DryRun,
    Live,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,mev_bot=debug,mev_strategies=debug,mev_simulator=info".into()),
        )
        .init();

    dotenvy::dotenv().ok();

    // Parse CLI args
    let args: Vec<String> = std::env::args().collect();
    let mode = if args.iter().any(|a| a == "--mode" || a == "-m") {
        let idx = args.iter().position(|a| a == "--mode" || a == "-m").unwrap();
        match args.get(idx + 1).map(|s| s.as_str()) {
            Some("live") => Mode::Live,
            Some("dry-run") | _ => Mode::DryRun,
        }
    } else {
        Mode::DryRun
    };

    // --simulate: cross-check each dry-run signal with EVM simulation
    let simulate = args.iter().any(|a| a == "--simulate");

    // Parse chain from --chain flag or MEV_CHAIN env var
    let chain = if let Some(idx) = args.iter().position(|a| a == "--chain") {
        match args.get(idx + 1).map(|s| s.as_str()) {
            Some("base") => mev_capture::types::Chain::Base,
            Some("arbitrum") | Some("arb") => mev_capture::types::Chain::Arbitrum,
            Some("polygon") => mev_capture::types::Chain::Polygon,
            Some("ethereum") | _ => mev_capture::types::Chain::Ethereum,
        }
    } else {
        match std::env::var("MEV_CHAIN").as_deref() {
            Ok("base") => mev_capture::types::Chain::Base,
            Ok("arbitrum") | Ok("arb") => mev_capture::types::Chain::Arbitrum,
            Ok("polygon") => mev_capture::types::Chain::Polygon,
            _ => mev_capture::types::Chain::Ethereum,
        }
    };

    info!(mode = ?mode, chain = %chain.as_str(), simulate, "starting MEV bot");

    // Load chain-specific configuration from environment
    let (rpc_http, rpc_ws) = match chain {
        mev_capture::types::Chain::Base => (
            std::env::var("BASE_RPC_HTTP")
                .unwrap_or_else(|_| "https://mainnet.base.org".to_string()),
            std::env::var("BASE_RPC_WS")
                .unwrap_or_else(|_| "wss://mainnet.base.org".to_string()),
        ),
        mev_capture::types::Chain::Arbitrum => (
            std::env::var("ARB_RPC_HTTP")
                .unwrap_or_else(|_| "https://arb1.arbitrum.io/rpc".to_string()),
            std::env::var("ARB_RPC_WS")
                .unwrap_or_else(|_| "wss://arb1.arbitrum.io/rpc".to_string()),
        ),
        _ => (
            std::env::var("ETH_RPC_URL")
                .or_else(|_| std::env::var("ETH_RPC_HTTP"))
                .unwrap_or_else(|_| "https://eth.llamarpc.com".to_string()),
            std::env::var("ETH_WS_URL")
                .or_else(|_| std::env::var("ETH_RPC_WS"))
                .unwrap_or_else(|_| "wss://eth.llamarpc.com".to_string()),
        ),
    };

    let chain_str = chain.as_str();
    let pool_path = std::env::var("POOL_TOKENS_PATH")
        .unwrap_or_else(|_| format!("data/pool_tokens_{chain_str}.json"));
    let contract_addr = std::env::var("MEVBOT_CONTRACT")
        .unwrap_or_else(|_| "0x0000000000000000000000000000000000000000".to_string());
    let min_profit_eth: f64 = std::env::var("MIN_PROFIT_ETH")
        .unwrap_or_else(|_| "0.001".to_string())
        .parse()?;
    let bribe_pct: f64 = std::env::var("BRIBE_PCT")
        .unwrap_or_else(|_| "0.85".to_string())
        .parse()?;

    let contract_address = Address::from_str(&contract_addr)?;

    // Load pool universe and build arb cycles
    info!(path = %pool_path, chain = %chain_str, "loading pool universe");
    let pools = pool_graph::load_pool_universe(&pool_path)?;
    let weth_addr = weth_for_chain(chain);
    let weth = Address::from_str(weth_addr)?;
    let mut cycles = pool_graph::find_arb_cycles(&pools, weth);

    // Add 3-hop cycles (capped at 10K to keep scan time reasonable)
    let cycles_3hop = pool_graph::find_arb_cycles_3hop(&pools, weth, 10_000);
    info!(two_hop = cycles.len(), three_hop = cycles_3hop.len(), "combining cycle sets");
    cycles.extend(cycles_3hop);

    if cycles.is_empty() {
        bail!("no arb cycles found - check pool_tokens.json");
    }

    // Initialize strategy
    let strategy = DexArbStrategy::new(
        chain,
        cycles,
        contract_address,
        rpc_http.clone(),
        min_profit_eth,
        bribe_pct,
        weth,
    );

    info!(
        min_profit_eth,
        bribe_pct,
        contract = %contract_address,
        "strategy configured"
    );

    // Initialize dry-run logger
    let dry_run_path = std::path::PathBuf::from(
        std::env::var("DRY_RUN_LOG").unwrap_or_else(|_| "dry_run.jsonl".to_string()),
    );
    let mut dry_run = DryRunExecutor::new(dry_run_path.clone());

    // Live mode setup
    let flashbots: Option<FlashbotsExecutor> = if mode == Mode::Live {
        let fb_key = std::env::var("FLASHBOTS_SIGNING_KEY")
            .map_err(|_| eyre::eyre!("FLASHBOTS_SIGNING_KEY required for live mode"))?;
        let signing_key = SigningKey::from_hex(&fb_key)?;
        Some(FlashbotsExecutor::new(signing_key))
    } else {
        None
    };

    let bundle_builder: Option<BundleBuilder<alloy::providers::RootProvider<alloy::network::Ethereum>>> =
        if mode == Mode::Live {
            let trading_key = std::env::var("TRADING_PRIVATE_KEY")
                .map_err(|_| eyre::eyre!("TRADING_PRIVATE_KEY required for live mode"))?;
            let url: alloy::transports::http::reqwest::Url = rpc_http.parse()?;
            let client = alloy::rpc::client::RpcClient::new_http(url);
            let provider =
                alloy::providers::RootProvider::<alloy::network::Ethereum>::new(client);
            let chain_id = match chain {
                mev_capture::types::Chain::Base => 8453,
                mev_capture::types::Chain::Arbitrum => 42161,
                mev_capture::types::Chain::Polygon => 137,
                _ => 1, // Ethereum mainnet
            };
            Some(BundleBuilder::new(&trading_key, provider, chain_id)?)
        } else {
            None
        };

    // Subscribe to new blocks via WebSocket
    info!(ws = %rpc_ws, "connecting to WebSocket for block subscription");

    let ws_provider = alloy::providers::ProviderBuilder::new()
        .connect(&rpc_ws)
        .await?;

    use alloy::providers::Provider;
    let sub = ws_provider.subscribe_blocks().await?;
    let mut stream = sub.into_stream();

    info!("block subscription active - waiting for new blocks");

    // Ctrl-C handler
    let running = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let r = running.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("shutting down...");
        r.store(false, std::sync::atomic::Ordering::Relaxed);
    });

    // Main event loop
    while running.load(std::sync::atomic::Ordering::Relaxed) {
        let header = tokio::select! {
            Some(header) = stream.next() => header,
            _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                warn!("no block received in 30s, checking connection...");
                continue;
            }
        };

        let block_start = Instant::now();

        // subscribe_blocks() returns alloy Header - .inner has the sealed header fields
        let block_number = header.inner.number;
        let base_fee = header
            .inner
            .base_fee_per_gas
            .unwrap_or(30_000_000_000); // 30 gwei default
        let base_fee_gwei = base_fee as f64 / 1e9;

        info!(
            block = block_number,
            base_fee_gwei = format!("{:.2}", base_fee_gwei),
            "new block"
        );

        // Build event for strategy
        let block_data = mev_capture::types::BlockData {
            chain,
            number: block_number,
            hash: header.hash,
            parent_hash: header.inner.parent_hash,
            timestamp: chrono::DateTime::from_timestamp(
                header.inner.timestamp as i64,
                0,
            )
            .unwrap_or_else(|| chrono::Utc::now()),
            base_fee: Some(U256::from(base_fee)),
            gas_used: header.inner.gas_used,
            gas_limit: header.inner.gas_limit,
            tx_count: 0,
            transactions: vec![],
        };

        let event = Event::NewBlock(block_data);

        // Run strategy
        match strategy.process_event(&event).await {
            Ok(actions) => {
                let elapsed = block_start.elapsed();

                for action in &actions {
                    // EVM simulation cross-check (if --simulate flag is set)
                    let (sim_success, sim_gas_used) = if simulate {
                        let replay = BlockReplay::new(rpc_http.clone());
                        match replay.fork_at_block(block_number, base_fee_gwei).await {
                            Ok(mut forked) => {
                                // Execute the arb calldata through the EVM
                                match forked.execute_tx(
                                    contract_address, // from: our bot contract (as caller)
                                    action.to,        // to: the MevBot contract
                                    action.calldata.clone(),
                                    action.value,
                                    action.estimated_gas,
                                ) {
                                    Ok(sim_result) => {
                                        if sim_result.success {
                                            info!(
                                                cycle = %action.cycle_label,
                                                gas_used = sim_result.gas_used,
                                                "SIM PASS"
                                            );
                                        } else {
                                            info!(
                                                cycle = %action.cycle_label,
                                                gas_used = sim_result.gas_used,
                                                "SIM REVERT"
                                            );
                                        }
                                        (Some(sim_result.success), Some(sim_result.gas_used))
                                    }
                                    Err(e) => {
                                        warn!(cycle = %action.cycle_label, err = %e, "SIM ERROR");
                                        (Some(false), None)
                                    }
                                }
                            }
                            Err(e) => {
                                warn!(err = %e, "failed to fork state for simulation");
                                (None, None)
                            }
                        }
                    } else {
                        (None, None)
                    };

                    let entry = DryRunEntry {
                        timestamp: chrono::Utc::now().to_rfc3339(),
                        block_number,
                        chain: chain_str.to_string(),
                        strategy: action.strategy.clone(),
                        cycle_label: action.cycle_label.clone(),
                        pools: action.pool_addresses.iter().map(|a| format!("{a}")).collect(),
                        input_amount_eth: action.input_amount_eth,
                        expected_output_eth: action.input_amount_eth + action.estimated_profit_eth,
                        gross_profit_eth: action.estimated_profit_eth + action.estimated_gas as f64 * base_fee_gwei * 1e-9 + action.estimated_profit_eth * action.bribe_pct,
                        gas_cost_eth: action.estimated_gas as f64 * base_fee_gwei * 1e-9,
                        bribe_eth: action.estimated_profit_eth * action.bribe_pct,
                        net_profit_eth: action.estimated_profit_eth,
                        estimated_gas: action.estimated_gas,
                        base_fee_gwei,
                        block_process_time_ms: elapsed.as_millis() as u64,
                        sim_success,
                        sim_gas_used,
                    };
                    dry_run.log_opportunity(&entry)?;

                    // Live mode: build and submit bundle
                    if mode == Mode::Live {
                        if let (Some(fb), Some(bb)) = (&flashbots, &bundle_builder) {
                            match bb
                                .action_to_bundle(
                                    action.to,
                                    action.calldata.clone(),
                                    action.value,
                                    action.estimated_gas,
                                    block_number,
                                    base_fee as u128,
                                )
                                .await
                            {
                                Ok(bundle) => {
                                    info!(block = block_number, "submitting bundle to relays");
                                    let state_block = format!("0x{:x}", block_number);
                                    let min_profit_wei = (action.estimated_profit_eth * 0.5 * 1e18) as u128;
                                    match fb.simulate_and_send(&bundle, &state_block, min_profit_wei).await {
                                        Ok(Some(results)) => {
                                            info!(
                                                block = block_number,
                                                relays = results.len(),
                                                "bundle submitted to relays"
                                            );
                                        }
                                        Ok(None) => {
                                            warn!(
                                                block = block_number,
                                                "bundle rejected (unprofitable sim or revert)"
                                            );
                                        }
                                        Err(e) => {
                                            error!(
                                                block = block_number,
                                                err = %e,
                                                "bundle submission failed"
                                            );
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!(err = %e, "failed to build bundle");
                                }
                            }
                        }
                    }
                }

                dry_run.record_block();

                let elapsed = block_start.elapsed();
                if elapsed.as_secs() > 6 {
                    warn!(
                        block = block_number,
                        elapsed_ms = elapsed.as_millis(),
                        "block processing exceeded 6s target"
                    );
                }

                if !actions.is_empty() {
                    info!(
                        block = block_number,
                        actions = actions.len(),
                        elapsed_ms = elapsed.as_millis(),
                        "block processed"
                    );
                }
            }
            Err(e) => {
                error!(block = block_number, err = %e, "strategy error");
            }
        }
    }

    // Print summary on shutdown
    dry_run.summary();
    info!(log = %dry_run_path.display(), "dry-run log written");

    Ok(())
}
