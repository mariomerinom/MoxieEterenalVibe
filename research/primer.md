# MEV Bot -- Project Primer

## What This Is

A block-reactive MEV (Maximal Extractable Value) bot for Ethereum, written in Rust. It watches every new block, detects arbitrage opportunities across DEX pools, simulates them locally via revm, and submits profitable bundles to block builders through Flashbots.

The project is split into three phases:

- **Phase 1 (Complete):** Data capture pipeline -- ingest swaps, receipts, and logs from Ethereum (and other chains) into DuckDB/Parquet for analysis.
- **Phase 2 (Complete):** Analysis and business sizing -- quantify the MEV market, identify which strategies are worth pursuing, model capital requirements and expected returns.
- **Phase 3 (Current):** Live execution pipeline -- pool graph, revm simulation, arb detection, Flashbots bundle submission, orchestrator binary.

---

## Architecture

```
                   WebSocket (newHeads)
                         |
                    +----v----+
                    | mev-bot |  Orchestrator binary
                    +----+----+
                         |
           +-------------+-------------+
           |                           |
    +------v------+            +-------v-------+
    | mev-strategies |          | mev-simulator  |
    | (dex_arb)      |          | (replay)       |
    +------+------+            +-------+-------+
           |                           |
           |  encode_arb()             |  revm fork
           v                           v
    +------+------+            Ethereum RPC
    | mev-executor |            (AlloyDB)
    | (flashbots)  |
    +------+------+
           |
           v
    Flashbots / Beaverbuild / Titan / rsync
    (block builder relays)
```

---

## Crate Map

### mev-capture

Data ingestion layer. Fetches blocks, transactions, receipts, and logs from multiple chains. Stores to DuckDB and Parquet. Supports Ethereum, Base, Arbitrum, Polygon, Scroll, Blast, and Solana.

Key types:
- `BlockData` -- a block with its transactions, used as the event input to strategies
- `Chain` -- enum of supported chains
- `TransactionData` -- hash, from, to, value, gas, input, logs

### mev-analysis

Post-hoc analysis of captured data. Concentration (how many bots dominate), profitability (revenue per strategy), competition (bot profiling), time series trends, and cross-chain scoring.

Used during Phase 2 to size the market and select strategies.

### mev-simulator

EVM simulation via revm. Two key capabilities:

1. **Block replay** (`replay.rs`) -- Fork Ethereum state at any block using `AlloyDB` (lazy RPC fetching) wrapped in `CacheDB` (local cache). Execute arbitrary transactions against the fork without touching mainnet. The `ForkedEvm` is reusable across multiple calls within the same block fork.

2. **Gas and profit models** (`gas_model.rs`, `profit.rs`) -- Estimate gas costs from recent block data, compute P&L (gross profit minus gas minus builder bribe).

### mev-strategies

Strategy implementations behind a common `Strategy` trait.

**The Strategy trait:**
```rust
trait Strategy: Send + Sync {
    fn name(&self) -> &str;
    fn chain(&self) -> Chain;
    async fn process_event(&self, event: &Event) -> Result<Vec<Action>>;
    fn params(&self) -> serde_json::Value;
    fn set_params(&mut self, params: serde_json::Value) -> Result<()>;
}
```

An `Event` is either `NewBlock(BlockData)` or `MevShareHint(...)`. A strategy processes an event and returns zero or more `Action`s.

An `Action` is:
```rust
struct Action {
    chain: Chain,
    strategy: String,        // "dex_arb", "backrun", etc.
    target_tx: Option<B256>, // for backrun/sandwich
    to: Address,             // MevBot contract address
    calldata: Vec<u8>,       // ABI-encoded function call
    value: U256,
    estimated_profit_eth: f64,
    estimated_gas: u64,
    bribe_pct: f64,          // fraction of profit to tip builder
}
```

**Implemented strategies:**

- `dex_arb` -- Cross-pool DEX arbitrage. On each block, forks state via revm, queries reserves for all V2 pool pairs in precomputed arb cycles, computes the analytically optimal input amount, and emits Actions for profitable opportunities (after gas and bribe).

- `backrun` / `liquidation` -- Stubs, not yet implemented.

**Pool graph** (`pool_graph.rs`):
- Loads the pool universe from `pool_tokens.json` (200 pools with resolved token pairs, decimals, symbols, protocol)
- Precomputes 2-hop arb cycles through WETH: for every pair of pools that share an intermediate token and both connect to WETH, that pair is an arb cycle
- Currently finds 174 cycles across 109 intermediate tokens
- Includes the analytical optimal V2-V2 amount formula:
  `x_opt = sqrt(rA1 * rB1 * rB2 * rA2) * 0.997 - rA1 / 0.997`

### mev-executor

Bundle construction and submission.

- **flashbots.rs** -- `FlashbotsExecutor` submits bundles to 4 builder relays in parallel: Flashbots, Beaverbuild, Titan, rsync. Handles `X-Flashbots-Signature` authentication (secp256k1 signing). `simulate_and_send()` simulates first via `eth_callBundle`, checks for reverts and minimum profit (coinbase_diff), then broadcasts.

- **contracts.rs** -- ABI bindings for MevBot.sol via alloy's `sol!` macro. Helpers: `encode_arb()`, `encode_swap_v2()`, `encode_swap_v3()`, `encode_pay_builder()`, etc.

- **bundle_builder.rs** -- Converts an `Action` into a signed Flashbots `Bundle`. Builds EIP-1559 transactions (max_fee = base_fee * 2, priority_fee = 0 since tip goes through the contract's `payBuilder()`). Signs with the trading wallet, RLP-encodes.

- **dry_run.rs** -- Paper trading mode. Logs every detected opportunity as JSONL with full detail: block, cycle, amounts, profit breakdown, gas, processing time. Tracks running totals and prints a summary on shutdown.

### mev-bot

The orchestrator binary. Event loop:

1. Load pool universe and precompute arb cycles
2. Connect to Ethereum via WebSocket (block subscription)
3. On each new block:
   - Build an `Event::NewBlock`
   - Pass to `DexArbStrategy::process_event()` -- forks state, scans 174 cycles, returns profitable Actions
   - For each Action: log to dry-run JSONL
   - In live mode: build signed bundle, simulate, submit to all relays
4. On shutdown: print summary (total opportunities, profit, gas costs)

Modes: `--mode dry-run` (default) or `--mode live`.

---

## On-Chain Contract

`contracts/MevBot.sol` -- Owner-only, atomic execution:

- `swapV2(pair, tokenIn, amountIn, amountOutMin, zeroForOne)` -- constant-product swap via getReserves + output calculation
- `swapV3(pool, zeroForOne, amountIn)` -- V3 swap with sqrtPriceLimit, includes `uniswapV3SwapCallback()`
- `executeArb(hops, tokenIn, amountIn, minProfit)` -- Multi-hop arb through mixed V2/V3 pools. Hops are encoded as 22-byte chunks: 20 bytes address + 1 byte isV3 + 1 byte zeroForOne. Reverts if final balance is not at least amountIn + minProfit.
- `payBuilder(amount)` / `payBuilderPercent(profitWei, bribePercent)` -- Tips the block builder via `block.coinbase.call{value: ...}`
- `wrapETH()`, `unwrapWETH()`, `withdrawETH()`, `withdrawToken()`, `approveToken()` -- Admin helpers

Not yet compiled or deployed.

---

## Configuration

Environment variables (via `.env`):

| Variable | Purpose |
|----------|---------|
| `ETH_RPC_HTTP` / `ETH_RPC_URL` | HTTP RPC endpoint (Alchemy, Infura, etc.) |
| `ETH_RPC_WS` / `ETH_WS_URL` | WebSocket RPC for block subscription |
| `POOL_TOKENS_PATH` | Path to pool_tokens.json (default: `data/pool_tokens.json`) |
| `MEVBOT_CONTRACT` | Deployed MevBot contract address |
| `MIN_PROFIT_ETH` | Minimum net profit threshold (default: 0.001 ETH) |
| `BRIBE_PCT` | Fraction of profit to tip builder (default: 0.85) |
| `FLASHBOTS_SIGNING_KEY` | secp256k1 key for Flashbots authentication (live mode) |
| `TRADING_PRIVATE_KEY` | EOA private key that owns the MevBot contract (live mode) |
| `DRY_RUN_LOG` | Output path for dry-run JSONL (default: `dry_run.jsonl`) |
| `RUST_LOG` | Tracing filter (e.g. `info,mev_bot=debug`) |

---

## Running

```bash
# Build
cargo build --release

# Dry-run (default) -- paper trading, no bundles submitted
./target/release/mev-bot --mode dry-run

# Live -- submits real bundles (requires funded contract + keys)
./target/release/mev-bot --mode live
```

---

## Data Flow: Block to Bundle

```
Block 24895629 arrives via WebSocket
    |
    v
DexArbStrategy::process_event(NewBlock)
    |
    +-- BlockReplay::fork_at_block(24895629)
    |       Creates ForkedEvm with AlloyDB -> CacheDB
    |
    +-- For each of 174 arb cycles:
    |       ForkedEvm::get_reserves_v2(pool1) -> (r0, r1)
    |       ForkedEvm::get_reserves_v2(pool2) -> (r0, r1)
    |       optimal_v2_v2_amount(rA1, rB1, rB2, rA2)
    |       -> If profit > min_profit after gas + bribe:
    |          encode_arb(hops, WETH, amount, minProfit)
    |          -> Action
    |
    v
DryRunExecutor::log_opportunity(entry)  [always]
    |
    v  [live mode only]
BundleBuilder::action_to_bundle(action)
    -> Sign EIP-1559 tx, RLP encode, wrap in Bundle
    |
    v
FlashbotsExecutor::simulate_and_send(bundle)
    -> eth_callBundle on Flashbots relay
    -> Check coinbase_diff > min_profit
    -> Send to all 4 relays in parallel
```

---

## Business Context

From Phase 2 analysis of 14.9 days of Ethereum swap data (1.1M swaps, 108K blocks, 186 priceable pools):

- DEX arbitrage market: ~$960K-1.9M/day total
- At 1% capture after 85% builder tips: ~$84K/month net
- Flash loans eliminate capital requirements for arb
- Processing target: <6 seconds per block (12s block time)
- Current performance: ~300ms per block for 174 cycles

---

## What Remains

| Task | Status |
|------|--------|
| Compile MevBot.sol | Not started -- needs solc |
| Deploy to mainnet | Not started -- needs funded wallet |
| Token approvals | Not started -- one-time per pool |
| 24h dry-run validation | Ready to start |
| V3 pool support | Not yet -- needs sqrtPrice math |
| 3-hop cycles | Not yet -- currently 2-hop only |
| Mempool integration | Not yet -- currently block-reactive only |
| Backrun strategy | Stub exists, not implemented |
| Liquidation strategy | Stub exists, not implemented |
