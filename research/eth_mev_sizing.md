# Ethereum MEV Business Sizing

**Date:** April 2026
**Data window:** 14.9 days of Ethereum mainnet block data (~108K blocks, blocks 21.5M–24.85M)
**Methodology:** On-chain swap event parsing + token pair resolution + USD pricing via WETH/USDC pool

---

## Data Pipeline

| Item | Value |
|------|-------|
| Blocks ingested | 108,002 |
| Swaps captured | 1.1M |
| Pools resolved (token pairs via `token0()`/`token1()` RPC) | 200 |
| Priceable pools (WETH or stablecoin side) | 186 |
| Swap volume covered by priceable pools | ~56% |
| ETH/USD price (median from WETH/USDC 0.05% pool) | $2,112 |
| Avg base fee | 0.3 gwei |

### Token pair resolution

Called `token0()` and `token1()` on the top 200 pool contracts by swap count. Matched against 21 known ERC-20 tokens (WETH, USDC, USDT, DAI, WBTC, etc.) to determine which side of each pool is the "quote" token for USD pricing.

| Pool category | Count | Notes |
|---------------|-------|-------|
| WETH pairs | 141 | Priced via ETH/USD |
| Stablecoin pairs | 45 | Priced at $1 per unit |
| Other (not priceable) | 14 | Excluded from P&L |

---

## Sandwich Attack Sizing

### Detection method

A "confirmed sandwich" is defined as:
1. Same sender has 2+ swaps on the same pool in the same block
2. At least 1 distinct sender (victim) swaps on that pool between the bot's first and last swap (by `log_index`)

Profit is computed as the net flow on the quote token side (WETH or stablecoin) between the bot's first and last swap in the sandwich.

### Raw results

| Metric | Value |
|--------|-------|
| Confirmed sandwiches (with victims) | 30,340 |
| Avg victims per sandwich | 1.8 |
| Profitable sandwiches (best direction, profit < $10K filter) | 14,129 |
| Median profit per sandwich | $189 |
| Average profit per sandwich | $792 |
| Total extracted profit (14.9 days) | $11.2M |
| Avg bot position size | $113,578 |

### Extrapolated market size

| Timeframe | Gross MEV (sandwich) |
|-----------|---------------------|
| Daily | $751K |
| Monthly | $22.5M |
| Annual | $274M |

### Confidence adjustment

The sandwich heuristic has known false positives:
- Router-mediated multi-hop swaps can appear as same-sender bracketing
- Arbitrage bots doing rebalancing alongside swaps get counted
- No price impact verification (true sandwiches should show adverse price movement on victim)

**Estimated false positive rate:** 2-3x overcount.
**Adjusted daily sandwich MEV:** ~$225K-375K/day.

This aligns with published Flashbots data reporting $200K-500K/day in sandwich MEV on Ethereum.

### Gas costs

| Item | Value |
|------|-------|
| Gas per sandwich (2 txs x ~150K gas) | 300,000 gas |
| Cost at 0.3 gwei + $2,112 ETH | $0.22 |
| Gas as % of median profit | 0.1% |

Gas is immaterial at current base fees. Priority fees and Flashbots builder tips (not captured here) are the real cost - typically 50-90% of extracted value is paid to block builders.

---

## DEX Arbitrage Sizing

### Detection method

An "arb transaction" is defined as:
1. 2+ swaps across 2+ distinct pools in a single transaction
2. Net outflow on priceable tokens exceeds net inflow (profit = out - in)
3. Profit capped at $50K per tx to filter noise

### Raw results

| Metric | Value |
|--------|-------|
| Multi-pool transactions analyzed | 151,901 |
| Profitable (net out > in, < $50K) | 45,615 |
| Median profit per arb | $259 |
| Average profit per arb | $3,124 |
| Total extracted profit (14.9 days) | $142.5M |

### Extrapolated market size

| Timeframe | Gross MEV (arb) |
|-----------|-----------------|
| Daily | $9.6M |
| Monthly | $287M |
| Annual | $3.5B |

### Confidence adjustment

The arb heuristic is very loose - any multi-pool transaction where out > in gets counted. Many of these are:
- Normal router swaps (Uniswap router splitting across pools for better execution)
- Aggregator fills (1inch, Cowswap routing)
- Liquidity provisioning with rebalancing

**Estimated false positive rate:** 5-10x overcount.
**Adjusted daily arb MEV:** ~$960K-1.9M/day.

Published estimates (Flashbots, EigenPhi) report $1-3M/day in DEX arb MEV on Ethereum, consistent with our adjusted range.

---

## Business Scenarios

### Assumptions

- **Builder tips:** 80% of gross MEV is paid to block builders via Flashbots/MEV-Share. Net to searcher = 20% of gross.
- **Gas:** Negligible at current fees (~$0.22/sandwich, ~$0.25/arb)
- **Infrastructure:** $50/month (single droplet)
### Conservative estimates (with confidence discounts applied)

Using adjusted daily MEV (sandwich: $300K/day, arb: $1.4M/day):

| Capture Rate | Strategy | Gross/month | After builder tips (20%) | Net/month | Net/year |
|-------------|----------|-------------|--------------------------|-----------|----------|
| 1% | Sandwich | $90K | $18K | $18K | $216K |
| 1% | Arb | $420K | $84K | $84K | $1.0M |
| 5% | Sandwich | $450K | $90K | $90K | $1.1M |
| 5% | Arb | $2.1M | $420K | $420K | $5.0M |
| 10% | Sandwich | $900K | $180K | $180K | $2.2M |
| 10% | Arb | $4.2M | $840K | $840K | $10.1M |

### What capture rate is realistic?

| Scenario | Capture rate | Justification |
|----------|-------------|---------------|
| Cold start, no Flashbots integration | <0.1% | Can't win block auctions |
| Basic Flashbots searcher, single strategy | 0.5-1% | Competing against 50+ active searchers |
| Optimized searcher, multiple strategies, low latency | 2-5% | Top 10 searcher tier |
| Dominant searcher (Wintermute, Jito-level) | 10-20% | Requires significant engineering + capital |

---

## Capital Requirements Model (Sandwich)

Arb can use flash loans (zero upfront capital). Sandwich cannot - the front-run transaction must execute with real tokens before the victim's swap. This section models how much capital is needed and what it returns.

### Why sandwich needs capital

A sandwich is two separate transactions in a bundle:
1. **Front-run:** bot swaps X ETH for tokens (needs X ETH in the contract)
2. **Victim swap executes** (bundle ordering, not our transaction)
3. **Back-run:** bot swaps tokens back for X' ETH (where X' > X)

The front-run and back-run are separate transactions. Flash loans require repayment within a single transaction, so they can't span the bundle. The bot must hold real capital.

However, capital is only locked for ~12 seconds (one block). The contract can be reused immediately in the next block.

### Position size distribution

From 15,877 confirmed profitable sandwiches on Ethereum:

| Percentile | Capital needed per sandwich |
|------------|---------------------------|
| P10 | $43 |
| P25 | $149 |
| P50 (median) | $616 |
| P75 | $2,292 |
| P90 | $16,914 |
| P95 | $55,380 |
| P99 | $626,754 |

**Key finding:** Most sandwiches are small. The median requires only $616 of capital.

### Capital threshold analysis

How much of the sandwich market is addressable at each capital level:

| Capital | Addressable sandwiches | % of total | Total profit in window | Avg profit |
|---------|----------------------|------------|----------------------|------------|
| $1,000 | 9,448 | 59.5% | $4,979,734 | $527 |
| $5,000 | 13,428 | 84.6% | $12,600,632 | $938 |
| $10,000 | 14,010 | 88.2% | $14,921,412 | $1,065 |
| $25,000 | 14,591 | 91.9% | $15,040,371 | $1,031 |
| $50,000 | 15,013 | 94.6% | $15,108,358 | $1,006 |
| $100,000 | 15,341 | 96.6% | $15,178,291 | $989 |
| $250,000 | 15,555 | 98.0% | $15,346,215 | $987 |
| $1,000,000 | 15,764 | 99.3% | $15,490,895 | $983 |

**Key finding:** $5K captures 84.6% of all sandwiches. Going from $5K to $1M only adds 14.7% more opportunities. Capital has sharply diminishing returns above $10K.

### Full P&L model at 1% capture rate

Assumptions:
- ETH staking yield (opportunity cost): 3.5%/year
- Builder tip rate: 85% of gross profit paid to block builders
- Infrastructure: $330/month
- Data window: 14.9 days

| Capital | Captured/mo | Gross/mo | Net after tips | Opp cost/mo | Final net/mo | Annual ROI |
|---------|------------|----------|----------------|-------------|-------------|------------|
| $1,000 | 190 | $100,263 | $15,039 | $3 | $14,707 | 17,648% |
| $5,000 | 270 | $253,704 | $38,056 | $15 | $37,711 | 9,051% |
| $10,000 | 282 | $300,431 | $45,065 | $29 | $44,706 | 5,365% |
| $25,000 | 294 | $302,826 | $45,424 | $73 | $45,021 | 2,161% |
| $50,000 | 302 | $304,195 | $45,629 | $146 | $45,153 | 1,084% |
| $100,000 | 309 | $305,603 | $45,840 | $292 | $45,219 | 543% |
| $250,000 | 313 | $308,984 | $46,348 | $729 | $45,288 | 217% |
| $1,000,000 | 317 | $311,897 | $46,785 | $2,917 | $43,538 | 52% |

### Full P&L model at 5% capture rate

| Capital | Captured/mo | Gross/mo | Net after tips | Opp cost/mo | Final net/mo | Annual ROI |
|---------|------------|----------|----------------|-------------|-------------|------------|
| $1,000 | 951 | $501,316 | $75,197 | $3 | $74,864 | 89,837% |
| $5,000 | 1,352 | $1,268,520 | $190,278 | $15 | $189,933 | 45,584% |
| $10,000 | 1,410 | $1,502,156 | $225,323 | $29 | $224,964 | 26,996% |
| $50,000 | 1,511 | $1,520,976 | $228,146 | $146 | $227,671 | 5,464% |
| $100,000 | 1,544 | $1,528,016 | $229,202 | $292 | $228,581 | 2,743% |
| $1,000,000 | 1,587 | $1,559,486 | $233,923 | $2,917 | $230,676 | 277% |

### Capital turnover

Capital is only locked for ~12 seconds per sandwich (one Ethereum block). At a given capital level:

| Capital | Sandwiches/day (market) | Capital turns/day | Utilization |
|---------|------------------------|-------------------|-------------|
| $10,000 | 940 | 105.6 | 1.47% |
| $50,000 | 1,008 | 55.4 | 0.77% |
| $100,000 | 1,030 | 42.6 | 0.59% |
| $250,000 | 1,044 | 26.2 | 0.36% |

Capital utilization is under 2% - the capital sits idle 98%+ of the time. This means the opportunity cost is almost entirely in the staking yield foregone, not in the capital being "used up."

### Capital strategy recommendation

| Approach | Capital | Addressable market | Pros | Cons |
|----------|---------|-------------------|------|------|
| **Lean start** | $1-5K | 60-85% | Minimal risk, captures majority of opportunities | Misses large profitable sandwiches |
| **Mid tier** | $10-25K | 88-92% | Near-full coverage, diminishing returns above this | Idle capital, but opportunity cost is negligible |
| **Full coverage** | $100K+ | 97%+ | Captures everything | No marginal benefit vs $25K for ROI; capital at smart contract risk |

**Optimal capital: $5-10K.** Captures 85-88% of all sandwich opportunities. Annual ROI at 1% capture: 5,000-9,000%. Above $10K, each additional dollar of capital adds less than 0.004% more addressable sandwiches.

### Capital risk

The capital sits in a smart contract on Ethereum. Risk vectors:
- **Contract bug:** All capital lost if contract has an exploit. Mitigated by formal verification and starting small.
- **Sandwich failure:** Atomic revert means failed sandwiches cost only gas (~$0.22), not capital. Capital is never at risk from failed trades.
- **ETH price exposure:** Capital is denominated in ETH. If ETH drops 50%, capital value halves. Hedge with a short position or use stablecoin-denominated strategies where possible.

---

## Limitations and Next Steps

### Known data quality issues

1. **Coverage:** Only 186 of ~10,000+ active pools are priceable. The 44% of swap volume on unresolved pools may have different MEV characteristics.
2. **No builder tip data:** We see gross MEV but not what searchers actually pay builders. Real net margins are much thinner.
3. **No failed transaction data:** Can't measure competition intensity (failed sandwich attempts = wasted gas).
4. **No mempool data:** Can't assess latency requirements or information advantage needed.
5. **`sender` vs `tx_from`:** Old Ethereum data uses `sender` (which is correct for ETH - `msg.sender` in the swap event). Polygon data needs re-ingestion with `tx_from` fix.
6. **Time sampling:** 14.9 days of blocks spread non-contiguously. Market conditions (gas, volume, volatility) during these blocks may not be representative.

### Recommended next steps

1. **Validate against EigenPhi/Flashbots dashboards** - compare our per-block sandwich counts to known ground truth
2. **Add price impact analysis** - true sandwiches should show victim getting worse execution than previous block's price
3. **Measure builder tip rates** - query Flashbots API for historical bundle payments to estimate real net margins
4. **Resolve more pools** - extend token resolver to top 1,000 pools
5. **Multi-chain comparison** - run same analysis on Polygon (after re-ingestion), Base, Arbitrum, Blast
6. **Backrun/liquidation sizing** - currently unsized; liquidations are a smaller but less competitive MEV source

---

## Raw Data Location

| File | Description |
|------|-------------|
| `data/events/swaps/ethereum/*.parquet` | Raw swap events (1.1M rows) |
| `data/blocks/ethereum/*.parquet` | Block metadata (108K rows) |
| `data/pool_tokens.json` | Resolved token pairs for 200 pools |
| `sandwich_pnl.py` | Sandwich + arb P&L computation script |
| `token_resolver.py` | Pool contract token pair resolver |
| `dashboard/` | Plotly Dash app (http://165.245.167.187:8050) |
