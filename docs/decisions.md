# Decision Log

Append-only. Each entry records what was decided, the evidence behind it, and what would reopen the question.

---

### 2026-04-17 — Kill: Ethereum V2-V2 dex arb (theoretical)

**Decision:** Deprioritize Ethereum V2-V2 arbitrage as a primary strategy.

**Evidence:** Phase 4 backtest of 100 blocks found 96.9% of V2-V2 arb opportunities had a competing arb tx in the same block. Only 3.1% were uncaptured.

**Gaps:** Based on backtest data, not live measurement from our infrastructure. We never measured our latency against actual competitors. Premium RPC or colocation could shift the equation.

**Reopen if:** Live dry-run with premium RPC shows >10% uncaptured rate, or infrastructure upgrade (colocation) materially changes latency.

---

### 2026-04-17 — Kill: Arbitrum dex arb via Camelot pools

**Decision:** Exclude Camelot pools from constant-product V2 arb cycles.

**Evidence:** The persistent wstETH V3→Camelot signal (0.31 ETH/block, every block) was 100% phantom. Direct `getAmountOut()` call on the Camelot pool returned 1.17 WETH per wstETH; our constant-product math predicted 2.77. Camelot uses Solidly stableswap invariant (`x³y + xy³ = k`) for correlated pairs. Our `x * y = k` formula overestimated output by 137%. Zero real signals remain on Arbitrum after filtering.

**Gaps:** Only Camelot stable pools are confirmed wrong. Camelot volatile pools (non-correlated pairs) might still use constant-product math. We haven't verified.

**Reopen if:** Solidly stableswap math is implemented (numerical solver or batched `getAmountOut()` via Multicall3), which would unlock Camelot pools with correct pricing. Or if Camelot volatile pools are verified to use standard constant-product.

---

### 2026-04-17 — Kill: Base dex arb via Aerodrome pools

**Decision:** Exclude Aerodrome pools from constant-product V2 arb cycles.

**Evidence:** All four Base signals (wstETH, cbETH, ezETH, rETH — all LST/WETH pairs) originated from Aerodrome pools. Aerodrome is a Solidly fork with the same stableswap issue as Camelot. Three of four "V2" pools were 45-byte EIP-1167 proxies that didn't even respond to `getReserves()`. The wstETH pool responded but its reserves produced wrong prices under constant-product math. Zero real signals remain on Base after filtering.

**Gaps:** Same as Camelot. Aerodrome volatile pools are unverified. 80 Aerodrome pools in our Base universe are now excluded wholesale.

**Reopen if:** Same as Camelot — implement proper swap math or verify volatile pool behavior.

---

### 2026-04-17 — Invalidated: EVM simulation cross-check results

**Decision:** Discard all `--simulate` results collected to date.

**Evidence:** The `--simulate` flag forks on-chain state and executes arb calldata via `ForkedEvm::execute_tx()`. It reported 100% pass rate across 256 Arbitrum blocks. Investigation revealed the contract address was `0x000...000` (placeholder default). Calling an empty address always succeeds in the EVM. Gas of 22,908 (vs 150k+ for real V3+V2 swaps) confirmed the simulation was vacuous.

**Gaps:** The ForkedEvm framework itself works correctly — state forking, CacheDB, transaction execution all function. Only the simulation target was wrong.

**Reopen if:** MevBot.sol is deployed (giving a real contract address), or simulation is restructured to call pool contracts directly instead of the arb contract.

---

### 2026-04-17 — Go: MEV-Share backrun as primary revenue path

**Decision:** Prioritize MEV-Share backrun on Ethereum (WS2) as the primary strategy. Gate execution code behind empirical validation from the 24-hour probe.

**Evidence (preliminary, 5-minute sample):**
- ~1.2M SSE hints/day from Flashbots MEV-Share stream
- 4.4% match our 200-pool Ethereum universe
- 1.5% are backrunnable (swap logs + hit our pools + have arb counterpart)
- ~17,500 backrunnable hints/day projected
- Top pairs: WETH/USDT, USDC/WETH, WBTC/USDT
- 33.5% of matched hints are backrunnable

**Gaps:** No profit sizing. No competition measurement. No deduplication (17.5K may overcount). 5-minute sample, not 24-hour. Previous "promising numbers" on dex arb turned out to be entirely phantom. These numbers answer "how many hints touch our pools" but not "how much money is in each" or "can we win the race."

**Reopen if:** 24-hour probe shows <1,000 unique backrunnable hints/day after dedup. Or profit estimation shows median opportunity <$0.50. Or competition analysis shows >95% of backrunnable hints are already captured by existing searchers.

**Next action:** Wait for 24-hour probe results. Run `analyze_backrun_probe.py`. Assess deduped volume, hourly distribution, per-pool density. Only then decide whether to build WS2 execution code.

---

### 2026-04-18 — Kill: MEV-Share backrun on small V2 pools (empirical)

**Decision:** MEV-Share backrun targeting small V2/V3 pools in our current 200-pool universe cannot reach $1,000/day. Do not build WS2 execution code for this approach.

**Evidence (three-stage empirical validation):**

1. **Probe (13.6h, 1.29M hints):** 84K backrunnable pool hits/day. Looked promising at the funnel level.

2. **Profit estimator (per-pool, on-chain state):** Only pools with <120 ETH liquidity showed any arb profit. All pools with >1,000 ETH liquidity showed 0.000000 profit at 1 ETH swap. The arb opportunity exists only when price impact is large enough (>1% of pool) to overcome fees on both legs.

3. **On-chain swap size validation (2,000 blocks, 7 pools):**
   - 6 of 7 "profitable" pools had ZERO swaps in 7 hours
   - The 1 active pool (0x397ff, USDC/WETH SushiV2, 57 ETH liq): median swap = 0.036 ETH, mean = 0.109 ETH
   - 68.8% of swaps < 0.1 ETH → zero profit after gas
   - Only 8.3% of swaps ≥ 0.5 ETH
   - Result: $5.53/day at 100% capture, $0.55/day at 10% capture

**Root cause:** The probe's per-pool frequency counted hints where a pool's address appeared in MEV-Share logs, but this doesn't mean swaps actually executed on that pool. Most hint traffic flows through deep V3 pools where price impact per swap is negligible. The small pools that would create exploitable price impact barely get traded.

**Gaps:**
- Only checked our 200-pool universe. A larger universe (1,000+ pools) might surface more active small pools.
- Did not test sandwich (front+back) which captures value differently from pure backrun.
- MEV-Share hints from bundle builders (not just individual txs) might have larger swap sizes.
- Did not measure competition — the $5.53/day assumes 100% capture which is unrealistic.

**Reopen if:**
- Pool universe expanded 5x+ AND shows materially different swap size distribution on small pools.
- Sandwich strategy is evaluated (different profit mechanics — captures from price impact on victim, not from arbing between pools).
- MEV-Share introduces features that reveal swap amounts pre-execution, enabling selective targeting of large swaps only.

---

### 2026-04-18 — Kill: Ethereum sandwich via public mempool

**Decision:** Public-mempool sandwich cannot reach $1,000/day. Do not build sandwich execution code targeting the public mempool.

**Evidence (three-stage pipeline):**

1. **Competition census (P1-S1, 15 days parquet data):** Market is $1.07M/day gross sandwich MEV. HHI = 0.09 (fragmented). 228 active bots. Top 3 capture 41%. 1% capture at 85% builder tip = $1,601/day. Market structure looks viable.

2. **Historical opportunity (P1-S2a, 15 days parquet data):** 134K swaps/day on 186 priceable pools. 2,124/day sandwiched (1.6%), 132K unsandwiched. Sandwich rate flat at 1-2.5% across all size buckets. 29,701 unsandwiched swaps/day >$1K. At 0.3% extraction, 85% tip, 1% capture: $1,831/day. Still looks viable from historical data.

3. **Mempool probe (P1-S2b, live Alchemy WebSocket, 10-min sample, stable rates):**
   - 881K pending txs/day in public mempool total
   - 19,344/day match known swap selectors (across ALL contracts, not just our routers)
   - 1,280/day match our 186-pool universe
   - 853/day sandwichable (>0.1 ETH ≈ $211, matching our pools)
   - 237 ETH/day total sandwichable volume ($501K)
   - **Visibility rate: 1,280 / 134,000 on-chain swaps = 0.96%**

**Revenue from public mempool:**
   - 237 ETH/day × $2,113 × 0.3% extraction = $1,503/day gross
   - After 85% builder tip: $225/day net at 100% capture
   - At 1% capture: $2.25/day ❌
   - Even with Universal Router decoding (2.8x more txs): ~$855/day net at 100% capture

**Root cause:** 99% of swap transactions use private channels (Flashbots Protect, MEV Blocker, builder private order flow). The public mempool is nearly empty of swap traffic. Existing sandwich bots operate via private mempool access (dedicated P2P nodes, builder relationships, MEV-Share bundles) — infrastructure we don't have and can't easily build.

**The opportunity exists ($1.07M/day) but the access method (public mempool) doesn't work.**

**Gaps:**
- Only decoded V2 Router + V3 exactInputSingle (35% of swap-like txs). Universal Router (65%) was counted but not decoded — larger swaps may hide there.
- 10-minute sample. Full 24-hour data will confirm rates.
- Only 186 pools. Expanded universe might increase pool matches.
- Did not test private mempool access (dedicated Geth node with P2P peering).

**Reopen if:**
- Acquire private mempool access (dedicated node with P2P connections to major validators).
- Builder partnership provides private order flow.
- Universal Router decoding reveals 5x more sandwichable volume than current estimate.
- MEV-Share changes to reveal swap amounts in hints.

---

### 2026-04-18 — Kill: Liquidations (Aave V3 + Compound V3)

**Decision:** Liquidation MEV cannot reach $1,000/day. Do not build liquidation execution code.

**Evidence (P5-S1, ~7h on-chain snapshot across 3 chains):**

| Chain | Liquidations/day | Daily Volume |
|-------|-----------------|-------------|
| Ethereum (Aave V3) | ~7 | $21,275 |
| Arbitrum (Aave V3) | ~10 | $4 |
| Base (Aave V3) | ~7 | $0 |
| Ethereum (Compound V3) | 0 | $0 |
| **Total** | **~25** | **$21,279** |

At 100bps net margin (optimistic): $213/day. Top-3 liquidators control 100% of volume on all chains. Would need 470% of all volume to reach $1K/day.

**Gaps:**
- 7-hour snapshot during calm market. Volatile days (large price drops) produce 10-100x more liquidation volume.
- Did not check other lending protocols (Morpho, Euler, Spark).
- Did not measure time-to-liquidation (blocks between HF < 1.0 and liquidation tx).

**Reopen if:**
- Major market crash creates sustained liquidation volume >$500K/day for multiple days.
- New lending protocol launches with less competition and meaningful TVL.
- Flash loan integration makes capital-free liquidation viable at scale.

---

### 2026-04-18 — Kill: CEX-DEX arb on niche tokens (Coinbase vs on-chain)

**Decision:** CEX-DEX arb on niche tokens cannot produce revenue. Do not build CEX-DEX execution code.

**Evidence (P6-S1+S2, divergence monitor, 10-min live sample, 78 CEX-DEX pairs across 3 chains):**

1. **Pair discovery:** 385 Coinbase Exchange tokens cross-referenced against pool universes (Ethereum 200, Arbitrum 559, Base 731). Found 78 tokens tradeable on both Coinbase and at least one DEX.

2. **Divergence monitoring (12s polls):**
   - 50 of 60 token/chain pairs with >0.3% divergence are ALWAYS ON — structural price differences present every poll tick. Not transient.
   - Only 10 pairs showed intermittent divergence (appearing and disappearing).
   - All 10 intermittent pairs are on pools with <10 ETH liquidity.
   - The one exception (PEPE/ethereum, 5789 ETH) shows 0.44% intermittent divergence but gas costs ($3/tx) exceed per-trade profit ($2.13).

3. **Revenue: $0.00/day** after filtering for:
   - Intermittent only (structural divergences not arbitrageable)
   - Pool liquidity >10 ETH
   - Gas costs

**Root cause:** "Divergence" between CEX and DEX prices on niche tokens is structural, not transient. Small pools sit at stale prices because nobody trades them. The divergence exists permanently = it's a different price equilibrium, not an arb opportunity. The only pools deep enough to trade profitably (>100 ETH) show <0.5% divergence, which is gas-negative on Ethereum.

**Gaps:**
- 10-minute sample. 24h data running for confirmation.
- Only checked Coinbase. Other CEXs (Binance, Kraken) may have different pricing.
- Did not test execution latency or slippage.
- Only monitored WETH pools. Stablecoin pools not checked.

**Reopen if:**
- 24h data shows transient spikes >1% on pools with >50 ETH, occurring >10 times/day.
- L2 gas drops below $0.001, making tiny-pool arb viable.
- A new CEX lists tokens with significant on-chain liquidity and initial pricing inefficiency.

---

### 2026-04-18 — Kill: Cross-chain arb (L2-to-L2, same token different chains)

**Decision:** Cross-chain arb on same-token pools cannot produce revenue. Do not build cross-chain arb code.

**Evidence (P7, divergence monitor + prior cross-chain sizing):**

1. **Static analysis (16 major tokens, 3 chains):** All deep-liquidity pools show <0.1% divergence across chains. DAI 2.1% and wstETH 1.0% divergences traced to low-liquidity outlier pools and Camelot (Solidly math).

2. **Live monitoring (10 min, 31 cross-chain pairs):** 24 of 25 pairs with >0.3% divergence have zero range (max-min = 0.00%). These are permanent structural differences, not transient opportunities. Only 1 pair (LDO) showed dynamic behavior (0.11-0.38% range) — below actionable threshold.

3. **Bridge constraint:** L2→L1 bridges take 7 days (optimistic rollup finality). L2↔L2 requires fast bridges (Across, Stargate) with ~0.05-0.1% fees, which consume most of any <0.3% divergence.

**Root cause:** AMM prices across chains converge to within bridge-fee margins. Professional market makers already keep prices aligned. Structural divergences between pools reflect different fee tiers, liquidity depths, or pool staleness — not exploitable mispricing.

**Gaps:**
- Only WETH pairs checked. Stablecoin or exotic pairs not tested.
- Did not test during major volatility events.

**Reopen if:**
- A new L2 launches with <$0.001 bridge fees and initial pricing inefficiency.
- Major volatility creates sustained >1% cross-chain divergence on deep pools for >1 hour.

---

### 2026-04-18 — Kill: Solidly math fix (Camelot + Aerodrome re-enabled)

**Decision:** Re-enabling Camelot and Aerodrome pools with correct `getAmountOut()` pricing produces zero new arb signals. Kill confirmed — the original math-mismatch kill was correct, AND there are no real opportunities even with correct pricing.

**Evidence (P3, live dry-run with Solidly getAmountOut batching):**

1. **Implementation:** Modified `dex_arb.rs` to batch Solidly `getAmountOut(uint256,address)` calls via Multicall3 at 5 reference amounts (0.001, 0.01, 0.1, 1.0, 10.0 ETH). Linear interpolation builds a swap function for binary search optimization.

2. **Arbitrum results (474 blocks, 44 Camelot pools, 1,112 cycles):** Zero profitable arb signals. Camelot pools' correct prices are within fee margins of V3 pool prices.

3. **Base results (60 blocks, 160 Aerodrome pools, 1,452 cycles):** Zero profitable arb signals. Same conclusion — correct Aerodrome pricing shows no arb vs V3 pools.

**Root cause:** The previous phantom signals were entirely from applying wrong constant-product math to Solidly pools. With correct pricing, these pools are efficiently arbitraged by existing market participants. No new opportunities hiding behind the math error.

---

### 2026-04-18 — Kill: Ethereum V2-V3 arb (expanded pool universe)

**Decision:** V2-V3 arb on Ethereum's expanded pool universe produces only phantom signals from dead pools. No real opportunities.

**Evidence (P4, live dry-run, 4,780 pools, 2,334 cycles):**

1. **Pool universe:** 4,780 Ethereum pools (2,276 V2/Sushi + 2,504 V3). 568 tokens with both V2 and V3 pools = V2-V3 arb candidates.

2. **Signal analysis (12 blocks):** 99 "arb opportunity" signals fired. All are identical across blocks — same profit amount every block, only gas cost varies with base fee. This is the hallmark of phantom signals from stale/dead pools.

3. **Top phantom signals (per block):**
   - XMR V3→V3: 148.097 ETH "profit" every block (input 447 ETH) — clearly phantom
   - Hold V3→V3: 3.463 ETH every block — phantom
   - RAIL V3→V2: 0.330 ETH every block — phantom
   - AUTO V2→V3: 0.543 ETH every block — phantom
   - "UNI-V2" is literally the Uniswap V2 LP token, not a tradeable asset

4. **Key indicator:** If these were real, $350K (XMR) and $8K (Hold) sitting on the table every single block would be captured instantly by existing bots. They persist because the pools are untradeable (zero liquidity, stale state, or LP tokens).

**Root cause:** Expanded pool universe includes thousands of dead pools with stale prices. V3 10% virtual reserve cap prevents infinite outputs but doesn't prevent phantom signals from pools with zero real volume. The V2-V3 price difference on active tokens is within fee margins (<0.3%).

**Gaps:**
- Did not check competition on the few signals that could be real (AGLD, DTH at ~0.01 ETH)
- Did not verify with EVM simulation (contract address still 0x000)

**Reopen if:**
- EVM simulation with real contract address confirms any signals are executable
- Competition analysis shows <50% capture rate on V2-V3 opportunities
- Pool filtering by on-chain volume removes dead pools and reveals real signals

---

### 2026-04-18 — Final Reassessment: all strategies exhausted

**Every strategy in the pipeline has been empirically tested and killed:**

| # | Strategy | Revenue Estimate | Kill Reason |
|---|----------|-----------------|-------------|
| 1 | Ethereum V2-V2 dex arb | ~$0/day | 96.9% same-block competition |
| 2 | Camelot pools (Arbitrum) | $0/day | Solidly stableswap math mismatch |
| 3 | Aerodrome pools (Base) | $0/day | Same Solidly issue + EIP-1167 proxies |
| 4 | MEV-Share backrun | $5.53/day at 100% capture | Small pools, tiny swap sizes |
| 5 | Ethereum sandwich (public mempool) | $2.25/day at 1% capture | 99% of swaps invisible (private channels) |
| 6 | Liquidations (Aave V3 + Compound V3) | $213/day at 100% capture | Only $21K/day total volume, 100% concentrated |
| 7 | Arbitrum expanded dex arb | $0/day | Zero signals across 559 pools, 432 blocks |
| 8 | CEX-DEX niche token arb | $0/day | All divergences structural (permanent), not transient |
| 9 | Cross-chain L2-L2 arb | $0/day | All divergences structural, within bridge-fee margins |
| 10 | Solidly math fix (Camelot+Aerodrome) | $0/day | Correct pricing confirms no arb vs V3 pools |
| 11 | Ethereum V2-V3 expanded arb | $0/day (real) | All signals are static phantoms from dead pools |

**11 strategies tested. All killed with empirical evidence.**

**What's left that hasn't been tested:**
1. **Private mempool sandwich** — $1.07M/day gross market exists, but requires infrastructure we don't have (dedicated Geth node with P2P peering, builder relationships). Cost: ~$200/month for dedicated node.
2. **Pool volume filtering** — Current expanded pool universe includes thousands of dead pools. Filtering by on-chain swap volume (>10 swaps/day) and re-running V2-V3 arb might surface real signals. But Phase 4 V2-V2 competition (96.9%) suggests even real signals would be captured by existing bots.
3. **EVM simulation deployment** — Deploy MevBot.sol to verify if any detected signals actually execute. Current contract address is 0x000 (vacuous simulation).

**Conclusion:** $500/day MEV is not achievable with public RPC infrastructure and no builder relationships. The market is efficient at every level we can access:
- Deep pools: prices within fee margins, no arb
- Small pools: arb exists but no volume to capture
- Public mempool: 99% of swaps use private channels
- Cross-chain: within bridge-fee margins
- CEX-DEX: structural differences, not transient

**The validation pipeline itself is the project's primary output.** It prevented building execution code for 11 strategies that would have produced zero revenue. Total cost: ~$80 in RPC fees, ~50 hours of research. Saved: months of development on false premises.

**Path forward requires one of:**
- A: Dedicated Geth node with P2P peering (~$200/month) → enables private mempool sandwich ($1.07M/day market)
- B: Builder partnership → enables MEV-Share bundle submission
- C: Accept that solo MEV at current infrastructure level is not viable at scale

---

### 2026-04-20 — Honest strategy-space audit

**Decision:** "One remaining viable path" is accurate for strategies we have specifically tested, but it oversimplifies the landscape. There are strategy categories that have not been evaluated and should not be assumed killed by association. Recording them explicitly so we don't develop false certainty about what's left.

**Strategies with empirical kills (12):** Ethereum V2-V2, Camelot, Aerodrome, MEV-Share backrun, public-mempool sandwich, Aave+Compound liquidations, Arbitrum expanded arb, CEX-DEX niche tokens, cross-chain L2-L2, Solidly math fix, Ethereum V2-V3 expanded, Solana direct cross-DEX arb. All documented with evidence above.

**In-flight (1):** Ethereum private-mempool sandwich via self-hosted Geth node. Blocked on OVH storage upgrade.

**Adjacent / untested categories:**

1. **JIT (just-in-time) liquidity.** Provide a V3 LP position right before a large swap seen in the mempool, earn the swap fee, remove the position. Uses the same Geth mempool infrastructure as sandwich, so it is effectively free to evaluate in parallel once the node is up. Different profit mechanic (fee capture, not slippage capture).

2. **Solana perpetuals liquidations.** Our liquidation sizing covered only Aave V3 + Compound V3 on EVM chains. Solana perp DEXes (Drift, Zeta, Mango) run their own liquidation markets not surveyed. Would require a separate data pipeline.

3. **Solana multi-hop cross-DEX arb.** B3 tested only direct single-hop routes (`onlyDirectRoutes=true`). A → B → C paths through intermediate tokens could surface opportunities that direct routes mask.

4. **Oracle MEV.** Chainlink / Pyth price-update transactions can be sandwiched against oracle-dependent protocols (lending collateral updates, perp mark prices). Narrow market, never evaluated.

5. **Builder partnerships / private order flow.** Not a technical strategy — a business development path. Applying to be a CoWSwap/UniswapX solver, or negotiating direct order flow from a builder, materially changes the economics of any execution strategy by removing the competition/visibility problem.

**Things NOT worth re-evaluating** (killed by structural association with failed tests):
- Sub-chain L2 MEV (Blast, Scroll, Polygon) — same structure as killed Ethereum/Arbitrum with less liquidity
- NFT MEV — tiny market by any measure
- Additional cross-chain bridge pairs — killed by L2-L2 analysis

**Policy note:** When any of items 1-5 gets evaluated, it gets its own decision entry here with evidence. "Reopened" is not a conclusion; only "killed" or "go" are.

---

### 2026-04-20 — Final kill: Solana cross-DEX arb (B3 live pool-state probe)

**Decision:** Solana cross-DEX direct arb is confirmed killed. The market is efficient on pairs that can be arbed; un-arbable pairs lack cross-DEX direct routes entirely.

**Evidence (B3, 1.34h live probe via Jupiter quote API, 3,880 ticks across 8 pairs):**

1. **Methodology:** Polled Jupiter's `/quote` endpoint every 10s with DEX filters (`dexes=Raydium`, `dexes=Whirlpool`, `dexes=Meteora DLMM`) and `onlyDirectRoutes=true`. Returns LIVE per-DEX output amounts reflecting on-chain pool state. Chose Jupiter over Raydium/Orca public APIs because the latter serve stale cached prices (Orca reported SOL/USDC at $127 when on-chain reality was $86, a 48% error).

2. **Multi-DEX coverage breakdown:**
   - SOL/USDC: 3 DEXs consistent coverage, 56% of 485 ticks had 2+ DEXs active
   - SOL/USDT: 3 DEXs, 67% coverage
   - USDC/USDT: 3 DEXs, 3% coverage
   - mSOL/SOL, jitoSOL/SOL, BONK/SOL, WIF/SOL, USDC→SOL (reverse): **zero** cross-DEX direct coverage → structurally un-arbable via single-hop routes

3. **Divergence distribution (612 multi-DEX observations):**
   - SOL/USDC: min 0.00% / median 0.15% / max 0.47%
   - SOL/USDT: min 0.01% / median 0.19% / max 0.46%
   - USDC/USDT: min 0.01% / median 0.57% / max 0.57% (pinned at fee boundary)
   - **ZERO observations above the 0.6% round-trip fee threshold**
   - **ZERO divergence events logged for follow-up analysis**

4. **Key finding:** The B1 "preliminary" kill was vindicated by methodologically superior B3 data. The backfill bias we flagged in B1 (only seeing divergence when both pools had a swap in the same window) did NOT mask hidden opportunities — the market really is this efficient.

5. **USDC/USDT at 0.57% is structural, not arb:** The divergence is pinned at nearly exactly the fee boundary and doesn't oscillate. This is two stablecoin pools priced at slightly different intercepts relative to their internal liquidity curves — arb activity has already compressed the spread to the point where one more hop costs more than the spread yields.

**Extrapolation:** 22 additional hours at current observed rate (0/hour) = 0 additional events. Even assuming 5 rare memecoin-pump events we'd miss in the sample, daily total = 5, far below the 50/day "go" threshold.

**Reopen if:**
- New major DEX launches on Solana with significant isolated liquidity
- A specific pair develops recurring transient divergence (would require event-driven monitoring, not polling)
- Multi-hop routing (arb via intermediate tokens) is evaluated — we only tested direct single-hop routes
- Private-mempool access via Jito bundle sniffing opens (different strategy entirely — requires joining Jito searcher network)

---

### 2026-04-19 — Preliminary: Solana cross-DEX arb sizing (B1)

**Decision:** Solana cross-DEX arb from historical swap data shows $190/day at 100% capture. Does not pass $500/day target at realistic capture rates. However, methodology has known limitations — proceed to B3 (live pool state probe) before final kill. (Updated 2026-04-20: B3 confirmed the kill.)

**Evidence (B1, 7-day backfill, 2M swaps from 2,000 Parquet files):**

1. **Dataset:** 7.46M total swaps across 7 days. 79.8% Jupiter, 11.1% Orca, 9.2% Raydium.
2. **Pair overlap:** 15,484 unique pairs. Only 394 have 2+ active pools (>=3 swaps each).
3. **Divergence analysis (0.6%-20% band, 10-slot windows):**
   - 70 pairs showed cross-pool divergence above round-trip fee threshold
   - 30 dynamic (intermittent) — potentially real arb
   - 40 static (always-on) — phantom/stale pools (same pattern as 11 killed EVM strategies)
4. **USDC/SOL dominates:** 427 divergent windows out of 3,876 overlap windows (11% rate), 1.35% median divergence, 9,532 SOL total volume. This is the most competitive pair on Solana — realistic capture rate <1%.
5. **Most pairs show 0 SOL volume** in divergence windows — prices diverge but no tradeable liquidity.
6. **Revenue:** $190/day at 100% capture, $19/day at 10%, $1.91/day at 1%.

**Methodology gap:** This analysis only sees divergence when both pools have a swap in the same 10-slot window. Real arb compares live pool state (reserves/sqrtPrice) which changes every slot. The backfill approach is structurally biased toward undercounting — a live pool-state probe (B3) would catch divergences invisible to this method.

**Reopen if:**
- Live pool-state probe (B3) shows 10x more opportunities than historical swap analysis
- New DEX launches with significant volume and slow arb convergence
- Solana DEX volume 5x's from current levels
