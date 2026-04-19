# MEV-Share Backrun: Empirical Profit Analysis

**Date:** 2026-04-18
**Verdict:** Kill. $5.53/day at 100% capture. Does not approach $1,000/day target.

---

## Methodology

Three-stage pipeline, each stage grounding the previous one in harder data:

1. **SSE Probe** (13.6h, 1.29M hints) — stream MEV-Share, cross-reference against our 200-pool Ethereum universe, classify as backrunnable
2. **Per-Pool Profit Estimator** — fetch on-chain state for each pool, simulate victim swap impact, compute optimal backrun arb against counterpart pools, weight by per-pool hint frequency
3. **On-Chain Swap Size Validation** — fetch actual Swap event logs from the 7 pools that showed profit, measure real swap size distributions

---

## Stage 1: Funnel

| Metric | Value |
|--------|-------|
| Total hints/day | ~2.27M |
| Matched our pools | 6.4% |
| Backrunnable | 4.4% (~84K/day) |
| Unique after dedup | ~81K/day (18% dup rate) |
| Top pairs | WETH/USDT, USDC/WETH, WBTC/WETH |

High volume. Looked promising.

---

## Stage 2: Per-Pool Profit

Backrun mechanics: victim swaps WETH→other on pool A, moving price. Backrunner buys other on undisturbed pool B, sells on pool A at the shifted price.

**Critical finding:** Only pools with <120 ETH liquidity produce any arb profit at 1 ETH swap size. All pools with >1,000 ETH liquidity show zero — the price impact per swap is too small to overcome two layers of swap fees.

| Pool | Pair | Liq (ETH) | Freq/day | Profit @1ETH | Daily Net |
|------|------|-----------|----------|--------------|-----------|
| 0x17c1ae | WETH/USDT | 62.5 | 642 | 0.0125 ETH | 5.16 ETH |
| 0x397ff1 | USDC/WETH | 57.8 | 216 | 0.0154 ETH | 2.36 ETH |
| 0x2e8135 | USDC/WETH | 60.2 | 334 | 0.0143 ETH | 3.29 ETH |
| 0xabb097 | DAI/WETH | 78.1 | 746 | 0.0112 ETH | 5.02 ETH |
| 0xb771f7 | ???/WETH | 8.3 | 55 | 0.0927 ETH | 4.81 ETH |

**Total at 1 ETH avg swap, 100% capture: 24 ETH/day (~$60K/day).** But this assumes every hint represents a 1 ETH swap on these tiny pools.

---

## Stage 3: Ground Truth

Fetched actual on-chain Swap events for the 7 "profitable" pools over 2,000 blocks (~7 hours).

**6 of 7 pools had zero swaps.** The probe was counting hints where the pool address appeared in logs — not swaps that executed on that pool.

The one active pool (0x397ff, USDC/WETH SushiV2, 57.8 ETH liquidity):

| Metric | Value |
|--------|-------|
| Swaps in 7h | 48 (~173/day) |
| Median swap | 0.036 ETH |
| Mean swap | 0.109 ETH |
| P25 / P75 | 0.006 / 0.072 ETH |
| % under 0.1 ETH | 68.8% |
| % over 0.5 ETH | 8.3% (~14/day) |

Applying the actual swap size distribution:

| Swap Bucket | Freq/day | Profit/swap | Net after gas | Daily |
|-------------|----------|-------------|---------------|-------|
| ~0.005 ETH | 44.6 | 0.000000 | 0.000000 | $0.00 |
| ~0.030 ETH | 68.6 | 0.000014 | 0.000000 | $0.00 |
| ~0.075 ETH | 20.6 | 0.000087 | 0.000000 | $0.00 |
| ~0.250 ETH | 17.1 | 0.000963 | 0.000000 | $0.00 |
| ~0.550 ETH | 13.7 | 0.004662 | 0.000162 | $0.56 |
| **Total** | **164.6** | | | **$5.53** |

**$5.53/day at 100% capture. $0.55/day at 10% capture.**

---

## Why It Fails

The strategy requires three conditions, and the third kills it:

1. **Price impact large enough to create arb** — needs small pools (< ~100 ETH). ✅ These exist.
2. **Counterpart pool to arb against** — needs the same pair on a second DEX. ✅ These exist.
3. **Enough large swaps hitting the small pool** — needs consistent flow of 0.5+ ETH swaps. ❌ Actual median is 0.036 ETH.

Swap routers (1inch, Uniswap router, aggregators) direct nearly all volume to the deepest pools. Small V2 pools get only dust-sized trades and occasional misrouted transactions. The arb profit per swap scales quadratically with swap size — a 0.036 ETH swap produces 1/770th the profit of a 1 ETH swap.

---

## Reopening Conditions

| Condition | What changes |
|-----------|-------------|
| Pool universe expanded 5x+ | More small active pools may exist outside our 200 |
| Sandwich strategy evaluated | Different profit mechanics — captures from price movement on victim, not inter-pool arb |
| MEV-Share reveals swap amounts | Enables selective targeting of only large swaps, avoiding gas waste |
| Market structure shift | If aggregator routing changes and small pools get more flow |

---

## Artifacts

| File | Purpose |
|------|---------|
| `scripts/research/mevshare_backrun_probe.py` | SSE streaming probe |
| `scripts/research/analyze_backrun_probe.py` | Probe data analyzer |
| `scripts/research/estimate_backrun_profit.py` | Per-pool profit estimator |
| `scripts/research/check_pool_swap_sizes.py` | On-chain swap size validator |
| `scripts/research/sensitivity_analysis.py` | Revenue sensitivity matrix |
| `research/data/mevshare_backrun_hints.jsonl` | 13.6h of raw probe data (on droplet) |
