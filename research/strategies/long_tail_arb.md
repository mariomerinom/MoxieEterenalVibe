# Strategy: Long-Tail Pool Arbitrage

## Status: KILLED

## Hypothesis

Pools with low daily volume (<$50K) and low TVL (<$500K) are underserved by MEV searchers because the profit per opportunity is small ($2-50). Major searchers optimize for large, high-frequency opportunities and ignore the long tail. But the long tail is wide: there are thousands of these pools vs. hundreds of liquid ones. If we can scan them cheaply, the aggregate could be meaningful.

Specifically: when a large-cap token (WETH, USDC, USDT) trades against a small-cap token on multiple pools, the small-cap pools update slowly. A swap on one pool creates a price discrepancy that persists for multiple blocks because nobody bothers to arb a $5 opportunity.

**Key bet:** The profit threshold of the competition is our opportunity. If top searchers have minimum thresholds of $50-100 per arb (to justify their infrastructure costs and bribe levels), everything below that threshold is open.

## Mechanism

Same as V2-V2 arb, but:
- Pool universe expanded to 2000+ pools (scan Uniswap V2/V3 factory events for all pools, not just the top 200)
- Lower min_profit_eth threshold ($1-5 vs. current $2-3)
- Lower bribe_pct (50-70% vs. 85%) -- we can bid less because nobody else is bidding
- Potentially batch multiple small arbs into a single bundle to amortize gas

## Estimated Opportunity

**Need data.** The research task is to answer:
1. How many V2 pools exist with WETH on one side and >$10K TVL?
2. Of those, how many have a duplicate (same pair, different pool)?
3. What does the reserve divergence look like block-to-block for these pools?
4. How many arbs on these pools were captured in the last 30 days (EigenPhi)?
5. What was the median profit of those arbs?

If there are 5,000+ pools, 500+ arb-eligible pairs, and the competition only captures opportunities >$50, we may have a window.

## Edge Thesis

**Scale + low overhead.** Our infrastructure cost is ~$20/month. We don't need $100/day to be profitable. A searcher running $50K/month in colocated infrastructure needs each strategy to clear that bar. We don't. Our breakeven is effectively zero.

This is the "cockroach" strategy -- survive on scraps that the big players leave behind. Not glamorous, but the math might work if the long tail is long enough.

## Kill Criteria

- If the expanded pool scan shows <100 additional arb-eligible pairs: not enough surface area
- If historical analysis shows these small arbs ARE being captured by existing bots: no gap exists
- If median profit per long-tail arb is <$0.50 after gas: even at low gas, not worth it
- If opportunities cluster in <10% of blocks: too sporadic to justify monitoring

## Data Requirements

- [ ] Full Uniswap V2 factory pool list (all pairs, not just top 200)
- [ ] Full Uniswap V3 factory pool list
- [ ] TVL estimates for each pool (can derive from reserves * price)
- [ ] Historical arb captures on small pools (EigenPhi or our own scan)
- [ ] Reserve divergence analysis: how often do paired small pools disagree by >0.5%?

## Implementation Estimate

- Pool universe expansion: 2-3 hours (query factory events, resolve tokens, filter by minimum TVL)
- Strategy changes: minimal (lower thresholds, same detection logic)
- Backtest: 3-4 hours (replay 7 days of blocks against expanded universe)

---

## Backtest Results

**Run: 2026-04-16** against expanded pool universe (4,780 pools, 2,276 V2+WETH).

Pool universe expansion results:
- 528,596 total WETH pools scanned (Uniswap V2 + SushiSwap + Uniswap V3)
- 19,560 arb-eligible (token in 2+ pools)
- 4,780 passed reserve filter (>1 ETH WETH reserve)
- 765 tokens with 2+ active pools
- 1,167 arb pairs total

Long-tail specific results:
- 1,261 long-tail pools (<=100 swaps in dataset)
- 542 liquid pools (>100 swaps)
- 62 V2-V2 arb pairs (across both categories)
- **Long-tail pairs produced only 15 opportunities (1.5% of total 968)**
- Long-tail median net divergence: 0.703% (higher than liquid 0.465%)
- But co-occurrence is the bottleneck: long-tail pools rarely trade in same block

Competition analysis on ALL V2-V2 opps:
- **96.9% of opportunities already captured by existing bots**
- Only 30/968 potentially uncaptured
- Even smaller pairs (REVV, FARM, BAND) are 71-88% captured

**Kill reason:** The long-tail hypothesis is half-right -- these pools DO have higher divergence when they co-trade. But they almost never co-trade because volume is too low. And when V2-V2 divergence does occur (in any pool), existing bots capture it at 97%. The "cockroach" niche doesn't exist -- the floor of competition is lower than expected.

## Paper Trade Results

Not yet run.

## Refinement Log

| Date | Change | Impact |
|------|--------|--------|

## Live Performance

Not yet live.
