# Strategy: CEX-DEX Arbitrage

## Status: Research

## Hypothesis

Centralized exchange prices move before on-chain prices. When Binance or Coinbase reprices ETH/USDC by 0.5%, the Uniswap pools still reflect the old price until someone arbs them. This is the dominant source of MEV on Ethereum today -- multiple analyses estimate it accounts for 50-80% of all arb revenue.

The latency between CEX price move and on-chain correction creates a predictable, recurring opportunity. The correction happens within 1-3 blocks. The question is whether we can be fast enough to capture it before the HFT-grade searchers who specialize in this.

## Mechanism

1. Maintain a real-time price feed from one or more CEXes (Binance WebSocket, Coinbase WebSocket)
2. Compare CEX mid-price to implied on-chain price from DEX reserves
3. When divergence exceeds threshold (e.g., 0.3% after fees):
   - Compute optimal trade size
   - Build arb transaction (buy cheap side, sell expensive side)
   - Submit via Flashbots within the same block
4. Direction can go either way: CEX-first (arb on-chain) or DEX-first (arb on CEX -- but this requires CEX capital)

For our use case, the one-directional version is simpler: CEX price moves, we correct the DEX pool and profit.

## Estimated Opportunity

From public data (Flashbots transparency reports, EigenPhi):
- CEX-DEX arb is ~$500K-1M/day on Ethereum
- Median opportunity: $50-500
- Frequency: 100-500 per day on major pairs
- Competition: **Extreme.** This is the most competitive MEV strategy. Winners have sub-10ms infrastructure.

## Edge Thesis

**We almost certainly don't have edge here in the near term.**

The winners at CEX-DEX are firms like Wintermute, Jump, and specialized MEV shops with:
- Colocated servers at both CEX data centers and Ethereum validators
- Custom networking stacks (kernel bypass, FPGA)
- Private order flow and builder relationships
- Millions in market-making inventory on both sides

Our latency disadvantage (200ms+) is disqualifying for the main pairs (ETH/USDC, ETH/USDT). However:

**Possible niche:** Less liquid CEX pairs that the big players ignore. If a small-cap token trades on both Binance and Uniswap V2, and the Binance volume is low enough that HFT firms don't bother, the same dynamic plays out at a smaller scale with less competition.

## Kill Criteria

- If we can't get CEX price data with <500ms latency: can't compete even on minor pairs
- If all CEX-DEX arbs on our target pairs are captured within 1 block by existing bots: no room
- If the infrastructure cost to be competitive exceeds expected revenue: negative ROI
- If Binance API rate limits prevent real-time monitoring of 50+ pairs: can't scale the niche approach

## Data Requirements

- [ ] Binance WebSocket feed latency test (how fast do we get price updates?)
- [ ] List of tokens that trade on both Binance and Uniswap with >$10K daily volume on each
- [ ] Historical CEX-DEX arb analysis: which pairs, how fast captured, by whom
- [ ] Infrastructure cost estimate for competitive latency

## Implementation Estimate

- CEX price feed integration: 4-6 hours
- Cross-venue price comparison engine: 3-4 hours
- Strategy implementation: 2-3 hours
- Total: 10-13 hours, plus ongoing latency optimization

This is a significant investment. Only proceed if the research phase shows a viable niche.

---

## Backtest Results

Not yet run.

## Paper Trade Results

Not yet run.

## Refinement Log

| Date | Change | Impact |
|------|--------|--------|

## Live Performance

Not yet live.
