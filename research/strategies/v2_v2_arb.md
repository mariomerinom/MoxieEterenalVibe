# Strategy: V2-V2 Cross-Pool Arbitrage

## Status: KILLED (on Ethereum mainnet V2-V2 only; evolving to V2-V3 mixed)

## Hypothesis

When two Uniswap V2 pools share a token pair (directly or through WETH as a base), their reserves drift apart between blocks due to different trade flow. The resulting price discrepancy creates a risk-free arbitrage: buy on the cheap pool, sell on the expensive pool, pocket the difference minus fees and gas.

This opportunity exists because V2 pools are passive -- they don't rebalance on their own. Every price correction requires someone to execute a trade. The opportunity persists because new trades continuously push prices apart again.

## Mechanism

1. On each new block, fork Ethereum state at block N via revm
2. For each precomputed 2-hop cycle (WETH -> Pool1 -> Intermediate -> Pool2 -> WETH):
   - Call `getReserves()` on both pools via the fork
   - Compute the analytically optimal input amount:
     `x_opt = sqrt(rA1 * rB1 * rB2 * rA2) * 0.997 - rA1 / 0.997`
   - Compute expected output and profit
3. If profit > gas cost + builder bribe + min_profit_eth:
   - Encode `executeArb(hops, WETH, amount, minProfit)` calldata
   - Emit Action for bundle submission
4. On-chain: MevBot.sol executes the multi-hop swap atomically, reverts if unprofitable

## Estimated Opportunity

- Frequency: Unknown -- 0 detected in initial dry-run across ~10 blocks
- Profit per event: Unknown
- Pool universe: 200 pools, 174 cycles, 109 intermediate tokens
- Competition: **Extreme.** Every MEV searcher runs this exact strategy. Top competitors have sub-100ms latency with colocated infrastructure.
- Gross daily: Need data
- After gas (0.05 gwei avg currently): Need data
- After bribe (85%): Need data

## Edge Thesis

**We currently have no edge on this strategy as configured.**

200 liquid pools are the most watched surface in MEV. Our infrastructure (VPS + public RPC + WebSocket) adds 200-500ms latency vs. colocated searchers with private mempools. For any opportunity that requires speed, we lose.

Possible paths to edge:
1. **Expand to long-tail pools** (2000+ pools) where competition thins out
2. **Add V3 and mixed-protocol** cycles that require more complex math
3. **Add 3-hop cycles** that are combinatorially harder to search

This strategy is currently serving as the **test bed for the pipeline**, not as a revenue source.

## Kill Criteria

- If 7 days of paper trading at 200 pools shows zero opportunities: confirmed that this surface is fully arbitraged by faster bots. Expected outcome.
- If expanding to 2000+ pools still shows zero after 7 days: the reserve-scanning approach itself may be too slow (opportunities are captured within the same block by mempool-aware bots before state is even committed).
- If expanding to 2000+ pools shows opportunities but all are <$1 net: not worth the operational complexity.

## Data Requirements

- [x] Pool token pairs with reserves (pool_tokens.json, 200 pools)
- [x] Expanded pool universe (pool_tokens_full.json, 4,780 pools from 528K scanned)
- [x] Historical swap-based backtest (134K swaps, 45K blocks)
- [x] Competition analysis: 96.9% capture rate, 3.1% gap
- [ ] Competition timing data (when do arbs actually get included relative to block time)

## Implementation Estimate

Already implemented. Running in paper trade mode.

---

## Backtest Results

**Run 1 (initial, 200 pools):** 20 opportunities in 465 days from 7 arb pairs. Median net divergence 0.18%. Confirmed: tiny surface, fully arbed.

**Run 2 (expanded, 4,780 pools): 2026-04-16**

Pool expansion scan:
- Scanned 528,596 WETH pools from Uniswap V2, SushiSwap, Uniswap V3 factory events
- 19,560 arb-eligible (token in 2+ pools), 4,780 passed reserve filter (>1 ETH)
- 2,276 V2+WETH pools, 2,504 V3+WETH pools
- 765 tokens with 2+ active pools, 1,167 total arb pairs

V2-V2 backtest (only V2/Sushi pools, same AMM mechanics):
- 66 V2-V2 arb pairs with swap data (out of 1,167)
- 134,159 swaps across 45,054 blocks (~465 days)
- **968 opportunities detected**
- 2.1 opps/day, median profit 0.0048 ETH ($10.10)
- Total theoretical: 628.5 ETH over 466 days (1.35 ETH/day)

Competition analysis:
- **96.9% of opportunities already captured by existing bots**
- Only 30/968 potentially uncaptured (3.1%)
- Even lower-profile pairs (REVV 71%, FARM 88%) are mostly taken
- Top pairs (TRU, AMP, REN) captured at 95-100%

**Kill reason for V2-V2:** The space is saturated. Expanding from 200 to 4,780 pools found real signal (968 opps vs 20 before), but existing searchers are already capturing it at 97%. Our infrastructure disadvantage (200ms+ latency) is disqualifying when competitors catch these within the same block.

**Next evolution:** V2-V3 and V3-V3 mixed arbs -- different AMM mechanics may create opportunities that current V2-only bots miss. Also: multi-chain (Base, Arbitrum) where competition may be thinner.

## Paper Trade Results

**Run 1: 2026-04-16, ~10 blocks**
- Blocks observed: ~10 (24895629 - 24895638)
- Cycles scanned per block: 174
- Scan time per block: ~300ms
- Opportunities found: 0
- Assessment: Pipeline validation only. As expected, 200 liquid pools fully arbitraged.

## Refinement Log

| Date | Change | Impact |
|------|--------|--------|
| 2026-04-16 | Initial implementation, 200 pools, V2-V2 only | Pipeline works, zero opportunities as expected |
| 2026-04-16 | Expanded to 4,780 pools (528K scanned) | 968 opps found, but 97% already captured |
| 2026-04-16 | Competition analysis: 96.9% capture rate | **KILL for V2-V2 on mainnet** |
| | Next: V2-V3 mixed arb | Research phase |
| | Next: multi-chain (Base, Arb) | Research phase |

## Live Performance

Not yet live. Killed before live deployment -- no edge.
