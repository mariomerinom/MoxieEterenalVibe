# Strategy R&D Process

## Why This Document Exists

The bot runs. The pipeline works. But running textbook arb against 200 liquid pools and finding nothing is the expected outcome -- that surface is already picked clean. The gap isn't engineering, it's strategy R&D: a disciplined process for finding, validating, and refining approaches that actually produce edge.

Every quantitative operation that survives learns the same lesson: the first version of any strategy doesn't work. The tenth version might. What separates the projects that get to version ten from those that don't is whether there's a repeatable process for moving from "idea" to "data says no" to "revised idea" to "data says maybe" fast enough that you don't run out of time or money.

This document defines that process.

---

## The Strategy Lifecycle

Every strategy moves through five stages. Most die at stage 2 or 3. That's normal and healthy -- killing bad ideas early is the point.

```
  RESEARCH          BACKTEST          PAPER TRADE        REFINE            LIVE
  ----------        ----------        ----------         ----------        ----------
  Hypothesis        Historical        Real-time          Parameter         Real money
  Data gathering    replay            dry-run            tuning            Risk limits
  Feasibility       Hit rate          Latency check      Edge decay        P&L tracking
  Kill criteria     Profit dist.      Execution gaps     Competition       Post-mortems
                                                         monitoring
      |                 |                  |                  |                 |
      v                 v                  v                  v                 v
   Go / Kill         Go / Kill          Go / Kill         Go / Kill       Continue /
                                                                          Retire
```

### Stage 1: Research

**Goal:** Decide whether to write any code at all.

Every strategy starts as a written hypothesis with specific, falsifiable predictions. Not "there might be arb in long-tail pools" but "pools with <$50K daily volume on Uniswap V2 have price corrections that lag liquid pools by 2-10 blocks, creating arb windows of $5-50 per event, occurring 20-100 times per day."

**Deliverable:** A strategy brief (template below) that lives in `research/strategies/`.

**Data work:**
- Query our existing DuckDB/Parquet data for evidence
- Write throwaway Python scripts in `scripts/research/` to probe the hypothesis
- Pull external data if needed (Dune, EigenPhi, Flashbots transparency dashboard, builder block data)

**Kill criteria check:** Before writing any Rust, the brief must show:
- Estimated daily opportunity count (with data backing)
- Estimated profit per opportunity (with data backing)
- Estimated competition (who else does this, how fast)
- Why we specifically could capture some fraction
- What would prove us wrong

If you can't fill in those fields, you don't have a strategy -- you have a vibe.

### Stage 2: Backtest

**Goal:** Replay historical data and measure what would have happened.

This is where most ideas die, and that's the point. The backtest doesn't need to be perfect -- it needs to be honest. Common ways backtests lie:

- **Latency assumption of zero.** Your backtest sees the block instantly. In production you see it 200ms+ late. Every opportunity that requires being first is fake in a backtest.
- **Infinite liquidity assumption.** Your trade doesn't move the price in the backtest. In production it does.
- **Survivor bias.** You test against pools that exist today. Many didn't exist 6 months ago. The ones that failed aren't in your dataset.
- **Gas estimation lies.** Static gas estimates miss storage slot cold/warm differences, EIP-2929 access lists, actual calldata costs.

**Process:**
1. Select a block range (minimum 7 days, ideally 30+)
2. For each block in range, run the strategy's detection logic against historical state
3. For each detected opportunity, simulate the actual transaction via revm fork at that block
4. Record: block, opportunity details, simulated profit, simulated gas, net P&L
5. Compute: hit rate, profit distribution (median, p10, p90), drawdown, Sharpe-like ratio

**Deliverable:** Backtest report appended to the strategy brief. Include the distribution, not just the average. A strategy with $100 average profit but $5 median profit is actually a strategy that almost never works and occasionally gets lucky.

**Kill criteria:**
- Median net profit per opportunity < 0 after realistic gas
- Hit rate < 5% (you're mostly paying gas for nothing)
- More than 50% of profit comes from < 5% of events (fragile)
- Simulated annual profit < $10K (not worth the operational risk)

### Stage 3: Paper Trade

**Goal:** Prove it works in real-time, not just in replay.

The backtest showed opportunity existed historically. Paper trading answers: can we actually see it and act on it fast enough, right now, with our actual infrastructure?

**Process:**
1. Implement the strategy behind the `Strategy` trait
2. Run the bot in `--mode dry-run`
3. Log every detection with full context (the DryRunEntry format)
4. Run for minimum 48 hours, ideally 7 days
5. Cross-reference: for each opportunity we logged, check EigenPhi/Flashbots explorer to see if someone else actually captured it. If nobody did, our detection may be wrong. If someone did, measure our timing gap.

**What you learn that backtesting can't teach:**
- Actual latency from block to detection to would-be-submission
- Whether reserve queries hit rate limits or timeout
- Whether the opportunity still exists by the time we'd submit (it usually doesn't for competitive strategies)
- Patterns in when opportunities cluster (time of day, gas price regimes, market volatility)

**Deliverable:** Paper trading report. Must include the timing analysis -- if we're consistently >2 seconds behind the bot that actually captured the opportunity, no amount of parameter tuning fixes that.

**Kill criteria:**
- Zero opportunities detected in 48 hours
- Consistently >3 seconds behind real captures
- Detection rate drops >50% from backtest (our model was overfitting historical data)

### Stage 4: Refine

**Goal:** Tune parameters, expand surface area, adapt to competition.

This is the stage most people skip, and it's the stage that actually produces edge. The first implementation of any strategy uses default parameters. Refinement means:

**Parameter optimization:**
- `min_profit_eth` -- too high misses small-but-consistent opportunities; too low sends bundles that get rejected
- `bribe_pct` -- too low gets outbid; too high gives away the profit. This is an adversarial optimization against other searchers
- Gas limits and safety margins
- Pool filters (minimum liquidity, maximum age, protocol whitelist)

**Surface area expansion:**
- Add pools (200 -> 2000+)
- Add protocols (V2 -> V3, Curve, Balancer)
- Add hop count (2-hop -> 3-hop)
- Add chain (Ethereum -> Base, Arbitrum)

**Competition monitoring:**
- Track which of our paper-trade opportunities were captured by others
- Identify the winning searcher addresses
- Measure their response time and bribe levels
- Detect if competition is increasing or decreasing over time

**Edge decay tracking:**
- Every edge decays. The question is how fast.
- Weekly review: is the opportunity count trending down? Is the average profit per opportunity shrinking? Are more competitors appearing?
- If decay is faster than refinement, the strategy is dying. Either pivot or retire.

**Deliverable:** Updated strategy brief with revised parameters, expanded scope, and competition analysis. This stage loops -- you refine, paper trade again, refine again.

### Stage 5: Live

**Goal:** Make money. Don't lose money.

**Go-live checklist:**
- [ ] Contract deployed and funded
- [ ] Token approvals set for all pools in scope
- [ ] Trading wallet funded with gas ETH (minimum 0.1 ETH)
- [ ] Flashbots signing key configured
- [ ] Risk limits set: max bundle value, max gas price, max position size
- [ ] Monitoring: alerts for failed bundles, reverts, balance depletion
- [ ] Kill switch: ability to halt in <30 seconds

**Risk limits (hardcoded, not configurable):**
- No single bundle > 5 ETH value
- No submission when gas > 200 gwei
- Halt if 10 consecutive bundle failures
- Halt if wallet balance < 0.02 ETH

**Post-mortems:**
- Every failed bundle gets logged with full context
- Weekly review: what worked, what didn't, what changed in the market
- Monthly review: total P&L, comparison to backtest projections, strategy health assessment

---

## Strategy Brief Template

Every strategy gets one of these. Lives in `research/strategies/{name}.md`.

```markdown
# Strategy: {Name}

## Status: {Research | Backtest | Paper Trade | Refine | Live | Retired}

## Hypothesis
One paragraph. What is the opportunity, why does it exist,
and why would it persist?

## Mechanism
How the strategy works, step by step.
What on-chain actions does it take?

## Estimated Opportunity
- Frequency: X per day (source: ...)
- Profit per event: $X median, $X mean (source: ...)
- Gross daily: $X
- After gas: $X (assuming Y gwei avg)
- After bribe (Z%): $X
- Competition: {none | low | medium | high | extreme}

## Edge Thesis
Why we specifically can capture this. What do we have or do
that others don't? If the answer is "nothing," say so --
that's a signal to find edge before proceeding.

## Kill Criteria
Specific, measurable conditions under which we abandon this:
- ...
- ...

## Data Requirements
What data do we need to validate? Where does it come from?

## Implementation Estimate
Hours to implement, dependencies on other work.

---

## Backtest Results
(Filled in at Stage 2)

## Paper Trade Results
(Filled in at Stage 3)

## Refinement Log
(Ongoing at Stage 4+)
| Date | Change | Impact |
|------|--------|--------|

## Live Performance
(Ongoing at Stage 5)
| Week | Opportunities | Bundles Sent | Included | Revenue | Gas | Net |
|------|--------------|-------------|----------|---------|-----|-----|
```

---

## Current Strategy Pipeline

| Strategy | Stage | Next Action |
|----------|-------|-------------|
| V2-V2 arb (200 pools) | Paper Trade | Running. Expand pool universe to find if long-tail has signal. |
| V2-V3 / V3-V3 arb | Research | Need sqrtPrice math. Write brief first. |
| 3-hop arb | Research | Combinatorial explosion concern. Write brief with cycle count estimates. |
| Long-tail pool arb | Research | Hypothesis: pools with <$50K volume have slower correction. Need data. |
| CEX-DEX arb | Research | Different infrastructure (CEX API feeds). Write brief with latency analysis. |
| MEV-Share backrun | Research | Need to evaluate MEV-Share hint stream quality. Write brief. |
| Liquidation | Research | Need Aave/Compound health factor monitoring. Write brief. |
| Cross-domain (L1-L2) | Not started | Requires bridge understanding. Low priority. |

---

## Weekly Process

**Every Monday (30 minutes):**

1. Review dry-run log from the past week
   - Opportunities detected (count, distribution)
   - Any that were actually captured by others (cross-ref EigenPhi)
   - Timing analysis (how late are we vs. the winner)

2. Review each active strategy brief
   - Is it progressing through stages?
   - Has anything invalidated the hypothesis?
   - Any parameter changes to test?

3. Pick one research item to advance
   - Write or update a strategy brief
   - Run an analysis script
   - Start a backtest

4. Update the pipeline table above

**The discipline is the edge.** The bot that wins isn't the one with the cleverest first idea. It's the one that systematically tests and discards 50 ideas to find the 3 that work, then relentlessly optimizes those 3 while monitoring for decay. The process is the product.

---

## File Structure

```
research/
  strategies/
    v2_v2_arb.md          # Current active strategy brief
    long_tail_arb.md      # Research stage
    v3_arb.md             # Research stage
    cex_dex.md            # Research stage
    ...
  data/
    backtest_v2_arb_apr2026.csv
    paper_trade_week1.jsonl
    ...

scripts/
  research/
    probe_longtail.py     # Throwaway analysis scripts
    backtest_runner.py
    competition_tracker.py
    ...
```
