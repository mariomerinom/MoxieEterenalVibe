# Strategy: MEV-Share Backrun

## Status: Research

## Hypothesis

Flashbots MEV-Share is a protocol where users submit transactions privately and searchers bid to backrun them. The user gets a share of the MEV, the searcher gets the rest, and the transaction is protected from frontrunning.

The key insight: MEV-Share provides a **hint stream** that reveals partial transaction information (which pools will be touched, approximate amounts) without revealing the full transaction. Searchers who can quickly interpret these hints and compute a profitable backrun get exclusive access to that MEV.

This is structurally different from open mempool MEV because:
1. The competition is limited to MEV-Share participants (smaller field)
2. The user transaction is guaranteed to be included (no risk of it disappearing)
3. The backrun is atomic with the user's transaction (no timing risk)

## Mechanism

1. Subscribe to MEV-Share hint stream (SSE endpoint)
2. For each hint:
   - Parse which pools/tokens are involved
   - Estimate the price impact of the user's trade
   - Compute the optimal backrun (arb the price dislocation the user's trade creates)
   - Build a bundle: [user_tx, our_backrun_tx]
   - Bid for inclusion (percentage of profit shared with user via mev_share protocol)
3. Submit bundle to Flashbots matchmaker

## Estimated Opportunity

From Flashbots MEV-Share dashboard:
- ~500-2000 hints per day
- Not all are backrunnable (many are simple transfers, small swaps)
- Backrunnable fraction: ~20-40%
- Revenue per successful backrun: $5-200 (highly variable)
- Searcher share after user refund: typically 50-90%
- Competition: **Medium.** Fewer searchers participate in MEV-Share than in open mempool. The protocol is newer and the hint parsing adds a barrier.

## Edge Thesis

**Possibly viable because the competition is structurally thinner.**

MEV-Share has a smaller searcher field than open MEV because:
- It requires integration with a specific protocol (barrier to entry)
- Hint parsing is non-trivial (you get partial info, not full calldata)
- The profit per event is smaller (shared with user)
- Big HFT firms focus on CEX-DEX, not MEV-Share

Our advantage would be speed of hint interpretation, not speed of network latency. If we can parse a hint and compute a backrun in <100ms, we're competitive even with our infrastructure disadvantage.

**Risk:** Flashbots controls the matchmaker. If they change the hint format, refund percentages, or matching algorithm, our strategy breaks. Platform dependency.

## Kill Criteria

- If MEV-Share hint volume drops below 100/day: insufficient flow
- If >80% of backrunnable hints are already captured by existing searchers at higher bids: outcompeted on price
- If hint parsing accuracy is <50% (we misidentify the opportunity): our interpretation is wrong
- If average net profit after user refund is <$2: not worth the complexity

## Data Requirements

- [ ] MEV-Share hint stream sample (subscribe for 24h, log all hints)
- [ ] Hint format analysis: what info is revealed, what's hidden
- [ ] Historical backrun success rates (Flashbots transparency data)
- [ ] Existing searcher bid levels (what percentage of profit are winners sharing)
- [ ] Our existing capture crate already has `MevShareCapture` -- assess what it provides

## Implementation Estimate

- Hint stream subscription: 1-2 hours (capture crate already has SSE support)
- Hint parser: 3-4 hours (depends on hint format complexity)
- Backrun computation: 2-3 hours (reuse arb math, but applied to post-trade state)
- Bundle format adaptation: 1-2 hours (MEV-Share bundles differ from standard Flashbots)
- Total: 7-11 hours

---

## Backtest Results

Not directly backtestable -- MEV-Share hints are real-time only. Can analyze historical matched bundles from Flashbots explorer.

## Paper Trade Results

Not yet run.

## Refinement Log

| Date | Change | Impact |
|------|--------|--------|

## Live Performance

Not yet live.
