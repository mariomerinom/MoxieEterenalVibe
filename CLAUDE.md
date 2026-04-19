# MEV Bot Project

## Objective

$1,000/day in profitable, repeatable MEV strategies.

Current stage: discovery and empirical validation of opportunities. We are not yet executing. We are finding out what works, with data, before committing capital.

## First Principle

Measure before you build. No execution code gets written until the opportunity it targets has been empirically quantified — with real data, from real infrastructure, cross-checked against ground truth. Theoretical projections, backtest estimates, and math-only signal detection are hypotheses, not evidence. The cost of a validation probe is always cheaper than building on a false premise.

This applies recursively. If a validation method itself hasn't been verified (e.g., a simulation that calls an empty address), it doesn't count as validation.

## Decision-Making

Decisions with resource implications get written down with their supporting evidence before work begins. "The numbers look good" is not evidence. Evidence is: raw data, sample size, methodology, known gaps, and what would falsify the conclusion. When evidence is thin, the next action is to collect more evidence — not to start building.

When a strategy is killed or approved, record why, what data supported it, and what would reopen the question. Dead ends are valuable if they're documented.
