# Testing Strategy

## Current Stage: Research Mode — Ad-Hoc Only

We are in discovery and empirical validation. Every Python research script
is single-use: it answers one question, produces output we read with our
eyes, and gets archived. Writing tests for throwaway code is waste.

The work product is not the code — it's the decisions in `decisions.md`.
Tests would validate code, not decisions. The decisions are cross-checked
by the probe/estimator/ground-truth triangulation itself.

**What substitutes for tests right now:**
- Output inspection (is the number plausible? $43M/day from stale pools got
  flagged on sight)
- Cross-validation (probe funnel numbers vs on-chain ground truth)
- Phantom-signal patterns we've learned to look for (identical value across
  blocks = dead pool; >50% divergence = stale price)
- The decision log itself — bad conclusions get reversed by later entries

**Rust crates (`capture`, `analysis`, etc.):** `cargo test` covers the
parser/math units that matter. No integration test suite — we lean on
empirical validation to catch logic errors (e.g., the Solidly math
mismatch surfaced in the decision log, not in a unit test).

## Threshold for Adding Tests

Tests become mandatory when execution code ships — the moment we start
sending bundles that can lose real money.

**First execution pipeline that goes live** (currently gated on the Geth
mempool probe) must have:

1. **Unit tests for swap math.** V2 constant-product, V3 exactInputSingle
   decode, Solidly `getAmountOut` interpolation. The Solidly kill taught us
   this: a math error cost weeks of phantom signals. Every swap-math branch
   in `dex_arb.rs` / `pool_graph.rs` needs a test anchored to an on-chain
   `getAmountOut` reference value.

2. **Simulation parity.** For a fixed block and fixed bundle, ForkedEvm
   must produce the same profit number as a direct on-chain `eth_call`.
   Run as a cargo integration test against a pinned block. This catches
   the "0x000 contract address" vacuous-sim class of bug before it costs
   us.

3. **Dry-run smoke before any live run.** A release of the sandwich
   binary submits one scripted bundle to the Flashbots **simulation**
   endpoint (`eth_callBundle`) and asserts profit > 0. If that fails,
   live mode stays disabled.

4. **Regression case per killed bug.** Every time a bug costs a real
   revert on-chain, it becomes a regression test with the exact calldata
   and state that triggered it. Don't re-introduce known-bad behavior.

**What we will NOT build:**
- TDD from scratch on research scripts. They're written to be discarded.
- Coverage targets. Coverage of throwaway code is a vanity metric.
- A CI pipeline for the research crates beyond `cargo check && cargo test`.
- Mocks of on-chain state. We fork real state via ForkedEvm; mocks would
  hide the kind of integration bugs that actually hurt us.

## Trigger

Add tests **the same commit** that wires a strategy from dry-run into
`--live` mode. Not before.
