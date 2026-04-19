# Infrastructure Competitiveness Analysis

## Date: 2026-04-17

## The Question

Our backtest found 968 V2-V2 arb opportunities over 466 days (2.1/day), with 1.35 ETH/day theoretical gross. Competition analysis shows 96.9% already captured. But "captured" means profitable — and if we're fast enough, we capture them instead.

What does it cost to be fast enough, and does the math work?

## What We Know

### Revenue Side (from backtest)
- 968 opps in 466 days = **2.1 opps/day** (V2-V2 only, Ethereum mainnet only)
- Median profit per opp: **0.0048 ETH** (~$10 at $2,100/ETH)
- Total theoretical: **1.35 ETH/day** (~$2,835/day)
- This is V2-V2 *only* — does not include:
  - V2-V3 mixed arbs (2,504 V3 pools not yet tested)
  - V3-V3 arbs
  - 3-hop cycles
  - Multi-chain (Base, Arbitrum)
- Realistic capture rate for a competitive mid-tier searcher: **10-30%** of opportunities

### Revenue Estimates by Capture Rate

| Capture Rate | Gross/Day | After 90% Bribe | After Gas ($3/tx) | Net/Day | Net/Month |
|---|---|---|---|---|---|
| 5% | 0.068 ETH ($142) | $14.20 | $13.88 | ~$14 | ~$420 |
| 10% | 0.135 ETH ($284) | $28.40 | $27.77 | ~$28 | ~$840 |
| 20% | 0.270 ETH ($567) | $56.70 | $55.44 | ~$55 | ~$1,650 |
| 30% | 0.405 ETH ($851) | $85.10 | $83.20 | ~$83 | ~$2,490 |

**Critical: these are V2-V2 only.** Adding V3 arbs + multi-chain could 3-5x the opportunity surface.

## Infrastructure Tiers

### Tier 1: Minimum Viable ($150-400/month)
- **Hetzner AX102** dedicated server: ~$130/month
  - 64GB RAM, 2x 1TB NVMe, AMD Ryzen 5
  - Located in Germany (EU data centers)
- **Self-hosted Reth node**: runs on same server
  - <1ms RPC latency (local)
  - Real-time mempool access
- **Flashbots bundle submission**: free
- **Total: ~$150/month**

**Expected edge:** Eliminates the 30-80ms Alchemy RPC latency. Own node means we see pending txs in mempool, not just committed state. Competitive on **long-tail arbs** where other searchers have higher profit thresholds.

**Break-even:** Need 10% capture rate → ~$840/month net. Very achievable at $150 cost.

### Tier 2: Competitive Small Searcher ($800-2,000/month)
- Everything in Tier 1, plus:
- **bloXroute BDN** ($420/month): 1-3ms faster mempool propagation
- **Hetzner AX162** upgrade ($200/month): more CPU for parallel simulations
- **Multiple builder endpoints**: Flashbots + Titan + beaverbuild
- **Total: ~$750-1,000/month**

**Expected edge:** Faster mempool awareness, multi-builder submission increases inclusion probability. Competitive on **mid-frequency arbs** ($5-50 profit range).

**Break-even:** Need 20% capture rate → ~$1,650/month net. Solid margin at $1K cost.

### Tier 3: Professional ($3,000-8,000/month)
- **Equinix colocation** (NY5 or similar): $800-2,500/month
- **Custom Reth node** with mempool streaming
- **Direct builder relationships**
- **Redundant infrastructure**
- **Total: ~$3,000-5,000/month**

**Expected edge:** Sub-millisecond latency to builders. Competitive on **all Ethereum mainnet arbs** including high-frequency.

**Break-even:** Need 30%+ capture on V2-V2 alone, or expand to V3 + multi-chain.

## Recommended Path

### Phase 1: Tier 1 ($150/month) — Validate
1. Provision Hetzner AX102
2. Sync Reth node
3. Deploy our existing Rust bot
4. Run with expanded pool universe (4,780 pools)
5. **Measure real capture rate over 2 weeks**
6. If >5% capture → confirm revenue, proceed to Phase 2
7. If 0% capture → latency still insufficient, need Tier 2

### Phase 2: Expand Strategy Surface
- Add V3 support (sqrtPrice math, V2-V3 mixed arbs)
- Add 3-hop cycles
- Add Base + Arbitrum chains
- Each addition multiplies opportunity surface without infra cost increase

### Phase 3: Tier 2 ($1K/month) — Scale
- Only after Phase 1+2 confirm revenue
- Add bloXroute BDN
- Multi-builder submission
- Expected: 2-3x capture rate improvement

## Key Insight

The bottleneck right now isn't strategy — it's that we're running on a shared VPS with remote RPC calls. A $150/month Hetzner server with a local Reth node eliminates the biggest latency component (RPC round-trip). We don't need Equinix colocation to start winning — we need to not be 50ms behind on every state read.

**The 97% capture rate tells us the money is there. The question is whether $150/month in infrastructure gets us from 0% to 10% of it.**

## Risk Factors

1. **Competition tightens:** Builder tips already at 85-95%. Could go higher.
2. **Gas spikes:** Low gas environment now (0.04 gwei). If gas rises, small arbs become unviable.
3. **Protocol changes:** EIP changes, new AMM designs, PBS evolution could shift dynamics.
4. **Reth sync time:** Full sync takes 2-3 days. Time to revenue isn't instant.
5. **Alchemy rate limits:** Current free tier may not support full expansion. Own node solves this.
