# MEV Execution Stack - Build Plan

**Date:** April 2026
**Goal:** Sub-1% capture rate on Ethereum DEX arb and sandwich MEV
**Prerequisite:** Completed data capture + business sizing (see `eth_mev_sizing.md`)

---

## Current State -> Target State

| | Now | Target |
|---|-----|--------|
| Mode | Offline observation | Live extraction |
| Latency | Hours (batch parquet) | <200ms (block-reactive), <50ms (mempool) |
| Execution | None | Atomic on-chain bundles |
| Revenue | $0 | $84K+/month at sub-1% capture (arb only) |

---

## The Four Layers

### Layer 1: On-chain contract

Solidity contract deployed on Ethereum for atomic execution. Two entry points:

**Sandwich:**
- Receive front-run parameters (pool, token, amount, min profit)
- Execute swap in (front-run direction)
- After victim tx executes (bundle ordering handles this)
- Execute swap out (back-run direction)
- Revert if profit < minimum threshold

**Arbitrage:**
- Flash loan from Aave/Balancer (no upfront capital needed)
- Route through 2-4 pool chain exploiting price discrepancy
- Repay flash loan + fee
- Revert if net profit < gas + builder tip

**Security:**
- Owner-only execution (no one else can call)
- Profit threshold enforced on-chain (atomic revert = no loss on failed attempts)
- No token approvals left dangling
- Withdraw function for owner only

### Layer 2: Simulation engine

Local EVM simulation using `revm` (Rust - fits our existing crate). For each candidate opportunity:

1. Fork current chain state at latest block
2. Inject candidate transaction(s)
3. Simulate our contract call around/after them
4. Read final balances -> compute exact profit
5. If profitable after gas + builder tip -> submit bundle

Key requirements:
- State access via archive node RPC or local Reth node
- Pool reserve/tick reads for V2 (getReserves) and V3 (slot0, liquidity, ticks)
- Optimal amount calculation: binary search over input size to maximize profit curve
- Must complete in <100ms to be competitive

### Layer 3: Bundle submission

Submit transaction bundles to block builders via Flashbots and competitors:

- **Flashbots `eth_sendBundle`** - primary relay, largest market share
- **MEV-Share** - newer Flashbots protocol, bid on orderflow hints
- **Beaverbuild, Titan, rsync** - parallel submission to multiple builders increases inclusion rate
- **Tip calculation** - `block.coinbase.transfer()` in contract. Start at 90% of profit to builder, optimize down as we learn inclusion rates

### Layer 4: Mempool feed

Real-time pending transaction stream. Required for sandwich, optional for arb.

- **Option A:** Full Geth/Reth node with `txpool_content` subscription (~2TB SSD, 32GB RAM)
- **Option B:** Bloxroute BDN subscription (managed, lower ops burden, $500-1,500/mo)
- **For block-reactive arb only:** Skip this layer entirely - subscribe to `newHeads` via websocket, react to confirmed blocks

---

## Build Order and Timeline

| # | Component | Time | Depends on | Unlocks |
|---|-----------|------|------------|---------|
| 1 | On-chain contract (sandwich + arb, Solidity) | 2-3 hours | - | Testing on fork |
| 2 | `revm` simulation engine (Rust, in our crate) | 3-4 hours | Archive RPC access | Accurate profit calc |
| 3 | Block-reactive arb bot | 2-3 hours | 1 + 2 | **First revenue** |
| 4 | Flashbots bundle submission | 1-2 hours | 1 | Live execution |
| 5 | Multi-builder submission (Beaverbuild, Titan) | 1 hour | 4 | Higher inclusion rate |
| 6 | Mempool listener | 2-3 hours | Full node or Bloxroute | Sandwich capability |
| 7 | Sandwich bot (mempool -> simulate -> bundle) | 2-3 hours | 1 + 2 + 4 + 6 | Sandwich revenue |
| 8 | Monitoring + alerting | 1-2 hours | 3 or 7 | Ops visibility |

**Total to first arb revenue (components 1-4):** ~10 hours
**Total to full sandwich + arb stack (all components):** ~18 hours

### Fastest path: block-reactive arb

Components 1 -> 2 -> 3 -> 4. No mempool needed. Watch new blocks, detect cross-pool price discrepancies, simulate, submit bundle. Can be live in a single day.

### Full stack: add sandwich

Components 6 -> 7 on top of the arb path. Requires mempool access (node or Bloxroute). Adds ~5 hours of build time plus infra setup.

---

## Infrastructure

### Minimum viable (arb only)

| Item | Monthly cost |
|------|-------------|
| Current droplet (capture + dashboard) | $50 |
| Alchemy Growth plan (archive RPC, websockets) | $200 |
| Execution droplet (4 vCPU, 8GB, co-located US-East) | $80 |
| **Total** | **$330/mo** |

### Full stack (arb + sandwich)

| Item | Monthly cost |
|------|-------------|
| Current droplet (capture + dashboard) | $50 |
| Reth archive node (dedicated, 2TB NVMe, 32GB) | $300-400 |
| OR Bloxroute BDN (mempool feed, no node needed) | $500-1,500 |
| Execution droplet | $80 |
| **Total** | **$430-1,630/mo** |

### Capital requirements

| Strategy | Capital needed | Notes |
|----------|---------------|-------|
| Arb (flash loan) | $0 upfront | Aave/Balancer flash loans, only pay fee on success |
| Arb (own capital) | $10-50K ETH | Saves flash loan fee (~0.05%), faster execution |
| Sandwich | $50-200K ETH | Need to front-run with real capital, no flash loan possible for sandwich |
| Sandwich (reduced) | $10-50K ETH | Smaller positions, fewer viable targets |

---

## Revenue Projections at Sub-1% Capture

From `eth_mev_sizing.md`, using confidence-adjusted estimates:

| Capture rate | Arb gross/mo | After builder tips (20% net) | Sandwich gross/mo | After tips | Combined net |
|-------------|-------------|------------------------------|-------------------|------------|-------------|
| 0.1% | $42K | $8.4K | $9K | $1.8K | $10.2K |
| 0.5% | $210K | $42K | $45K | $9K | $51K |
| 1.0% | $420K | $84K | $90K | $18K | $102K |

### Break-even analysis

| Scenario | Monthly cost | Break-even capture rate |
|----------|-------------|------------------------|
| Arb only, minimal infra | $330 | 0.004% |
| Full stack, Bloxroute | $1,630 | 0.03% |
| Full stack + $100K capital cost (opportunity) | $5,630 | 0.1% |

Even at 0.1% capture, the unit economics work. The constraint is engineering execution speed and builder tip optimization, not cost.

---

## Risk Factors

| Risk | Impact | Mitigation |
|------|--------|------------|
| Contract bug (funds drained) | Total loss of capital | Extensive fork testing, formal verification, start with flash-loan-only (no capital at risk) |
| Builder collusion / exclusive orderflow | Can't get bundles included | Multi-builder submission, MEV-Share participation |
| EIP changes reducing MEV (e.g., encrypted mempools) | Market shrinks | Arb survives encrypted mempools (block-reactive), only sandwich is affected |
| Competition drives tips to 99%+ | Margin compression | Focus on long-tail pools with less competition, multi-chain expansion |
| Smart contract gets blacklisted by builders | Execution blocked | Deploy multiple contract instances, rotate addresses |

---

## Decision Point

**Start with arb-only (components 1-4, ~10 hours, $330/mo)?**
- Zero capital risk with flash loans
- Simpler to build and debug
- Lower competition than sandwich on many pool pairs
- Can validate the full pipeline before adding sandwich complexity

**Or go straight to full stack (all components, ~18 hours, $430-1,630/mo)?**
- Sandwich is higher gross MEV per opportunity
- But requires capital and mempool access
- More moving parts to debug live
