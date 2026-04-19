#!/usr/bin/env python3
"""
Revenue estimate v2: count divergence EPISODES, not individual poll ticks.

An "episode" = a continuous period where divergence stays >0.3%.
If divergence fires every poll tick, that's ONE episode (one arb opportunity),
not N opportunities.
"""
import json
from collections import defaultdict

events = [json.loads(l) for l in open("research/data/divergence_events.jsonl")]
ts_range = max(e["ts"] for e in events) - min(e["ts"] for e in events)
hours = ts_range / 3600
daily_mult = 24 / max(hours, 0.001)

ETH_USD = 2350
POLL_INTERVAL = 12  # seconds between polls
EPISODE_GAP = 60  # seconds gap to count as new episode

# Pool liquidity
POOL_LIQ = {
    ("PEPE", "ethereum"): 5789, ("UNI", "ethereum"): 1095,
    ("SHIB", "ethereum"): 627, ("AAVE", "ethereum"): 352,
    ("LDO", "ethereum"): 90, ("CRV", "ethereum"): 16,
    ("LINK", "ethereum"): 23, ("MORPHO", "base"): 77,
    ("FAI", "base"): 41, ("SUP", "base"): 16,
    ("KEYCAT", "base"): 5, ("COOKIE", "base"): 1,
    ("PEPE", "arbitrum"): 0.006, ("UNI", "arbitrum"): 0.14,
}

cex = [e for e in events if e["type"] == "cex_dex" and e["divergence_pct"] < 50]

# Group by symbol+chain
by_key = defaultdict(list)
for e in cex:
    if e["divergence_pct"] >= 0.3:
        by_key[(e["symbol"], e["chain"])].append(e)

print(f"=== DIVERGENCE EPISODES ({hours:.2f}h sample) ===")
print(f"An episode = continuous >0.3% divergence (gap > {EPISODE_GAP}s = new episode)\n")

hdr = f"{'Token/Chain':<20} {'Liq(ETH)':>8} {'Ticks':>6} {'Episodes':>8} {'Ep/day':>8} {'Continuous?':>12} {'MedDiv%':>8}"
print(hdr)
print("-" * len(hdr))

for (sym, chain), evts in sorted(by_key.items(), key=lambda x: -len(x[1])):
    liq = POOL_LIQ.get((sym, chain), 0)

    # Count episodes (gaps between events > EPISODE_GAP)
    sorted_evts = sorted(evts, key=lambda e: e["ts"])
    episodes = 1
    for i in range(1, len(sorted_evts)):
        if sorted_evts[i]["ts"] - sorted_evts[i-1]["ts"] > EPISODE_GAP:
            episodes += 1

    ep_daily = episodes * daily_mult

    # Is it continuous? (fires every poll)
    continuous = len(sorted_evts) > 3 and episodes == 1

    divs = [e["divergence_pct"] for e in evts]
    med_div = sorted(divs)[len(divs) // 2]

    status = "ALWAYS ON" if continuous else f"{episodes} ep"

    print(f"{sym + '/' + chain:<20} {liq:>8.1f} {len(evts):>6} {episodes:>8} {ep_daily:>8.0f} "
          f"{status:>12} {med_div:>8.2f}")

# Summary
print(f"\n=== INTERPRETATION ===")
always_on = [(k, v) for k, v in by_key.items()
             if len(v) > 3 and len(set(int(e["ts"]) for e in sorted(v, key=lambda e: e["ts"]))) > 1]

continuous_count = 0
intermittent_count = 0
for (sym, chain), evts in by_key.items():
    sorted_evts = sorted(evts, key=lambda e: e["ts"])
    episodes = 1
    for i in range(1, len(sorted_evts)):
        if sorted_evts[i]["ts"] - sorted_evts[i-1]["ts"] > EPISODE_GAP:
            episodes += 1
    if episodes == 1 and len(evts) > 3:
        continuous_count += 1
    else:
        intermittent_count += 1

print(f"  Continuous divergences (ALWAYS ON): {continuous_count}")
print(f"  → These are structural price differences, NOT transient arb opportunities")
print(f"  → They mean the pool price doesn't match the CEX price permanently")
print(f"  → Causes: wrong token (symbol collision), frozen pool, different fee tier")
print(f"  → You can only arb this ONCE, and likely it won't actually close")
print(f"")
print(f"  Intermittent divergences: {intermittent_count}")
print(f"  → These appear and disappear = potentially real arb")

# Realistic revenue: only intermittent episodes on pools with >10 ETH
print(f"\n=== REALISTIC REVENUE (intermittent only, >10 ETH pools) ===")
total_rev = 0
for (sym, chain), evts in sorted(by_key.items(), key=lambda x: -len(x[1])):
    liq = POOL_LIQ.get((sym, chain), 0)
    if liq < 10:
        continue

    sorted_evts = sorted(evts, key=lambda e: e["ts"])
    episodes = 1
    for i in range(1, len(sorted_evts)):
        if sorted_evts[i]["ts"] - sorted_evts[i-1]["ts"] > EPISODE_GAP:
            episodes += 1

    if episodes == 1 and len(evts) > 3:
        continue  # Continuous = not real arb

    ep_daily = episodes * daily_mult
    divs = [e["divergence_pct"] for e in evts]
    med_div = sorted(divs)[len(divs) // 2] / 100

    trade_eth = min(500 / ETH_USD, liq * 0.05)
    gas = 3.0 if chain == "ethereum" else 0.03
    rev_per = trade_eth * med_div * ETH_USD - gas
    if rev_per > 0:
        daily_rev = ep_daily * rev_per * 0.5
        total_rev += daily_rev
        print(f"  {sym}/{chain}: {ep_daily:.0f} episodes/day x ${rev_per:.2f} net = ${daily_rev:.2f}/day")

if total_rev == 0:
    print(f"  NO intermittent arb opportunities on pools with >10 ETH liquidity")

print(f"\n  Total realistic daily revenue: ${total_rev:.2f}")
print(f"  vs $500/day target: {'VIABLE' if total_rev >= 500 else 'KILL'}")
