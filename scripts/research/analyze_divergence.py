#!/usr/bin/env python3
"""Analyze divergence monitor output."""
import json
from collections import defaultdict

events = [json.loads(l) for l in open("research/data/divergence_events.jsonl")]
print(f"Total events: {len(events)}")

ts_range = max(e["ts"] for e in events) - min(e["ts"] for e in events)
print(f"Time span: {ts_range/60:.1f} minutes")

cex = [e for e in events if e["type"] == "cex_dex" and e["divergence_pct"] < 50]
cc = [e for e in events if e["type"] == "cross_chain" and e["divergence_pct"] < 50]

# CEX-DEX stability
by_key = defaultdict(list)
for e in cex:
    key = e["symbol"] + "_" + e["chain"]
    by_key[key].append(e["divergence_pct"])

print("\n=== CEX-DEX DIVERGENCE STABILITY ===")
print(f"{'Key':<25} {'N':>4} {'Min%':>8} {'Max%':>8} {'Range':>8} {'Static?':>8}")
print("-" * 65)
for key, divs in sorted(by_key.items(), key=lambda x: -max(x[1])):
    if max(divs) < 0.3:
        continue
    mn, mx = min(divs), max(divs)
    rng = mx - mn
    static = "YES" if rng < 0.15 else "no"
    print(f"{key:<25} {len(divs):>4} {mn:>8.2f} {mx:>8.2f} {rng:>8.2f} {static:>8}")

# Cross-chain stability
by_key2 = defaultdict(list)
for e in cc:
    key = e["symbol"] + "_" + e["chain_a"] + "_" + e["chain_b"]
    by_key2[key].append(e["divergence_pct"])

print("\n=== CROSS-CHAIN DIVERGENCE STABILITY ===")
print(f"{'Key':<35} {'N':>4} {'Min%':>8} {'Max%':>8} {'Range':>8} {'Static?':>8}")
print("-" * 75)
for key, divs in sorted(by_key2.items(), key=lambda x: -max(x[1])):
    if max(divs) < 0.3:
        continue
    mn, mx = min(divs), max(divs)
    rng = mx - mn
    static = "YES" if rng < 0.15 else "no"
    print(f"{key:<35} {len(divs):>4} {mn:>8.2f} {mx:>8.2f} {rng:>8.2f} {static:>8}")

# Summary
print("\n=== SUMMARY ===")
static_cex = sum(1 for divs in by_key.values() if max(divs) >= 0.3 and (max(divs)-min(divs)) < 0.15)
dynamic_cex = sum(1 for divs in by_key.values() if max(divs) >= 0.3 and (max(divs)-min(divs)) >= 0.15)
print(f"CEX-DEX >0.3%: {static_cex} static pairs (pricing error), {dynamic_cex} dynamic (potentially real)")

static_cc = sum(1 for divs in by_key2.values() if max(divs) >= 0.3 and (max(divs)-min(divs)) < 0.15)
dynamic_cc = sum(1 for divs in by_key2.values() if max(divs) >= 0.3 and (max(divs)-min(divs)) >= 0.15)
print(f"Cross-chain >0.3%: {static_cc} static pairs (pricing error), {dynamic_cc} dynamic (potentially real)")
