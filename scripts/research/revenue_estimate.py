#!/usr/bin/env python3
"""Estimate CEX-DEX arb revenue from divergence monitor data."""
import json
from collections import defaultdict

events = [json.loads(l) for l in open("research/data/divergence_events.jsonl")]
ts_range = max(e["ts"] for e in events) - min(e["ts"] for e in events)
hours = ts_range / 3600
daily_mult = 24 / max(hours, 0.001)

ETH_USD = 2350

# Pool liquidity (ETH) from on-chain check
POOL_LIQ = {
    ("PEPE", "ethereum"): 5789, ("UNI", "ethereum"): 1095,
    ("SHIB", "ethereum"): 627, ("AAVE", "ethereum"): 352,
    ("LDO", "ethereum"): 90, ("CRV", "ethereum"): 16,
    ("LINK", "ethereum"): 23, ("MORPHO", "base"): 77,
    ("FAI", "base"): 41, ("SUP", "base"): 16,
    ("KEYCAT", "base"): 5, ("COOKIE", "base"): 1,
    ("PEPE", "arbitrum"): 0.006, ("UNI", "arbitrum"): 0.14,
    ("1INCH", "arbitrum"): 0.0003, ("LINK", "arbitrum"): 6.4,
    ("CRV", "arbitrum"): 1.4, ("PEPE", "base"): 0.03,
    ("PRIME", "base"): 5.3, ("FARM", "base"): 2.2,
    ("LINK", "base"): 0.7, ("AAVE", "base"): 1.8,
    ("CRV", "base"): 1.9, ("AAVE", "arbitrum"): 0.37,
}

# CEX-DEX events with <50% divergence (filter obvious phantoms)
cex = [e for e in events if e["type"] == "cex_dex" and e["divergence_pct"] < 50]

# === ACTIONABLE: >0.3% divergence, >10 ETH liquidity ===
actionable = [e for e in cex
              if e["divergence_pct"] >= 0.3
              and POOL_LIQ.get((e["symbol"], e["chain"]), 0) >= 10]

print(f"=== ACTIONABLE CEX-DEX ({hours:.1f}h sample) ===")
print(f"Criteria: >0.3% divergence, >10 ETH pool liquidity\n")

by_sym = defaultdict(list)
for e in actionable:
    by_sym[(e["symbol"], e["chain"])].append(e["divergence_pct"])

hdr = f"{'Token/Chain':<20} {'Liq(ETH)':>10} {'Events':>6} {'Proj/day':>10} {'MedDiv%':>8} {'Rev/day$':>10}"
print(hdr)
print("-" * len(hdr))

total_rev = 0
for (sym, chain), divs in sorted(by_sym.items(), key=lambda x: -len(x[1])):
    liq = POOL_LIQ.get((sym, chain), 0)
    daily_events = len(divs) * daily_mult
    med_div = sorted(divs)[len(divs) // 2] / 100

    # Trade size: min($500 worth of ETH, 5% of pool)
    trade_eth = min(500 / ETH_USD, liq * 0.05)
    rev_per = trade_eth * med_div * ETH_USD
    daily_rev = daily_events * rev_per * 0.5  # 50% execution rate
    total_rev += daily_rev

    print(f"{sym + '/' + chain:<20} {liq:>10.1f} {len(divs):>6} {daily_events:>10.0f} "
          f"{med_div * 100:>8.2f} {daily_rev:>10.2f}")

print(f"\nTotal estimated daily revenue (actionable): ${total_rev:.2f}")
print(f"vs $500/day target: {'VIABLE' if total_rev >= 500 else 'SHORT'}")

# === ALL CEX-DEX >0.3% ===
all_sig = [e for e in cex if e["divergence_pct"] >= 0.3]
all_daily = len(all_sig) * daily_mult
gross = all_daily * 0.003 * 500 * 0.5
print(f"\n=== THEORETICAL MAX (all >0.3%, ignoring liquidity) ===")
print(f"Events: {len(all_sig)} in {hours:.1f}h = {all_daily:.0f}/day")
print(f"Revenue at 0.3% x $500 x 50% exec: ${gross:.0f}/day")

# === REALISTIC: subtract gas costs ===
print(f"\n=== GAS COST IMPACT ===")
# Ethereum: ~$3-5 per swap tx at moderate gas
# Base/Arbitrum: ~$0.01-0.05
gas_eth = 3.0
gas_l2 = 0.03
for (sym, chain), divs in sorted(by_sym.items(), key=lambda x: -len(x[1])):
    liq = POOL_LIQ.get((sym, chain), 0)
    daily_events = len(divs) * daily_mult
    med_div = sorted(divs)[len(divs) // 2] / 100
    trade_eth = min(500 / ETH_USD, liq * 0.05)
    rev_per = trade_eth * med_div * ETH_USD
    gas = gas_eth if chain == "ethereum" else gas_l2
    net_per = rev_per - gas
    if net_per > 0:
        daily_net = daily_events * net_per * 0.5
        print(f"  {sym}/{chain}: ${rev_per:.2f}/trade gross - ${gas:.2f} gas = ${net_per:.2f} net -> ${daily_net:.2f}/day")
    else:
        print(f"  {sym}/{chain}: ${rev_per:.2f}/trade gross - ${gas:.2f} gas = NEGATIVE")

# Cross-chain summary
print(f"\n=== CROSS-CHAIN SUMMARY ===")
cc = [e for e in events if e["type"] == "cross_chain" and e["divergence_pct"] < 50]
cc_dynamic = [e for e in cc if e["divergence_pct"] >= 0.3]
# Group and check range
cc_by_key = defaultdict(list)
for e in cc_dynamic:
    k = e["symbol"] + "_" + e["chain_a"] + "_" + e["chain_b"]
    cc_by_key[k].append(e["divergence_pct"])

dynamic_cc = 0
for k, divs in cc_by_key.items():
    if max(divs) - min(divs) >= 0.15:  # truly varying
        dynamic_cc += 1
        print(f"  Dynamic: {k} range={min(divs):.2f}-{max(divs):.2f}%")

if dynamic_cc == 0:
    print("  No dynamic cross-chain divergences found")
    print("  All cross-chain >0.3% are static structural spreads (not arbitrageable)")
