#!/usr/bin/env python3
"""
Analyze whatever Solana B3 probe data we have so far and extrapolate.

Works with partial data — designed to be run while the probe is still
collecting. Projects daily rates and gives a preliminary go/kill read
based on current observations.
"""
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

TICK_FILE = Path("research/data/solana_b3.jsonl")
EVENT_FILE = Path("research/data/solana_b3_events.jsonl")

ROUND_TRIP_FEE_PCT = 0.6


def load_jsonl(path):
    if not path.exists():
        return []
    out = []
    for line in open(path):
        try:
            out.append(json.loads(line))
        except Exception:
            continue
    return out


def analyze():
    ticks = load_jsonl(TICK_FILE)
    events = load_jsonl(EVENT_FILE)

    if not ticks:
        print("No tick data yet.")
        return

    start_ts = min(t["ts"] for t in ticks)
    end_ts = max(t["ts"] for t in ticks)
    duration_sec = end_ts - start_ts
    duration_hours = duration_sec / 3600
    daily_mult = 24 / max(duration_hours, 0.001)

    print(f"=== Solana B3 Live Analysis ===")
    print(f"Duration so far: {duration_sec:.0f}s ({duration_hours:.2f}h)")
    print(f"Total ticks collected: {len(ticks)}")
    print(f"Total divergence events logged: {len(events)}")

    # Group by pair
    by_pair = defaultdict(list)
    for t in ticks:
        by_pair[t["pair"]].append(t)

    print(f"\n--- Per-pair coverage ---")
    hdr = f"{'Pair':<18} {'Ticks':>6} {'2+DEX%':>7} {'DEXs seen':>25}"
    print(hdr)
    print("-" * len(hdr))

    for pair, ts in sorted(by_pair.items()):
        n = len(ts)
        two_dex = sum(1 for t in ts if len(t.get("prices_by_dex", {})) >= 2)
        two_dex_pct = two_dex / n * 100 if n else 0
        dexes_seen = set()
        for t in ts:
            dexes_seen.update(t.get("prices_by_dex", {}).keys())
        print(f"{pair:<18} {n:>6} {two_dex_pct:>6.0f}% {','.join(sorted(dexes_seen)):>25}")

    # Divergence analysis
    print(f"\n--- Divergence distribution (from live tick data) ---")
    all_divs_by_pair = defaultdict(list)
    for t in ticks:
        prices = list(t.get("prices_by_dex", {}).values())
        if len(prices) >= 2:
            hi = max(prices)
            lo = min(prices)
            if lo > 0:
                div_pct = (hi - lo) / lo * 100
                all_divs_by_pair[t["pair"]].append(div_pct)

    hdr = f"{'Pair':<18} {'N':>5} {'Min%':>6} {'Med%':>6} {'Mean%':>6} {'Max%':>6} {'>fee%':>7}"
    print(hdr)
    print("-" * len(hdr))
    for pair, divs in sorted(all_divs_by_pair.items()):
        if not divs:
            continue
        above_fee = sum(1 for d in divs if d > ROUND_TRIP_FEE_PCT)
        above_pct = above_fee / len(divs) * 100
        print(f"{pair:<18} {len(divs):>5} {min(divs):>5.2f} {statistics.median(divs):>5.2f} "
              f"{statistics.mean(divs):>5.2f} {max(divs):>5.2f} {above_pct:>6.1f}%")

    # Follow-up events
    if events:
        print(f"\n--- Divergence follow-up events ({len(events)} total) ---")
        closed = sum(1 for e in events if e.get("closed"))
        persisted = sum(1 for e in events if e.get("persisted"))
        other = len(events) - closed - persisted
        print(f"  Closed within 30s: {closed} ({closed/len(events)*100:.0f}%)")
        print(f"  Persisted >30s: {persisted} ({persisted/len(events)*100:.0f}%)")
        print(f"  Indeterminate: {other}")
        if events:
            print(f"\n  Top 5 by initial divergence:")
            for e in sorted(events, key=lambda x: -x.get("initial_div", 0))[:5]:
                status = "closed" if e["closed"] else "persisted" if e["persisted"] else "mid"
                print(f"    {e['pair']}: {e['initial_div']:.2f}% → "
                      f"{e.get('followup_div', 'null')}% ({status})")

    # Projection
    print(f"\n=== DAILY PROJECTION ===")
    total_daily_above_fees = 0
    total_daily_episodes = 0

    for pair, divs in sorted(all_divs_by_pair.items()):
        if not divs:
            continue
        above_fee = sum(1 for d in divs if d > ROUND_TRIP_FEE_PCT)
        daily_above = above_fee * daily_mult
        total_daily_above_fees += daily_above
        # Count episodes as distinct divergence runs (approximate: each above-fee tick = episode)
        total_daily_episodes += daily_above
        if daily_above > 0:
            med_div = statistics.median([d for d in divs if d > ROUND_TRIP_FEE_PCT])
            print(f"  {pair}: {daily_above:.0f} above-fee ticks/day (median {med_div:.2f}%)")

    print(f"\n  TOTAL projected profitable-looking divergence ticks: {total_daily_above_fees:.0f}/day")

    # Go/kill verdict
    print(f"\n=== PROVISIONAL VERDICT ===")
    if total_daily_above_fees < 10:
        print(f"  🛑 KILL: <10 above-fee ticks projected per day")
        print(f"     Pattern looks consistent with B1 finding: efficient market on deep pairs,")
        print(f"     no cross-DEX routes on long-tails.")
    elif total_daily_above_fees < 100:
        print(f"  ⚠️  AMBIGUOUS: {total_daily_above_fees:.0f}/day — keep running")
    else:
        print(f"  ✅ INTERESTING: {total_daily_above_fees:.0f}/day — investigate competition next")

    # Confidence caveat
    if duration_hours < 1:
        print(f"\n  ⚠️ Low confidence: only {duration_hours:.2f}h of data")
        print(f"     Extrapolation is {daily_mult:.0f}x — rare transients not captured yet")
    elif duration_hours < 4:
        print(f"\n  ⚠️ Medium confidence: {duration_hours:.2f}h of data")
        print(f"     Time-of-day effects not yet averaged in")
    else:
        print(f"\n  ✓ Reasonable confidence: {duration_hours:.1f}h of data")


if __name__ == "__main__":
    analyze()
