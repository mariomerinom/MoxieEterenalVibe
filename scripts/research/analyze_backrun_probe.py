#!/usr/bin/env python3
"""
Analyze the output of mevshare_backrun_probe.py.

Reads mevshare_backrun_hints.jsonl and produces a deeper analysis than
the real-time summary:

  1. Deduplication — how many unique opportunities vs repeat hint hashes
  2. Time-of-day distribution — hourly breakdown of hint flow
  3. Per-pool competition density — which pools get hit most
  4. Burst analysis — how many hints arrive within the same block window
  5. Token pair concentration — where's the volume

Usage:
  python analyze_backrun_probe.py [path/to/mevshare_backrun_hints.jsonl]
"""

import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path


def load_entries(path: str) -> list[dict]:
    entries = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return entries


def analyze(entries: list[dict]):
    if not entries:
        print("No entries.")
        return

    total = len(entries)
    matched = [e for e in entries if e.get("matched")]
    backrunnable = [e for e in entries if e.get("backrunnable")]

    # Time range
    ts_start = entries[0].get("ts", 0)
    ts_end = entries[-1].get("ts", 0)
    duration_s = max(ts_end - ts_start, 1)
    duration_h = duration_s / 3600

    print("=" * 70)
    print(f"  MEV-SHARE BACKRUN PROBE ANALYSIS")
    print(f"  {total:,} entries over {duration_h:.1f} hours")
    print("=" * 70)

    # ── 1. Funnel ──
    print(f"\n  FUNNEL:")
    print(f"    Total hints:          {total:>8,}")
    print(f"    Matched our pools:    {len(matched):>8,}  ({len(matched)/total*100:.1f}%)")
    print(f"    Backrunnable:         {len(backrunnable):>8,}  ({len(backrunnable)/total*100:.1f}%)")

    # ── 2. Deduplication ──
    all_hashes = [e.get("hash", "") for e in entries if e.get("hash")]
    unique_hashes = len(set(all_hashes))
    br_hashes = [e.get("hash", "") for e in backrunnable if e.get("hash")]
    unique_br_hashes = len(set(br_hashes))

    dup_ratio = 1 - unique_hashes / len(all_hashes) if all_hashes else 0
    br_dup_ratio = 1 - unique_br_hashes / len(br_hashes) if br_hashes else 0

    print(f"\n  DEDUPLICATION:")
    print(f"    Total hints:          {len(all_hashes):>8,}  unique: {unique_hashes:>8,}  dup rate: {dup_ratio*100:.1f}%")
    print(f"    Backrunnable hints:   {len(br_hashes):>8,}  unique: {unique_br_hashes:>8,}  dup rate: {br_dup_ratio*100:.1f}%")
    print(f"    Unique backrunnable/day (projected): {unique_br_hashes / duration_h * 24:,.0f}")

    # ── 3. Hourly distribution ──
    hourly = Counter()
    hourly_br = Counter()
    for e in entries:
        ts = e.get("ts", 0)
        hour = datetime.fromtimestamp(ts, tz=timezone.utc).hour
        hourly[hour] += 1
        if e.get("backrunnable"):
            hourly_br[hour] += 1

    if hourly:
        print(f"\n  HOURLY DISTRIBUTION (UTC):")
        print(f"    {'Hour':>4}  {'Total':>7}  {'Backrunnable':>12}  {'BR %':>6}  Bar")
        for h in range(24):
            t = hourly.get(h, 0)
            b = hourly_br.get(h, 0)
            pct = b / t * 100 if t > 0 else 0
            bar = "#" * int(b / max(max(hourly_br.values(), default=1), 1) * 30)
            if t > 0:
                print(f"    {h:>4}  {t:>7,}  {b:>12,}  {pct:>5.1f}%  {bar}")

    # ── 4. Per-pool breakdown ──
    pool_total = Counter()
    pool_br = Counter()
    for e in matched:
        for p in e.get("matched_pools", []):
            pool_total[p] += 1
    for e in backrunnable:
        for p in e.get("matched_pools", []):
            pool_br[p] += 1

    if pool_total:
        print(f"\n  TOP POOLS (matched):")
        print(f"    {'Pool':>44}  {'Matched':>8}  {'BR':>6}  {'BR%':>5}  /hour")
        for pool, count in pool_total.most_common(20):
            br_count = pool_br.get(pool, 0)
            pct = br_count / count * 100 if count > 0 else 0
            per_hour = count / duration_h
            print(f"    {pool}  {count:>8}  {br_count:>6}  {pct:>4.0f}%  {per_hour:>5.1f}")

    # ── 5. Pair concentration ──
    pair_counts = Counter()
    pair_br = Counter()
    for e in entries:
        for ev in e.get("swap_events", []):
            s0 = ev.get("symbol0", "?")
            s1 = ev.get("symbol1", "?")
            pair = f"{s0}/{s1}"
            pair_counts[pair] += 1
            if e.get("backrunnable"):
                pair_br[pair] += 1

    if pair_counts:
        print(f"\n  TOKEN PAIR CONCENTRATION:")
        print(f"    {'Pair':>25}  {'Hits':>7}  {'BR':>6}  {'BR%':>5}")
        for pair, count in pair_counts.most_common(15):
            br = pair_br.get(pair, 0)
            pct = br / count * 100 if count > 0 else 0
            print(f"    {pair:>25}  {count:>7}  {br:>6}  {pct:>4.0f}%")

    # ── 6. Hint class distribution ──
    class_counts = Counter(e.get("hint_class", "?") for e in entries)
    print(f"\n  HINT CLASS DISTRIBUTION:")
    for cls, count in class_counts.most_common():
        print(f"    {cls:<15} {count:>8,}  ({count/total*100:.1f}%)")

    # ── 7. Burst analysis ──
    # Group backrunnable hints by 12-second windows (~ 1 Ethereum block)
    if backrunnable:
        windows = Counter()
        for e in backrunnable:
            window = int(e.get("ts", 0)) // 12
            windows[window] += 1

        burst_sizes = list(windows.values())
        single = sum(1 for b in burst_sizes if b == 1)
        multi = sum(1 for b in burst_sizes if b > 1)
        max_burst = max(burst_sizes) if burst_sizes else 0

        print(f"\n  BURST ANALYSIS (12s windows):")
        print(f"    Total windows with BR hints: {len(windows)}")
        print(f"    Single-hint windows:         {single} ({single/len(windows)*100:.0f}%)")
        print(f"    Multi-hint windows:          {multi} ({multi/len(windows)*100:.0f}%)")
        print(f"    Max hints in one window:     {max_burst}")
        print(f"    Avg hints per window:        {sum(burst_sizes)/len(burst_sizes):.1f}")

    # ── 8. Arb partner richness ──
    partner_counts = Counter()
    for e in backrunnable:
        pc = e.get("arb_partner_count", 0)
        partner_counts[pc] += 1

    if partner_counts:
        print(f"\n  ARB PARTNER COUNT (backrunnable hints):")
        for pc, count in sorted(partner_counts.items()):
            print(f"    {pc} partners: {count:>6} hints")

    # ── Summary line ──
    print(f"\n{'=' * 70}")
    print(f"  BOTTOM LINE: {unique_br_hashes} unique backrunnable hints in {duration_h:.1f}h")
    print(f"  Projected: {unique_br_hashes / duration_h * 24:,.0f} unique backrunnable/day")
    print(f"  Top pairs: {', '.join(p for p, _ in pair_br.most_common(3))}")
    print(f"  Dominant pools: {pool_br.most_common(3)}")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "/root/mev/research/data/mevshare_backrun_hints.jsonl"
    if not Path(path).exists():
        print(f"File not found: {path}")
        sys.exit(1)
    entries = load_entries(path)
    analyze(entries)
