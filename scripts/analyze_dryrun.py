#!/usr/bin/env python3
"""Analyze dry-run JSONL logs from the MEV bot.

Reads dry_run.jsonl and produces:
- Opportunity frequency distribution
- Profit distribution histogram
- Per-cycle breakdown (which cycles fire most)
- Chain-level summary
- Signal quality indicators (input size, phantom detection)
"""

import json
import sys
from collections import Counter, defaultdict
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


def classify_chain(entry: dict) -> str:
    """Classify chain from entry data."""
    if "chain" in entry and entry["chain"]:
        return entry["chain"]
    # Fallback: classify by block number range
    bn = entry.get("block_number", 0)
    if bn > 400_000_000:
        return "arbitrum"
    elif bn > 40_000_000:
        return "base"
    else:
        return "ethereum"


def analyze(entries: list[dict]):
    if not entries:
        print("No entries found.")
        return

    # Split by chain
    by_chain: dict[str, list[dict]] = defaultdict(list)
    for e in entries:
        chain = classify_chain(e)
        by_chain[chain].append(e)

    print(f"{'='*70}")
    print(f"  DRY-RUN ANALYSIS — {len(entries)} total entries")
    print(f"{'='*70}")

    for chain, chain_entries in sorted(by_chain.items()):
        print(f"\n{'─'*70}")
        print(f"  CHAIN: {chain.upper()} ({len(chain_entries)} entries)")
        print(f"{'─'*70}")

        # Block coverage
        blocks = sorted(set(e["block_number"] for e in chain_entries))
        block_range = blocks[-1] - blocks[0] + 1 if blocks else 0
        unique_blocks = len(blocks)

        # Opportunity rate
        opp_rate = len(chain_entries) / unique_blocks if unique_blocks > 0 else 0

        print(f"\n  Block range: {blocks[0]:,} — {blocks[-1]:,} ({block_range:,} blocks)")
        print(f"  Unique blocks with opps: {unique_blocks:,}")
        print(f"  Avg opps/block: {opp_rate:.2f}")

        # Profit distribution
        profits = [e["net_profit_eth"] for e in chain_entries]
        inputs = [e.get("input_amount_eth", 0) for e in chain_entries]

        print(f"\n  PROFIT DISTRIBUTION:")
        print(f"    Total net profit (theoretical): {sum(profits):.4f} ETH")
        print(f"    Mean: {sum(profits)/len(profits):.6f} ETH")
        print(f"    Min:  {min(profits):.6f} ETH")
        print(f"    Max:  {max(profits):.6f} ETH")

        # Profit buckets
        buckets = {"<0.001": 0, "0.001-0.01": 0, "0.01-0.1": 0, "0.1-1.0": 0, ">1.0": 0}
        for p in profits:
            if p < 0.001:
                buckets["<0.001"] += 1
            elif p < 0.01:
                buckets["0.001-0.01"] += 1
            elif p < 0.1:
                buckets["0.01-0.1"] += 1
            elif p < 1.0:
                buckets["0.1-1.0"] += 1
            else:
                buckets[">1.0"] += 1

        print(f"\n    Profit buckets:")
        for bucket, count in buckets.items():
            pct = count / len(profits) * 100
            bar = "#" * int(pct / 2)
            print(f"      {bucket:>12s}: {count:5d} ({pct:5.1f}%) {bar}")

        # Input amount analysis (phantom detection)
        zero_input = sum(1 for x in inputs if x == 0.0)
        tiny_input = sum(1 for x in inputs if 0 < x < 0.01)
        normal_input = sum(1 for x in inputs if x >= 0.01)

        print(f"\n  INPUT AMOUNT (phantom indicator):")
        print(f"    Zero input (0.0):    {zero_input:5d} ({zero_input/len(inputs)*100:.1f}%)")
        print(f"    Tiny (<0.01 ETH):    {tiny_input:5d} ({tiny_input/len(inputs)*100:.1f}%)")
        print(f"    Normal (>=0.01 ETH): {normal_input:5d} ({normal_input/len(inputs)*100:.1f}%)")

        if normal_input == 0 and zero_input > 0:
            print(f"    ** WARNING: All entries have zero/tiny input — likely all phantom **")

        # Per-cycle breakdown (top 15)
        cycle_counts = Counter()
        cycle_profits = defaultdict(list)
        for e in chain_entries:
            label = e.get("cycle_label", "unknown")
            cycle_counts[label] += 1
            cycle_profits[label].append(e["net_profit_eth"])

        print(f"\n  TOP CYCLES (by frequency):")
        for label, count in cycle_counts.most_common(15):
            avg_p = sum(cycle_profits[label]) / len(cycle_profits[label])
            print(f"    {count:5d}x  avg={avg_p:.6f} ETH  {label[:80]}")

        # Processing time
        times = [e.get("block_process_time_ms", 0) for e in chain_entries if e.get("block_process_time_ms")]
        if times:
            print(f"\n  PROCESSING TIME:")
            print(f"    Mean: {sum(times)/len(times):.0f} ms")
            print(f"    Max:  {max(times)} ms")
            over_6s = sum(1 for t in times if t > 6000)
            print(f"    >6s:  {over_6s} ({over_6s/len(times)*100:.1f}%)")

        # Simulation results (if available)
        simmed = [e for e in chain_entries if e.get("sim_success") is not None]
        if simmed:
            sim_pass = sum(1 for e in simmed if e["sim_success"])
            print(f"\n  SIMULATION CROSS-CHECK:")
            print(f"    Simulated: {len(simmed)}")
            print(f"    Pass:      {sim_pass} ({sim_pass/len(simmed)*100:.1f}%)")
            print(f"    Revert:    {len(simmed)-sim_pass} ({(len(simmed)-sim_pass)/len(simmed)*100:.1f}%)")

        # Pool frequency
        pool_counts = Counter()
        for e in chain_entries:
            for p in e.get("pools", []):
                if p != "0x0000000000000000000000000000000000000000":
                    pool_counts[p] += 1

        if pool_counts:
            print(f"\n  TOP POOLS (most active):")
            for pool, count in pool_counts.most_common(10):
                print(f"    {count:5d}x  {pool}")

    # Cross-chain summary
    print(f"\n{'='*70}")
    print(f"  CROSS-CHAIN SUMMARY")
    print(f"{'='*70}")
    for chain, chain_entries in sorted(by_chain.items()):
        total_p = sum(e["net_profit_eth"] for e in chain_entries)
        blocks = len(set(e["block_number"] for e in chain_entries))
        print(f"  {chain:>10s}: {len(chain_entries):6d} opps / {blocks:6d} blocks = {len(chain_entries)/blocks:.2f}/block, total {total_p:.4f} ETH (theoretical)")


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "dry_run.jsonl"
    if not Path(path).exists():
        print(f"File not found: {path}")
        sys.exit(1)

    entries = load_entries(path)
    analyze(entries)
