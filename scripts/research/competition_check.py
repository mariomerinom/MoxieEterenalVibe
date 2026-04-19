#!/usr/bin/env python3
"""
Competition Analysis: check which backtest arb opportunities were
captured by existing searchers on-chain.

For each opportunity (block + pool pair), check if there was an arb
transaction in that block. Uses our captured swap/arb data.

Answers: what fraction of theoretical opportunities are already taken?
"""
import csv
import duckdb
import json
import sys
from collections import defaultdict


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--opps", default="/root/mev/research/data/backtest_v2_arb.csv",
                        help="Path to backtest opportunities CSV")
    parser.add_argument("--pools", default="/root/mev/data/pool_tokens_full.json",
                        help="Path to pool_tokens JSON")
    args = parser.parse_args()

    con = duckdb.connect("/root/mev/data/mev.duckdb", read_only=True)
    con.execute("SET threads=2")

    # Load backtest opportunities
    with open(args.opps) as f:
        reader = csv.DictReader(f)
        opps = list(reader)

    print(f"Loaded {len(opps)} backtest opportunities")

    if not opps:
        print("No opportunities to analyze.")
        return

    # Load pool info
    with open(args.pools) as f:
        pool_tokens = json.load(f)

    # Get unique blocks with opportunities
    opp_blocks = set(int(o["block"]) for o in opps)
    print(f"Across {len(opp_blocks)} unique blocks")

    block_sql = ",".join(str(b) for b in opp_blocks)

    # Query: in these blocks, how many swaps happened on our pools?
    # Multiple swaps in same block on same pool = likely arb
    pool_addrs = set(pool_tokens.keys())
    pool_sql = ",".join(f"'{p}'" for p in pool_addrs)

    print("Querying swap activity in opportunity blocks...")
    swaps_in_blocks = con.execute(f"""
        SELECT block_number, lower(pool), COUNT(*) as swap_count,
               COUNT(DISTINCT tx_hash) as tx_count
        FROM read_parquet('/root/mev/data/events/swaps/ethereum/*.parquet')
        WHERE block_number IN ({block_sql})
          AND lower(pool) IN ({pool_sql})
        GROUP BY block_number, lower(pool)
        ORDER BY block_number
    """).fetchall()

    print(f"Found {len(swaps_in_blocks)} (block, pool) entries")

    # Build lookup: block -> pool -> swap_count
    block_pool_swaps = defaultdict(lambda: defaultdict(int))
    for block, pool, swap_count, tx_count in swaps_in_blocks:
        block_pool_swaps[block][pool] = tx_count

    # For each opportunity, check if there were multiple txs touching
    # the same pools in the same block (indicator of arb capture)
    captured = 0
    uncaptured = 0
    unknown = 0

    for opp in opps:
        block = int(opp["block"])
        # The pair field is like "SYM0/SYM1 vs SYM2/SYM3"
        # We need to find which pools were involved
        # For now, check if any pool in this block had >1 tx (likely arbed)
        pool_activity = block_pool_swaps.get(block, {})
        multi_tx_pools = sum(1 for v in pool_activity.values() if v > 1)

        if multi_tx_pools > 0:
            captured += 1
        elif len(pool_activity) > 0:
            uncaptured += 1
        else:
            unknown += 1

    sep = "=" * 60
    print(f"\n{sep}")
    print("COMPETITION ANALYSIS")
    print(sep)
    print(f"Total opportunities:   {len(opps)}")
    print(f"Likely captured:       {captured} ({captured/len(opps)*100:.1f}%)")
    print(f"Possibly uncaptured:   {uncaptured} ({uncaptured/len(opps)*100:.1f}%)")
    print(f"Unknown (no data):     {unknown} ({unknown/len(opps)*100:.1f}%)")

    if captured + uncaptured > 0:
        capture_rate = captured / (captured + uncaptured) * 100
        print(f"\nCapture rate (excl unknown): {capture_rate:.1f}%")
        print(f"Uncaptured gap:             {100 - capture_rate:.1f}%")

    # Per-pair breakdown
    print(f"\nPer-pair breakdown:")
    by_pair = defaultdict(lambda: {"captured": 0, "uncaptured": 0, "total": 0})
    for opp in opps:
        pair = opp["pair"]
        block = int(opp["block"])
        pool_activity = block_pool_swaps.get(block, {})
        multi = sum(1 for v in pool_activity.values() if v > 1)
        by_pair[pair]["total"] += 1
        if multi > 0:
            by_pair[pair]["captured"] += 1
        else:
            by_pair[pair]["uncaptured"] += 1

    for pair, stats in sorted(by_pair.items(), key=lambda x: -x[1]["total"]):
        rate = stats["captured"] / stats["total"] * 100 if stats["total"] else 0
        print(f"  {pair:>40}: {stats['total']:>4} opps, "
              f"{rate:.0f}% captured")


if __name__ == "__main__":
    main()
