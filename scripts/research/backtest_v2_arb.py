#!/usr/bin/env python3
"""
Backtest: V2-V2 Cross-Pool Arbitrage

Computes normalized prices from swap amounts, detects price divergence
between pools sharing the same pair within the same block.
"""
import duckdb
import json
import math
from collections import defaultdict

WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
FEE = 0.003
MIN_NET_DIV = 0.001  # 0.1% net


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--pools", default="/root/mev/data/pool_tokens.json",
                        help="Path to pool_tokens JSON file")
    parser.add_argument("--min-net-div", type=float, default=MIN_NET_DIV,
                        help="Minimum net divergence (default 0.001 = 0.1%%)")
    args = parser.parse_args()

    con = duckdb.connect("/root/mev/data/mev.duckdb", read_only=True)
    con.execute("SET threads=2")

    print(f"Loading pools from: {args.pools}")
    with open(args.pools) as f:
        pool_tokens = json.load(f)

    MIN_NET = args.min_net_div

    # V2+WETH pools (Uniswap V2 + SushiSwap -- both use same AMM math)
    v2_pools = {}
    for addr, info in pool_tokens.items():
        proto = info.get("protocol", "uniswapv2")
        if proto not in ("uniswapv2", "sushiswap"):
            continue
        t0 = info["token0"].lower()
        t1 = info["token1"].lower()
        if WETH not in (t0, t1):
            continue
        weth_is_0 = t0 == WETH
        d_other = info.get("decimals1" if weth_is_0 else "decimals0", 18)
        other = t1 if weth_is_0 else t0
        v2_pools[addr.lower()] = {
            "other_token": other,
            "weth_is_token0": weth_is_0,
            "d_other": d_other,
            "sym0": info.get("symbol0", "?"),
            "sym1": info.get("symbol1", "?"),
        }

    print(f"V2+WETH pools: {len(v2_pools)}")

    # Arb pairs: pools sharing same other_token
    by_other = defaultdict(list)
    for a, i in v2_pools.items():
        by_other[i["other_token"]].append(a)

    arb_pairs = []
    for tok, pools in by_other.items():
        if len(pools) < 2:
            continue
        for i in range(len(pools)):
            for j in range(i + 1, len(pools)):
                arb_pairs.append((pools[i], pools[j], tok))

    print(f"Arb pairs: {len(arb_pairs)}")

    all_addrs = set()
    for p1, p2, _ in arb_pairs:
        all_addrs.add(p1)
        all_addrs.add(p2)

    if not all_addrs:
        print("No arb pairs found. Check pool_tokens.json for V2+WETH pool pairs.")
        return

    pool_sql = ",".join(f"'{p}'" for p in all_addrs)

    print(f"Querying swaps for {len(all_addrs)} pools...")

    swaps = con.execute(f"""
        SELECT block_number, lower(pool),
               CAST(amount_in AS DOUBLE), CAST(amount_out AS DOUBLE)
        FROM read_parquet('/root/mev/data/events/swaps/ethereum/*.parquet')
        WHERE lower(pool) IN ({pool_sql})
          AND CAST(amount_in AS DOUBLE) > 0
          AND CAST(amount_out AS DOUBLE) > 0
        ORDER BY block_number
    """).fetchall()

    print(f"Swaps: {len(swaps):,}")

    # Compute normalized price: other_token per WETH
    block_pool_prices = defaultdict(lambda: defaultdict(list))

    for block, pool, amt_in, amt_out in swaps:
        if pool not in v2_pools:
            continue
        info = v2_pools[pool]
        d_other = info["d_other"]
        ratio = amt_out / amt_in

        # Normalize to: other_token_human_units per 1 WETH
        if ratio < 1:
            # amt_in is large (WETH-scale), amt_out is small (other-scale)
            # Selling WETH for other
            price = (amt_out / 10 ** d_other) / (amt_in / 1e18)
        else:
            # amt_in is small (other-scale), amt_out is large (WETH-scale)
            # Selling other for WETH
            price = (amt_in / 10 ** d_other) / (amt_out / 1e18)

        if price > 0 and math.isfinite(price):
            block_pool_prices[block][pool].append(price)

    print(f"Blocks with prices: {len(block_pool_prices):,}")

    # Sanity check: print median prices for each pool
    for pool in sorted(all_addrs):
        info = v2_pools[pool]
        prices = []
        for b, pp in block_pool_prices.items():
            if pool in pp:
                prices.extend(pp[pool])
        if prices:
            prices.sort()
            label = f"{info['sym0']}/{info['sym1']}"
            print(f"  {pool[:12]}.. {label:>15}: {len(prices):>6} prices, "
                  f"median={prices[len(prices) // 2]:.4f}")

    # Detect arb
    opps = []
    cooccur = 0

    for p1, p2, intermediate in arb_pairs:
        i1, i2 = v2_pools[p1], v2_pools[p2]
        label = f"{i1['sym0']}/{i1['sym1']} vs {i2['sym0']}/{i2['sym1']}"

        pair_opps = 0
        pair_checks = 0

        for block, pp in block_pool_prices.items():
            if p1 not in pp or p2 not in pp:
                continue
            cooccur += 1
            pair_checks += 1

            prices1 = sorted(pp[p1])
            prices2 = sorted(pp[p2])
            med1 = prices1[len(prices1) // 2]
            med2 = prices2[len(prices2) // 2]

            if med1 <= 0 or med2 <= 0:
                continue

            div = abs(med1 - med2) / min(med1, med2)
            net = div - 2 * FEE

            if net > MIN_NET:
                pair_opps += 1
                opps.append({
                    "block": block,
                    "pair": label,
                    "price1": med1,
                    "price2": med2,
                    "div_pct": div * 100,
                    "net_pct": net * 100,
                })

        if pair_checks > 0:
            rate = pair_opps / pair_checks * 100 if pair_checks else 0
            print(f"\n  {label}: {pair_checks:,} co-occur, "
                  f"{pair_opps} opps ({rate:.1f}%)")

    # Results
    sep = "=" * 70
    print(f"\n{sep}")
    print("BACKTEST RESULTS: V2-V2 Cross-Pool Arb")
    print(sep)
    print(f"V2+WETH pools:     {len(v2_pools)}")
    print(f"Arb pairs:         {len(arb_pairs)}")
    print(f"Blocks scanned:    {len(block_pool_prices):,}")
    print(f"Co-occurrences:    {cooccur:,}")
    print(f"Opportunities:     {len(opps):,}")

    if opps:
        divs = sorted(o["net_pct"] for o in opps)
        print(f"\nNet divergence distribution (%):")
        print(f"  p10:    {divs[int(len(divs) * 0.1)]:.3f}%")
        print(f"  median: {divs[len(divs) // 2]:.3f}%")
        print(f"  p90:    {divs[int(len(divs) * 0.9)]:.3f}%")
        print(f"  max:    {divs[-1]:.3f}%")

        profits = [d / 100 for d in divs]  # 1 ETH input
        blocks_with = len(set(o["block"] for o in opps))
        total_blocks = len(block_pool_prices)
        mn = min(block_pool_prices.keys())
        mx = max(block_pool_prices.keys())
        days = (mx - mn) / 7200

        print(f"\nBlocks with opps: {blocks_with:,} / {total_blocks:,}")
        print(f"Period: {days:.1f} days")
        print(f"Opps/day: {len(opps) / max(days, 1):.1f}")
        print(f"Median profit (1 ETH input): {profits[len(profits) // 2]:.6f} ETH")
        total = sum(profits)
        print(f"Total theoretical: {total:.4f} ETH over {days:.0f} days")
        print(f"Daily theoretical: {total / max(days, 1):.4f} ETH/day")

        # Save
        import csv
        with open("/root/mev/research/data/backtest_v2_arb.csv", "w") as f:
            w = csv.DictWriter(f, fieldnames=list(opps[0].keys()))
            w.writeheader()
            for o in sorted(opps, key=lambda x: x["block"]):
                w.writerow(o)
        print(f"\nSaved to research/data/backtest_v2_arb.csv")
    else:
        print("\nNo opportunities found.")
        print(f"Co-occurrences: {cooccur}")
        if cooccur == 0:
            print("Paired pools never trade in same block. Need more pools.")
        else:
            print("Prices never diverge enough after fees. Pools are well-arbed.")


if __name__ == "__main__":
    main()
