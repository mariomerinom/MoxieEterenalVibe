#!/usr/bin/env python3
"""
Solana cross-DEX arb sizing v2 — phantom filter applied.

Lessons from 11 killed EVM strategies:
1. Static divergences = stale pools, not arb opportunities
2. Huge divergences (>50%) = broken/dead pools
3. Must check if divergence FLUCTUATES (appears and disappears)
4. Only count opportunities where both pools have recent activity
"""
import os
import sys
from collections import defaultdict
from pathlib import Path

import pyarrow.parquet as pq
import numpy as np

DATA_DIR = Path(os.environ.get("DATA_DIR", "./data/events/swaps/solana"))
SOL_MINT = "So11111111111111111111111111111111111111112"
SOL_DECIMALS = 9
SOL_USD = float(os.environ.get("SOL_USD", "145"))

ROUND_TRIP_FEE = 0.006  # 0.6%
MAX_REAL_DIVERGENCE = 20.0  # >20% is phantom (stale pool)

# Known mints for labeling
KNOWN_MINTS = {
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": "USDC",
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": "USDT",
    SOL_MINT: "SOL",
    "So11111111111111111111111111111111111111112": "SOL",
    "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So": "mSOL",
    "7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs": "ETH",
    "J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn": "jitoSOL",
    "cbbtcf3aa214zXGn9PwMHoYdrbFBfEkHUvMFiAVYPmR6": "cbBTC",
}


def label_mint(mint):
    if mint in KNOWN_MINTS:
        return KNOWN_MINTS[mint]
    return mint[:6] + ".."


def load_swaps(max_files=None):
    files = sorted(DATA_DIR.glob("*.parquet"))
    if max_files:
        files = files[:max_files]
    if not files:
        print(f"No parquet files in {DATA_DIR}")
        sys.exit(1)

    print(f"Loading {len(files)} parquet files...")
    COLS = ["slot", "pool", "protocol", "token_in_mint", "token_out_mint", "amount_in", "amount_out"]

    all_rows = []
    for i, f in enumerate(files):
        try:
            table = pq.read_table(f, columns=COLS)
            batch = table.to_pydict()
            n = len(batch["slot"])
            for j in range(n):
                ain = batch["amount_in"][j]
                aout = batch["amount_out"][j]
                ai = int(ain) if ain else 0
                ao = int(aout) if aout else 0
                if ai == 0 or ao == 0:
                    continue
                all_rows.append((
                    batch["slot"][j],
                    batch["pool"][j],
                    batch["protocol"][j],
                    batch["token_in_mint"][j],
                    batch["token_out_mint"][j],
                    ai, ao,
                ))
        except Exception:
            continue
        if (i + 1) % 1000 == 0:
            print(f"  {i+1}/{len(files)} files, {len(all_rows)} swaps...")

    print(f"Loaded {len(all_rows)} non-zero swaps")
    return all_rows


def analyze(rows):
    if not rows:
        print("No data")
        return

    slots = [r[0] for r in rows]
    slot_range = max(slots) - min(slots)
    days = slot_range / (2.5 * 3600 * 24)
    daily_mult = 1 / max(days, 0.001)

    print(f"\n=== DATASET: {len(rows)} swaps, {days:.1f} days ===")

    # Protocol breakdown
    by_proto = defaultdict(int)
    for r in rows:
        by_proto[r[2]] += 1
    for p, c in sorted(by_proto.items(), key=lambda x: -x[1]):
        print(f"  {p}: {c:,} ({c/len(rows)*100:.1f}%)")

    # Group by normalized pair
    # For each pair, group swaps by (pool, protocol) to get per-pool pricing
    pair_pool_swaps = defaultdict(lambda: defaultdict(list))
    # pair -> pool_key -> [(slot, price, amount_sol)]

    for slot, pool, proto, tin, tout, ain, aout in rows:
        a, b = sorted([tin, tout])
        pair = (a, b)
        pool_key = (pool, proto)

        # Price = amount_out / amount_in, but normalize direction
        if tin == a:  # forward: a -> b
            price = aout / ain
        else:  # reverse: b -> a, so price of a in b terms = ain / aout
            price = ain / aout

        # SOL volume
        vol_sol = 0
        if tin == SOL_MINT:
            vol_sol = ain / 10**SOL_DECIMALS
        elif tout == SOL_MINT:
            vol_sol = aout / 10**SOL_DECIMALS

        pair_pool_swaps[pair][pool_key].append((slot, price, vol_sol))

    # Find pairs with 2+ active pools
    multi_pool_pairs = {}
    for pair, pools in pair_pool_swaps.items():
        active_pools = {pk: swps for pk, swps in pools.items() if len(swps) >= 3}
        if len(active_pools) >= 2:
            multi_pool_pairs[pair] = active_pools

    print(f"\n  Total pairs: {len(pair_pool_swaps)}")
    print(f"  Pairs with 2+ active pools (>=3 swaps each): {len(multi_pool_pairs)}")

    # For each multi-pool pair, check price divergence
    # Group by slot windows (~10 slots = 4 seconds)
    WINDOW = 10  # slots

    results = []  # (pair, n_windows, n_divergent, median_div, is_dynamic, total_vol, protocols)

    for pair, pools in multi_pool_pairs.items():
        pool_keys = list(pools.keys())

        # Build slot -> pool -> price mapping
        slot_pool_price = defaultdict(dict)
        for pk, swps in pools.items():
            for slot, price, vol in swps:
                window = slot // WINDOW
                # Keep latest price per window per pool
                slot_pool_price[window][pk] = (price, vol)

        # Find windows where 2+ pools have prices
        divergences = []
        volumes = []

        for window, pool_prices in slot_pool_price.items():
            if len(pool_prices) < 2:
                continue

            prices = [p for p, v in pool_prices.values()]
            vols = [v for p, v in pool_prices.values()]

            max_p = max(prices)
            min_p = min(prices)
            if min_p <= 0:
                continue

            div_pct = (max_p - min_p) / min_p * 100

            # Filter phantoms
            if div_pct > MAX_REAL_DIVERGENCE:
                continue

            if div_pct > ROUND_TRIP_FEE * 100:
                divergences.append(div_pct)
                volumes.append(max(vols))

        if not divergences:
            continue

        # Check if divergence is dynamic (fluctuates) vs static
        n_windows_with_overlap = sum(1 for w, pp in slot_pool_price.items() if len(pp) >= 2)
        div_rate = len(divergences) / max(n_windows_with_overlap, 1)

        # Dynamic = divergence appears <80% of the time (not always-on)
        is_dynamic = div_rate < 0.8

        protos = set(pk[1] for pk in pool_keys)
        total_vol = sum(volumes)

        results.append((
            pair, n_windows_with_overlap, len(divergences),
            np.median(divergences), is_dynamic, total_vol, protos
        ))

    # Sort by divergent window count
    results.sort(key=lambda r: -r[2])

    print(f"\n=== CROSS-POOL DIVERGENCES (>{ROUND_TRIP_FEE*100:.1f}%, <{MAX_REAL_DIVERGENCE}%) ===")
    print(f"  Pairs with any divergence: {len(results)}")
    dynamic = [r for r in results if r[4]]
    static = [r for r in results if not r[4]]
    print(f"  Dynamic (intermittent): {len(dynamic)}")
    print(f"  Static (always-on = likely phantom): {len(static)}")

    print(f"\n--- DYNAMIC divergences (potentially real arb) ---")
    hdr = f"{'Pair':<25} {'Protocols':<35} {'Overlap':>7} {'DivWin':>6} {'Rate':>6} {'MedDiv%':>8} {'VolSOL':>10}"
    print(hdr)
    print("-" * len(hdr))

    total_daily_arb = 0
    for pair, n_overlap, n_div, med_div, is_dyn, vol, protos in dynamic[:40]:
        pair_label = f"{label_mint(pair[0])}/{label_mint(pair[1])}"
        proto_label = ",".join(sorted(protos))
        rate = n_div / max(n_overlap, 1)
        daily_div = n_div * daily_mult

        # Revenue estimate per divergence window
        if vol > 0:
            net_spread = (med_div / 100) - ROUND_TRIP_FEE
            if net_spread > 0:
                trade_sol = min(vol * 0.3, 10)  # cap at 10 SOL per trade
                profit_sol = trade_sol * net_spread
                net_sol = profit_sol * 0.2  # after 80% Jito tip
                daily_sol = net_sol * daily_mult * n_div
                daily_usd = daily_sol * SOL_USD
                total_daily_arb += daily_usd

        print(f"{pair_label:<25} {proto_label:<35} {n_overlap:>7} {n_div:>6} {rate:>5.1%} {med_div:>8.2f} {vol:>10.2f}")

    print(f"\n--- STATIC divergences (likely phantom / stale pool) ---")
    for pair, n_overlap, n_div, med_div, is_dyn, vol, protos in static[:20]:
        pair_label = f"{label_mint(pair[0])}/{label_mint(pair[1])}"
        proto_label = ",".join(sorted(protos))
        rate = n_div / max(n_overlap, 1)
        print(f"{pair_label:<25} {proto_label:<35} {n_overlap:>7} {n_div:>6} {rate:>5.1%} {med_div:>8.2f} {vol:>10.2f}")

    # Revenue summary
    print(f"\n=== REVENUE ESTIMATE (dynamic only, capped 10 SOL/trade, 80% Jito tip) ===")
    print(f"  Daily at 100% capture: ${total_daily_arb:.2f}")
    print(f"  Daily at 10% capture: ${total_daily_arb * 0.1:.2f}")
    print(f"  Daily at 1% capture: ${total_daily_arb * 0.01:.2f}")
    print(f"  vs $500/day target: {'VIABLE' if total_daily_arb * 0.1 >= 500 else 'NEEDS INVESTIGATION' if total_daily_arb * 0.01 >= 50 else 'KILL'}")


if __name__ == "__main__":
    sample = None
    if "--sample" in sys.argv:
        idx = sys.argv.index("--sample")
        sample = int(sys.argv[idx + 1])
    load = load_swaps(max_files=sample)
    analyze(load)
