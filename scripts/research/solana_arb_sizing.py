#!/usr/bin/env python3
"""
Solana cross-DEX arb sizing (B1).

Reads 7-day Solana swap Parquet data and answers:
1. Which token pairs trade on multiple DEXs?
2. How often do prices diverge between DEXs by more than fees?
3. What's the swap size distribution on overlapping pairs?
4. How many arb opportunities exist per day?

Parquet schema: slot, signature, tx_index, instruction_index,
    pool, protocol, token_in_mint, token_out_mint,
    amount_in (string), amount_out (string), signer
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
SOL_USD = float(os.environ.get("SOL_USD", "145"))  # approximate

# Typical DEX fees
DEX_FEE = {
    "raydium_amm": 0.0025,      # 0.25%
    "orca_whirlpool": 0.003,     # 0.3% (varies by pool)
    "jupiter_v6": 0.003,         # aggregator, varies
}
ROUND_TRIP_FEE = 0.006  # ~0.6% for buy on one + sell on another

def load_swaps(max_files=None):
    """Load swap Parquet files, keeping only columns we need to save RAM."""
    files = sorted(DATA_DIR.glob("*.parquet"))
    if max_files:
        files = files[:max_files]
    if not files:
        print(f"No parquet files in {DATA_DIR}")
        sys.exit(1)

    print(f"Loading {len(files)} parquet files from {DATA_DIR}...")
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
                all_rows.append((
                    batch["slot"][j],
                    batch["pool"][j],
                    batch["protocol"][j],
                    batch["token_in_mint"][j],
                    batch["token_out_mint"][j],
                    int(ain) if ain else 0,
                    int(aout) if aout else 0,
                ))
        except Exception as e:
            if (i + 1) % 1000 == 0:
                print(f"  Error reading {f.name}: {e}")
            continue

        if (i + 1) % 1000 == 0:
            print(f"  Loaded {i+1}/{len(files)} files, {len(all_rows)} swaps so far...")

    print(f"Total swaps loaded: {len(all_rows)}")
    # Convert to list of named dicts at the end
    keys = COLS
    return [dict(zip(keys, r)) for r in all_rows]


def analyze(swaps):
    slot_range = max(s["slot"] for s in swaps) - min(s["slot"] for s in swaps)
    # ~2.5 slots/sec on Solana
    hours = slot_range / (2.5 * 3600)
    days = hours / 24
    daily_mult = 1 / max(days, 0.001)

    print(f"\n=== DATASET: {len(swaps)} swaps across {days:.1f} days ===")
    print(f"Slot range: {slot_range} slots")

    # --- 1. Protocol breakdown ---
    by_protocol = defaultdict(int)
    for s in swaps:
        by_protocol[s["protocol"]] += 1
    print(f"\n--- Protocol breakdown ---")
    for p, c in sorted(by_protocol.items(), key=lambda x: -x[1]):
        print(f"  {p}: {c:,} swaps ({c/len(swaps)*100:.1f}%)")

    # --- 2. Find pairs that trade on multiple DEXs ---
    # Normalize pair as (min_mint, max_mint) to handle both directions
    # Track which protocols each pair trades on
    pair_protocols = defaultdict(set)       # (mintA, mintB) -> {protocols}
    pair_pools = defaultdict(set)           # (mintA, mintB) -> {pools}
    pair_swaps = defaultdict(list)          # (mintA, mintB) -> [swap dicts]

    for s in swaps:
        if s["amount_in"] == 0 or s["amount_out"] == 0:
            continue
        a, b = sorted([s["token_in_mint"], s["token_out_mint"]])
        pair = (a, b)
        pair_protocols[pair].add(s["protocol"])
        pair_pools[pair].add(s["pool"])
        pair_swaps[pair].append(s)

    total_pairs = len(pair_protocols)
    multi_dex_pairs = {p: protos for p, protos in pair_protocols.items() if len(protos) >= 2}
    multi_pool_pairs = {p: pools for p, pools in pair_pools.items() if len(pools) >= 2}

    print(f"\n--- Pair overlap ---")
    print(f"  Total unique pairs: {total_pairs}")
    print(f"  Pairs on 2+ DEX protocols: {len(multi_dex_pairs)}")
    print(f"  Pairs on 2+ pools (any protocol): {len(multi_pool_pairs)}")

    # --- 3. For multi-DEX pairs, compute price divergence per slot ---
    print(f"\n--- Cross-DEX price divergence (top 30 by swap count) ---")

    # Sort multi-dex pairs by swap count
    multi_dex_by_count = sorted(
        multi_dex_pairs.keys(),
        key=lambda p: len(pair_swaps[p]),
        reverse=True
    )[:30]

    arb_opportunities = []  # (pair, slot, divergence_pct, volume_sol)

    hdr = f"{'Pair':<30} {'Protocols':<30} {'Swaps':>8} {'Pools':>6} {'DivSlots':>8} {'MedDiv%':>8}"
    print(hdr)
    print("-" * len(hdr))

    for pair in multi_dex_by_count:
        protos = pair_protocols[pair]
        pools = pair_pools[pair]
        swps = pair_swaps[pair]
        n_swaps = len(swps)

        # Group swaps by slot, then by protocol, compute implied price
        slot_prices = defaultdict(lambda: defaultdict(list))  # slot -> protocol -> [prices]

        for s in swps:
            # Price = amount_out / amount_in (how much output per unit input)
            # Normalize to "price of token_in_mint in terms of token_out_mint"
            if s["amount_in"] > 0:
                price = s["amount_out"] / s["amount_in"]
                # Track direction to make prices comparable
                direction = "forward" if s["token_in_mint"] == pair[0] else "reverse"
                slot_prices[s["slot"]][s["protocol"]].append((price, direction, s))

        # Find slots where 2+ protocols have prices
        divergence_slots = 0
        divergences = []

        for slot, proto_prices in slot_prices.items():
            if len(proto_prices) < 2:
                continue

            # Get avg price per protocol (same direction only)
            proto_avg = {}
            for proto, price_list in proto_prices.items():
                # Use forward direction prices
                fwd = [p for p, d, _ in price_list if d == "forward"]
                rev = [1/p for p, d, _ in price_list if d == "reverse" and p > 0]
                prices = fwd + rev
                if prices:
                    proto_avg[proto] = np.median(prices)

            if len(proto_avg) < 2:
                continue

            price_vals = list(proto_avg.values())
            max_p = max(price_vals)
            min_p = min(price_vals)
            if min_p > 0:
                div_pct = (max_p - min_p) / min_p * 100
                if div_pct > ROUND_TRIP_FEE * 100:
                    divergences.append(div_pct)
                    divergence_slots += 1

                    # Estimate volume
                    vol_sol = 0
                    for proto, price_list in proto_prices.items():
                        for _, _, sw in price_list:
                            if sw["token_in_mint"] == SOL_MINT:
                                vol_sol += sw["amount_in"] / 10**SOL_DECIMALS
                            elif sw["token_out_mint"] == SOL_MINT:
                                vol_sol += sw["amount_out"] / 10**SOL_DECIMALS

                    arb_opportunities.append((pair, slot, div_pct, vol_sol))

        # Truncate mint addresses for display
        def short(mint):
            if mint == SOL_MINT:
                return "SOL"
            return mint[:6] + ".." + mint[-4:]

        pair_label = f"{short(pair[0])}/{short(pair[1])}"
        proto_label = ",".join(sorted(protos))
        med_div = f"{np.median(divergences):.2f}" if divergences else "-"

        print(f"{pair_label:<30} {proto_label:<30} {n_swaps:>8} {len(pools):>6} {divergence_slots:>8} {med_div:>8}")

    # --- 4. Aggregate arb opportunity sizing ---
    print(f"\n=== ARB OPPORTUNITY SUMMARY ===")
    print(f"  Total cross-DEX divergence slots (>{ROUND_TRIP_FEE*100:.1f}% spread): {len(arb_opportunities)}")

    if arb_opportunities:
        daily_opps = len(arb_opportunities) * daily_mult
        divs = [o[2] for o in arb_opportunities]
        vols = [o[3] for o in arb_opportunities]
        vols_nonzero = [v for v in vols if v > 0]

        print(f"  Daily opportunities: {daily_opps:.0f}")
        print(f"  Divergence: median {np.median(divs):.2f}%, mean {np.mean(divs):.2f}%, max {max(divs):.2f}%")
        if vols_nonzero:
            print(f"  SOL volume per opp: median {np.median(vols_nonzero):.3f}, mean {np.mean(vols_nonzero):.3f}")
        print()

        # Revenue estimate
        # For each opportunity: profit = volume * divergence_pct * (1 - fee_overhead)
        total_rev = 0
        profitable = 0
        for pair, slot, div_pct, vol_sol in arb_opportunities:
            if vol_sol <= 0:
                continue
            # Arb profit = price difference minus round-trip fees
            net_spread = (div_pct / 100) - ROUND_TRIP_FEE
            if net_spread <= 0:
                continue
            profit_sol = vol_sol * net_spread
            # Jito tip (~80% of profit)
            net_profit_sol = profit_sol * 0.2
            total_rev += net_profit_sol
            profitable += 1

        daily_rev_sol = total_rev * daily_mult
        daily_rev_usd = daily_rev_sol * SOL_USD

        print(f"  Profitable opportunities: {profitable} ({profitable * daily_mult:.0f}/day)")
        print(f"  Daily revenue (after 80% Jito tip): {daily_rev_sol:.3f} SOL = ${daily_rev_usd:.2f}")
        print(f"  At 100% capture. At 10% capture: ${daily_rev_usd * 0.1:.2f}")
        print(f"  vs $500/day target: {'VIABLE' if daily_rev_usd * 0.1 >= 500 else 'NEEDS MORE ANALYSIS' if daily_rev_usd * 0.1 >= 50 else 'LIKELY KILL'}")
    else:
        print(f"  NO cross-DEX arbitrage opportunities found above fee threshold")

    # --- 5. Top overlapping pairs detail ---
    print(f"\n=== TOP 10 PAIRS BY ARB FREQUENCY ===")
    pair_arb_count = defaultdict(list)
    for pair, slot, div, vol in arb_opportunities:
        pair_arb_count[pair].append((div, vol))

    for pair, opps in sorted(pair_arb_count.items(), key=lambda x: -len(x[1]))[:10]:
        def short(mint):
            if mint == SOL_MINT:
                return "SOL"
            return mint[:8] + ".."
        divs = [o[0] for o in opps]
        vols = [o[1] for o in opps if o[1] > 0]
        daily = len(opps) * daily_mult
        print(f"  {short(pair[0])}/{short(pair[1])}: {len(opps)} opps ({daily:.0f}/day), "
              f"med div {np.median(divs):.2f}%, "
              f"med vol {np.median(vols):.3f} SOL" if vols else f"  {short(pair[0])}/{short(pair[1])}: {len(opps)} opps, no SOL volume")


if __name__ == "__main__":
    # Use --sample N to limit files loaded (for testing / low RAM)
    sample = None
    if "--sample" in sys.argv:
        idx = sys.argv.index("--sample")
        sample = int(sys.argv[idx + 1])
        print(f"Sampling {sample} files only")

    swaps = load_swaps(max_files=sample)
    analyze(swaps)
