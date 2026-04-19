#!/usr/bin/env python3
"""
MEV-Share Backrun Profit Estimator

For each arb-able pool pair in our universe, fetches current on-chain state,
simulates a range of swap sizes hitting the primary pool, computes the optimal
backrun arb across counterpart pools, and multiplies by observed hint frequency.

This answers: "What's the daily revenue potential of MEV-Share backrunning,
given our pool universe and current market conditions?"

Usage:
  python estimate_backrun_profit.py [--pool-file data/pool_tokens.json]
                                    [--hints-file research/data/mevshare_backrun_hints.jsonl]
"""

import argparse
import json
import math
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path

_RPC = ["https://eth.llamarpc.com"]

def eth_call(to, data, rpc=None):
    if rpc is None:
        rpc = _RPC[0]
    payload = json.dumps({
        "jsonrpc": "2.0", "id": 1, "method": "eth_call",
        "params": [{"to": to, "data": data}, "latest"]
    })
    r = subprocess.run(
        ["curl", "-s", "-X", "POST", rpc, "-H", "Content-Type: application/json",
         "-d", payload],
        capture_output=True, text=True
    )
    try:
        return json.loads(r.stdout).get("result", "0x")
    except:
        return "0x"


def get_v2_state(pool_addr):
    """Fetch V2 pool reserves and token order."""
    reserves_hex = eth_call(pool_addr, "0x0902f1ac")
    if not reserves_hex or reserves_hex == "0x" or len(reserves_hex) < 130:
        return None
    r0 = int(reserves_hex[2:66], 16)
    r1 = int(reserves_hex[66:130], 16)
    return {"reserve0": r0, "reserve1": r1}


def get_v3_state(pool_addr):
    """Fetch V3 pool sqrtPriceX96, tick, liquidity, fee."""
    slot0_hex = eth_call(pool_addr, "0x3850c7bd")
    if not slot0_hex or slot0_hex == "0x" or len(slot0_hex) < 130:
        return None
    sqrt_price_x96 = int(slot0_hex[2:66], 16)
    tick_raw = int(slot0_hex[66:130], 16)
    if tick_raw > 0x7fffff:
        tick = tick_raw - 0x1000000
    else:
        tick = tick_raw

    liq_hex = eth_call(pool_addr, "0x1a686502")
    if not liq_hex or liq_hex == "0x" or len(liq_hex) < 66:
        return None
    liquidity = int(liq_hex[2:66], 16)

    fee_hex = eth_call(pool_addr, "0xddca3f43")
    fee = int(fee_hex[2:66], 16) if fee_hex and fee_hex != "0x" and len(fee_hex) >= 66 else 3000

    return {
        "sqrtPriceX96": sqrt_price_x96,
        "tick": tick,
        "liquidity": liquidity,
        "fee": fee,
    }


def v2_swap_output(amount_in, reserve_in, reserve_out, fee_bps=30):
    """Constant-product swap: amount_in -> amount_out."""
    if reserve_in <= 0 or reserve_out <= 0 or amount_in <= 0:
        return 0
    fee_mult = 10000 - fee_bps
    numerator = amount_in * fee_mult * reserve_out
    denominator = reserve_in * 10000 + amount_in * fee_mult
    return numerator / denominator


def v3_swap_output(amount_in, liquidity, sqrt_price_x96, fee_bps, zero_for_one):
    """
    Single-tick V3 swap approximation using virtual reserves.
    Returns output amount. Caps input at 10% of virtual reserve.
    """
    if liquidity <= 0 or sqrt_price_x96 <= 0:
        return 0

    sqrt_p = sqrt_price_x96 / (2**96)
    # Virtual reserves: x = L/sqrtP (token0), y = L*sqrtP (token1)
    virt_x = liquidity / sqrt_p  # token0
    virt_y = liquidity * sqrt_p  # token1

    if zero_for_one:
        # token0 in -> token1 out
        cap = virt_x * 0.10
        effective_in = min(amount_in, cap)
    else:
        # token1 in -> token0 out
        cap = virt_y * 0.10
        effective_in = min(amount_in, cap)

    in_after_fee = effective_in * (1_000_000 - fee_bps) / 1_000_000

    if zero_for_one:
        out = in_after_fee * virt_y / (virt_x + in_after_fee)
    else:
        out = in_after_fee * virt_x / (virt_y + in_after_fee)

    return out


def v3_post_swap_price(liquidity, sqrt_price_x96, amount_in, fee_bps, zero_for_one):
    """
    Estimate sqrtPriceX96 after a single-tick swap.
    Uses the relationship: new_sqrtP = L / (L/sqrtP + dx) for zero_for_one.
    """
    if liquidity <= 0 or sqrt_price_x96 <= 0:
        return sqrt_price_x96

    sqrt_p = sqrt_price_x96 / (2**96)
    in_after_fee = amount_in * (1_000_000 - fee_bps) / 1_000_000

    if zero_for_one:
        # token0 in -> price goes down (sqrtP decreases)
        virt_x = liquidity / sqrt_p
        cap = virt_x * 0.10
        effective = min(in_after_fee, cap)
        new_sqrt_p = liquidity / (liquidity / sqrt_p + effective)
    else:
        # token1 in -> price goes up (sqrtP increases)
        virt_y = liquidity * sqrt_p
        cap = virt_y * 0.10
        effective = min(in_after_fee, cap)
        new_sqrt_p = (liquidity * sqrt_p + effective) / liquidity

    return int(new_sqrt_p * (2**96))


def compute_v3_price(sqrt_price_x96, token0_is_weth):
    """Compute price in WETH terms from sqrtPriceX96."""
    p = (sqrt_price_x96 / (2**96)) ** 2  # price = token1/token0
    if token0_is_weth:
        return 1.0 / p if p > 0 else 0  # invert: we want "other per WETH"
    return p  # already "WETH per other"


def compute_v2_price(r0, r1, token0_is_weth):
    """Compute price in WETH terms from reserves."""
    if token0_is_weth:
        return r1 / r0 if r0 > 0 else 0  # other per WETH
    return r0 / r1 if r1 > 0 else 0


def estimate_arb_profit_for_pair(pool_a, pool_b, state_a, state_b, weth_addr,
                                 swap_sizes_eth):
    """
    For a range of hypothetical swap sizes hitting pool_a, compute the
    backrun arb profit by arbing pool_a's post-swap price against pool_b.

    Backrun logic: victim swaps on pool A, moving its price.
    Backrunner buys on pool B (undisturbed) and sells on pool A (reverse
    direction from victim).

    Arb path: WETH → pool_b → other → pool_a (reverse) → WETH

    Returns list of (swap_size_eth, arb_profit_eth, arb_size_eth) tuples.
    """
    results = []
    weth = weth_addr.lower()

    a_is_v3 = "sqrtPriceX96" in state_a
    b_is_v3 = "sqrtPriceX96" in state_b

    t0_a = pool_a.get("token0", "").lower()
    t0_b = pool_b.get("token0", "").lower()

    a_weth_is_t0 = t0_a == weth
    b_weth_is_t0 = t0_b == weth

    arb_sizes = [0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5,
                 1.0, 2.0, 5.0, 10.0]

    for swap_eth in swap_sizes_eth:
        swap_wei = swap_eth * 1e18

        # Victim swaps WETH -> other on pool A
        # Pool A: WETH goes in, other comes out
        # After: pool A has more WETH, less other → other is expensive on A
        # Backrun: buy other on B (cheap), sell other on A (expensive)
        # = WETH→B→other, then other→A→WETH

        # Step 1: compute pool A's post-swap state
        if a_is_v3:
            victim_zfo = a_weth_is_t0  # WETH in = zero_for_one if WETH is token0
            new_sqrt_a = v3_post_swap_price(
                state_a["liquidity"], state_a["sqrtPriceX96"],
                swap_wei, state_a["fee"], victim_zfo
            )
        else:
            if a_weth_is_t0:
                r_weth_a, r_other_a = state_a["reserve0"], state_a["reserve1"]
            else:
                r_weth_a, r_other_a = state_a["reserve1"], state_a["reserve0"]
            other_out = v2_swap_output(swap_wei, r_weth_a, r_other_a, 30)
            new_r_weth_a = r_weth_a + swap_wei
            new_r_other_a = r_other_a - other_out

        # Step 2: find optimal arb size
        best_profit = 0
        best_arb_size = 0

        for arb_frac in arb_sizes:
            arb_wei = arb_frac * 1e18

            # Leg 1: buy "other" on pool B (WETH in → other out)
            if b_is_v3:
                b_zfo = b_weth_is_t0  # WETH in on B
                other_from_b = v3_swap_output(
                    arb_wei, state_b["liquidity"],
                    state_b["sqrtPriceX96"], state_b["fee"], b_zfo
                )
            else:
                if b_weth_is_t0:
                    other_from_b = v2_swap_output(
                        arb_wei, state_b["reserve0"], state_b["reserve1"], 30
                    )
                else:
                    other_from_b = v2_swap_output(
                        arb_wei, state_b["reserve1"], state_b["reserve0"], 30
                    )

            if other_from_b <= 0:
                continue

            # Leg 2: sell "other" on pool A (other in → WETH out)
            # This is REVERSE direction from victim on pool A
            if a_is_v3:
                # Reverse direction: other in = NOT victim_zfo
                reverse_zfo = not victim_zfo
                weth_from_a = v3_swap_output(
                    other_from_b, state_a["liquidity"],
                    new_sqrt_a, state_a["fee"], reverse_zfo
                )
            else:
                # V2 post-swap: sell other into A's post-swap reserves
                # other in → WETH out
                weth_from_a = v2_swap_output(
                    other_from_b, new_r_other_a, new_r_weth_a, 30
                )

            profit_wei = weth_from_a - arb_wei
            profit_eth = profit_wei / 1e18
            if profit_eth > best_profit:
                best_profit = profit_eth
                best_arb_size = arb_frac

        results.append((swap_eth, best_profit, best_arb_size))

    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pool-file", default="data/pool_tokens.json")
    parser.add_argument("--hints-file",
                        default="research/data/mevshare_backrun_hints.jsonl")
    parser.add_argument("--rpc", default=None)
    args = parser.parse_args()

    rpc = args.rpc
    if rpc is None:
        # Try env var
        import os
        rpc = os.environ.get("ETH_RPC_HTTP", os.environ.get("ETH_RPC_URL", "https://eth.llamarpc.com"))
    _RPC[0] = rpc
    print(f"Using RPC: {rpc[:40]}...")

    # Load pool universe
    with open(args.pool_file) as f:
        pool_data = json.load(f)

    weth = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

    # Known decimals for common tokens
    KNOWN_DECIMALS = {
        "0xdac17f958d2ee523a2206206994597c13d831ec7": 6,   # USDT
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": 6,   # USDC
        "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": 8,   # WBTC
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": 18,  # WETH
        "0x6b175474e89094c44da98b954eedeac495271d0f": 18,  # DAI
    }

    # Build pair index
    pair_pools = defaultdict(list)
    for addr, info in pool_data.items():
        t0 = info.get("token0", "").lower()
        t1 = info.get("token1", "").lower()
        if t0 and t1:
            pair = tuple(sorted([t0, t1]))
            # Attach decimals
            info_copy = dict(info)
            info_copy["decimals0"] = info.get("decimals0", KNOWN_DECIMALS.get(t0, 18))
            info_copy["decimals1"] = info.get("decimals1", KNOWN_DECIMALS.get(t1, 18))
            pair_pools[pair].append({"address": addr.lower(), **info_copy})

    # Load hint frequency per pool
    pool_freq = Counter()  # backrunnable hints per pool
    total_hints = 0
    duration_hours = 0
    if Path(args.hints_file).exists():
        first_ts = None
        last_ts = None
        with open(args.hints_file) as f:
            for line in f:
                try:
                    e = json.loads(line.strip())
                except:
                    continue
                total_hints += 1
                ts = e.get("ts", 0)
                if first_ts is None:
                    first_ts = ts
                last_ts = ts
                if e.get("backrunnable"):
                    for p in e.get("matched_pools", []):
                        pool_freq[p] += 1
        if first_ts and last_ts:
            duration_hours = max((last_ts - first_ts) / 3600, 1)
        print(f"Loaded {total_hints:,} hints over {duration_hours:.1f}h")
        print(f"Backrunnable pool hits: {sum(pool_freq.values()):,}")
    else:
        print(f"No hints file found at {args.hints_file}, using frequency=1 for all")
        duration_hours = 1

    # Find arb-able pairs with WETH on one side
    arb_pairs = []
    for pair, pools in pair_pools.items():
        if len(pools) < 2:
            continue
        if weth not in pair:
            continue
        arb_pairs.append((pair, pools))

    print(f"\nFound {len(arb_pairs)} WETH pairs with arb partners")

    # Fetch state and estimate profits for top pairs by hint frequency
    swap_sizes = [0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0]

    print(f"\n{'='*80}")
    print(f"  BACKRUN PROFIT ESTIMATES BY POOL PAIR")
    print(f"{'='*80}")

    all_pair_results = []

    for pair, pools in arb_pairs:
        # Sum frequency across pools in this pair
        pair_freq = sum(pool_freq.get(p["address"], 0) for p in pools)
        freq_per_day = pair_freq / duration_hours * 24 if duration_hours > 0 else 0

        if pair_freq == 0 and total_hints > 0:
            continue  # skip pairs with no hint traffic

        other_token = pair[0] if pair[1] == weth else pair[1]
        label = pools[0].get("symbol0", "?") + "/" + pools[0].get("symbol1", "?")

        print(f"\n  --- {label} ({len(pools)} pools, {pair_freq} hits, "
              f"~{freq_per_day:.0f}/day) ---")

        # Fetch on-chain state for each pool
        states = {}
        for p in pools:
            addr = p["address"]
            proto = p.get("protocol", "uniswapv2")
            d0 = int(p.get("decimals0", 18))
            d1 = int(p.get("decimals1", 18))
            if proto == "uniswapv3":
                state = get_v3_state(addr)
                if state:
                    state["decimals0"] = d0
                    state["decimals1"] = d1
                    states[addr] = state
                    liq = state["liquidity"]
                    sqrt_p = state["sqrtPriceX96"]
                    price_raw = (sqrt_p / (2**96)) ** 2
                    # Adjust for decimals: price_adj = price_raw * 10^(d0-d1)
                    price_adj = price_raw * (10 ** (d0 - d1))
                    virt_x = liq / (sqrt_p / (2**96))
                    virt_y = liq * (sqrt_p / (2**96))
                    print(f"    V3 {addr[:10]}... fee={state['fee']}  "
                          f"liq={liq:.2e}  price_adj={price_adj:.6f}  "
                          f"virt=({virt_x/10**d0:.1f}, {virt_y/10**d1:.1f})")
                else:
                    print(f"    V3 {addr[:10]}... FAILED to fetch state")
            else:
                state = get_v2_state(addr)
                if state:
                    state["decimals0"] = d0
                    state["decimals1"] = d1
                    states[addr] = state
                    r0 = state["reserve0"]
                    r1 = state["reserve1"]
                    print(f"    V2 {addr[:10]}... reserves=({r0/10**d0:.2f}, "
                          f"{r1/10**d1:.2f})")
                else:
                    print(f"    V2 {addr[:10]}... FAILED to fetch state")

        if len(states) < 2:
            print(f"    SKIP: need 2+ pools with valid state")
            continue

        # Per-pool estimation: for each pool A (victim swaps here),
        # find best arb against counterpart pools B. Weight by pool A's
        # own hint frequency, NOT the pair-level total.
        pool_addrs = [p["address"] for p in pools if p["address"] in states]
        gas_cost_eth = 150000 * 30e-9  # 150k gas at 30 gwei

        pair_daily_gross = 0
        pair_daily_net = 0

        print(f"\n    {'Pool A (victim)':>14}  {'freq':>5}  {'freq/d':>7}  "
              f"{'liq(ETH)':>10}  {'profit@1ETH':>12}  {'best_arb':>8}  "
              f"{'daily_net':>10}  Pool B")

        for addr_a in pool_addrs:
            p_a = next(p for p in pools if p["address"] == addr_a)
            a_freq = pool_freq.get(addr_a, 0)
            a_freq_day = a_freq / duration_hours * 24 if duration_hours > 0 else 0

            if a_freq == 0 and total_hints > 0:
                continue  # no hints hit this pool

            # Compute liquidity in ETH terms for context
            a_is_v3 = "sqrtPriceX96" in states[addr_a]
            if a_is_v3:
                s = states[addr_a]
                sqrt_p = s["sqrtPriceX96"] / (2**96)
                if p_a.get("token0", "").lower() == weth:
                    liq_eth = s["liquidity"] / sqrt_p / 1e18
                else:
                    liq_eth = s["liquidity"] * sqrt_p / 1e18
            else:
                s = states[addr_a]
                if p_a.get("token0", "").lower() == weth:
                    liq_eth = s["reserve0"] / 1e18
                else:
                    liq_eth = s["reserve1"] / 1e18

            # Find best counterpart pool B for 1 ETH swap on A
            best_profit_1eth = 0
            best_arb_size = 0
            best_b_label = ""

            for addr_b in pool_addrs:
                if addr_a == addr_b:
                    continue
                p_b = next(p for p in pools if p["address"] == addr_b)

                results = estimate_arb_profit_for_pair(
                    p_a, p_b, states[addr_a], states[addr_b],
                    weth, [1.0]
                )
                if results:
                    _, profit_eth, arb_size = results[0]
                    if profit_eth > best_profit_1eth:
                        best_profit_1eth = profit_eth
                        best_arb_size = arb_size
                        best_b_label = addr_b[:10]

            net_per_opp = max(best_profit_1eth - gas_cost_eth, 0)
            daily_g = best_profit_1eth * a_freq_day
            daily_n = net_per_opp * a_freq_day
            pair_daily_gross += daily_g
            pair_daily_net += daily_n

            print(f"    {addr_a[:10]}...  {a_freq:>5}  {a_freq_day:>7.0f}  "
                  f"{liq_eth:>10.1f}  {best_profit_1eth:>12.6f}  "
                  f"{best_arb_size:>8.3f}  {daily_n:>10.4f}  {best_b_label}")

        if pair_daily_gross > 0 or pair_daily_net > 0:
            print(f"\n    PAIR DAILY (1 ETH avg swap, 100% capture):")
            print(f"      Gross: {pair_daily_gross:.4f} ETH/day  "
                  f"Net: {pair_daily_net:.4f} ETH/day")

        all_pair_results.append({
            "label": label,
            "pools": len(pools),
            "freq_per_day": freq_per_day,
            "daily_gross": pair_daily_gross,
            "daily_net": pair_daily_net,
        })

    # Summary
    print(f"\n{'='*80}")
    print(f"  AGGREGATE DAILY REVENUE ESTIMATE")
    print(f"{'='*80}")

    total_daily_gross = 0
    total_daily_net = 0

    for pr in all_pair_results:
        daily_g = pr.get("daily_gross", 0)
        daily_n = pr.get("daily_net", 0)
        total_daily_gross += daily_g
        total_daily_net += daily_n
        if daily_g > 0.001:
            print(f"  {pr['label']:>15}  "
                  f"gross={daily_g:.4f} ETH  net={daily_n:.4f} ETH")

    print(f"\n  TOTAL (100% capture, 1 ETH avg swap, 30 gwei gas):")
    print(f"    Gross: {total_daily_gross:.4f} ETH/day")
    print(f"    Net:   {total_daily_net:.4f} ETH/day")

    # Reality check
    for capture_rate in [0.50, 0.10, 0.05, 0.01]:
        adjusted = total_daily_net * capture_rate
        usd = adjusted * 2500  # rough ETH price
        print(f"    At {capture_rate*100:>5.1f}% capture: {adjusted:.4f} ETH/day "
              f"= ~${usd:.0f}/day")

    print(f"\n  NOTE: These estimates assume single-tick V3 approximation,")
    print(f"  30 gwei gas, 1 ETH average swap size, and uniform swap direction.")
    print(f"  Real profits depend on actual swap sizes (often larger than 1 ETH")
    print(f"  on major pairs) and competition dynamics.")


if __name__ == "__main__":
    main()
