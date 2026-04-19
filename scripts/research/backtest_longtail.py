#!/usr/bin/env python3
"""
Long-Tail MEV Opportunity Analysis by Pool Liquidity Tier

Hypothesis: low-liquidity pools have less MEV competition and slower price
correction, creating more exploitable arbitrage windows.

Steps:
  1. Load pool universe from pool_tokens_arbitrum.json
     (new format: {"chain", "weth", "pools": [{address, token0, token1, ...}], "tokens": {...}})
  2. Query on-chain reserves via Multicall3 to compute current TVL in USD
  3. Classify pools into tiers: micro/small/medium/large/whale
  4. For each tier: pool count, arb-eligible token count, swap event frequency
  5. Print summary table and save to /root/mev/research/data/longtail_analysis.json

Run on the droplet:
  python3 scripts/research/backtest_longtail.py
  ARB_RPC_HTTP=<url> python3 scripts/research/backtest_longtail.py
"""

import json
import os
import sys
import time
from collections import defaultdict

from web3 import Web3
from eth_abi import encode, decode

# ===== Constants =====

WETH_ARB = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
MULTICALL3 = "0xcA11bde05977b3631167028862bE2a173976CA11"
ETH_PRICE_USD = 3200.0  # Reference price for TVL estimation

# Liquidity tier thresholds (USD TVL)
TIERS = [
    ("micro",  0,         1_000),
    ("small",  1_000,     10_000),
    ("medium", 10_000,    100_000),
    ("large",  100_000,   1_000_000),
    ("whale",  1_000_000, float("inf")),
]

# Pool data path (on droplet)
POOLS_PATH = "/root/mev/data/pool_tokens_arbitrum.json"

# Output path (on droplet)
OUT_PATH = "/root/mev/research/data/longtail_analysis.json"

# Swap event topic: Swap(address,uint256,uint256,uint256,uint256,address)  (V2)
# We'll scan both V2 and V3 Swap signatures
SWAP_TOPIC_V2 = Web3.keccak(text="Swap(address,uint256,uint256,uint256,uint256,address)").hex()
SWAP_TOPIC_V3 = Web3.keccak(text="Swap(address,address,int256,int256,uint160,uint128,int24)").hex()

# Function selectors used in multicall
SEL_GET_RESERVES = Web3.keccak(text="getReserves()")[:4]
SEL_LIQUIDITY    = Web3.keccak(text="liquidity()")[:4]
SEL_AGGREGATE3   = Web3.keccak(text="aggregate3((address,bool,bytes)[])")[:4]


# ===== RPC helpers =====

def get_rpc() -> str:
    """Load ARB_RPC_HTTP from env or /root/mev/.env."""
    url = os.environ.get("ARB_RPC_HTTP")
    if not url:
        for env_path in ["/root/mev/.env", os.path.join(os.path.dirname(__file__), "../../.env")]:
            try:
                with open(env_path) as f:
                    for line in f:
                        line = line.strip()
                        if line.startswith("ARB_RPC_HTTP="):
                            url = line.split("=", 1)[1].strip().strip('"').strip("'")
                            break
                if url:
                    break
            except FileNotFoundError:
                continue
    if not url:
        print("ERROR: ARB_RPC_HTTP not set. Export it or add to /root/mev/.env", file=sys.stderr)
        sys.exit(1)
    return url


def multicall3_batch(w3: Web3, calls: list, batch_size: int = 500) -> list:
    """
    Execute calls via Multicall3.aggregate3 in batches.

    calls: list of (target_address_str, calldata_bytes)
    Returns: list of (success: bool, return_data: bytes) in the same order.
    Failures are returned as (False, b"") rather than raising.
    """
    mc_addr = Web3.to_checksum_address(MULTICALL3)
    results = []

    for batch_start in range(0, len(calls), batch_size):
        batch = calls[batch_start : batch_start + batch_size]

        call_structs = [
            (Web3.to_checksum_address(target), True, calldata)
            for target, calldata in batch
        ]
        encoded_args = encode(["(address,bool,bytes)[]"], [call_structs])
        full_calldata = SEL_AGGREGATE3 + encoded_args

        try:
            raw = w3.eth.call({"to": mc_addr, "data": "0x" + full_calldata.hex()})
            decoded = decode(["(bool,bytes)[]"], raw)[0]
            for success, ret_data in decoded:
                results.append((success, bytes(ret_data)))
        except Exception as e:
            if batch_size > 50:
                sub = multicall3_batch(w3, batch, batch_size // 2)
                results.extend(sub)
            else:
                print(f"  [multicall] batch failed ({len(batch)} calls): {e}")
                results.extend((False, b"") for _ in batch)

    return results


def batched_get_logs(w3: Web3, params: dict, start: int, end: int, batch_size: int = 50_000) -> list:
    """Fetch eth_getLogs in block-range batches, halving on error."""
    all_logs = []
    cur = start
    while cur <= end:
        to = min(cur + batch_size - 1, end)
        p = {**params, "fromBlock": cur, "toBlock": to}
        try:
            logs = w3.eth.get_logs(p)
            all_logs.extend(logs)
        except Exception as e:
            if batch_size > 2_000:
                all_logs.extend(batched_get_logs(w3, params, cur, to, batch_size // 2))
            else:
                print(f"  [logs] skipping {cur}-{to}: {e}")
        cur = to + 1
    return all_logs


# ===== Pool loading =====

def load_pools(path: str, weth_lower: str) -> tuple[list, dict]:
    """
    Load pools from pool_tokens_arbitrum.json.

    Supports two formats:
      New: {"chain": ..., "weth": ..., "pools": [{address, token0, token1, protocol, fee?}], "tokens": {...}}
      Old: {address: {token0, token1, protocol, ...}}  (flat dict)

    Returns (pools_list, tokens_dict).
    pools_list entries: {"address": str, "token0": str, "token1": str, "protocol": str, "fee"?: int}
    tokens_dict: {address_lower: {"symbol": str, "decimals": int}}
    """
    with open(path) as f:
        raw = json.load(f)

    tokens = {}

    if isinstance(raw, dict) and "pools" in raw:
        # New format
        pools = []
        for p in raw["pools"]:
            pools.append({
                "address":  p["address"].lower(),
                "token0":   p["token0"].lower(),
                "token1":   p["token1"].lower(),
                "protocol": p.get("protocol", "unknown"),
                **( {"fee": p["fee"]} if "fee" in p else {} ),
            })
        # tokens section: {address: {symbol, decimals}} or {address: {symbol, decimals, ...}}
        for addr, meta in raw.get("tokens", {}).items():
            tokens[addr.lower()] = {
                "symbol":   meta.get("symbol", addr[:10]),
                "decimals": meta.get("decimals", 18),
            }
        weth_meta = tokens.get(weth_lower)
        if not weth_meta:
            tokens[weth_lower] = {"symbol": "WETH", "decimals": 18}
    else:
        # Old flat-dict format: {address: {token0, token1, symbol0, symbol1, decimals0, decimals1, protocol}}
        pools = []
        for addr, info in raw.items():
            pools.append({
                "address":  addr.lower(),
                "token0":   info["token0"].lower(),
                "token1":   info["token1"].lower(),
                "protocol": info.get("protocol", "unknown"),
                **( {"fee": info["fee"]} if "fee" in info else {} ),
            })
            for side in ("0", "1"):
                tok = info.get(f"token{side}", "").lower()
                if tok and tok not in tokens:
                    tokens[tok] = {
                        "symbol":   info.get(f"symbol{side}", tok[:10]),
                        "decimals": info.get(f"decimals{side}", 18),
                    }
        if weth_lower not in tokens:
            tokens[weth_lower] = {"symbol": "WETH", "decimals": 18}

    return pools, tokens


# ===== Reserve queries =====

def query_reserves(w3: Web3, pools: list, weth_lower: str) -> dict:
    """
    Query getReserves() for V2-style pools and liquidity() for V3 via Multicall3.

    Returns {pool_address_lower: weth_reserve_raw_int}
    For V3 the raw liquidity value is used as a proxy (no direct WETH amount).
    """
    print(f"Querying reserves for {len(pools):,} pools via Multicall3...")
    t0 = time.time()

    calls = []
    for p in pools:
        proto = p.get("protocol", "")
        if proto == "uniswapv3":
            calls.append((p["address"], SEL_LIQUIDITY))
        else:
            calls.append((p["address"], SEL_GET_RESERVES))

    results = multicall3_batch(w3, calls, batch_size=500)

    reserves = {}
    for i, (success, data) in enumerate(results):
        if not success or not data:
            continue
        p = pools[i]
        proto = p.get("protocol", "")
        addr = p["address"]
        t0_addr = p["token0"]

        if proto == "uniswapv3":
            # liquidity() → uint128  (not directly a WETH amount; mark as v3)
            if len(data) >= 32:
                liq = int.from_bytes(data[:32], "big")
                # Store under special key to distinguish later
                reserves[addr] = ("v3", liq)
        else:
            # getReserves() → (uint112 r0, uint112 r1, uint32 ts)
            if len(data) >= 64:
                r0 = int.from_bytes(data[0:32], "big")
                r1 = int.from_bytes(data[32:64], "big")
                weth_reserve = r0 if t0_addr == weth_lower else r1
                reserves[addr] = ("v2", weth_reserve)

    elapsed = time.time() - t0
    print(f"  Got reserve data for {len(reserves):,} / {len(pools):,} pools ({elapsed:.1f}s)")
    return reserves


# ===== TVL classification =====

def compute_tvl_usd(reserve_entry, eth_price: float = ETH_PRICE_USD) -> float:
    """
    Convert a reserve entry to approximate USD TVL.

    For V2: WETH reserve × 2 × eth_price (both sides assumed equal value at equilibrium).
    For V3: liquidity value is not directly comparable; we treat it as an ordinal
            proxy and assign a representative TVL based on sqrt(liq) heuristic.
            This is a rough estimate — the script flags V3 pools accordingly.
    """
    kind, value = reserve_entry
    if kind == "v2":
        weth_amount = value / 1e18  # raw units → ETH
        return weth_amount * 2 * eth_price  # both tokens ≈ equal value
    else:
        # V3: liquidity is in sqrt-price-adjusted units; use sqrt as rough ETH proxy
        # Real TVL would need slot0 + tick math. This gives a useful ordering.
        approx_eth = (value ** 0.5) / 1e9  # scale to bring into ETH-like range
        return approx_eth * 2 * eth_price


def classify_tier(tvl_usd: float) -> str:
    for name, lo, hi in TIERS:
        if lo <= tvl_usd < hi:
            return name
    return "whale"


# ===== Swap frequency =====

def query_swap_frequency(w3: Web3, pools_by_tier: dict, latest_block: int, lookback: int = 10_000) -> dict:
    """
    Count Swap events in the last `lookback` blocks for each pool.

    Returns {pool_address_lower: swap_count}
    """
    all_pool_addrs = []
    for pools in pools_by_tier.values():
        all_pool_addrs.extend(p["address"] for p in pools)

    if not all_pool_addrs:
        return {}

    start_block = max(0, latest_block - lookback)
    print(f"\nQuerying Swap events blocks {start_block:,} – {latest_block:,} ({lookback:,} blocks)...")

    # Batch pools into chunks of 500 for address-filter log queries
    # (some RPCs limit address arrays)
    swap_counts = defaultdict(int)
    chunk_size = 200

    for chunk_start in range(0, len(all_pool_addrs), chunk_size):
        chunk = all_pool_addrs[chunk_start : chunk_start + chunk_size]
        checksum_chunk = [Web3.to_checksum_address(a) for a in chunk]

        for swap_topic in (SWAP_TOPIC_V2, SWAP_TOPIC_V3):
            logs = batched_get_logs(
                w3,
                {"address": checksum_chunk, "topics": ["0x" + swap_topic]},
                start_block,
                latest_block,
                batch_size=20_000,
            )
            for log in logs:
                pool_addr = log["address"].lower()
                swap_counts[pool_addr] += 1

        pct = min(chunk_start + chunk_size, len(all_pool_addrs))
        print(f"  Scanned {pct:,}/{len(all_pool_addrs):,} pools...", end="\r", flush=True)

    print(f"\n  Swap event totals collected for {len(swap_counts):,} pools")
    return dict(swap_counts)


# ===== Output formatting =====

def fmt_usd(v: float) -> str:
    if v >= 1_000_000:
        return f"${v/1_000_000:.1f}M"
    if v >= 1_000:
        return f"${v/1_000:.1f}K"
    return f"${v:.0f}"


def print_table(tier_stats: dict):
    """Print a formatted summary table to stdout."""
    header = (
        f"{'Tier':<8} {'TVL Range':<18} {'Pools':>6} {'V2':>5} {'V3':>5} "
        f"{'ArbTokens':>10} {'ArbPairs':>9} {'Swaps/10K':>10} {'AvgSwaps':>9}"
    )
    sep = "=" * len(header)
    print(f"\n{sep}")
    print("MEV OPPORTUNITY ANALYSIS BY POOL LIQUIDITY TIER")
    print(f"ETH price: ${ETH_PRICE_USD:,.0f}  |  Lookback: last 10,000 blocks")
    print(sep)
    print(header)
    print("-" * len(header))

    tier_order = [t[0] for t in TIERS]
    for tier_name in tier_order:
        if tier_name not in tier_stats:
            continue
        s = tier_stats[tier_name]
        lo, hi = s["tvl_range_usd"]
        rng = f"{fmt_usd(lo)}-{fmt_usd(hi) if hi < float('inf') else '∞'}"
        avg_swaps = s["total_swaps"] / s["pools_with_swap_data"] if s["pools_with_swap_data"] else 0
        print(
            f"{tier_name:<8} {rng:<18} {s['pool_count']:>6} "
            f"{s['v2_count']:>5} {s['v3_count']:>5} "
            f"{s['arb_eligible_tokens']:>10} {s['arb_pairs']:>9} "
            f"{s['total_swaps']:>10,} {avg_swaps:>9.1f}"
        )

    print(sep)
    print()

    # Hypothesis check
    tiers_with_data = [t for t in tier_order if t in tier_stats and tier_stats[t]["pool_count"] > 0]
    if len(tiers_with_data) >= 2:
        lo_tier = tiers_with_data[0]
        hi_tier = tiers_with_data[-1]
        lo_avg = (tier_stats[lo_tier]["total_swaps"] / tier_stats[lo_tier]["pools_with_swap_data"]
                  if tier_stats[lo_tier]["pools_with_swap_data"] else 0)
        hi_avg = (tier_stats[hi_tier]["total_swaps"] / tier_stats[hi_tier]["pools_with_swap_data"]
                  if tier_stats[hi_tier]["pools_with_swap_data"] else 0)
        ratio = hi_avg / lo_avg if lo_avg > 0 else float("inf")
        print(f"Hypothesis check: {hi_tier} pools have {ratio:.1f}x more swaps/pool than {lo_tier} pools")
        print(f"  => {'SUPPORTED' if ratio > 3 else 'NOT SUPPORTED'}: lower-tier pools show "
              f"{'much less' if ratio > 3 else 'similar'} competition pressure")
        print()


# ===== Main =====

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Analyze MEV opportunities by pool liquidity tier (Arbitrum)")
    parser.add_argument("--pools",    default=POOLS_PATH,   help="Path to pool_tokens_arbitrum.json")
    parser.add_argument("--out",      default=OUT_PATH,     help="Output JSON path")
    parser.add_argument("--eth-price", type=float, default=ETH_PRICE_USD, help="ETH price in USD")
    parser.add_argument("--lookback", type=int,   default=10_000, help="Blocks to scan for swap events")
    parser.add_argument("--no-swaps", action="store_true", help="Skip swap frequency queries (faster)")
    args = parser.parse_args()

    eth_price = args.eth_price

    # --- Connect ---
    rpc_url = get_rpc()
    print(f"Connecting to Arbitrum RPC...")
    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 120}))
    try:
        latest = w3.eth.block_number
        print(f"Connected. Latest block: {latest:,}")
    except Exception as e:
        print(f"ERROR: Cannot connect to RPC: {e}", file=sys.stderr)
        sys.exit(1)

    weth_lower = WETH_ARB.lower()

    # --- Load pools ---
    print(f"\nLoading pools from: {args.pools}")
    try:
        pools, tokens = load_pools(args.pools, weth_lower)
    except FileNotFoundError:
        print(f"ERROR: Pool file not found: {args.pools}", file=sys.stderr)
        sys.exit(1)

    print(f"Loaded {len(pools):,} pools, {len(tokens):,} tokens")

    if not pools:
        print("No pools found. Check the pool file format.")
        sys.exit(1)

    # --- Query reserves via Multicall3 ---
    reserves = query_reserves(w3, pools, weth_lower)

    # --- Classify pools into tiers ---
    print("\nClassifying pools into liquidity tiers...")

    tier_pools: dict[str, list] = {t[0]: [] for t in TIERS}
    no_reserve = 0
    pool_tvl: dict[str, float] = {}

    for p in pools:
        addr = p["address"]
        if addr not in reserves:
            no_reserve += 1
            continue
        tvl = compute_tvl_usd(reserves[addr], eth_price)
        pool_tvl[addr] = tvl
        tier = classify_tier(tvl)
        tier_pools[tier].append(p)

    print(f"  Classified: {sum(len(v) for v in tier_pools.values()):,} pools "
          f"({no_reserve:,} skipped — no reserve data)")
    for name, _, _ in TIERS:
        print(f"  {name:<8}: {len(tier_pools[name]):>5} pools")

    # --- Compute arb-eligible tokens and pairs per tier ---
    print("\nComputing arb-eligible tokens and pairs per tier...")

    def arb_stats(pool_list: list) -> tuple[int, int]:
        """Return (arb_eligible_token_count, arb_pair_count)."""
        by_other: dict[str, list] = defaultdict(list)
        for p in pool_list:
            other = p["token1"] if p["token0"] == weth_lower else p["token0"]
            by_other[other].append(p["address"])
        arb_tokens = sum(1 for pools in by_other.values() if len(pools) >= 2)
        arb_pairs  = sum(len(ps) * (len(ps) - 1) // 2 for ps in by_other.values() if len(ps) >= 2)
        return arb_tokens, arb_pairs

    # --- Swap frequency ---
    swap_counts: dict[str, int] = {}
    if not args.no_swaps:
        swap_counts = query_swap_frequency(w3, tier_pools, latest, args.lookback)
    else:
        print("\nSkipping swap frequency queries (--no-swaps)")

    # --- Build tier stats ---
    tier_stats: dict[str, dict] = {}

    for tier_name, lo, hi in TIERS:
        pool_list = tier_pools[tier_name]
        if not pool_list:
            continue

        v2_count = sum(1 for p in pool_list if p.get("protocol", "") != "uniswapv3")
        v3_count = sum(1 for p in pool_list if p.get("protocol", "") == "uniswapv3")
        arb_tokens, arb_pairs = arb_stats(pool_list)

        total_swaps = sum(swap_counts.get(p["address"], 0) for p in pool_list)
        pools_with_data = sum(1 for p in pool_list if p["address"] in swap_counts)

        # Collect TVL stats for pools in this tier
        tvl_values = sorted(pool_tvl[p["address"]] for p in pool_list if p["address"] in pool_tvl)
        tvl_median = tvl_values[len(tvl_values) // 2] if tvl_values else 0.0
        tvl_mean   = sum(tvl_values) / len(tvl_values) if tvl_values else 0.0

        tier_stats[tier_name] = {
            "tvl_range_usd":       [lo, hi if hi != float("inf") else -1],
            "pool_count":          len(pool_list),
            "v2_count":            v2_count,
            "v3_count":            v3_count,
            "arb_eligible_tokens": arb_tokens,
            "arb_pairs":           arb_pairs,
            "total_swaps_10k_blocks": total_swaps,
            "pools_with_swap_data":   pools_with_data,
            "avg_swaps_per_pool":     round(total_swaps / pools_with_data, 2) if pools_with_data else 0,
            "tvl_median_usd":      round(tvl_median, 2),
            "tvl_mean_usd":        round(tvl_mean, 2),
            # Also export the raw counts under total_swaps for the formatter
            "total_swaps":         total_swaps,
        }

    # --- Print table ---
    print_table(tier_stats)

    # --- Build full output ---
    output = {
        "metadata": {
            "generated_at_block": latest,
            "eth_price_usd":      eth_price,
            "pools_file":         args.pools,
            "lookback_blocks":    args.lookback,
            "total_pools_loaded": len(pools),
            "pools_with_reserves": len(reserves),
            "pools_skipped_no_reserve": no_reserve,
        },
        "tiers": tier_stats,
        # Per-pool detail (address, tier, tvl, swap_count) for downstream analysis
        "pool_detail": [
            {
                "address":      p["address"],
                "token0":       p["token0"],
                "token1":       p["token1"],
                "protocol":     p.get("protocol", "unknown"),
                "tier":         classify_tier(pool_tvl.get(p["address"], 0)),
                "tvl_usd":      round(pool_tvl.get(p["address"], 0), 2),
                "swaps_10k_blocks": swap_counts.get(p["address"], 0),
                "symbol0":      tokens.get(p["token0"], {}).get("symbol", "?"),
                "symbol1":      tokens.get(p["token1"], {}).get("symbol", "?"),
            }
            for p in pools
            if p["address"] in pool_tvl
        ],
    }

    # --- Save output ---
    out_dir = os.path.dirname(args.out)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    with open(args.out, "w") as f:
        json.dump(output, f, indent=2)
    print(f"Saved analysis to: {args.out}")

    # Quick per-tier summary line for logging
    print("\nTier summary:")
    for tier_name, _, _ in TIERS:
        if tier_name in tier_stats:
            s = tier_stats[tier_name]
            print(f"  {tier_name:<8}: {s['pool_count']:>4} pools, "
                  f"{s['arb_eligible_tokens']:>3} arb-tokens, "
                  f"{s['arb_pairs']:>4} pairs, "
                  f"{s['total_swaps']:>6,} swaps/10K blocks")


if __name__ == "__main__":
    main()
