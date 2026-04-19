#!/usr/bin/env python3
"""
Bridge-pool expansion: add non-WETH↔non-WETH pools to enable 3-hop cycle detection.

find_arb_cycles_3hop (crates/strategies/src/pool_graph.rs) needs pools of shape
X↔Y where neither is WETH. The existing universe only has WETH-paired pools, so
the 3-hop detector returns 0 cycles.

This script:
  1. Loads the existing pool_tokens_{chain}.json.
  2. Picks the top-N non-WETH tokens by WETH-pool count.
  3. For every pair (A,B) of those tokens, queries each configured factory:
       - V2 factories: getPair(A,B) via multicall3
       - V3 factories: getPool(A,B,fee) for common fee tiers
  4. Filters results for non-zero address and non-dust reserves/liquidity.
  5. Merges new pools into the JSON (dedup by address) and rewrites it.

Usage:
  python expand_pools_bridges.py --chain arbitrum --top 20
  python expand_pools_bridges.py --chain base --top 20 --dry-run
  python expand_pools_bridges.py --chain all
"""
import json
import os
import sys
import time
from collections import defaultdict
from web3 import Web3
from eth_abi import encode, decode

# Reuse chain configs / helpers / selectors from the sibling script
sys.path.insert(0, os.path.dirname(__file__))
from expand_pools_multichain import (  # noqa: E402
    CHAINS,
    MULTICALL3,
    SEL_AGGREGATE3,
    SEL_TOKEN0,
    SEL_TOKEN1,
    SEL_GET_RESERVES,
    SEL_LIQUIDITY,
    get_rpc,
    multicall3_batch,
    resolve_tokens_multicall,
)

# V2 factory: getPair(address,address) -> address
SEL_GET_PAIR = Web3.keccak(text="getPair(address,address)")[:4]          # 0xe6a43905
# V3 factory: getPool(address,address,uint24) -> address
SEL_GET_POOL = Web3.keccak(text="getPool(address,address,uint24)")[:4]   # 0x1698ee82

V3_FEES = [100, 500, 3000, 10000]

ZERO_ADDR = "0x" + "00" * 20

# Min reserve thresholds (token units, NOT wei) for the V2 dust filter.
# For any token that isn't in this list, we require raw reserve >= 1 * 10^decimals.
MIN_TOKEN_UNITS = {
    "USDC": 5_000,
    "USDT": 5_000,
    "USD₮0": 5_000,
    "DAI":  5_000,
    "FRAX": 5_000,
    "USDC.E": 5_000,
    "USDBC": 5_000,
    "WETH": 2,
    "WBTC": 0.05,
    "ARB":  10_000,
    "WSTETH": 2,
    "RETH": 2,
    "CBETH": 2,
}


def load_universe(path):
    with open(path) as f:
        return json.load(f)


def top_non_weth_tokens(universe, n):
    weth = universe["weth"].lower()
    cnt = defaultdict(int)
    # Preserve checksum casing from the first time we see the token in a pool.
    checksum = {}
    for p in universe["pools"]:
        for side in ("token0", "token1"):
            addr_cs = p[side]
            addr_lc = addr_cs.lower()
            if addr_lc == weth:
                continue
            cnt[addr_lc] += 1
            checksum.setdefault(addr_lc, addr_cs)
    ranked = sorted(cnt.items(), key=lambda x: -x[1])[:n]
    return [(checksum[lc], lc, c) for lc, c in ranked]


def build_candidate_pairs(tokens):
    """All unordered pairs."""
    pairs = []
    for i in range(len(tokens)):
        for j in range(i + 1, len(tokens)):
            pairs.append((tokens[i], tokens[j]))
    return pairs


def encode_get_pair(a, b):
    return SEL_GET_PAIR + encode(["address", "address"], [Web3.to_checksum_address(a),
                                                           Web3.to_checksum_address(b)])


def encode_get_pool(a, b, fee):
    return SEL_GET_POOL + encode(
        ["address", "address", "uint24"],
        [Web3.to_checksum_address(a), Web3.to_checksum_address(b), fee],
    )


def query_factory_pools(w3, factory, pairs):
    """V2: one call per pair. Returns list of (pair_tuple, pool_address_lower or None)."""
    calls = [(factory["address"], encode_get_pair(a[0], b[0])) for (a, b) in pairs]
    results = multicall3_batch(w3, calls, batch_size=500)
    out = []
    for (pair, (ok, data)) in zip(pairs, results):
        if not ok or len(data) < 32:
            out.append((pair, None))
            continue
        addr = "0x" + data[-20:].hex()
        out.append((pair, None if addr.lower() == ZERO_ADDR else addr.lower()))
    return out


def query_v3_factory_pools(w3, factory, pairs, fees=V3_FEES):
    """V3: one call per (pair, fee)."""
    calls = []
    meta = []   # parallel list of (pair, fee) for each call
    for (a, b) in pairs:
        for fee in fees:
            calls.append((factory["address"], encode_get_pool(a[0], b[0], fee)))
            meta.append(((a, b), fee))
    results = multicall3_batch(w3, calls, batch_size=500)
    out = []
    for ((pair, fee), (ok, data)) in zip(meta, results):
        if not ok or len(data) < 32:
            continue
        addr = "0x" + data[-20:].hex()
        if addr.lower() == ZERO_ADDR:
            continue
        out.append((pair, fee, addr.lower()))
    return out


def verify_tokens_multicall(w3, pools):
    """Verify token0/token1 and pool types for candidate addresses.

    Returns dict: addr_lower -> (token0_cs, token1_cs) or None on failure.
    """
    if not pools:
        return {}
    addrs = sorted({p["address"].lower() for p in pools})
    calls = []
    for a in addrs:
        calls.append((a, SEL_TOKEN0))
        calls.append((a, SEL_TOKEN1))
    results = multicall3_batch(w3, calls, batch_size=500)
    out = {}
    for i, a in enumerate(addrs):
        s0, d0 = results[2 * i]
        s1, d1 = results[2 * i + 1]
        if not s0 or not s1 or len(d0) < 20 or len(d1) < 20:
            out[a] = None
            continue
        t0 = Web3.to_checksum_address("0x" + d0[-20:].hex())
        t1 = Web3.to_checksum_address("0x" + d1[-20:].hex())
        out[a] = (t0, t1)
    return out


def filter_by_liquidity(w3, candidates, token_meta):
    """Check getReserves (V2) / liquidity (V3). Keep non-dust pools.

    candidates: list of dicts {address, token0, token1, protocol, fee?}
    token_meta: dict of lowercase addr -> {symbol, decimals}
    Returns filtered list.
    """
    if not candidates:
        return []
    calls = []
    for c in candidates:
        if c["protocol"] == "uniswapv3":
            calls.append((c["address"], SEL_LIQUIDITY))
        else:
            calls.append((c["address"], SEL_GET_RESERVES))
    results = multicall3_batch(w3, calls, batch_size=500)

    kept = []
    for c, (ok, data) in zip(candidates, results):
        if not ok:
            continue
        if c["protocol"] == "uniswapv3":
            if len(data) < 32:
                continue
            liq = int.from_bytes(data[:32], "big")
            if liq > 0:
                kept.append(c)
            continue

        # V2-style
        if len(data) < 64:
            continue
        r0 = int.from_bytes(data[0:32], "big")
        r1 = int.from_bytes(data[32:64], "big")
        if r0 == 0 or r1 == 0:
            continue

        m0 = token_meta.get(c["token0"].lower(), {"symbol": "?", "decimals": 18})
        m1 = token_meta.get(c["token1"].lower(), {"symbol": "?", "decimals": 18})
        min0 = MIN_TOKEN_UNITS.get(m0["symbol"].upper(), 1)
        min1 = MIN_TOKEN_UNITS.get(m1["symbol"].upper(), 1)
        if r0 / (10 ** m0["decimals"]) < min0:
            continue
        if r1 / (10 ** m1["decimals"]) < min1:
            continue
        kept.append(c)
    return kept


def process_chain(chain_name, config, top_n, dry_run):
    print(f"\n{'=' * 60}\nCHAIN: {chain_name.upper()} (bridge pools)\n{'=' * 60}")

    # Load existing universe
    local_path = os.path.join(os.path.dirname(__file__), f"../../data/{config['out_file']}")
    if not os.path.exists(local_path):
        print(f"  ERROR: {local_path} not found. Run expand_pools_multichain.py first.")
        return
    universe = load_universe(local_path)

    rpc_url = get_rpc(config)
    if not rpc_url:
        return
    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 120}))
    try:
        latest = w3.eth.block_number
        print(f"  Connected: block {latest:,}")
    except Exception as e:
        print(f"  Failed to connect: {e}")
        return

    # Top-N non-WETH tokens
    tokens = top_non_weth_tokens(universe, top_n)
    print(f"  Top {len(tokens)} non-WETH tokens:")
    token_meta_existing = {k.lower(): v for k, v in universe.get("tokens", {}).items()}
    for cs, lc, cnt in tokens:
        sym = token_meta_existing.get(lc, {}).get("symbol", "?")
        print(f"    {sym:12s} {cs}  ({cnt} WETH pools)")

    pairs = build_candidate_pairs(tokens)
    print(f"  Candidate pairs: {len(pairs)}")
    if not pairs:
        return

    # Query each factory for candidate pool addresses
    all_candidates = []
    for factory in config["factories"]:
        try:
            if factory["type"] == "v3":
                print(f"  Querying V3 factory {factory['name']} getPool for {len(V3_FEES)} fees/pair...")
                hits = query_v3_factory_pools(w3, factory, pairs)
                print(f"    Non-zero V3 pools: {len(hits)}")
                for (pair, fee, addr) in hits:
                    (a_cs, _, _), (b_cs, _, _) = pair
                    all_candidates.append({
                        "address": addr,
                        "token0_hint": a_cs,  # verified later via token0()
                        "token1_hint": b_cs,
                        "protocol": factory["name"],
                        "fee": fee,
                    })
            else:
                print(f"  Querying V2 factory {factory['name']} getPair...")
                hits = query_factory_pools(w3, factory, pairs)
                nonzero = [(pair, addr) for (pair, addr) in hits if addr]
                print(f"    Non-zero V2 pools: {len(nonzero)}")
                for (pair, addr) in nonzero:
                    (a_cs, _, _), (b_cs, _, _) = pair
                    all_candidates.append({
                        "address": addr,
                        "token0_hint": a_cs,
                        "token1_hint": b_cs,
                        "protocol": factory["name"],
                    })
        except Exception as e:
            print(f"    ERROR on {factory['name']}: {e}")
            continue

    # Dedup against existing pools & within candidates
    existing_addrs = {p["address"].lower() for p in universe["pools"]}
    uniq = {}
    for c in all_candidates:
        if c["address"] in existing_addrs:
            continue
        uniq.setdefault(c["address"], c)
    candidates = list(uniq.values())
    print(f"  New candidate pools (before verify): {len(candidates)}")
    if not candidates:
        return

    # Verify token0/token1 — some factories may return unexpected ordering
    verified = verify_tokens_multicall(w3, candidates)
    verified_candidates = []
    for c in candidates:
        tk = verified.get(c["address"])
        if not tk:
            continue
        t0, t1 = tk
        entry = {
            "address": Web3.to_checksum_address(c["address"]),
            "token0": t0,
            "token1": t1,
            "protocol": "uniswapv3" if c["protocol"].lower().endswith("v3") else c["protocol"],
        }
        if "fee" in c:
            entry["fee"] = c["fee"]
        verified_candidates.append(entry)
    print(f"  Verified: {len(verified_candidates)}")

    # Resolve any missing token metadata (should be none, but be safe)
    known_lc = set(token_meta_existing.keys())
    need_meta = set()
    for c in verified_candidates:
        for side in ("token0", "token1"):
            if c[side].lower() not in known_lc:
                need_meta.add(c[side].lower())
    if need_meta:
        print(f"  Resolving metadata for {len(need_meta)} new tokens...")
        extra_meta = resolve_tokens_multicall(w3, need_meta, universe["weth"].lower())
        for lc, meta in extra_meta.items():
            token_meta_existing[lc] = {"symbol": meta["symbol"], "decimals": meta["decimals"]}

    # Dust filter
    kept = filter_by_liquidity(w3, verified_candidates, token_meta_existing)
    print(f"  Passed liquidity filter: {len(kept)}")

    if dry_run:
        print("\n  [dry-run] Would add the following bridge pools:")
        for p in kept[:50]:
            s0 = token_meta_existing.get(p["token0"].lower(), {}).get("symbol", "?")
            s1 = token_meta_existing.get(p["token1"].lower(), {}).get("symbol", "?")
            tag = f"V3/{p['fee']}" if p["protocol"] == "uniswapv3" else "V2"
            print(f"    {tag:8s} {p['address']}  {s0}/{s1}  [{p['protocol']}]")
        if len(kept) > 50:
            print(f"    ... and {len(kept) - 50} more")
        return

    if not kept:
        print("  Nothing to add.")
        return

    # Merge into universe
    universe["pools"].extend(kept)

    # Make sure tokens block contains metadata for every new token (keyed by checksum)
    tokens_block = universe.setdefault("tokens", {})
    existing_lc_to_key = {k.lower(): k for k in tokens_block.keys()}
    for p in kept:
        for side in ("token0", "token1"):
            addr_cs = p[side]
            lc = addr_cs.lower()
            if lc in existing_lc_to_key:
                continue
            meta = token_meta_existing.get(lc)
            if not meta:
                continue
            tokens_block[addr_cs] = {
                "symbol": meta.get("symbol", "?"),
                "decimals": meta.get("decimals", 18),
                "address": addr_cs,
            }
            existing_lc_to_key[lc] = addr_cs

    with open(local_path, "w") as f:
        json.dump(universe, f, indent=2)
    print(f"  Wrote {local_path} — total pools now {len(universe['pools'])}")

    # Mirror to /root/mev/data if it exists (matches sibling script behavior)
    remote = f"/root/mev/data/{config['out_file']}"
    try:
        if os.path.isdir(os.path.dirname(remote)):
            with open(remote, "w") as f:
                json.dump(universe, f, indent=2)
            print(f"  Also wrote {remote}")
    except Exception:
        pass


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--chain", choices=list(CHAINS.keys()) + ["all"], default="all")
    parser.add_argument("--top", type=int, default=20,
                        help="Number of top non-WETH tokens to cross-pair (default: 20)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be added without writing files")
    args = parser.parse_args()

    chains = CHAINS.items() if args.chain == "all" else [(args.chain, CHAINS[args.chain])]
    for name, cfg in chains:
        process_chain(name, cfg, args.top, args.dry_run)


if __name__ == "__main__":
    main()
