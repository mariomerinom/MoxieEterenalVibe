#!/usr/bin/env python3
"""
Enumerate V2-V3 arbitrage cycles in the pool universe.

Finds all 2-hop WETH cycles (WETH -> Token A -> WETH) and classifies them:
  - V2-V3: one leg Uniswap V2/Sushi, other leg Uniswap V3 (different protocol type)
  - V2-V2: both legs V2 (different DEXes, e.g., Uni V2 vs Sushi)
  - V3-V3 same fee: both legs V3 with identical fee tier
  - V3-V3 diff fee: both legs V3 with different fee tiers

Usage:
  python enumerate_v2v3_cycles.py [--pools PATH_TO_POOL_JSON] [--chain ethereum|base|arbitrum]
"""

import argparse
import json
import os
import sys
from collections import defaultdict
from itertools import combinations


# WETH addresses per chain
WETH_BY_CHAIN = {
    "ethereum": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
    "base": "0x4200000000000000000000000000000000000006",
    "arbitrum": "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
}

# Protocol classification
V2_PROTOCOLS = {"uniswapv2", "sushiswap", "camelot", "aerodrome"}
V3_PROTOCOLS = {"uniswapv3"}


def load_pools(path):
    """Load pools from JSON, handling both flat-dict and structured formats.

    Flat format (Ethereum/Base): {pool_addr: {token0, token1, protocol, fee?, ...}}
    Structured format (Arbitrum): {chain, weth, pools: [{address, token0, token1, protocol, fee?}]}

    Returns: (list_of_pool_dicts, chain_name_or_None)
    """
    with open(path) as f:
        data = json.load(f)

    if isinstance(data, dict) and "pools" in data:
        # Structured format
        chain = data.get("chain")
        pools = []
        for p in data["pools"]:
            pools.append({
                "address": p["address"].lower(),
                "token0": p["token0"].lower(),
                "token1": p["token1"].lower(),
                "protocol": p.get("protocol", "unknown"),
                "fee": p.get("fee"),
                "symbol0": p.get("symbol0"),
                "symbol1": p.get("symbol1"),
            })
        return pools, chain
    elif isinstance(data, dict):
        # Flat format: keys are pool addresses
        pools = []
        for addr, info in data.items():
            pools.append({
                "address": addr.lower(),
                "token0": info["token0"].lower(),
                "token1": info["token1"].lower(),
                "protocol": info.get("protocol", "unknown"),
                "fee": info.get("fee"),
                "symbol0": info.get("symbol0"),
                "symbol1": info.get("symbol1"),
            })
        return pools, None
    else:
        print(f"Error: unrecognized JSON format in {path}")
        sys.exit(1)


def classify_pool(protocol):
    """Return 'v2' or 'v3' based on protocol name."""
    if protocol in V2_PROTOCOLS:
        return "v2"
    elif protocol in V3_PROTOCOLS:
        return "v3"
    return "other"


def enumerate_cycles(pools, weth):
    """Find all 2-hop WETH cycles and classify them.

    A cycle is: WETH --(pool_A)--> TokenX --(pool_B)--> WETH
    where pool_A != pool_B and both pools touch WETH and TokenX.
    """

    # Group pools by the "other" token (non-WETH side)
    # Each pool must have WETH on one side
    by_token = defaultdict(list)
    weth_pools = 0
    non_weth_pools = 0

    for p in pools:
        t0, t1 = p["token0"], p["token1"]
        if t0 == weth:
            other = t1
        elif t1 == weth:
            other = t0
        else:
            non_weth_pools += 1
            continue
        weth_pools += 1
        by_token[other].append(p)

    print(f"  WETH pools: {weth_pools}")
    print(f"  Non-WETH pools (skipped): {non_weth_pools}")
    print(f"  Unique tokens paired with WETH: {len(by_token)}")

    # For each token with 2+ pools, enumerate all pairs
    cycle_types = defaultdict(list)
    all_cycles = []

    for token, token_pools in by_token.items():
        if len(token_pools) < 2:
            continue

        for pool_a, pool_b in combinations(token_pools, 2):
            type_a = classify_pool(pool_a["protocol"])
            type_b = classify_pool(pool_b["protocol"])

            if "other" in (type_a, type_b):
                continue

            # Classify the cycle
            pair = tuple(sorted([type_a, type_b]))

            if pair == ("v2", "v3"):
                cycle_type = "V2-V3"
            elif pair == ("v2", "v2"):
                # Check if different DEXes
                if pool_a["protocol"] != pool_b["protocol"]:
                    cycle_type = "V2-V2 (cross-DEX)"
                else:
                    cycle_type = "V2-V2 (same DEX)"
            elif pair == ("v3", "v3"):
                fee_a = pool_a.get("fee")
                fee_b = pool_b.get("fee")
                if fee_a is not None and fee_b is not None and fee_a != fee_b:
                    cycle_type = "V3-V3 (diff fee)"
                else:
                    cycle_type = "V3-V3 (same fee)"
            else:
                cycle_type = f"{type_a}-{type_b}"

            cycle = {
                "token": token,
                "pool_a": pool_a,
                "pool_b": pool_b,
                "type": cycle_type,
            }
            cycle_types[cycle_type].append(cycle)
            all_cycles.append(cycle)

    return cycle_types, by_token, all_cycles


def get_symbol(pool, token, weth):
    """Get the symbol for a token from pool metadata."""
    if pool["token0"] == token:
        return pool.get("symbol0") or token[:10]
    elif pool["token1"] == token:
        return pool.get("symbol1") or token[:10]
    return token[:10]


def main():
    parser = argparse.ArgumentParser(description="Enumerate V2-V3 arbitrage cycles")
    parser.add_argument("--pools", default=None,
                        help="Path to pool_tokens JSON file")
    parser.add_argument("--chain", default=None,
                        help="Chain name (ethereum, base, arbitrum)")
    args = parser.parse_args()

    # Resolve pool file
    data_dir = os.path.join(os.path.dirname(__file__), "../../data")

    if args.pools:
        pool_path = args.pools
    else:
        # Try Ethereum files in order of preference
        candidates = [
            os.path.join(data_dir, "pool_tokens_full.json"),
            os.path.join(data_dir, "pool_tokens.json"),
            os.path.join(data_dir, "pool_tokens_base.json"),
            os.path.join(data_dir, "pool_tokens_arbitrum.json"),
        ]
        pool_path = None
        for c in candidates:
            if os.path.exists(c):
                pool_path = c
                break
        if not pool_path:
            print("Error: no pool_tokens file found. Searched:")
            for c in candidates:
                print(f"  {c}")
            sys.exit(1)

    print(f"Loading pools from: {pool_path}")
    pools, detected_chain = load_pools(pool_path)
    print(f"  Loaded {len(pools)} pools")

    # Determine chain and WETH address
    chain = args.chain or detected_chain
    if chain and chain in WETH_BY_CHAIN:
        weth = WETH_BY_CHAIN[chain]
        print(f"  Chain: {chain}, WETH: {weth}")
    else:
        # Auto-detect from pool data
        weth = None
        for candidate_chain, candidate_weth in WETH_BY_CHAIN.items():
            count = sum(1 for p in pools
                        if p["token0"] == candidate_weth or p["token1"] == candidate_weth)
            if count > 0:
                if weth is None or count > sum(1 for p in pools
                                                if p["token0"] == weth or p["token1"] == weth):
                    weth = candidate_weth
                    chain = candidate_chain
        if not weth:
            weth = WETH_BY_CHAIN["ethereum"]
            chain = "ethereum"
        print(f"  Auto-detected chain: {chain}, WETH: {weth}")

    # Count by protocol
    proto_counts = defaultdict(int)
    for p in pools:
        proto_counts[p["protocol"]] += 1
    print("\n  Protocol breakdown:")
    for proto, count in sorted(proto_counts.items(), key=lambda x: -x[1]):
        print(f"    {proto}: {count}")

    # Enumerate cycles
    print(f"\n{'='*60}")
    print("ENUMERATING 2-HOP WETH CYCLES")
    print(f"{'='*60}\n")

    cycle_types, by_token, all_cycles = enumerate_cycles(pools, weth)

    # --- Report ---
    print(f"\n{'='*60}")
    print("RESULTS")
    print(f"{'='*60}")

    print(f"\nTotal cycles found: {len(all_cycles)}")
    print(f"\nCycles by type:")
    for ctype in ["V2-V3", "V2-V2 (cross-DEX)", "V2-V2 (same DEX)",
                   "V3-V3 (diff fee)", "V3-V3 (same fee)"]:
        count = len(cycle_types.get(ctype, []))
        if count > 0:
            print(f"  {ctype}: {count}")

    # Any other types we didn't anticipate
    for ctype, cycles in sorted(cycle_types.items()):
        if ctype not in ["V2-V3", "V2-V2 (cross-DEX)", "V2-V2 (same DEX)",
                         "V3-V3 (diff fee)", "V3-V3 (same fee)"]:
            print(f"  {ctype}: {len(cycles)}")

    # Unique tokens involved
    tokens_by_type = defaultdict(set)
    for cycle in all_cycles:
        tokens_by_type[cycle["type"]].add(cycle["token"])

    print(f"\nUnique tokens involved (across all cycle types): "
          f"{len(set(c['token'] for c in all_cycles))}")
    for ctype, tokens in sorted(tokens_by_type.items(), key=lambda x: -len(x[1])):
        print(f"  {ctype}: {len(tokens)} tokens")

    # Tokens with the most V2-V3 pairs
    v2v3_cycles = cycle_types.get("V2-V3", [])
    if v2v3_cycles:
        token_v2v3_count = defaultdict(int)
        for c in v2v3_cycles:
            token_v2v3_count[c["token"]] += 1

        print(f"\n{'='*60}")
        print("TOP TOKENS BY V2-V3 CYCLE COUNT")
        print(f"{'='*60}")

        sorted_tokens = sorted(token_v2v3_count.items(), key=lambda x: -x[1])
        for token, count in sorted_tokens[:30]:
            # Get symbol from any pool that has this token
            symbol = token[:10]
            for p in by_token.get(token, []):
                s = get_symbol(p, token, weth)
                if s and not s.startswith("0x"):
                    symbol = s
                    break
            # Count V2 and V3 pools separately
            v2_pools = [p for p in by_token[token] if classify_pool(p["protocol"]) == "v2"]
            v3_pools = [p for p in by_token[token] if classify_pool(p["protocol"]) == "v3"]
            v3_fees = sorted(set(p.get("fee", "?") for p in v3_pools))
            fee_str = ",".join(str(f) for f in v3_fees)
            print(f"  {symbol:20s} ({token[:10]}...): {count:3d} V2-V3 cycles "
                  f"({len(v2_pools)} V2, {len(v3_pools)} V3 [fees: {fee_str}])")

    # Detailed V3-V3 diff fee analysis
    v3v3_diff = cycle_types.get("V3-V3 (diff fee)", [])
    if v3v3_diff:
        print(f"\n{'='*60}")
        print("TOP TOKENS BY V3-V3 (DIFF FEE) CYCLE COUNT")
        print(f"{'='*60}")

        token_v3v3_count = defaultdict(int)
        for c in v3v3_diff:
            token_v3v3_count[c["token"]] += 1

        sorted_tokens = sorted(token_v3v3_count.items(), key=lambda x: -x[1])
        for token, count in sorted_tokens[:20]:
            symbol = token[:10]
            for p in by_token.get(token, []):
                s = get_symbol(p, token, weth)
                if s and not s.startswith("0x"):
                    symbol = s
                    break
            v3_pools = [p for p in by_token[token] if classify_pool(p["protocol"]) == "v3"]
            fees = sorted(set(p.get("fee", "?") for p in v3_pools))
            print(f"  {symbol:20s}: {count:3d} cycles, fee tiers: {fees}")

    # Summary table: tokens with pools across most protocol types
    print(f"\n{'='*60}")
    print("TOKENS WITH BROADEST PROTOCOL COVERAGE")
    print(f"{'='*60}")

    token_protocols = defaultdict(lambda: defaultdict(int))
    for token, token_pools in by_token.items():
        for p in token_pools:
            key = p["protocol"]
            if p.get("fee"):
                key += f"_{p['fee']}"
            token_protocols[token][key] += 1

    sorted_by_coverage = sorted(token_protocols.items(), key=lambda x: -len(x[1]))
    for token, protos in sorted_by_coverage[:20]:
        symbol = token[:10]
        for p in by_token.get(token, []):
            s = get_symbol(p, token, weth)
            if s and not s.startswith("0x"):
                symbol = s
                break
        total_pools = sum(protos.values())
        proto_str = ", ".join(f"{k}:{v}" for k, v in sorted(protos.items()))
        print(f"  {symbol:20s}: {total_pools} pools across {len(protos)} variants "
              f"({proto_str})")

    # Arb opportunity density
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    arb_eligible_tokens = len([t for t in by_token if len(by_token[t]) >= 2])
    total_tokens = len(by_token)
    print(f"  Total WETH-paired tokens: {total_tokens}")
    print(f"  Arb-eligible tokens (2+ pools): {arb_eligible_tokens}")
    print(f"  Total 2-hop cycles: {len(all_cycles)}")

    if v2v3_cycles:
        print(f"  V2-V3 cycles (primary target): {len(v2v3_cycles)}")
    if v3v3_diff:
        print(f"  V3-V3 diff-fee cycles: {len(v3v3_diff)}")


if __name__ == "__main__":
    main()
