#!/usr/bin/env python3
"""
Multi-chain pool expansion using Multicall3 for fast batch queries.

Uses Multicall3 to batch allPairs + token0/token1 calls (500 per RPC call).
This turns 3M individual RPC calls into ~6K batch calls.
V3 factories use PoolCreated event logs (much fewer events than V2).
"""
import json
import os
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from web3 import Web3
from eth_abi import encode, decode

# Multicall3 — same address on all EVM chains
MULTICALL3 = "0xcA11bde05977b3631167028862bE2a173976CA11"

# ===== Chain configurations =====

CHAINS = {
    "base": {
        "weth": "0x4200000000000000000000000000000000000006",
        "rpc_env": "BASE_RPC_HTTP",
        "factories": [
            {
                "name": "uniswapv2",
                "address": "0x8909Dc15e40173Ff4699343b6eB8132c65e18eC6",
                "type": "v2",
            },
            {
                "name": "aerodrome",
                "address": "0x420DD381b31aEf6683db6B902084cB0FFECe40Da",
                "type": "v2",
            },
            {
                "name": "uniswapv3",
                "address": "0x33128a8fC17869897dcE68Ed026d694621f6FDfD",
                "type": "v3",
                "start_block": 2000000,
            },
        ],
        "min_reserve_eth": 0.5,
        "out_file": "pool_tokens_base.json",
    },
    "arbitrum": {
        "weth": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "rpc_env": "ARB_RPC_HTTP",
        "factories": [
            {
                "name": "uniswapv2",
                "address": "0xf1D7CC64Fb4452F05c498126312eBE29f30Fbcf9",
                "type": "v2",
            },
            {
                "name": "sushiswap",
                "address": "0xc35DADB65012eC5796536bD9864eD8773aBc74C4",
                "type": "v2",
            },
            {
                "name": "camelot",
                "address": "0x6EcCab422D763aC031210895C81787E87B43A652",
                "type": "v2",
            },
            {
                "name": "uniswapv3",
                "address": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
                "type": "v3",
                "start_block": 300000,
            },
        ],
        "min_reserve_eth": 0.5,
        "out_file": "pool_tokens_arbitrum.json",
    },
}

# Function selectors
SEL_ALL_PAIRS_LENGTH = Web3.keccak(text="allPairsLength()")[:4]  # 0x574f2ba3
SEL_ALL_PAIRS = Web3.keccak(text="allPairs(uint256)")[:4]  # 0x1e3dd18b
SEL_TOKEN0 = Web3.keccak(text="token0()")[:4]  # 0x0dfe1681
SEL_TOKEN1 = Web3.keccak(text="token1()")[:4]  # 0xd21220a7
SEL_GET_RESERVES = Web3.keccak(text="getReserves()")[:4]  # 0x0902f1ac
SEL_LIQUIDITY = Web3.keccak(text="liquidity()")[:4]  # 0x1a686502
SEL_SYMBOL = Web3.keccak(text="symbol()")[:4]  # 0x95d89b41
SEL_DECIMALS = Web3.keccak(text="decimals()")[:4]  # 0x313ce567

# Solidly/Aerodrome alternative selectors
SEL_ALL_POOLS_LENGTH = Web3.keccak(text="allPoolsLength()")[:4]
SEL_ALL_POOLS = Web3.keccak(text="allPools(uint256)")[:4]

# Multicall3 aggregate3 selector
SEL_AGGREGATE3 = Web3.keccak(text="aggregate3((address,bool,bytes)[])")[:4]  # 0x82ad56cb


def get_rpc(chain_config):
    """Get RPC URL from env or .env file."""
    env_key = chain_config["rpc_env"]
    url = os.environ.get(env_key)
    if not url:
        for env_path in ["/root/mev/.env", os.path.join(os.path.dirname(__file__), "../../.env")]:
            try:
                with open(env_path) as f:
                    for line in f:
                        line = line.strip()
                        if line.startswith(f"{env_key}="):
                            url = line.split("=", 1)[1].strip().strip('"').strip("'")
                            break
                if url:
                    break
            except FileNotFoundError:
                continue
    if not url:
        print(f"  WARNING: {env_key} not set, skipping chain")
        return None
    return url


def multicall3_batch(w3, calls, batch_size=500):
    """
    Execute batched calls via Multicall3.aggregate3.
    calls: list of (target_address, calldata_bytes)
    Returns: list of (success: bool, return_data: bytes) in same order.
    """
    mc_addr = Web3.to_checksum_address(MULTICALL3)
    results = []

    for batch_start in range(0, len(calls), batch_size):
        batch = calls[batch_start:batch_start + batch_size]

        # Build aggregate3 calldata
        # aggregate3((address target, bool allowFailure, bytes callData)[])
        call_structs = []
        for target, calldata in batch:
            call_structs.append((Web3.to_checksum_address(target), True, calldata))

        encoded_args = encode(
            ["(address,bool,bytes)[]"],
            [call_structs]
        )
        full_calldata = SEL_AGGREGATE3 + encoded_args

        try:
            raw = w3.eth.call({"to": mc_addr, "data": "0x" + full_calldata.hex()})
            # Decode: returns (bool success, bytes returnData)[]
            decoded = decode(["(bool,bytes)[]"], raw)[0]
            for success, ret_data in decoded:
                results.append((success, ret_data))
        except Exception as e:
            # If batch fails, try smaller batches
            if batch_size > 50:
                half = batch_size // 2
                sub_results = multicall3_batch(w3, batch, half)
                results.extend(sub_results)
            else:
                # Mark all as failed
                for _ in batch:
                    results.append((False, b""))

    return results


def fetch_v2_pools_multicall(w3, factory_config, weth_lower):
    """Fetch V2 WETH pools using Multicall3 batching."""
    factory_addr = factory_config["address"]
    name = factory_config["name"]

    print(f"  Scanning {name} factory {factory_addr}...")

    # Step 1: Get total pair count - try allPairsLength, then allPoolsLength (Solidly forks)
    total_pairs = None
    pair_selector = SEL_ALL_PAIRS

    for length_sel, pair_sel, label in [
        (SEL_ALL_PAIRS_LENGTH, SEL_ALL_PAIRS, "allPairs"),
        (SEL_ALL_POOLS_LENGTH, SEL_ALL_POOLS, "allPools"),
    ]:
        try:
            raw = w3.eth.call({
                "to": Web3.to_checksum_address(factory_addr),
                "data": "0x" + length_sel.hex()
            })
            total_pairs = int.from_bytes(raw, "big")
            pair_selector = pair_sel
            print(f"    Using {label} interface")
            break
        except Exception:
            continue

    if total_pairs is None:
        print(f"    ERROR: Factory doesn't support allPairsLength or allPoolsLength. Skipping.")
        return []

    print(f"    Total pairs: {total_pairs:,}")

    # Cap enumeration for factories with millions of pairs (mostly dead meme tokens)
    MAX_PAIRS = 200000  # Only enumerate last 200K pairs; older ones are dead meme tokens
    if total_pairs > MAX_PAIRS:
        start_idx = total_pairs - MAX_PAIRS
        print(f"    Capping to last {MAX_PAIRS:,} pairs (index {start_idx:,} to {total_pairs:,})")
    else:
        start_idx = 0

    # Step 2: Fetch pair addresses via multicall (500 per RPC call)
    enum_count = total_pairs - start_idx
    print(f"    Fetching {enum_count:,} pair addresses via multicall3...")
    t0 = time.time()

    pair_calls = []
    for i in range(start_idx, total_pairs):
        calldata = pair_selector + encode(["uint256"], [i])
        pair_calls.append((factory_addr, calldata))

    pair_results = multicall3_batch(w3, pair_calls, batch_size=500)
    pair_addresses = []
    for success, data in pair_results:
        if success and len(data) >= 32:
            addr = "0x" + data[-20:].hex()
            pair_addresses.append(addr)

    elapsed = time.time() - t0
    print(f"    Got {len(pair_addresses):,} pair addresses in {elapsed:.1f}s")

    # Step 3: Fetch token0+token1 for all pairs via multicall
    print(f"    Fetching token0/token1 via multicall3...")
    t0 = time.time()

    token_calls = []
    for addr in pair_addresses:
        token_calls.append((addr, SEL_TOKEN0))
        token_calls.append((addr, SEL_TOKEN1))

    token_results = multicall3_batch(w3, token_calls, batch_size=500)

    weth_pools = []
    for i in range(0, len(token_results), 2):
        if i + 1 >= len(token_results):
            break
        s0, d0 = token_results[i]
        s1, d1 = token_results[i + 1]
        if not s0 or not s1 or len(d0) < 20 or len(d1) < 20:
            continue

        t0_addr = "0x" + d0[-20:].hex()
        t1_addr = "0x" + d1[-20:].hex()
        pair_addr = pair_addresses[i // 2]

        if t0_addr.lower() == weth_lower or t1_addr.lower() == weth_lower:
            weth_pools.append({
                "address": Web3.to_checksum_address(pair_addr),
                "token0": Web3.to_checksum_address(t0_addr),
                "token1": Web3.to_checksum_address(t1_addr),
                "protocol": name,
            })

    elapsed = time.time() - t0
    print(f"    WETH pools: {len(weth_pools):,} (resolved in {elapsed:.1f}s)")
    return weth_pools


def batched_get_logs(w3, params, start_block, end_block, batch_size=100000):
    """Fetch logs in batches."""
    all_logs = []
    current = start_block
    while current <= end_block:
        to_block = min(current + batch_size - 1, end_block)
        p = dict(params)
        p["fromBlock"] = current
        p["toBlock"] = to_block
        try:
            logs = w3.eth.get_logs(p)
            all_logs.extend(logs)
        except Exception as e:
            if batch_size > 5000:
                half = batch_size // 2
                all_logs.extend(batched_get_logs(w3, params, current, to_block, half))
            else:
                print(f"    Warning: skipping {current}-{to_block}: {e}")
        current = to_block + 1
    return all_logs


def fetch_v3_pools(w3, factory_config, weth_lower, latest_block):
    """Fetch PoolCreated events from a V3 factory."""
    factory = factory_config["address"]
    start = factory_config.get("start_block", 0)

    topic0 = Web3.keccak(text="PoolCreated(address,address,uint24,int24,address)")
    weth_padded = "0x" + weth_lower[2:].lower().zfill(64)

    print(f"  Scanning V3 factory {factory}...")

    logs_0 = batched_get_logs(w3, {
        "address": Web3.to_checksum_address(factory),
        "topics": ["0x" + topic0.hex(), weth_padded],
    }, start, latest_block)

    logs_1 = batched_get_logs(w3, {
        "address": Web3.to_checksum_address(factory),
        "topics": ["0x" + topic0.hex(), None, weth_padded],
    }, start, latest_block)

    print(f"    token0: {len(logs_0)}, token1: {len(logs_1)}")

    pools = []
    for log in logs_0 + logs_1:
        token0 = "0x" + log["topics"][1].hex()[-40:]
        token1 = "0x" + log["topics"][2].hex()[-40:]
        fee = int(log["topics"][3].hex(), 16)
        data = log["data"].hex()
        # V3 PoolCreated data: [int24 tickSpacing (32B)] [address pool (32B)]
        pool_addr = "0x" + data[88:128]

        pools.append({
            "address": Web3.to_checksum_address(pool_addr),
            "token0": Web3.to_checksum_address(token0),
            "token1": Web3.to_checksum_address(token1),
            "protocol": "uniswapv3",
            "fee": fee,
        })

    seen = set()
    unique = []
    for p in pools:
        key = p["address"].lower()
        if key not in seen:
            seen.add(key)
            unique.append(p)

    print(f"    Unique V3 pools: {len(unique)}")
    return unique


def check_reserves_multicall(w3, pools, weth_lower, min_reserve_eth):
    """Check reserves/liquidity for pools using multicall3."""
    print(f"\nChecking reserves ({len(pools)} pools) via multicall3...")
    t0 = time.time()

    reserve_calls = []
    for p in pools:
        if p["protocol"] == "uniswapv3":
            reserve_calls.append((p["address"], SEL_LIQUIDITY))
        else:
            reserve_calls.append((p["address"], SEL_GET_RESERVES))

    results = multicall3_batch(w3, reserve_calls, batch_size=500)

    kept = []
    for i, (success, data) in enumerate(results):
        if not success:
            continue
        p = pools[i]
        t0_addr = p["token0"].lower()

        if p["protocol"] == "uniswapv3":
            if len(data) < 16:
                continue
            liq = int.from_bytes(data[:32], "big") if len(data) >= 32 else int.from_bytes(data, "big")
            if liq > 0:
                kept.append(p)
        else:
            if len(data) < 64:
                continue
            r0 = int.from_bytes(data[0:32], "big")
            r1 = int.from_bytes(data[32:64], "big")
            weth_reserve = r0 if t0_addr == weth_lower else r1
            if weth_reserve / 1e18 >= min_reserve_eth:
                kept.append(p)

    elapsed = time.time() - t0
    print(f"  Passed: {len(kept)} ({elapsed:.1f}s)")
    return kept


def resolve_tokens_multicall(w3, token_addrs, weth_lower):
    """Resolve token symbols and decimals via multicall3."""
    print(f"Resolving {len(token_addrs)} token metadata via multicall3...")

    meta = {weth_lower: {"symbol": "WETH", "decimals": 18}}

    addrs = [a for a in token_addrs if a != weth_lower]
    if not addrs:
        return meta

    # symbol + decimals calls
    calls = []
    for addr in addrs:
        calls.append((addr, SEL_SYMBOL))
        calls.append((addr, SEL_DECIMALS))

    results = multicall3_batch(w3, calls, batch_size=500)

    for i in range(0, len(results), 2):
        if i + 1 >= len(results):
            break
        addr = addrs[i // 2]
        s_ok, s_data = results[i]
        d_ok, d_data = results[i + 1]

        symbol = addr[:10]
        decimals = 18

        if s_ok and len(s_data) > 0:
            try:
                # Try ABI-encoded string first
                if len(s_data) >= 64:
                    decoded = decode(["string"], s_data)
                    symbol = decoded[0]
                elif len(s_data) == 32:
                    # bytes32 format
                    symbol = s_data.rstrip(b'\x00').decode('utf-8', errors='replace')
            except Exception:
                try:
                    symbol = s_data.rstrip(b'\x00').decode('utf-8', errors='replace')
                except Exception:
                    symbol = addr[:10]

        if d_ok and len(d_data) >= 32:
            try:
                decimals = int.from_bytes(d_data[:32], "big")
                if decimals > 100:
                    decimals = 18
            except Exception:
                decimals = 18

        meta[addr] = {"symbol": symbol, "decimals": decimals}

    print(f"  Resolved {len(meta)} tokens")
    return meta


def process_chain(chain_name, config):
    """Process a single chain: scan factories, filter, resolve, save."""
    print(f"\n{'='*60}")
    print(f"CHAIN: {chain_name.upper()}")
    print(f"{'='*60}")

    rpc_url = get_rpc(config)
    if not rpc_url:
        return

    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 120}))
    try:
        latest = w3.eth.block_number
        print(f"Connected: block {latest:,}")
    except Exception as e:
        print(f"  Failed to connect: {e}")
        return

    weth_lower = config["weth"].lower()
    all_pools = []

    for factory in config["factories"]:
        try:
            if factory["type"] == "v3":
                pools = fetch_v3_pools(w3, factory, weth_lower, latest)
            else:
                pools = fetch_v2_pools_multicall(w3, factory, weth_lower)
            all_pools.extend(pools)
        except Exception as e:
            print(f"  ERROR scanning {factory['name']}: {e}")
            continue

    # Dedup across factories
    seen = set()
    unique_pools = []
    for p in all_pools:
        key = p["address"].lower()
        if key not in seen:
            seen.add(key)
            unique_pools.append(p)

    print(f"\nTotal unique WETH pools: {len(unique_pools)}")

    # Pre-filter: arb-eligible only (2+ pools per non-WETH token)
    by_other = defaultdict(list)
    for p in unique_pools:
        t0, t1 = p["token0"].lower(), p["token1"].lower()
        other = t1 if t0 == weth_lower else t0
        by_other[other].append(p)

    arb_eligible = []
    for other, pools in by_other.items():
        if len(pools) >= 2:
            arb_eligible.extend(pools)

    print(f"Arb-eligible: {len(arb_eligible)} (skipping {len(unique_pools) - len(arb_eligible)} single-pool tokens)")

    if not arb_eligible:
        print("No arb-eligible pools. Done.")
        return

    # Check reserves via multicall
    kept_pools = check_reserves_multicall(w3, arb_eligible, weth_lower, config["min_reserve_eth"])

    # Re-filter: arb-eligible after reserve check
    by_other2 = defaultdict(list)
    for p in kept_pools:
        t0, t1 = p["token0"].lower(), p["token1"].lower()
        other = t1 if t0 == weth_lower else t0
        by_other2[other].append(p)

    final_pools = []
    for other, pools in by_other2.items():
        if len(pools) >= 2:
            final_pools.extend(pools)

    print(f"Final arb-eligible after reserve filter: {len(final_pools)}")

    if not final_pools:
        print("No pools survived filtering. Done.")
        return

    # Resolve token metadata via multicall
    tokens = set()
    for p in final_pools:
        t0, t1 = p["token0"].lower(), p["token1"].lower()
        if t0 != weth_lower:
            tokens.add(t0)
        if t1 != weth_lower:
            tokens.add(t1)

    token_meta = resolve_tokens_multicall(w3, tokens, weth_lower)

    # Build result
    result = {}
    for p in final_pools:
        addr = p["address"].lower()
        t0 = p["token0"].lower()
        t1 = p["token1"].lower()
        m0 = token_meta.get(t0, {"symbol": t0[:10], "decimals": 18})
        m1 = token_meta.get(t1, {"symbol": t1[:10], "decimals": 18})

        entry = {
            "token0": t0,
            "token1": t1,
            "symbol0": m0["symbol"],
            "symbol1": m1["symbol"],
            "decimals0": m0["decimals"],
            "decimals1": m1["decimals"],
            "protocol": p["protocol"],
        }
        if "fee" in p:
            entry["fee"] = p["fee"]
        result[addr] = entry

    # Stats
    v2_count = sum(1 for v in result.values() if v["protocol"] != "uniswapv3")
    v3_count = sum(1 for v in result.values() if v["protocol"] == "uniswapv3")

    by_other_final = defaultdict(list)
    for addr, info in result.items():
        other = info["token1"] if info["token0"] == weth_lower else info["token0"]
        by_other_final[other].append(addr)

    arb_tokens = sum(1 for pools in by_other_final.values() if len(pools) >= 2)
    arb_pairs = sum(len(p) * (len(p) - 1) // 2 for p in by_other_final.values() if len(p) >= 2)

    print(f"\n--- {chain_name.upper()} RESULTS ---")
    print(f"Final pools: {len(result)}")
    print(f"  V2-style: {v2_count}")
    print(f"  V3: {v3_count}")
    print(f"  Arb-eligible tokens: {arb_tokens}")
    print(f"  Arb pairs: {arb_pairs}")

    # Save
    out_path = f"/root/mev/data/{config['out_file']}"
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"Saved to {out_path}")

    # Also save locally
    local_path = os.path.join(os.path.dirname(__file__), f"../../data/{config['out_file']}")
    try:
        with open(local_path, "w") as f:
            json.dump(result, f, indent=2)
        print(f"Also saved to {local_path}")
    except Exception:
        pass


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--chain", choices=list(CHAINS.keys()) + ["all"], default="all")
    args = parser.parse_args()

    if args.chain == "all":
        for name, config in CHAINS.items():
            process_chain(name, config)
    else:
        process_chain(args.chain, CHAINS[args.chain])


if __name__ == "__main__":
    main()
