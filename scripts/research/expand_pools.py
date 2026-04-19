#!/usr/bin/env python3
"""
Expand pool universe by querying Uniswap V2 and V3 factory contracts.

Finds all pools containing WETH, resolves token metadata (symbol, decimals),
filters by minimum reserve threshold, and saves to pool_tokens_full.json.

Uses eth_getLogs to scan PairCreated (V2) and PoolCreated (V3) events,
then multicall getReserves/slot0 for current state.
"""

import json
import os
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from web3 import Web3

# Config
WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
MIN_RESERVE_ETH = 1.0  # Minimum WETH in pool to include (filters dust pools)

# Factory addresses
UNISWAP_V2_FACTORY = "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"
SUSHISWAP_FACTORY = "0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac"
UNISWAP_V3_FACTORY = "0x1F98431c8aD98523631AE4a59f267346ea31F984"

# Event signatures (keep as HexBytes for web3.py topic filtering)
PAIR_CREATED_TOPIC = Web3.keccak(text="PairCreated(address,address,address,uint256)")
POOL_CREATED_TOPIC = Web3.keccak(text="PoolCreated(address,address,uint24,int24,address)")

# ABIs (minimal)
ERC20_ABI = json.loads('[{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"type":"function"}]')
PAIR_ABI = json.loads('[{"constant":true,"inputs":[],"name":"getReserves","outputs":[{"name":"","type":"uint112"},{"name":"","type":"uint112"},{"name":"","type":"uint32"}],"type":"function"},{"constant":true,"inputs":[],"name":"token0","outputs":[{"name":"","type":"address"}],"type":"function"},{"constant":true,"inputs":[],"name":"token1","outputs":[{"name":"","type":"address"}],"type":"function"}]')
SLOT0_ABI = json.loads('[{"inputs":[],"name":"slot0","outputs":[{"name":"sqrtPriceX96","type":"uint160"},{"name":"tick","type":"int24"},{"name":"observationIndex","type":"uint16"},{"name":"observationCardinality","type":"uint16"},{"name":"observationCardinalityNext","type":"uint16"},{"name":"feeProtocol","type":"uint8"},{"name":"unlocked","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"liquidity","outputs":[{"name":"","type":"uint128"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"token0","outputs":[{"name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"token1","outputs":[{"name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"fee","outputs":[{"name":"","type":"uint24"}],"stateMutability":"view","type":"function"}]')


def get_rpc():
    """Get RPC URL from environment or .env file."""
    url = os.environ.get("ETH_RPC_HTTP") or os.environ.get("ETH_RPC_URL")
    if not url:
        # Try reading from .env files
        for env_path in [
            os.path.join(os.path.dirname(__file__), "../../.env"),
            "/root/mev/.env",
        ]:
            try:
                with open(env_path) as f:
                    for line in f:
                        line = line.strip()
                        if line.startswith("ETH_RPC_HTTP="):
                            url = line.split("=", 1)[1].strip().strip('"').strip("'")
                            break
                        elif line.startswith("ETH_RPC_URL="):
                            url = line.split("=", 1)[1].strip().strip('"').strip("'")
                if url:
                    break
            except FileNotFoundError:
                continue
    if not url:
        print("Set ETH_RPC_HTTP or ETH_RPC_URL environment variable, or add to .env")
        sys.exit(1)
    return url


def batched_get_logs(w3, params, start_block, end_block, batch_size=50000):
    """Fetch logs in batches to avoid RPC limits."""
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
            # If batch too large, halve it
            if batch_size > 5000:
                half = batch_size // 2
                all_logs.extend(batched_get_logs(w3, params, current, to_block, half))
            else:
                print(f"    Warning: skipping {current}-{to_block}: {e}")
        current = to_block + 1
    return all_logs


def fetch_v2_pools(w3, factory, protocol_name, weth_lower):
    """Fetch all PairCreated events from a V2 factory."""
    print(f"\nScanning {protocol_name} factory {factory}...")

    weth_padded = "0x" + weth_lower[2:].lower().zfill(64)
    latest = w3.eth.block_number

    pools = []

    # Query with WETH as token0
    logs_0 = batched_get_logs(w3, {
        "address": Web3.to_checksum_address(factory),
        "topics": ["0x" + PAIR_CREATED_TOPIC.hex(), weth_padded],
    }, 10000000, latest)
    print(f"  WETH as token0: {len(logs_0)} pools")

    # Query with WETH as token1
    logs_1 = batched_get_logs(w3, {
        "address": Web3.to_checksum_address(factory),
        "topics": ["0x" + PAIR_CREATED_TOPIC.hex(), None, weth_padded],
    }, 10000000, latest)
    print(f"  WETH as token1: {len(logs_1)} pools")

    for log in logs_0 + logs_1:
        # PairCreated(token0, token1, pair, uint)
        # token0 and token1 are indexed (topics[1], topics[2])
        # pair address is in the data (first 32 bytes)
        token0 = "0x" + log["topics"][1].hex()[-40:]
        token1 = "0x" + log["topics"][2].hex()[-40:]
        pair = "0x" + log["data"].hex()[24:64]  # address is bytes 12-32 of first word
        pools.append({
            "address": Web3.to_checksum_address(pair),
            "token0": Web3.to_checksum_address(token0),
            "token1": Web3.to_checksum_address(token1),
            "protocol": protocol_name,
        })

    # Deduplicate
    seen = set()
    unique = []
    for p in pools:
        if p["address"].lower() not in seen:
            seen.add(p["address"].lower())
            unique.append(p)

    print(f"  Unique WETH pools: {len(unique)}")
    return unique


def fetch_v3_pools(w3, weth_lower):
    """Fetch all PoolCreated events from Uniswap V3 factory."""
    print(f"\nScanning Uniswap V3 factory {UNISWAP_V3_FACTORY}...")

    weth_padded = "0x" + weth_lower[2:].lower().zfill(64)
    latest = w3.eth.block_number

    pools = []

    # WETH as token0
    logs_0 = batched_get_logs(w3, {
        "address": Web3.to_checksum_address(UNISWAP_V3_FACTORY),
        "topics": ["0x" + POOL_CREATED_TOPIC.hex(), weth_padded],
    }, 12369621, latest)
    print(f"  WETH as token0: {len(logs_0)} pools")

    # WETH as token1
    logs_1 = batched_get_logs(w3, {
        "address": Web3.to_checksum_address(UNISWAP_V3_FACTORY),
        "topics": ["0x" + POOL_CREATED_TOPIC.hex(), None, weth_padded],
    }, 12369621, latest)
    print(f"  WETH as token1: {len(logs_1)} pools")

    for log in logs_0 + logs_1:
        token0 = "0x" + log["topics"][1].hex()[-40:]
        token1 = "0x" + log["topics"][2].hex()[-40:]
        # fee is topics[3] for V3
        fee = int(log["topics"][3].hex(), 16)
        # pool address is in the data
        data = log["data"].hex()
        # PoolCreated data: tickSpacing (int24 at word 0), pool (address at word 1)
        pool_addr = "0x" + data[88:128]  # second 32-byte word, last 20 bytes

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
        if p["address"].lower() not in seen:
            seen.add(p["address"].lower())
            unique.append(p)

    print(f"  Unique WETH pools: {len(unique)}")
    return unique


def _resolve_one_token(rpc_url, addr):
    """Resolve symbol/decimals for one token (for use in thread pool)."""
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    try:
        contract = w3.eth.contract(address=Web3.to_checksum_address(addr), abi=ERC20_ABI)
        symbol = contract.functions.symbol().call()
        decimals = contract.functions.decimals().call()
        return addr.lower(), {"symbol": symbol, "decimals": decimals}
    except Exception:
        return addr.lower(), {"symbol": addr[:10], "decimals": 18}


def resolve_token_metadata(rpc_url, tokens):
    """Resolve symbol and decimals for a set of token addresses (parallel)."""
    metadata = {}
    total = len(tokens)
    done = 0
    with ThreadPoolExecutor(max_workers=20) as pool:
        futures = {pool.submit(_resolve_one_token, rpc_url, addr): addr for addr in tokens}
        for future in as_completed(futures):
            addr_lower, meta = future.result()
            metadata[addr_lower] = meta
            done += 1
            if done % 50 == 0:
                print(f"  Resolving tokens: {done}/{total}...", end="\r")
    print(f"  Resolved {len(metadata)} tokens                ")
    return metadata


def _check_pool_state(rpc_url, pool):
    """Check reserves/liquidity for one pool (for use in thread pool)."""
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    addr = pool["address"]
    protocol = pool["protocol"]

    if protocol in ("uniswapv2", "sushiswap"):
        try:
            contract = w3.eth.contract(address=Web3.to_checksum_address(addr), abi=PAIR_ABI)
            r0, r1, _ = contract.functions.getReserves().call()
            return pool, ("v2", r0, r1)
        except Exception:
            return pool, None
    else:
        try:
            contract = w3.eth.contract(address=Web3.to_checksum_address(addr), abi=SLOT0_ABI)
            liq = contract.functions.liquidity().call()
            return pool, ("v3", liq)
        except Exception:
            return pool, None


def main():
    rpc_url = get_rpc()
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    print(f"Connected: block {w3.eth.block_number}")

    weth_lower = WETH.lower()

    # Fetch all WETH pools from factories
    v2_uni = fetch_v2_pools(w3, UNISWAP_V2_FACTORY, "uniswapv2", weth_lower)
    v2_sushi = fetch_v2_pools(w3, SUSHISWAP_FACTORY, "sushiswap", weth_lower)
    v3_pools = fetch_v3_pools(w3, weth_lower)

    all_pools = v2_uni + v2_sushi + v3_pools
    print(f"\nTotal WETH pools found: {len(all_pools)}")

    # --- Phase 1: Pre-filter by arb eligibility ---
    # Only keep pools whose "other token" appears in 2+ pools (arb-eligible)
    # This cuts 500K pools down to maybe 50K before we do expensive RPC checks
    from collections import defaultdict
    by_other_raw = defaultdict(list)
    for p in all_pools:
        t0, t1 = p["token0"].lower(), p["token1"].lower()
        other = t1 if t0 == weth_lower else t0
        by_other_raw[other].append(p)

    arb_eligible_pools = []
    for other_token, pools in by_other_raw.items():
        if len(pools) >= 2:
            arb_eligible_pools.extend(pools)

    print(f"Arb-eligible pools (other_token in 2+ pools): {len(arb_eligible_pools)}")
    print(f"  (Skipping {len(all_pools) - len(arb_eligible_pools)} single-pool tokens)")

    # --- Phase 2: Collect unique non-WETH tokens (only from arb-eligible) ---
    tokens = set()
    for p in arb_eligible_pools:
        t0, t1 = p["token0"].lower(), p["token1"].lower()
        if t0 != weth_lower:
            tokens.add(t0)
        if t1 != weth_lower:
            tokens.add(t1)

    print(f"Unique non-WETH tokens to resolve: {len(tokens)}")

    # --- Phase 3: Check reserves/liquidity (parallel) ---
    print("\nChecking reserves/liquidity (filtering by min WETH reserve)...")
    result_pools = []
    checked = 0
    kept = 0
    total = len(arb_eligible_pools)

    with ThreadPoolExecutor(max_workers=30) as pool_executor:
        futures = {pool_executor.submit(_check_pool_state, rpc_url, p): p for p in arb_eligible_pools}
        for future in as_completed(futures):
            pool_info, state = future.result()
            checked += 1
            if checked % 500 == 0:
                print(f"  Checked {checked}/{total}, kept {kept}...", flush=True)

            if state is None:
                continue

            t0 = pool_info["token0"].lower()
            t1 = pool_info["token1"].lower()

            if state[0] == "v2":
                _, r0, r1 = state
                weth_reserve = r0 if t0 == weth_lower else r1
                weth_eth = weth_reserve / 1e18
                if weth_eth < MIN_RESERVE_ETH:
                    continue
            else:
                _, liq = state
                if liq == 0:
                    continue

            kept += 1
            result_pools.append(pool_info)

    print(f"\nPools passing reserve filter: {kept} (from {total} arb-eligible)")

    # --- Phase 4: Resolve token metadata (parallel, only for kept pools) ---
    kept_tokens = set()
    for p in result_pools:
        t0, t1 = p["token0"].lower(), p["token1"].lower()
        if t0 != weth_lower:
            kept_tokens.add(t0)
        if t1 != weth_lower:
            kept_tokens.add(t1)

    print(f"\nResolving metadata for {len(kept_tokens)} tokens...")
    token_meta = {weth_lower: {"symbol": "WETH", "decimals": 18}}
    token_meta.update(resolve_token_metadata(rpc_url, list(kept_tokens)))

    # --- Phase 5: Build result ---
    result = {}
    for pool_info in result_pools:
        addr = pool_info["address"].lower()
        t0 = pool_info["token0"].lower()
        t1 = pool_info["token1"].lower()
        m0 = token_meta.get(t0, {"symbol": t0[:10], "decimals": 18})
        m1 = token_meta.get(t1, {"symbol": t1[:10], "decimals": 18})

        entry = {
            "token0": t0,
            "token1": t1,
            "symbol0": m0["symbol"],
            "symbol1": m1["symbol"],
            "decimals0": m0["decimals"],
            "decimals1": m1["decimals"],
            "protocol": pool_info["protocol"],
        }
        if "fee" in pool_info:
            entry["fee"] = pool_info["fee"]
        result[addr] = entry

    print(f"\nFinal pool count: {len(result)} (from {len(all_pools)} total)")

    # Stats
    v2_count = sum(1 for v in result.values() if v["protocol"] in ("uniswapv2", "sushiswap"))
    v3_count = sum(1 for v in result.values() if v["protocol"] == "uniswapv3")
    print(f"  V2 (Uniswap + Sushi): {v2_count}")
    print(f"  V3: {v3_count}")

    # Count arb-eligible pairs (same other_token, different pool)
    by_other = defaultdict(list)
    for addr, info in result.items():
        other = info["token1"] if info["token0"] == weth_lower else info["token0"]
        by_other[other].append(addr)

    arb_eligible = sum(1 for pools in by_other.values() if len(pools) >= 2)
    arb_pairs = sum(len(p) * (len(p) - 1) // 2 for p in by_other.values() if len(p) >= 2)
    print(f"  Tokens with 2+ pools (arb-eligible): {arb_eligible}")
    print(f"  Total arb pairs: {arb_pairs}")

    # Save
    out_path = "/root/mev/data/pool_tokens_full.json"
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"\nSaved to {out_path}")

    # Also save locally if running from dev machine
    local_path = os.path.join(os.path.dirname(__file__), "../../data/pool_tokens_full.json")
    try:
        with open(local_path, "w") as f:
            json.dump(result, f, indent=2)
        print(f"Also saved to {local_path}")
    except Exception:
        pass


if __name__ == "__main__":
    main()
