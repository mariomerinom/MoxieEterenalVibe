#!/usr/bin/env python3
"""Quick script to scan only Arbitrum V3 factory, then merge with V2 results and save."""
import json, os, sys, time
from web3 import Web3
import eth_abi

# Config
WETH = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
V3_FACTORY = "0x1F98431c8aD98523631AE4a59f267346ea31F984"
START_BLOCK = 300000
MIN_RESERVE_ETH = 0.5
MULTICALL3 = "0xcA11bde05977b3631167028862bE2a173976CA11"
OUT_FILE = "/root/mev/data/pool_tokens_arbitrum.json"

rpc = os.environ.get("ARB_RPC_HTTP", "https://arb1.arbitrum.io/rpc")
w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 60}))
print(f"Connected: block {w3.eth.block_number:,}")

weth_lower = WETH.lower()
weth_padded = "0x" + weth_lower[2:].zfill(64)
topic0 = Web3.keccak(text="PoolCreated(address,address,uint24,int24,address)")
latest = w3.eth.block_number

def batched_get_logs(params, start, end, batch_size=2000000):
    all_logs = []
    current = start
    total = end - start
    while current <= end:
        to_block = min(current + batch_size - 1, end)
        p = dict(params)
        p["fromBlock"] = current
        p["toBlock"] = to_block
        try:
            logs = w3.eth.get_logs(p)
            all_logs.extend(logs)
        except Exception as e:
            if batch_size > 50000:
                half = batch_size // 2
                all_logs.extend(batched_get_logs(params, current, to_block, half))
            else:
                print(f"  Warning: skip {current}-{to_block}: {e}")
        current = to_block + 1
        if total > 0:
            pct = int((current - start) * 100 / total)
            if pct % 5 == 0:
                print(f"  {pct}% scanned, {len(all_logs)} events found", flush=True)
    return all_logs

print(f"Scanning V3 PoolCreated events from block {START_BLOCK:,} to {latest:,} ({latest-START_BLOCK:,} blocks)...")

t0 = time.time()
# WETH as token0
logs_0 = batched_get_logs({
    "address": Web3.to_checksum_address(V3_FACTORY),
    "topics": ["0x" + topic0.hex(), weth_padded],
}, START_BLOCK, latest)
print(f"  token0 scan done: {len(logs_0)} events in {time.time()-t0:.1f}s")

t1 = time.time()
# WETH as token1
logs_1 = batched_get_logs({
    "address": Web3.to_checksum_address(V3_FACTORY),
    "topics": ["0x" + topic0.hex(), None, weth_padded],
}, START_BLOCK, latest)
print(f"  token1 scan done: {len(logs_1)} events in {time.time()-t1:.1f}s")

v3_pools = []
for log in logs_0 + logs_1:
    token0 = "0x" + log["topics"][1].hex()[-40:]
    token1 = "0x" + log["topics"][2].hex()[-40:]
    fee = int(log["topics"][3].hex(), 16)
    data = log["data"].hex()
    pool_addr = "0x" + data[88:128]
    v3_pools.append({
        "address": Web3.to_checksum_address(pool_addr),
        "token0": Web3.to_checksum_address(token0),
        "token1": Web3.to_checksum_address(token1),
        "protocol": "uniswapv3",
        "fee": fee,
    })

# Dedup
seen = set()
unique_v3 = []
for p in v3_pools:
    k = p["address"].lower()
    if k not in seen:
        seen.add(k)
        unique_v3.append(p)

print(f"V3 pools with WETH: {len(unique_v3)}")

# Now load V2 results from the previous partial run
# We need to reconstruct from the log or re-scan V2
# Actually let me just re-run the full script with the V2 data cached
# For now, save V3 results and merge
v3_file = "/root/mev/data/arb_v3_pools.json"
with open(v3_file, "w") as f:
    json.dump(unique_v3, f)
print(f"V3 pools saved to {v3_file}")

# Now run full merge - scan V2 again (fast with Multicall3) and combine
print("\nRe-scanning V2 factories (fast with Multicall3)...")
SEL_ALL_PAIRS_LENGTH = Web3.keccak(text="allPairsLength()")[:4]
SEL_ALL_PAIRS = Web3.keccak(text="allPairs(uint256)")[:4]
SEL_TOKEN0 = Web3.keccak(text="token0()")[:4]
SEL_TOKEN1 = Web3.keccak(text="token1()")[:4]
SEL_ALL_POOLS_LENGTH = Web3.keccak(text="allPoolsLength()")[:4]
SEL_ALL_POOLS = Web3.keccak(text="allPools(uint256)")[:4]
SEL_AGGREGATE3 = Web3.keccak(text="aggregate3((address,bool,bytes)[])")[:4]
SEL_GET_RESERVES = Web3.keccak(text="getReserves()")[:4]
SEL_SYMBOL = Web3.keccak(text="symbol()")[:4]
SEL_DECIMALS = Web3.keccak(text="decimals()")[:4]
SEL_LIQUIDITY = Web3.keccak(text="liquidity()")[:4]

MAX_PAIRS = 200000

def multicall3_batch(calls, batch_size=500):
    results = []
    for batch_start in range(0, len(calls), batch_size):
        batch = calls[batch_start:batch_start + batch_size]
        encoded_calls = []
        for target, calldata in batch:
            encoded_calls.append((Web3.to_checksum_address(target), True, calldata))
        agg_data = SEL_AGGREGATE3 + eth_abi.encode(
            ["(address,bool,bytes)[]"], [encoded_calls]
        )
        try:
            raw = w3.eth.call({"to": Web3.to_checksum_address(MULTICALL3), "data": "0x" + agg_data.hex()})
            decoded = eth_abi.decode(["(bool,bytes)[]"], raw)[0]
            results.extend(decoded)
        except Exception as e:
            if batch_size > 50:
                half = batch_size // 2
                sub = multicall3_batch(batch, half)
                results.extend(sub)
            else:
                for _ in batch:
                    results.append((False, b""))
    return results

V2_FACTORIES = [
    {"name": "uniswapv2", "address": "0xf1D7CC64Fb4452F05c498126312eBE29f30Fbcf9"},
    {"name": "sushiswap", "address": "0xc35DADB65012eC5796536bD9864eD8773aBc74C4"},
    {"name": "camelot", "address": "0x6EcCab422D763aC031210895C81787E87B43A652"},
]

all_v2_pools = []
for fac in V2_FACTORIES:
    addr = fac["address"]
    name = fac["name"]
    
    # Get total pairs
    total_pairs = 0
    pair_sel = SEL_ALL_PAIRS
    for length_sel, p_sel, label in [
        (SEL_ALL_PAIRS_LENGTH, SEL_ALL_PAIRS, "allPairs"),
        (SEL_ALL_POOLS_LENGTH, SEL_ALL_POOLS, "allPools"),
    ]:
        try:
            raw = w3.eth.call({"to": Web3.to_checksum_address(addr), "data": "0x" + length_sel.hex()})
            total_pairs = int.from_bytes(raw, "big")
            pair_sel = p_sel
            print(f"  {name}: {total_pairs:,} total pairs")
            break
        except:
            continue
    
    if total_pairs == 0:
        continue
    
    start_idx = max(0, total_pairs - MAX_PAIRS)
    count = total_pairs - start_idx
    
    # Fetch pair addresses
    pair_calls = [(addr, pair_sel + eth_abi.encode(["uint256"], [i])) for i in range(start_idx, total_pairs)]
    t0 = time.time()
    pair_results = multicall3_batch(pair_calls)
    pair_addrs = []
    for ok, data in pair_results:
        if ok and len(data) >= 32:
            pair_addrs.append("0x" + data[-20:].hex())
    print(f"    Got {len(pair_addrs):,} pairs in {time.time()-t0:.1f}s")
    
    # Fetch token0/token1
    token_calls = []
    for pa in pair_addrs:
        token_calls.append((pa, SEL_TOKEN0))
        token_calls.append((pa, SEL_TOKEN1))
    
    t0 = time.time()
    token_results = multicall3_batch(token_calls)
    
    for i in range(0, len(token_results), 2):
        ok0, d0 = token_results[i]
        ok1, d1 = token_results[i + 1] if i + 1 < len(token_results) else (False, b"")
        if ok0 and ok1 and len(d0) >= 32 and len(d1) >= 32:
            t0_addr = Web3.to_checksum_address("0x" + d0[-20:].hex())
            t1_addr = Web3.to_checksum_address("0x" + d1[-20:].hex())
            if t0_addr.lower() == weth_lower or t1_addr.lower() == weth_lower:
                pa = pair_addrs[i // 2]
                all_v2_pools.append({
                    "address": Web3.to_checksum_address(pa),
                    "token0": t0_addr,
                    "token1": t1_addr,
                    "protocol": name,
                })
    print(f"    WETH pools: {len([p for p in all_v2_pools if p['protocol'] == name]):,} (resolved in {time.time()-t0:.1f}s)")

print(f"\nTotal V2 WETH pools: {len(all_v2_pools):,}")
print(f"Total V3 WETH pools: {len(unique_v3):,}")

# Merge and dedup
all_pools = all_v2_pools + unique_v3
seen = set()
deduped = []
for p in all_pools:
    k = p["address"].lower()
    if k not in seen:
        seen.add(k)
        deduped.append(p)

print(f"Total unique WETH pools: {len(deduped):,}")

# Filter arb-eligible: need 2+ pools per non-WETH token
from collections import Counter
token_counts = Counter()
for p in deduped:
    t0_addr = p["token0"].lower()
    t1_addr = p["token1"].lower()
    other = t1_addr if t0_addr == weth_lower else t0_addr
    token_counts[other] += 1

arb_eligible = [t for t, c in token_counts.items() if c >= 2]
print(f"Arb-eligible tokens: {len(arb_eligible)}")

eligible_set = set(arb_eligible)
filtered = []
for p in deduped:
    t0_addr = p["token0"].lower()
    t1_addr = p["token1"].lower()
    other = t1_addr if t0_addr == weth_lower else t0_addr
    if other in eligible_set:
        filtered.append(p)

print(f"Arb-eligible pools: {len(filtered)}")

# Check reserves
print("\nChecking reserves...")
reserve_calls = []
for p in filtered:
    if p.get("protocol", "").startswith("uniswapv3") or p.get("fee"):
        reserve_calls.append((p["address"], SEL_LIQUIDITY))
    else:
        reserve_calls.append((p["address"], SEL_GET_RESERVES))

t0 = time.time()
reserve_results = multicall3_batch(reserve_calls)

active = []
for i, (ok, data) in enumerate(reserve_results):
    p = filtered[i]
    if not ok or len(data) < 32:
        continue
    if p.get("fee"):
        # V3: check non-zero liquidity
        liq = int.from_bytes(data[:32], "big")
        if liq > 0:
            active.append(p)
    else:
        # V2: check WETH reserve >= min
        if len(data) >= 64:
            r0 = int.from_bytes(data[:32], "big")
            r1 = int.from_bytes(data[32:64], "big")
            t0_addr = p["token0"].lower()
            weth_reserve = r0 if t0_addr == weth_lower else r1
            if weth_reserve >= int(MIN_RESERVE_ETH * 1e18):
                active.append(p)

print(f"Active pools with reserves: {len(active)} (checked in {time.time()-t0:.1f}s)")

# Resolve token metadata
print("Resolving token metadata...")
tokens_needed = set()
for p in active:
    t0_addr = p["token0"].lower()
    t1_addr = p["token1"].lower()
    other = t1_addr if t0_addr == weth_lower else t0_addr
    tokens_needed.add(other)

meta_calls = []
token_list = sorted(tokens_needed)
for t in token_list:
    meta_calls.append((t, SEL_SYMBOL))
    meta_calls.append((t, SEL_DECIMALS))

t0 = time.time()
meta_results = multicall3_batch(meta_calls)

token_meta = {}
for i in range(0, len(meta_results), 2):
    t = token_list[i // 2]
    ok_s, d_s = meta_results[i]
    ok_d, d_d = meta_results[i + 1] if i + 1 < len(meta_results) else (False, b"")
    
    symbol = "UNKNOWN"
    decimals = 18
    if ok_s and len(d_s) > 0:
        try:
            symbol = eth_abi.decode(["string"], d_s)[0]
        except:
            try:
                symbol = d_s.rstrip(b"\x00").decode("utf-8", errors="replace")
            except:
                symbol = "UNKNOWN"
    if ok_d and len(d_d) >= 32:
        decimals = int.from_bytes(d_d[:32], "big")
    
    token_meta[t] = {"symbol": symbol, "decimals": decimals}

print(f"Resolved {len(token_meta)} tokens in {time.time()-t0:.1f}s")

# Build output
weth_meta = {"symbol": "WETH", "decimals": 18, "address": WETH}
output = {"chain": "arbitrum", "weth": WETH, "pools": [], "tokens": {WETH: weth_meta}}

for p in active:
    t0_addr = p["token0"].lower()
    t1_addr = p["token1"].lower()
    other = t1_addr if t0_addr == weth_lower else t0_addr
    other_cs = Web3.to_checksum_address(other)
    
    meta = token_meta.get(other, {"symbol": "UNKNOWN", "decimals": 18})
    
    pool_entry = {
        "address": p["address"],
        "token0": p["token0"],
        "token1": p["token1"],
        "protocol": p.get("protocol", "unknown"),
    }
    if p.get("fee"):
        pool_entry["fee"] = p["fee"]
    
    output["pools"].append(pool_entry)
    
    if other_cs not in output["tokens"]:
        output["tokens"][other_cs] = {
            "symbol": meta["symbol"],
            "decimals": meta["decimals"],
            "address": other_cs,
        }

with open(OUT_FILE, "w") as f:
    json.dump(output, f, indent=2)

print(f"\n=== SUMMARY ===")
print(f"Total pools: {len(output[pools])}")
print(f"Tokens: {len(output[tokens])}")

# Count arb pairs
pair_count = 0
for t, c in token_counts.items():
    if t in eligible_set:
        pair_count += c * (c - 1) // 2
print(f"Arb pairs: {pair_count}")
print(f"Saved to {OUT_FILE}")
