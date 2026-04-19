"""
Token pair resolver: call pool contracts to get token0/token1 addresses.
Then build a pricing table for sandwich/arb P&L estimation.
"""
import json
import urllib.request
import duckdb
import time
import os

ETH_RPC = os.environ.get("ETH_RPC_HTTP", "").strip()
if not ETH_RPC:
    # Read from .env
    with open("/root/mev/.env") as f:
        for line in f:
            if line.startswith("ETH_RPC_HTTP="):
                ETH_RPC = line.strip().split("=", 1)[1]
                break

print(f"RPC: {ETH_RPC[:50]}...")

con = duckdb.connect()

# Get top 200 pools by swap count
top_pools = con.execute("""
    SELECT pool, protocol, count(*) as swaps
    FROM read_parquet('data/events/swaps/ethereum/*.parquet', union_by_name=true)
    GROUP BY pool, protocol
    ORDER BY swaps DESC
    LIMIT 200
""").fetchall()

print(f"Resolving token pairs for top {len(top_pools)} pools...")

# ERC20 token0() and token1() function signatures
# token0(): 0x0dfe1681
# token1(): 0xd21220a7
TOKEN0_SIG = "0x0dfe1681"
TOKEN1_SIG = "0xd21220a7"

def eth_call(to_addr, data):
    """Make an eth_call to get token addresses."""
    payload = json.dumps({
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [{"to": to_addr, "data": data}, "latest"],
        "id": 1
    }).encode()
    
    req = urllib.request.Request(ETH_RPC, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            if "result" in result and result["result"] != "0x":
                # Extract address from 32-byte padded response
                hex_val = result["result"]
                if len(hex_val) >= 42:
                    return "0x" + hex_val[-40:]
            return None
    except Exception as e:
        return None

# Known token addresses -> (symbol, decimals)
KNOWN_TOKENS = {
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": ("WETH", 18),
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": ("USDC", 6),
    "0xdac17f958d2ee523a2206206994597c13d831ec7": ("USDT", 6),
    "0x6b175474e89094c44da98b954eedeac495271d0f": ("DAI", 18),
    "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": ("WBTC", 8),
    "0x514910771af9ca656af840dff83e8264ecf986ca": ("LINK", 18),
    "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984": ("UNI", 18),
    "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9": ("AAVE", 18),
    "0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce": ("SHIB", 18),
    "0x6982508145454ce325ddbe47a25d4ec3d2311933": ("PEPE", 18),
    "0xae7ab96520de3a18e5e111b5eaab095312d7fe84": ("stETH", 18),
    "0xbe9895146f7af43049ca1c1ae358b0541ea49704": ("cbETH", 18),
    "0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0": ("wstETH", 18),
    "0x4d224452801aced8b2f0aebe155379bb5d594381": ("APE", 18),
    "0x853d955acef822db058eb8505911ed77f175b99e": ("FRAX", 18),
    "0x5a98fcbea516cf06857215779fd812ca3bef1b32": ("LDO", 18),
    "0xd533a949740bb3306d119cc777fa900ba034cd52": ("CRV", 18),
    "0x111111111117dc0aa78b770fa6a738034120c302": ("1INCH", 18),
    "0x3845badade8e6dff049820680d1f14bd3903a5d0": ("SAND", 18),
    "0x0d8775f648430679a709e98d2b0cb6250d2887ef": ("BAT", 18),
    "0x4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b": ("CVX", 18),
}

# Batch resolve
resolved = {}
errors = 0
batch_size = 10

for i in range(0, len(top_pools), batch_size):
    batch = top_pools[i:i+batch_size]
    for pool_addr, protocol, swaps in batch:
        t0 = eth_call(pool_addr, TOKEN0_SIG)
        t1 = eth_call(pool_addr, TOKEN1_SIG)
        
        if t0 and t1:
            t0 = t0.lower()
            t1 = t1.lower()
            t0_info = KNOWN_TOKENS.get(t0, (t0[:10], 18))  # default 18 decimals
            t1_info = KNOWN_TOKENS.get(t1, (t1[:10], 18))
            resolved[pool_addr] = {
                "token0": t0, "token1": t1,
                "symbol0": t0_info[0], "symbol1": t1_info[0],
                "decimals0": t0_info[1], "decimals1": t1_info[1],
                "swaps": swaps, "protocol": protocol,
            }
        else:
            errors += 1
    
    # Rate limit
    time.sleep(0.5)
    if (i // batch_size) % 5 == 0:
        print(f"  Resolved {len(resolved)}/{i+len(batch)} pools ({errors} errors)...")

print(f"\nResolved: {len(resolved)} pools, {errors} errors")

# Categorize pools
weth_pools = {}
stable_pools = {}
other_pools = {}

WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
STABLES = {"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC
           "0xdac17f958d2ee523a2206206994597c13d831ec7",  # USDT
           "0x6b175474e89094c44da98b954eedeac495271d0f"}  # DAI

for pool, info in resolved.items():
    if info["token0"] == WETH or info["token1"] == WETH:
        weth_pools[pool] = info
    elif info["token0"] in STABLES or info["token1"] in STABLES:
        stable_pools[pool] = info
    else:
        other_pools[pool] = info

weth_swaps = sum(v["swaps"] for v in weth_pools.values())
stable_swaps = sum(v["swaps"] for v in stable_pools.values())
other_swaps = sum(v["swaps"] for v in other_pools.values())
total_resolved_swaps = weth_swaps + stable_swaps + other_swaps
total_all = con.execute("SELECT count(*) FROM read_parquet('data/events/swaps/ethereum/*.parquet', union_by_name=true)").fetchone()[0]

print(f"\n=== POOL CATEGORIES ===")
print(f"  WETH pairs:   {len(weth_pools):>4} pools, {weth_swaps:>10,} swaps ({weth_swaps*100//total_all}%)")
print(f"  Stable pairs: {len(stable_pools):>4} pools, {stable_swaps:>10,} swaps ({stable_swaps*100//total_all}%)")
print(f"  Other pairs:  {len(other_pools):>4} pools, {other_swaps:>10,} swaps ({other_swaps*100//total_all}%)")
print(f"  Priceable:    {len(weth_pools)+len(stable_pools):>4} pools, {(weth_swaps+stable_swaps):>10,} swaps ({(weth_swaps+stable_swaps)*100//total_all}%)")

# Show top WETH pairs
print(f"\n=== TOP WETH PAIRS ===")
for pool, info in sorted(weth_pools.items(), key=lambda x: -x[1]["swaps"])[:15]:
    other = info["symbol1"] if info["token0"] == WETH else info["symbol0"]
    print(f"  {pool[:18]}...  WETH/{other:<8} {info['protocol']:<12} {info['swaps']:>10,} swaps")

# Save resolved data for the pricing script
with open("/root/mev/data/pool_tokens.json", "w") as f:
    json.dump(resolved, f, indent=2)
print(f"\nSaved pool token data to data/pool_tokens.json")

