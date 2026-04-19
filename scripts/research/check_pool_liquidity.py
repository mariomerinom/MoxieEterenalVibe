#!/usr/bin/env python3
"""Check on-chain liquidity for dynamic divergence tokens."""
import json
import subprocess

def rpc(url, method, params):
    r = subprocess.run(
        ["curl", "-s", "-X", "POST", url, "-H", "Content-Type: application/json",
         "-d", json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})],
        capture_output=True, text=True, timeout=15
    )
    return json.loads(r.stdout).get("result")

# Load env
env = {}
for line in open("/root/mev/.env"):
    line = line.strip()
    if "=" in line and not line.startswith("#"):
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip('"')

RPCS = {
    "ethereum": env.get("ETH_RPC_HTTP"),
    "arbitrum": env.get("ARB_RPC_HTTP"),
    "base": env.get("BASE_RPC_HTTP"),
}

# Load pool data to find dynamic tokens
pool_files = {
    "ethereum": "data/pool_tokens.json",
    "arbitrum": "data/pool_tokens_arbitrum.json",
    "base": "data/pool_tokens_base.json",
}

WETH = {
    "ethereum": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
    "arbitrum": "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
    "base": "0x4200000000000000000000000000000000000006",
}

# Dynamic tokens with >0.3% varying divergence
check_tokens = {"SUP", "KEYCAT", "COOKIE", "PRIME", "PEPE", "CRV", "SHIB", "AAVE",
                "FARM", "LDO", "MORPHO", "ATH", "FAI", "COMP", "ETHFI", "TOSHI",
                "1INCH", "UNI", "LINK"}

print(f"{'Sym':<10} {'Chain':<10} {'Pool':<44} {'Liq (ETH)':>12}")
print("-" * 80)

for chain, fp in pool_files.items():
    try:
        pools = json.load(open(fp))
    except FileNotFoundError:
        continue

    weth = WETH[chain].lower()
    rpc_url = RPCS.get(chain)
    if not rpc_url:
        continue

    checked = set()
    for addr, info in pools.items():
        t0, t1 = info["token0"].lower(), info["token1"].lower()
        if t0 != weth and t1 != weth:
            continue
        proto = info.get("protocol", "").lower()
        if "camelot" in proto or "aerodrome" in proto:
            continue
        sym0 = info.get("symbol0", "?").upper()
        sym1 = info.get("symbol1", "?").upper()
        token_sym = sym0 if t0 != weth else sym1
        token0_is_weth = t0 == weth
        d0 = info.get("decimals0", 18)
        d1 = info.get("decimals1", 18)

        key = (token_sym, chain)
        if token_sym not in check_tokens or key in checked:
            continue
        checked.add(key)

        # Check V3 liquidity or V2 reserves
        if "v3" in proto:
            # Get slot0 for price, then estimate TVL from liquidity
            r = rpc(rpc_url, "eth_call", [{"to": addr, "data": "0x1a686502"}, "latest"])
            if r and len(r) > 2:
                liq = int(r, 16)
                # Very rough: for concentrated liquidity, TVL ~ liquidity * price_range
                # Better: check balance of WETH in pool
                bal_data = "0x70a08231" + "0" * 24 + addr[2:]
                bal_r = rpc(rpc_url, "eth_call", [{"to": weth, "data": bal_data}, "latest"])
                weth_bal = 0
                if bal_r and len(bal_r) > 2:
                    weth_bal = int(bal_r, 16) / 1e18
                print(f"{token_sym:<10} {chain:<10} {addr:<44} {weth_bal:>12.4f}")
        else:
            r = rpc(rpc_url, "eth_call", [{"to": addr, "data": "0x0902f1ac"}, "latest"])
            if r and len(r) > 130:
                data = r[2:]
                r0 = int(data[0:64], 16) / 10**d0
                r1 = int(data[64:128], 16) / 10**d1
                weth_amt = r0 if token0_is_weth else r1
                print(f"{token_sym:<10} {chain:<10} {addr:<44} {weth_amt:>12.4f}")
