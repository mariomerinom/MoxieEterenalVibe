#!/usr/bin/env python3
"""
Check actual recent swap sizes on the small pools that show backrun profit.
Uses eth_getLogs to fetch recent Swap events and compute average swap size.
"""

import json
import os
import subprocess

RPC = os.environ.get("ETH_RPC_HTTP", "https://eth.llamarpc.com")

# Pools that showed meaningful profit in the estimator
# Format: (address, name, protocol, weth_is_token0, liq_eth)
POOLS = [
    ("0x397ff1542f962076d0bfe58ea045ffa2d347aca0", "USDC/WETH SushiV2", "v2", False, 57.8),
    ("0x2e8135be71230c6b1b4045696d41571df754afa2", "USDC/WETH UniV2(?)", "v2", False, 60.2),
    ("0x3aa370aacf4cb08c7e1e7aa8e8ff9418d73b7886", "USDC/WETH UniV2", "v2", False, 114.8),
    ("0x17c1ae82d99379240b780f094c0d7c7f3f3e5b7f", "WETH/USDT UniV2", "v2", True, 62.5),
    ("0xabb097c7dc3a3b3da8fba54c8c3cbb71a3b01e77", "DAI/WETH UniV3-500", "v3", False, 78.1),
    ("0xb771f724783dc25e6a8c5a9c79c9e9593ba2bf97", "???/WETH V2", "v2", False, 8.3),
    ("0xc0a6bb3d015213e0b9b4bb81ac0f05e8df43e7ae", "???/WETH V2", "v2", False, 79.1),
]

# V2 Swap event topic
SWAP_V2_TOPIC = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
# V3 Swap event topic
SWAP_V3_TOPIC = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"

def rpc_call(method, params):
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    r = subprocess.run(
        ["curl", "-s", "-X", "POST", RPC, "-H", "Content-Type: application/json", "-d", payload],
        capture_output=True, text=True
    )
    try:
        return json.loads(r.stdout).get("result")
    except:
        return None

def get_block_number():
    result = rpc_call("eth_blockNumber", [])
    return int(result, 16) if result else 0

def get_swap_logs(pool_addr, topic, from_block, to_block):
    result = rpc_call("eth_getLogs", [{
        "address": pool_addr,
        "topics": [topic],
        "fromBlock": hex(from_block),
        "toBlock": hex(to_block),
    }])
    return result if result else []

def decode_v2_swap(log_data):
    """Decode V2 Swap event: amount0In, amount1In, amount0Out, amount1Out"""
    data = log_data[2:]  # strip 0x
    if len(data) < 256:
        return None
    a0_in = int(data[0:64], 16)
    a1_in = int(data[64:128], 16)
    a0_out = int(data[128:192], 16)
    a1_out = int(data[192:256], 16)
    return a0_in, a1_in, a0_out, a1_out

def decode_v3_swap(log_data):
    """Decode V3 Swap event: amount0, amount1 (signed int256)"""
    data = log_data[2:]
    if len(data) < 320:
        return None
    # amount0 and amount1 are signed int256
    a0 = int(data[0:64], 16)
    if a0 >= 2**255:
        a0 -= 2**256
    a1 = int(data[64:128], 16)
    if a1 >= 2**255:
        a1 -= 2**256
    return a0, a1

def main():
    print(f"RPC: {RPC[:40]}...")
    current_block = get_block_number()
    print(f"Current block: {current_block}")

    # Look back ~2000 blocks (~7 hours)
    from_block = current_block - 2000

    print(f"\nChecking swap sizes for last 2000 blocks ({from_block} to {current_block})")
    print("=" * 90)

    for addr, name, proto, weth_is_t0, liq_eth in POOLS:
        topic = SWAP_V2_TOPIC if proto == "v2" else SWAP_V3_TOPIC
        logs = get_swap_logs(addr, topic, from_block, current_block)

        if not logs:
            print(f"\n  {name} ({addr[:10]}...): 0 swaps in last 2000 blocks")
            continue

        # Decode swap sizes in WETH terms
        weth_amounts = []
        for log in logs:
            if proto == "v2":
                decoded = decode_v2_swap(log.get("data", "0x"))
                if not decoded:
                    continue
                a0_in, a1_in, a0_out, a1_out = decoded
                if weth_is_t0:
                    weth_in = a0_in / 1e18
                    weth_out = a0_out / 1e18
                else:
                    weth_in = a1_in / 1e18
                    weth_out = a1_out / 1e18
                weth_amounts.append(max(weth_in, weth_out))
            else:
                decoded = decode_v3_swap(log.get("data", "0x"))
                if not decoded:
                    continue
                a0, a1 = decoded
                if weth_is_t0:
                    weth_amounts.append(abs(a0) / 1e18)
                else:
                    weth_amounts.append(abs(a1) / 1e18)

        if not weth_amounts:
            print(f"\n  {name} ({addr[:10]}...): {len(logs)} logs but 0 decoded")
            continue

        weth_amounts.sort()
        n = len(weth_amounts)
        median = weth_amounts[n // 2]
        mean = sum(weth_amounts) / n
        p25 = weth_amounts[n // 4]
        p75 = weth_amounts[3 * n // 4]

        per_hour = n / (2000 * 12 / 3600)  # blocks * 12s / 3600
        per_day = per_hour * 24

        print(f"\n  {name} ({addr[:10]}...): {n} swaps in ~7h ({per_day:.0f}/day)")
        print(f"    Liq: {liq_eth:.1f} ETH")
        print(f"    WETH per swap: median={median:.4f}  mean={mean:.4f}  "
              f"p25={p25:.4f}  p75={p75:.4f}")
        print(f"    Min={min(weth_amounts):.4f}  Max={max(weth_amounts):.4f}")

        # Distribution buckets
        buckets = [(0, 0.01), (0.01, 0.05), (0.05, 0.1), (0.1, 0.5),
                   (0.5, 1.0), (1.0, 5.0), (5.0, 50.0), (50.0, 1e6)]
        print(f"    Size distribution:")
        for lo, hi in buckets:
            count = sum(1 for x in weth_amounts if lo <= x < hi)
            if count > 0:
                pct = count / n * 100
                bar = "#" * int(pct / 2)
                print(f"      {lo:>6.2f}-{hi:>6.1f} ETH: {count:>4} ({pct:>5.1f}%) {bar}")

if __name__ == "__main__":
    main()
