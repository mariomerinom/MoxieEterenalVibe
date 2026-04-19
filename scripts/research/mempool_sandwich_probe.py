#!/usr/bin/env python3
"""
P1-S2b: Mempool Sandwich Probe

Monitors ALL pending transactions in the public mempool.
Identifies swap transactions by function selector.
Answers: "How many sandwichable swaps are visible in the public mempool?"

Uses Alchemy's alchemy_pendingTransactions WebSocket subscription.
Two-phase approach:
  1. Subscribe to all pending tx hashes (low bandwidth)
  2. For txs with known swap selectors → log details

Run for 24h, then analyze.
"""

import asyncio
import json
import os
import signal
import sys
import time
from datetime import datetime

try:
    import websockets
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "websockets", "-q"])
    import websockets

# ── CONFIG ──
WS_URL = os.environ.get("ETH_RPC_WS", "wss://eth-mainnet.g.alchemy.com/v2/demo")
OUTPUT_FILE = "research/data/mempool_swaps.jsonl"
STATS_INTERVAL = 300  # Print stats every 5 min

WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

# ── KNOWN SWAP FUNCTION SELECTORS (4 bytes) ──
# These are the most common swap function signatures across all DEX routers
SWAP_SELECTORS = {
    # Uniswap V2 Router
    "38ed1739": "swapExactTokensForTokens",
    "7ff36ab5": "swapExactETHForTokens",
    "18cbafe5": "swapExactTokensForETH",
    "8803dbee": "swapTokensForExactTokens",
    "fb3bdb41": "swapETHForExactTokens",
    "5c11d795": "swapExactTokensForTokensFee",
    "b6f9de95": "swapExactETHForTokensFee",
    "791ac947": "swapExactTokensForETHFee",
    # Uniswap V3 SwapRouter
    "414bf389": "exactInputSingle",
    "c04b8d59": "exactInput",
    "db3e2198": "exactOutputSingle",
    "f28c0498": "exactOutput",
    # Uniswap V3 SwapRouter02 / multicalls
    "5ae401dc": "multicall_deadline",
    "ac9650d8": "multicall_basic",
    "1f0464d1": "multicall_previous",
    # Universal Router
    "3593564c": "execute",
    "24856bc3": "execute_deadline",
    # 1inch v5
    "12aa3caf": "swap_1inch",
    "e449022e": "uniswapV3Swap_1inch",
    "0502b1c5": "unoswap_1inch",
    "f78dc253": "unoswapTo_1inch",
    # 1inch v6
    "07ed2379": "swap_1inchv6",
    # 0x
    "d9627aa4": "sellToUniswap_0x",
    "415565b0": "transformERC20_0x",
    # Paraswap
    "54e3f31b": "multiSwap_paraswap",
    "a94e78ef": "megaSwap_paraswap",
    # Cowswap / direct pool calls
    "022c0d9f": "swap_v2pool",  # Uniswap V2 pool direct swap
    "128acb08": "swap_v3pool",  # Uniswap V3 pool direct swap
}

# Known router addresses for labeling
KNOWN_ROUTERS = {
    "0x7a250d5630b4cf539739df2c5dacb4c659f2488d": "UniV2Router",
    "0xe592427a0aece92de3edee1f18e0157c05861564": "UniV3Router",
    "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45": "UniV3Router02",
    "0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad": "UniversalRouter",
    "0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f": "SushiRouter",
    "0x1111111254eeb25477b68fb85ed929f73a960582": "1inchV5",
    "0x111111125421ca6dc452d289314280a0f8842a65": "1inchV6",
    "0xdef1c0ded9bec7f1a1670819833240f027b25eff": "0xProxy",
    "0xdef171fe48cf0115b1d80b88dc8eab59176fee57": "ParaswapV5",
    "0x881d40237659c251811cec9c364ef91dc08d300c": "MetamaskSwap",
}


def load_pool_universe():
    try:
        with open("data/pool_tokens.json") as f:
            pools = json.load(f)
    except FileNotFoundError:
        return set(), {}
    pool_set = set(p.lower() for p in pools)
    pair_pools = {}
    for addr, info in pools.items():
        t0 = info["token0"].lower()
        t1 = info["token1"].lower()
        pair = tuple(sorted([t0, t1]))
        pair_pools.setdefault(pair, []).append(addr.lower())
    return pool_set, pair_pools


def decode_v2_router_swap(calldata, value_wei):
    """Decode V2 Router swap. Returns path and amounts or None."""
    if len(calldata) < 10:
        return None
    selector = calldata[2:10].lower()
    data = calldata[10:]

    try:
        if selector in ("38ed1739", "18cbafe5", "5c11d795", "791ac947"):
            # (uint256 amountIn, uint256 amountOutMin, address[] path, address to, uint256 deadline)
            amount_in = int(data[0:64], 16)
            amount_out_min = int(data[64:128], 16)
            path_offset = int(data[128:192], 16) * 2
            path_length = int(data[path_offset:path_offset+64], 16)
            path = []
            for i in range(path_length):
                addr_start = path_offset + 64 + i * 64
                addr = "0x" + data[addr_start+24:addr_start+64].lower()
                path.append(addr)
            return {"path": path, "amount_in": amount_in, "amount_out_min": amount_out_min,
                    "value_wei": 0, "func": SWAP_SELECTORS[selector]}

        elif selector in ("7ff36ab5", "b6f9de95"):
            # swapExactETHForTokens: (uint256 amountOutMin, address[] path, ...)
            amount_out_min = int(data[0:64], 16)
            path_offset = int(data[64:128], 16) * 2
            path_length = int(data[path_offset:path_offset+64], 16)
            path = []
            for i in range(path_length):
                addr_start = path_offset + 64 + i * 64
                addr = "0x" + data[addr_start+24:addr_start+64].lower()
                path.append(addr)
            return {"path": path, "amount_in": value_wei, "amount_out_min": amount_out_min,
                    "value_wei": value_wei, "func": SWAP_SELECTORS[selector]}

        elif selector in ("8803dbee",):
            amount_out = int(data[0:64], 16)
            amount_in_max = int(data[64:128], 16)
            path_offset = int(data[128:192], 16) * 2
            path_length = int(data[path_offset:path_offset+64], 16)
            path = []
            for i in range(path_length):
                addr_start = path_offset + 64 + i * 64
                addr = "0x" + data[addr_start+24:addr_start+64].lower()
                path.append(addr)
            return {"path": path, "amount_in": amount_in_max, "amount_out_min": amount_out,
                    "value_wei": 0, "func": "swapTokensForExactTokens"}

        elif selector == "fb3bdb41":
            amount_out = int(data[0:64], 16)
            path_offset = int(data[64:128], 16) * 2
            path_length = int(data[path_offset:path_offset+64], 16)
            path = []
            for i in range(path_length):
                addr_start = path_offset + 64 + i * 64
                addr = "0x" + data[addr_start+24:addr_start+64].lower()
                path.append(addr)
            return {"path": path, "amount_in": value_wei, "amount_out_min": amount_out,
                    "value_wei": value_wei, "func": "swapETHForExactTokens"}

    except (ValueError, IndexError):
        return None
    return None


def decode_v3_exactInputSingle(calldata, value_wei):
    """Decode V3 exactInputSingle."""
    data = calldata[10:]
    try:
        token_in = "0x" + data[24:64].lower()
        token_out = "0x" + data[88:128].lower()
        fee = int(data[128:192], 16)
        amount_in = int(data[256:320], 16)
        amount_out_min = int(data[320:384], 16)
        if value_wei > 0 and token_in == WETH:
            amount_in = value_wei
        return {"path": [token_in, token_out], "amount_in": amount_in,
                "amount_out_min": amount_out_min, "value_wei": value_wei,
                "fee": fee, "func": "exactInputSingle"}
    except (ValueError, IndexError):
        return None


def estimate_eth_value(decoded, eth_price=2500):
    """Estimate swap value in ETH."""
    if not decoded:
        return 0
    path = decoded.get("path", [])
    amount_in = decoded.get("amount_in", 0)
    value_wei = decoded.get("value_wei", 0)

    if value_wei > 0:
        return value_wei / 1e18
    if path and path[0] == WETH:
        return amount_in / 1e18
    if path and path[-1] == WETH:
        return decoded.get("amount_out_min", 0) / 1e18

    stables_6dec = {
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        "0xdac17f958d2ee523a2206206994597c13d831ec7",
    }
    if path and path[0] in stables_6dec:
        return (amount_in / 1e6) / eth_price
    return 0


class ProbeStats:
    def __init__(self):
        self.start_time = time.time()
        self.total_txs = 0
        self.swap_txs = 0
        self.decoded_swaps = 0
        self.pool_matches = 0
        self.by_selector = {}
        self.by_router = {}
        self.eth_values = []
        self.sandwichable_count = 0
        self.sandwichable_eth = 0.0
        self.last_print = time.time()

    def print_stats(self):
        elapsed = time.time() - self.start_time
        hours = elapsed / 3600
        daily = 24 / max(hours, 0.001)

        print(f"\n{'='*70}", flush=True)
        print(f"  MEMPOOL PROBE — {hours:.2f}h elapsed")
        print(f"{'='*70}")
        print(f"  Total pending txs: {self.total_txs:,} ({self.total_txs/max(hours,0.001):,.0f}/h)")
        print(f"  Swap-like txs: {self.swap_txs:,} ({self.swap_txs/max(hours,0.001):,.0f}/h, {self.swap_txs*daily:,.0f}/day)")
        print(f"  Fully decoded: {self.decoded_swaps:,} ({self.decoded_swaps*daily:,.0f}/day)")
        print(f"  Pool matches: {self.pool_matches:,} ({self.pool_matches*daily:,.0f}/day)")
        print(f"  Sandwichable (>0.1 ETH): {self.sandwichable_count:,} ({self.sandwichable_count*daily:,.0f}/day)")
        if self.sandwichable_count:
            print(f"  Sandwichable ETH: {self.sandwichable_eth:.2f} ({self.sandwichable_eth*daily:.0f}/day)")

        if self.by_selector:
            print(f"\n  By selector (top 15):")
            for sel, count in sorted(self.by_selector.items(), key=lambda x: -x[1])[:15]:
                name = SWAP_SELECTORS.get(sel, "unknown")
                print(f"    {sel} ({name}): {count:,}")

        if self.by_router:
            print(f"\n  By contract (top 10):")
            for router, count in sorted(self.by_router.items(), key=lambda x: -x[1])[:10]:
                label = KNOWN_ROUTERS.get(router, router[:14] + "...")
                print(f"    {label}: {count:,}")

        if self.eth_values:
            vals = sorted(self.eth_values)
            n = len(vals)
            print(f"\n  Swap size distribution (decoded, ETH value):")
            print(f"    Count: {n}  Median: {vals[n//2]:.4f}  Mean: {sum(vals)/n:.4f}")
            if n >= 4:
                print(f"    P25: {vals[n//4]:.4f}  P75: {vals[3*n//4]:.4f}  P90: {vals[int(n*0.9)]:.4f}")
            buckets = [
                (0, 0.01), (0.01, 0.1), (0.1, 0.5), (0.5, 1.0),
                (1.0, 5.0), (5.0, 10.0), (10.0, 50.0), (50.0, 1e6)
            ]
            for lo, hi in buckets:
                c = sum(1 for v in vals if lo <= v < hi)
                if c > 0:
                    print(f"      {lo:>6.2f}-{hi:>6.1f} ETH: {c:>6,} ({c/n*100:.1f}%)")

        print(f"{'='*70}\n", flush=True)
        self.last_print = time.time()


async def run_probe():
    pool_set, pair_pools = load_pool_universe()
    print(f"Loaded {len(pool_set)} pools, {len(pair_pools)} unique pairs")

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    stats = ProbeStats()
    outfile = open(OUTPUT_FILE, "a")

    shutdown = asyncio.Event()
    def handle_signal(sig, frame):
        print(f"\nReceived signal {sig}, shutting down...")
        shutdown.set()
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    eth_price = 2500

    while not shutdown.is_set():
        try:
            print(f"Connecting to {WS_URL[:50]}...")
            async with websockets.connect(WS_URL, ping_interval=30, ping_timeout=60,
                                          max_size=50*1024*1024) as ws:
                # Subscribe to ALL pending txs with full data
                sub_msg = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_subscribe",
                    "params": [
                        "alchemy_pendingTransactions",
                        {"hashesOnly": False}
                    ]
                }
                await ws.send(json.dumps(sub_msg))
                resp = await ws.recv()
                resp_data = json.loads(resp)
                if "result" in resp_data:
                    print(f"Subscribed: {resp_data['result']}")
                else:
                    print(f"Subscription error: {resp_data}")
                    await asyncio.sleep(5)
                    continue

                while not shutdown.is_set():
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=30)
                    except asyncio.TimeoutError:
                        continue
                    except websockets.ConnectionClosed:
                        print("Connection closed, reconnecting...")
                        break

                    data = json.loads(msg)
                    if "params" not in data:
                        continue

                    tx = data["params"]["result"]
                    stats.total_txs += 1

                    calldata = tx.get("input", "0x")
                    if len(calldata) < 10:
                        continue

                    selector = calldata[2:10].lower()
                    if selector not in SWAP_SELECTORS:
                        continue

                    # This is a swap-like tx
                    stats.swap_txs += 1
                    stats.by_selector[selector] = stats.by_selector.get(selector, 0) + 1

                    to_addr = (tx.get("to") or "").lower()
                    stats.by_router[to_addr] = stats.by_router.get(to_addr, 0) + 1

                    value_wei = int(tx.get("value", "0x0"), 16) if tx.get("value") else 0
                    tx_hash = tx.get("hash", "")
                    gas_price = int(tx.get("gasPrice", "0x0"), 16) if tx.get("gasPrice") else 0
                    func_name = SWAP_SELECTORS[selector]

                    # Try to decode
                    decoded = None
                    if selector in ("38ed1739", "7ff36ab5", "18cbafe5", "8803dbee",
                                    "fb3bdb41", "5c11d795", "b6f9de95", "791ac947"):
                        decoded = decode_v2_router_swap(calldata, value_wei)
                    elif selector == "414bf389":
                        decoded = decode_v3_exactInputSingle(calldata, value_wei)

                    eth_val = 0
                    pool_match = False
                    matched_pools = []

                    if decoded:
                        stats.decoded_swaps += 1
                        eth_val = estimate_eth_value(decoded, eth_price)
                        if eth_val > 0:
                            stats.eth_values.append(eth_val)

                        path = decoded.get("path", [])
                        for i in range(len(path) - 1):
                            pair = tuple(sorted([path[i], path[i+1]]))
                            if pair in pair_pools:
                                pool_match = True
                                matched_pools.extend(pair_pools[pair])

                        if pool_match and eth_val >= 0.1:
                            stats.sandwichable_count += 1
                            stats.sandwichable_eth += eth_val

                        stats.pool_matches += (1 if pool_match else 0)

                    # Log swap tx
                    record = {
                        "ts": time.time(),
                        "tx_hash": tx_hash,
                        "to": to_addr,
                        "selector": selector,
                        "func": func_name,
                        "path": decoded.get("path") if decoded else None,
                        "amount_in": str(decoded.get("amount_in", 0)) if decoded else None,
                        "eth_value": eth_val,
                        "gas_price_gwei": gas_price / 1e9,
                        "pool_match": pool_match,
                        "matched_pools": matched_pools[:3],
                        "decoded": decoded is not None,
                    }
                    outfile.write(json.dumps(record) + "\n")

                    # Flush periodically
                    if stats.swap_txs % 100 == 0:
                        outfile.flush()

                    # Periodic stats
                    if time.time() - stats.last_print >= STATS_INTERVAL:
                        stats.print_stats()

        except (websockets.ConnectionClosed, ConnectionRefusedError, OSError) as e:
            print(f"Connection error: {e}, reconnecting in 5s...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"Unexpected error: {e}, reconnecting in 10s...")
            import traceback
            traceback.print_exc()
            await asyncio.sleep(10)

    stats.print_stats()
    outfile.close()
    print(f"Data saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    asyncio.run(run_probe())
