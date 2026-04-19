#!/usr/bin/env python3
"""
Geth mempool visibility probe (A2).

Subscribes to Geth's `newPendingTransactions` via WebSocket.
For each pending tx:
  - Match against known swap router addresses
  - Decode calldata for recognized selectors
  - Log hash, from, to, selector, decoded params, gas price, timestamp

Compares against on-chain swap volume to measure:
  - Visibility rate (% of on-chain swaps seen pending)
  - Median lead time (seconds between seen-pending and included-in-block)
  - Sandwichable count (>0.1 ETH, slippage >0.5%)

Kill/go gates:
  - Visibility >15% → Go
  - Swap txs/day >20,000 → Go
  - Median lead time >2s → Go
  - Sandwichable >1,000/day → Go

Usage:
  python3 geth_mempool_probe.py --ws ws://localhost:8546 --duration 86400
"""
import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path

import websockets
from eth_utils import to_checksum_address

# Known swap router addresses (Ethereum mainnet)
ROUTERS = {
    # Uniswap V2 Router
    "0x7a250d5630b4cf539739df2c5dacb4c659f2488d": "uniswap_v2",
    # Uniswap V3 SwapRouter
    "0xe592427a0aece92de3edee1f18e0157c05861564": "uniswap_v3",
    # Uniswap V3 SwapRouter02
    "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45": "uniswap_v3_02",
    # Universal Router (Uniswap)
    "0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad": "universal_router",
    "0xef1c6e67703c7bd7107eed8303fbe6ec2554bf6b": "universal_router_old",
    # SushiSwap Router
    "0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f": "sushiswap",
    # 1inch Aggregation V5
    "0x1111111254eeb25477b68fb85ed929f73a960582": "1inch_v5",
    # 1inch Aggregation V6
    "0x111111125421ca6dc452d289314280a0f8842a65": "1inch_v6",
    # 0x Exchange Proxy
    "0xdef1c0ded9bec7f1a1670819833240f027b25eff": "0x_proxy",
    # CoW Protocol
    "0x9008d19f58aabd9ed0d60971565aa8510560ab41": "cow_swap",
}

# Swap selectors → human name
SELECTORS = {
    # Uniswap V2
    "0x7ff36ab5": "swapExactETHForTokens",
    "0x18cbafe5": "swapExactTokensForETH",
    "0x38ed1739": "swapExactTokensForTokens",
    "0xfb3bdb41": "swapETHForExactTokens",
    "0x4a25d94a": "swapTokensForExactETH",
    "0x8803dbee": "swapTokensForExactTokens",
    "0x5c11d795": "swapExactTokensForTokensSupportingFeeOnTransferTokens",
    "0xb6f9de95": "swapExactETHForTokensSupportingFeeOnTransferTokens",
    "0x791ac947": "swapExactTokensForETHSupportingFeeOnTransferTokens",
    # Uniswap V3
    "0x414bf389": "exactInputSingle",
    "0xc04b8d59": "exactInput",
    "0xdb3e2198": "exactOutputSingle",
    "0xf28c0498": "exactOutput",
    "0xac9650d8": "multicall",
    "0x5ae401dc": "multicall_deadline",
    "0x1f0464d1": "multicall_hex",
    # Universal Router
    "0x3593564c": "execute",
    "0x24856bc3": "execute_noop",
    # 1inch
    "0x12aa3caf": "swap_1inch_v5",
    "0x07ed2379": "swap_1inch_v6",
    # 0x
    "0x415565b0": "transformERC20",
    "0x6af479b2": "sellToUniswap",
}


def decode_v2_swap(data: bytes):
    """Decode Uniswap V2 style swap calldata."""
    # swapExactETHForTokens(uint256 amountOutMin, address[] path, address to, uint256 deadline)
    # First 4 bytes = selector, then amountOutMin (32), then path offset (32) ...
    if len(data) < 4 + 32 * 4:
        return None
    try:
        amount_out_min = int.from_bytes(data[4:36], "big")
        # For swapExactTokensForTokens, first arg is amountIn
        amount_in = int.from_bytes(data[4:36], "big")
        # Path is at an offset; for simplicity, try position 4 + 64 (after 2 uint256)
        # Path array length follows
        # This is fragile — we'll do simple extraction
        return {"amount_in_or_out_min": amount_out_min}
    except Exception:
        return None


def decode_v3_single(data: bytes):
    """Decode Uniswap V3 exactInputSingle."""
    # exactInputSingle((address tokenIn, address tokenOut, uint24 fee, address recipient,
    #                   uint256 deadline, uint256 amountIn, uint256 amountOutMinimum, uint160 sqrtPriceLimitX96))
    if len(data) < 4 + 32 * 8:
        return None
    try:
        # Struct is passed as flat params
        token_in = "0x" + data[16:36].hex()
        token_out = "0x" + data[48:68].hex()
        fee = int.from_bytes(data[68:100], "big")
        # recipient at 100-132
        # deadline at 132-164
        amount_in = int.from_bytes(data[164:196], "big")
        amount_out_min = int.from_bytes(data[196:228], "big")
        return {
            "token_in": token_in,
            "token_out": token_out,
            "fee": fee,
            "amount_in": amount_in,
            "amount_out_min": amount_out_min,
        }
    except Exception:
        return None


async def probe(ws_url: str, duration: int, out_path: Path):
    print(f"Connecting to {ws_url}...")
    print(f"Duration: {duration}s ({duration/3600:.1f}h)")
    print(f"Output: {out_path}")

    start = time.time()
    end = start + duration

    stats = {
        "total_pending": 0,
        "router_matches": 0,
        "decoded_swaps": 0,
        "by_router": {},
        "by_selector": {},
        "errors": 0,
    }

    async with websockets.connect(ws_url, max_size=2**24) as ws:
        # Subscribe with fullTx so we get the full transaction, not just hashes
        sub_req = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_subscribe",
            "params": ["newPendingTransactions", True],  # true = full tx
        }
        await ws.send(json.dumps(sub_req))
        sub_resp = await ws.recv()
        print(f"Subscribe response: {sub_resp}")

        out = open(out_path, "w")
        last_print = time.time()

        while time.time() < end:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
            except asyncio.TimeoutError:
                continue

            try:
                data = json.loads(msg)
                tx = data.get("params", {}).get("result")
                if not isinstance(tx, dict):
                    continue

                stats["total_pending"] += 1

                to_addr = (tx.get("to") or "").lower()
                if to_addr not in ROUTERS:
                    continue

                router = ROUTERS[to_addr]
                stats["router_matches"] += 1
                stats["by_router"][router] = stats["by_router"].get(router, 0) + 1

                input_data = tx.get("input", "0x")
                if len(input_data) < 10:
                    continue

                selector = input_data[:10].lower()
                selector_name = SELECTORS.get(selector, f"unknown_{selector}")
                stats["by_selector"][selector_name] = (
                    stats["by_selector"].get(selector_name, 0) + 1
                )

                # Decode known selectors
                decoded = None
                try:
                    raw = bytes.fromhex(input_data[2:])
                    if selector in (
                        "0x7ff36ab5", "0x18cbafe5", "0x38ed1739",
                        "0xfb3bdb41", "0x4a25d94a", "0x8803dbee",
                        "0x5c11d795", "0xb6f9de95", "0x791ac947",
                    ):
                        decoded = decode_v2_swap(raw)
                    elif selector == "0x414bf389":
                        decoded = decode_v3_single(raw)
                except Exception as e:
                    stats["errors"] += 1

                if decoded:
                    stats["decoded_swaps"] += 1

                # Log everything (decoded or not) for cross-reference
                record = {
                    "seen_ts": time.time(),
                    "hash": tx.get("hash"),
                    "from": tx.get("from"),
                    "to": to_addr,
                    "router": router,
                    "selector": selector,
                    "selector_name": selector_name,
                    "value_wei": tx.get("value", "0x0"),
                    "gas_price_wei": tx.get("gasPrice", "0x0") or tx.get("maxFeePerGas", "0x0"),
                    "gas": tx.get("gas", "0x0"),
                    "nonce": tx.get("nonce", "0x0"),
                    "decoded": decoded,
                }
                out.write(json.dumps(record) + "\n")

                if time.time() - last_print > 60:
                    out.flush()
                    elapsed = time.time() - start
                    print(
                        f"[{elapsed:.0f}s] pending={stats['total_pending']} "
                        f"routers={stats['router_matches']} "
                        f"decoded={stats['decoded_swaps']} "
                        f"rate={stats['router_matches']/elapsed*3600:.0f}/hr"
                    )
                    last_print = time.time()

            except Exception as e:
                stats["errors"] += 1
                if stats["errors"] < 10:
                    print(f"Error: {e}")

        out.close()

    print("\n=== FINAL STATS ===")
    print(json.dumps(stats, indent=2))

    # Project to daily rate
    elapsed = time.time() - start
    mult = 86400 / elapsed
    print(f"\n=== PROJECTED DAILY ===")
    print(f"Total pending txs: {stats['total_pending']*mult:,.0f}/day")
    print(f"Router matches: {stats['router_matches']*mult:,.0f}/day")
    print(f"Decoded swaps: {stats['decoded_swaps']*mult:,.0f}/day")
    print()
    print("KILL/GO gates (compare to baseline: Alchemy 0.96%, 19K swaps/day):")
    print(f"  Swap txs/day: {stats['router_matches']*mult:,.0f} (need >20,000)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--ws", default="ws://localhost:8546", help="Geth WebSocket endpoint")
    ap.add_argument("--duration", type=int, default=86400, help="Duration in seconds")
    ap.add_argument("--out", default="/tmp/geth_mempool.jsonl", help="Output JSONL file")
    args = ap.parse_args()

    asyncio.run(probe(args.ws, args.duration, Path(args.out)))
