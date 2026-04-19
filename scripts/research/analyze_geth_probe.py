#!/usr/bin/env python3
"""
Analyze the Geth mempool probe output.

Compares pending txs seen by Geth against on-chain confirmed swaps in the
same time window to compute:
  - Visibility rate (% of on-chain swaps we saw pending first)
  - Median lead time (seconds between seen-pending and included-on-chain)
  - Sandwichable count (>0.1 ETH, slippage room)

Requires:
  - Probe JSONL output (from geth_mempool_probe.py)
  - On-chain swap receipts for the same time window (via RPC or parquet data)
"""
import argparse
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

import requests


def load_probe(path):
    """Load probe JSONL into a dict keyed by tx hash."""
    by_hash = {}
    with open(path) as f:
        for line in f:
            try:
                rec = json.loads(line)
                by_hash[rec["hash"].lower()] = rec
            except Exception:
                continue
    return by_hash


def fetch_receipt(rpc_url, tx_hash):
    """Fetch tx receipt + block timestamp from RPC."""
    r = requests.post(rpc_url, json={
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getTransactionReceipt",
        "params": [tx_hash]
    }).json()
    receipt = r.get("result")
    if not receipt or not receipt.get("blockNumber"):
        return None

    block_num = receipt["blockNumber"]
    r2 = requests.post(rpc_url, json={
        "jsonrpc": "2.0",
        "id": 2,
        "method": "eth_getBlockByNumber",
        "params": [block_num, False]
    }).json()
    block = r2.get("result")
    if not block:
        return None

    return {
        "block_number": int(block_num, 16),
        "block_ts": int(block["timestamp"], 16),
        "status": int(receipt.get("status", "0x0"), 16),
        "gas_used": int(receipt.get("gasUsed", "0x0"), 16),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--probe", required=True, help="Probe JSONL file")
    ap.add_argument("--rpc", required=True, help="RPC URL for receipt lookups")
    ap.add_argument("--sample", type=int, default=1000, help="Sample N txs for receipt lookup")
    args = ap.parse_args()

    probe = load_probe(args.probe)
    print(f"Loaded {len(probe)} pending txs from probe")

    # Breakdown
    by_router = defaultdict(int)
    by_selector = defaultdict(int)
    v3_swaps_size = []
    for rec in probe.values():
        by_router[rec["router"]] += 1
        by_selector[rec["selector_name"]] += 1
        if rec.get("decoded") and "amount_in" in rec["decoded"]:
            v3_swaps_size.append(rec["decoded"]["amount_in"])

    print(f"\nBy router:")
    for r, c in sorted(by_router.items(), key=lambda x: -x[1]):
        print(f"  {r}: {c}")

    print(f"\nBy selector:")
    for s, c in sorted(by_selector.items(), key=lambda x: -x[1])[:20]:
        print(f"  {s}: {c}")

    if v3_swaps_size:
        print(f"\nV3 exactInputSingle amount_in distribution (decoded only):")
        print(f"  n={len(v3_swaps_size)}")
        print(f"  median: {statistics.median(v3_swaps_size)/1e18:.4f} ETH equiv (raw units)")
        big = sum(1 for s in v3_swaps_size if s > 0.1e18)
        print(f"  >0.1 ETH-equiv: {big} ({big/len(v3_swaps_size)*100:.1f}%)")

    # Sample txs for receipt lookup
    sample_hashes = list(probe.keys())[:args.sample]
    print(f"\nFetching receipts for {len(sample_hashes)} sample txs...")

    confirmed = 0
    lead_times = []
    for i, h in enumerate(sample_hashes):
        r = fetch_receipt(args.rpc, h)
        if r and r["status"] == 1:
            confirmed += 1
            lead = r["block_ts"] - probe[h]["seen_ts"]
            lead_times.append(lead)
        if (i + 1) % 100 == 0:
            print(f"  {i+1}/{len(sample_hashes)} checked, {confirmed} confirmed...")

    print(f"\n=== CONFIRMATION STATS ===")
    print(f"  Sampled: {len(sample_hashes)}")
    print(f"  Confirmed on-chain: {confirmed} ({confirmed/len(sample_hashes)*100:.1f}%)")
    if lead_times:
        print(f"  Lead time median: {statistics.median(lead_times):.1f}s")
        print(f"  Lead time mean: {statistics.mean(lead_times):.1f}s")
        good_lead = sum(1 for lt in lead_times if lt >= 2)
        print(f"  Lead time >=2s: {good_lead} ({good_lead/len(lead_times)*100:.1f}%)")


if __name__ == "__main__":
    main()
