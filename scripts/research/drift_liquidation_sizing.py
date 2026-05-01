#!/usr/bin/env python3
"""
Drift perp/spot liquidation sizing probe.

Solana perp liquidations are one of the 5 untested categories from our
strategy audit. Drift is the dominant Solana perp DEX (~70% market share
historically). This probe samples recent transactions to the Drift
program and counts liquidation events to size the addressable market.

Approach:
1. Use `getSignaturesForAddress` to pull recent Drift program signatures
   (paginated, up to N hours back).
2. For each batch, fetch full transactions via `getTransactions`.
3. Filter for liquidation instructions by Anchor discriminator (first 8
   bytes of sha256("global:<instruction_name>")).
4. Count: liquidations/day, by type, top liquidator addresses (HHI).
5. Sample bonus amounts where parseable.

Go/kill gates:
  Kill if <10 liquidations/day across all types
  Kill if top-3 liquidator HHI > 0.5 (>50% concentrated)
  Kill if median liquidation bonus < $1 USD
  Go if >50/day AND HHI < 0.3 AND median bonus > $5
"""
import argparse
import base64
import json
import os
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

import requests

# Drift program ID (mainnet)
DRIFT_PROGRAM = "dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH"

# Anchor instruction discriminators (first 8 bytes of sha256("global:<name>"))
LIQ_DISCRIMINATORS = {
    "4b2377f7bf128b02": "liquidate_perp",
    "6b00802923e5fb12": "liquidate_spot",
    "a911205acf94d11b": "liquidate_borrow_for_perp_pnl",
    "ed4bc6ebe9ba4b23": "liquidate_perp_pnl_for_deposit",
    "5f6f7c6956a9bb22": "liquidate_perp_with_fill",
    "e010b0d6a2d5b7de": "resolve_perp_bankruptcy",
    "7cc2f0fec6d5347a": "resolve_spot_bankruptcy",
}

RPC_URL = os.environ.get("SOLANA_RPC_HTTP", "https://api.mainnet-beta.solana.com")
OUTPUT_DIR = Path("research/data")


def rpc(method, params, retries=3):
    for attempt in range(retries):
        try:
            r = requests.post(RPC_URL, json={
                "jsonrpc": "2.0", "id": 1,
                "method": method, "params": params,
            }, timeout=20)
            if r.status_code == 200:
                return r.json().get("result")
            if r.status_code == 429:
                time.sleep(1.5 * (attempt + 1))
                continue
            return None
        except (requests.RequestException, ValueError):
            time.sleep(0.5 * (attempt + 1))
    return None


def fetch_signatures(before=None, limit=1000):
    """Get up to `limit` signatures, optionally before a given signature."""
    params = [DRIFT_PROGRAM, {"limit": limit}]
    if before:
        params[1]["before"] = before
    return rpc("getSignaturesForAddress", params) or []


def fetch_tx(sig):
    return rpc("getTransaction", [sig, {
        "encoding": "base64",
        "maxSupportedTransactionVersion": 0,
        "commitment": "finalized",
    }])


def find_liquidation_instructions(tx):
    """Look in top-level + inner instructions for Drift liquidation discriminators.

    Returns list of (ix_type_name, accounts_list).
    """
    if not tx or not tx.get("transaction"):
        return []

    msg = tx["transaction"][0] if isinstance(tx["transaction"], list) else tx["transaction"]["message"]
    # tx is base64 encoded - we need to decode the binary message
    # Easier path: use jsonParsed for instructions, but need to detect Drift program calls
    # Re-fetch with json encoding instead
    return None  # placeholder - we use the json path below


def fetch_tx_json(sig):
    return rpc("getTransaction", [sig, {
        "encoding": "json",
        "maxSupportedTransactionVersion": 0,
        "commitment": "finalized",
    }])


def extract_drift_liquidations(tx_json):
    """Returns list of dicts: {type, accounts, fee_payer, slot, succeeded}.

    Walks both top-level instructions and inner CPI instructions.
    Records BOTH successful and failed liquidation attempts — failures
    represent liquidator competition (lost races).
    """
    if not tx_json or not tx_json.get("transaction"):
        return []

    txn = tx_json["transaction"]
    meta = tx_json.get("meta", {})
    succeeded = not (meta and meta.get("err"))

    msg = txn["message"]
    account_keys = msg["accountKeys"]
    # If versioned tx with address lookup tables, append loaded accounts
    loaded = meta.get("loadedAddresses") or {}
    if loaded.get("writable") or loaded.get("readonly"):
        account_keys = (account_keys
                        + (loaded.get("writable") or [])
                        + (loaded.get("readonly") or []))

    liquidations = []

    def check_ix(program_id_index, data_b58_or_b64, accounts_idx, source):
        if program_id_index >= len(account_keys):
            return
        if account_keys[program_id_index] != DRIFT_PROGRAM:
            return
        # Anchor instructions have data as base58 (json encoding) or base64 (jsonParsed)
        try:
            import base58
            raw = base58.b58decode(data_b58_or_b64)
        except Exception:
            try:
                raw = base64.b64decode(data_b58_or_b64)
            except Exception:
                return
        if len(raw) < 8:
            return
        disc = raw[:8].hex()
        if disc in LIQ_DISCRIMINATORS:
            ix_accts = [account_keys[i] for i in accounts_idx if i < len(account_keys)]
            liquidations.append({
                "type": LIQ_DISCRIMINATORS[disc],
                "source": source,
                "fee_payer": account_keys[0],
                "first_accounts": ix_accts[:5],
                "succeeded": succeeded,
            })

    for ix in msg.get("instructions", []):
        check_ix(ix["programIdIndex"], ix["data"], ix.get("accounts", []), "top")

    for inner in (meta.get("innerInstructions") or []):
        for ix in inner.get("instructions", []):
            check_ix(ix["programIdIndex"], ix["data"], ix.get("accounts", []), "inner")

    return liquidations


def run(hours=24, max_sigs=20000):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_log = open(OUTPUT_DIR / "drift_liquidations.jsonl", "a")
    print(f"Drift liquidation probe — looking back ~{hours}h, max {max_sigs} sigs")

    end_ts = time.time() - hours * 3600
    cursor = None
    total_sigs = 0
    total_txs_checked = 0
    total_liqs = 0          # both successful and failed
    successful_liqs = 0
    failed_liqs = 0
    by_type = Counter()
    by_type_success = Counter()
    liquidator_counter = Counter()       # fee_payer of all attempts
    successful_liquidator_counter = Counter()
    timestamps = []

    while total_sigs < max_sigs:
        sigs = fetch_signatures(before=cursor, limit=1000)
        if not sigs:
            break

        # Filter to in-window signatures and reset cursor for next page
        in_window = [s for s in sigs if (s.get("blockTime") or 0) >= end_ts]
        if not in_window:
            break

        for sig_obj in in_window:
            total_sigs += 1
            sig = sig_obj["signature"]
            tx = fetch_tx_json(sig)
            total_txs_checked += 1
            if not tx:
                continue

            liqs = extract_drift_liquidations(tx)
            if liqs:
                for liq in liqs:
                    by_type[liq["type"]] += 1
                    liquidator_counter[liq["fee_payer"]] += 1
                    timestamps.append(sig_obj.get("blockTime"))
                    total_liqs += 1
                    if liq["succeeded"]:
                        successful_liqs += 1
                        by_type_success[liq["type"]] += 1
                        successful_liquidator_counter[liq["fee_payer"]] += 1
                    else:
                        failed_liqs += 1
                    out_log.write(json.dumps({
                        "signature": sig,
                        "block_time": sig_obj.get("blockTime"),
                        "slot": sig_obj.get("slot"),
                        **liq,
                    }) + "\n")

            # Light pacing to avoid public RPC rate limits
            if total_txs_checked % 50 == 0:
                out_log.flush()
                print(f"  scanned {total_sigs} sigs, {total_txs_checked} txs, "
                      f"{total_liqs} liquidations found")

        cursor = in_window[-1]["signature"]
        # If the page was entirely in-window, fetch next page
        if len(in_window) == len(sigs):
            continue
        # Otherwise we've crossed the window boundary
        break

    out_log.close()

    # Stats
    actual_hours = (time.time() - min(timestamps)) / 3600 if timestamps else hours
    daily_mult = 24 / max(actual_hours, 0.001)

    print(f"\n=== RESULTS over {actual_hours:.2f}h ===")
    print(f"Total Drift program sigs scanned: {total_sigs}")
    print(f"Liquidation attempts (success+fail): {total_liqs} ({total_liqs * daily_mult:.0f}/day)")
    print(f"  succeeded: {successful_liqs} ({successful_liqs * daily_mult:.0f}/day)")
    print(f"  failed: {failed_liqs} ({failed_liqs * daily_mult:.0f}/day, {failed_liqs/max(total_liqs,1)*100:.0f}% race-loss)")
    print(f"\nBy type (all attempts):")
    for t, c in by_type.most_common():
        s = by_type_success[t]
        print(f"  {t}: {c} attempts, {s} succeeded ({c * daily_mult:.0f}/{s * daily_mult:.0f} per day)")

    # HHI competition concentration
    if liquidator_counter:
        total = sum(liquidator_counter.values())
        shares = [c / total for c in liquidator_counter.values()]
        hhi = sum(s * s for s in shares)
        top_3 = sum(c for _, c in liquidator_counter.most_common(3)) / total * 100
        print(f"\nLiquidator concentration:")
        print(f"  Unique liquidators: {len(liquidator_counter)}")
        print(f"  HHI: {hhi:.3f} ({'CONCENTRATED' if hhi > 0.25 else 'fragmented'})")
        print(f"  Top 3 share: {top_3:.1f}%")
        print(f"  Top 5 liquidators:")
        for addr, c in liquidator_counter.most_common(5):
            print(f"    {addr}: {c} ({c/total*100:.1f}%)")

    # Verdict
    print(f"\n=== VERDICT ===")
    daily_liqs = total_liqs * daily_mult
    if daily_liqs < 10:
        print(f"  🛑 KILL: <10 liquidations/day projected")
    elif liquidator_counter and hhi > 0.5:
        print(f"  🛑 KILL: HHI {hhi:.2f} indicates >50% concentrated by top liquidator")
    else:
        print(f"  ⚠️  CONTINUE: {daily_liqs:.0f}/day, HHI {hhi if liquidator_counter else 'N/A'}")
        print(f"     Next: measure median bonus, our latency, our access")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=float, default=24.0)
    ap.add_argument("--max-sigs", type=int, default=20000)
    args = ap.parse_args()
    run(args.hours, args.max_sigs)
