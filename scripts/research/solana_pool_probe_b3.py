#!/usr/bin/env python3
"""
Solana cross-DEX live pool-state probe (B3) — via Jupiter quote API.

Purpose: measure TRUE cross-DEX price divergence, not B1's backfill-biased
number. B1 only saw divergence when both pools had a swap in the same
10-slot window (structurally undercounts). This polls Jupiter's /quote
endpoint with DEX filters to get per-DEX prices reflecting current
on-chain pool state.

Why Jupiter:
- Raydium's v3 /pools/info API serves stale prices for concentrated
  liquidity pools (the `price` field can be wrong).
- Orca's public API serves very stale cached data (SOL/USDC at $127
  when on-chain reality is $86).
- Jupiter's /quote aggregator queries pools LIVE and returns precise
  amounts accounting for fees, slippage, and concentrated liquidity.

Methodology:
1. For each watchlist pair, poll Jupiter once per DEX (Raydium, Whirlpool,
   Meteora) with onlyDirectRoutes=true. Fixed input amount = 1 SOL or
   equivalent.
2. Convert outAmount → implied spot price per DEX.
3. Detect cross-DEX divergence > ROUND_TRIP_FEE.
4. For each divergence event, schedule a follow-up poll 30s later to
   check if the gap closed (= competitor arbed) or persisted (= structural
   pricing issue, not a real arb opportunity).

Outputs:
  research/data/solana_b3.jsonl — per-tick observations
  research/data/solana_b3_events.jsonl — divergence events with follow-up

Go/kill gates (after 24h):
  Kill if <10 profitable episodes/day (>0.6% net, >1 SOL trade size)
  Kill if >90% close within 30s (100% captured)
  Go if 50+ profitable episodes/day AND <70% same-window close rate
"""
import argparse
import asyncio
import json
import sys
import time
from collections import defaultdict
from pathlib import Path

import aiohttp

# ── Config ─────────────────────────────────────────────────────────────────
SOL = "So11111111111111111111111111111111111111112"
USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
mSOL = "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So"
JITOSOL = "J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn"
BONK = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"
WIF = "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm"

DECIMALS = {SOL: 9, USDC: 6, USDT: 6, mSOL: 9, JITOSOL: 9, BONK: 5, WIF: 6}
SYMBOLS = {SOL: "SOL", USDC: "USDC", USDT: "USDT", mSOL: "mSOL",
           JITOSOL: "jitoSOL", BONK: "BONK", WIF: "WIF"}

# (input_mint, output_mint, input_amount_in_mint_units)
# Input amounts chosen to be ~$50 equivalent (moderate liquidity probe)
WATCHLIST = [
    (SOL, USDC, 1_000_000_000),           # 1 SOL
    (SOL, USDT, 1_000_000_000),           # 1 SOL
    (USDC, USDT, 100_000_000),            # 100 USDC
    (mSOL, SOL, 1_000_000_000),           # 1 mSOL
    (JITOSOL, SOL, 1_000_000_000),        # 1 jitoSOL
    (SOL, BONK, 1_000_000_000),           # 1 SOL → BONK
    (SOL, WIF, 1_000_000_000),            # 1 SOL → WIF
    (USDC, SOL, 100_000_000),             # 100 USDC → SOL (reverse)
]

DEXES = ["Raydium", "Whirlpool", "Meteora DLMM"]  # Try 3 major DEXs

JUPITER_QUOTE = "https://api.jup.ag/swap/v1/quote"

ROUND_TRIP_FEE = 0.006  # 0.6%
POLL_INTERVAL_SEC = 10   # per-pair polling interval (reduces API load)
FOLLOWUP_DELAY_SEC = 30  # seconds until divergence re-check
OUTPUT_DIR = Path("research/data")


async def jupiter_quote(session, input_mint, output_mint, amount, dex):
    """Query Jupiter for a single-DEX route. Returns outAmount or None."""
    params = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": str(amount),
        "onlyDirectRoutes": "true",
        "dexes": dex,
        "slippageBps": "50",
    }
    try:
        async with session.get(JUPITER_QUOTE, params=params, timeout=8) as r:
            if r.status != 200:
                return None
            d = await r.json()
            if "outAmount" in d:
                return {
                    "out": int(d["outAmount"]),
                    "usd": float(d.get("swapUsdValue") or 0),
                    "route_pools": [h["swapInfo"]["ammKey"]
                                    for h in d.get("routePlan", [])],
                    "slot": int(d.get("contextSlot", 0)),
                }
            return None
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
        return None


def price_from_quote(in_mint, out_mint, in_amount, quote):
    """Compute output-per-input as a float (quote_per_base)."""
    if not quote:
        return None
    in_dec = DECIMALS.get(in_mint, 9)
    out_dec = DECIMALS.get(out_mint, 9)
    in_h = in_amount / 10**in_dec
    out_h = quote["out"] / 10**out_dec
    if in_h <= 0:
        return None
    return out_h / in_h


async def probe_pair(session, pair):
    """Poll all DEXes for a pair. Return dict keyed by DEX name."""
    in_mint, out_mint, amount = pair
    tasks = [jupiter_quote(session, in_mint, out_mint, amount, d) for d in DEXES]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    out = {}
    for dex, q in zip(DEXES, results):
        if isinstance(q, Exception) or q is None:
            continue
        price = price_from_quote(in_mint, out_mint, amount, q)
        if price is None:
            continue
        out[dex] = {"price": price, "slot": q["slot"], "usd": q["usd"],
                    "pools": q["route_pools"]}
    return out


def compute_max_divergence(prices_by_dex):
    """Returns (max_dex, min_dex, divergence_pct) or None."""
    if len(prices_by_dex) < 2:
        return None
    items = [(d, v["price"]) for d, v in prices_by_dex.items()]
    items.sort(key=lambda x: x[1], reverse=True)
    hi_d, hi_p = items[0]
    lo_d, lo_p = items[-1]
    if lo_p <= 0:
        return None
    return (hi_d, lo_d, (hi_p - lo_p) / lo_p * 100)


async def run_probe(duration_sec):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    tick_log = open(OUTPUT_DIR / "solana_b3.jsonl", "a")
    event_log = open(OUTPUT_DIR / "solana_b3_events.jsonl", "a")

    pair_labels = [f"{SYMBOLS.get(p[0], p[0][:6])}→{SYMBOLS.get(p[1], p[1][:6])}"
                   for p in WATCHLIST]
    print(f"B3 probe: {len(WATCHLIST)} pairs × {len(DEXES)} DEXes, "
          f"{POLL_INTERVAL_SEC}s interval, duration {duration_sec}s "
          f"({duration_sec/3600:.1f}h)")
    print(f"Pairs: {', '.join(pair_labels)}")

    start = time.time()
    end = start + duration_sec
    pending_followups = []

    stats = defaultdict(lambda: {
        "ticks": 0, "two_dex_ticks": 0, "div_above_fees": 0,
        "followup_closed": 0, "followup_persisted": 0,
    })

    async with aiohttp.ClientSession() as session:
        while time.time() < end:
            tick_start = time.time()

            for pair, label in zip(WATCHLIST, pair_labels):
                result = await probe_pair(session, pair)
                record = {"ts": tick_start, "pair": label,
                          "in_mint": pair[0], "out_mint": pair[1],
                          "in_amount": pair[2],
                          "prices_by_dex": {d: v["price"] for d, v in result.items()},
                          "slots_by_dex": {d: v["slot"] for d, v in result.items()},
                          "usd_by_dex": {d: v["usd"] for d, v in result.items()},
                          "pools_by_dex": {d: v["pools"] for d, v in result.items()}}
                tick_log.write(json.dumps(record) + "\n")

                stats[label]["ticks"] += 1
                if len(result) >= 2:
                    stats[label]["two_dex_ticks"] += 1
                    div = compute_max_divergence(result)
                    if div and div[2] > ROUND_TRIP_FEE * 100:
                        stats[label]["div_above_fees"] += 1
                        pending_followups.append({
                            "fire_at": tick_start + FOLLOWUP_DELAY_SEC,
                            "pair": pair,
                            "label": label,
                            "initial_ts": tick_start,
                            "initial_div": div[2],
                            "initial_hi": div[0],
                            "initial_lo": div[1],
                            "initial_result": result,
                        })

                # Tiny sleep between pairs to spread requests
                await asyncio.sleep(0.3)

            # Process due follow-ups
            now = time.time()
            still_pending = []
            for fu in pending_followups:
                if now < fu["fire_at"]:
                    still_pending.append(fu)
                    continue
                new_result = await probe_pair(session, fu["pair"])
                new_div = compute_max_divergence(new_result) if len(new_result) >= 2 else None
                event = {
                    "ts_initial": fu["initial_ts"],
                    "ts_followup": now,
                    "pair": fu["label"],
                    "initial_div": fu["initial_div"],
                    "followup_div": new_div[2] if new_div else None,
                    "initial_hi_dex": fu["initial_hi"],
                    "initial_lo_dex": fu["initial_lo"],
                    "closed": new_div is None or new_div[2] < 0.2,
                    "persisted": new_div is not None and new_div[2] > ROUND_TRIP_FEE * 100,
                    "initial_prices": {d: v["price"] for d, v in fu["initial_result"].items()},
                    "followup_prices": {d: v["price"] for d, v in new_result.items()},
                }
                event_log.write(json.dumps(event) + "\n")
                if event["closed"]:
                    stats[fu["label"]]["followup_closed"] += 1
                elif event["persisted"]:
                    stats[fu["label"]]["followup_persisted"] += 1
            pending_followups = still_pending

            # Status every 60s
            if int(tick_start) % 60 < POLL_INTERVAL_SEC:
                tick_log.flush()
                event_log.flush()
                mins = (tick_start - start) / 60
                print(f"\n[{mins:.1f}min elapsed]")
                for k, v in stats.items():
                    print(f"  {k}: ticks={v['ticks']} 2+dex={v['two_dex_ticks']} "
                          f"above_fees={v['div_above_fees']} closed={v['followup_closed']} "
                          f"persisted={v['followup_persisted']}")

            # Pace
            elapsed = time.time() - tick_start
            sleep_for = max(0, POLL_INTERVAL_SEC - elapsed)
            await asyncio.sleep(sleep_for)

    tick_log.close()
    event_log.close()

    hours = (time.time() - start) / 3600
    mult = 24 / max(hours, 0.001)
    print("\n=== FINAL STATS ===")
    for k, v in stats.items():
        print(f"  {k}: {dict(v)}")
    print("\n=== PROJECTED DAILY ===")
    for k, v in stats.items():
        daily = v["div_above_fees"] * mult
        denom = max(v["followup_closed"] + v["followup_persisted"], 1)
        close_rate = v["followup_closed"] / denom * 100
        print(f"  {k}: {daily:.0f} profitable episodes/day "
              f"(close rate {close_rate:.0f}% within 30s)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=int, default=86400)
    args = ap.parse_args()
    asyncio.run(run_probe(args.duration))
