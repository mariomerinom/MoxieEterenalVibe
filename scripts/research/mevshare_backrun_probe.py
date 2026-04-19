#!/usr/bin/env python3
"""
MEV-Share Backrun Opportunity Probe

Connects to the Flashbots MEV-Share SSE stream and cross-references every
hint against our actual pool universe. For hints that touch our pools, it
fetches current reserves/prices, simulates the hint's likely impact on pool
state, and evaluates whether a profitable backrun arb exists.

This answers the question: "How many real, actionable backrun opportunities
per day does MEV-Share give us, and what's the profit distribution?"

Outputs:
  mevshare_backrun_hints.jsonl  — every hint with match/arb analysis
  mevshare_backrun_summary.json — aggregated statistics

Usage:
  python mevshare_backrun_probe.py --duration-minutes 60 [--pool-file data/pool_tokens.json]

Requires: requests, web3 (pip install requests web3)
"""

import argparse
import json
import os
import signal
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

MEVSHARE_SSE_URL = "https://mev-share.flashbots.net"

# Uniswap V2 Swap event topic
SWAP_V2_TOPIC = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
# Uniswap V3 Swap event topic
SWAP_V3_TOPIC = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
# Sync event (V2 reserve update)
SYNC_V2_TOPIC = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"

_stop = False

def _handle_signal(signum, frame):
    global _stop
    _stop = True

signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


def load_pool_universe(path: str) -> dict:
    """Load our pool universe. Returns {lowercase_address: pool_info}."""
    with open(path) as f:
        data = json.load(f)

    pools = {}
    if isinstance(data, dict) and "pools" in data:
        # New format
        for entry in data["pools"]:
            addr = entry.get("address", "").lower()
            if addr:
                pools[addr] = entry
    elif isinstance(data, dict):
        # Legacy format: {address: {token0, token1, ...}}
        for addr, info in data.items():
            pools[addr.lower()] = info
    return pools


def build_arb_index(pools: dict) -> dict:
    """
    Build an index: pool_address -> list of other pools sharing a token pair.
    This lets us quickly find arb counterparts when a hint touches a pool.
    """
    # Index by token pair (sorted tuple)
    pair_to_pools = defaultdict(list)
    for addr, info in pools.items():
        t0 = info.get("token0", "").lower()
        t1 = info.get("token1", "").lower()
        if t0 and t1:
            pair = tuple(sorted([t0, t1]))
            pair_to_pools[pair].append(addr)

    # For each pool, find counterpart pools (same pair, different pool)
    arb_partners = {}
    for addr, info in pools.items():
        t0 = info.get("token0", "").lower()
        t1 = info.get("token1", "").lower()
        pair = tuple(sorted([t0, t1]))
        partners = [p for p in pair_to_pools[pair] if p != addr]
        if partners:
            arb_partners[addr] = partners
    return arb_partners


def iter_sse_events(response):
    """Yield (event_type, data_str) from SSE stream."""
    event_type = "message"
    data_lines = []

    for raw_line in response.iter_lines(decode_unicode=True):
        if raw_line is None:
            continue
        line = raw_line.rstrip("\r\n") if isinstance(raw_line, str) else raw_line.decode("utf-8").rstrip("\r\n")
        if line == "":
            if data_lines:
                yield event_type, "\n".join(data_lines)
            event_type = "message"
            data_lines = []
            continue
        if line.startswith(":"):
            continue
        if ":" in line:
            field, _, value = line.partition(":")
            value = value.lstrip(" ")
            if field == "event":
                event_type = value
            elif field == "data":
                data_lines.append(value)


def classify_log_event(topic0: str) -> str:
    """Map topic0 to a human-readable event name."""
    known = {
        SWAP_V2_TOPIC: "SwapV2",
        SWAP_V3_TOPIC: "SwapV3",
        SYNC_V2_TOPIC: "SyncV2",
        "0x7a53080ba414158be7ec69b987b5fbbf07cad4fcb4a4a862e8b2e71e2694ee9e": "MintV3",
        "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c": "BurnV3",
    }
    return known.get(topic0, "Unknown")


def analyze_hint(hint: dict, pool_universe: dict, arb_partners: dict) -> dict:
    """
    Analyze a single hint against our pool universe.

    Returns a dict with:
      matched: bool — does this hint touch any of our pools?
      matched_pools: list — which of our pools are affected
      has_arb_partner: bool — does the affected pool have a counterpart?
      swap_direction: str — what kind of swap event was logged
      backrunnable: bool — logs present + matches our pools + has counterpart
    """
    result = {
        "matched": False,
        "matched_pools": [],
        "matched_protocols": [],
        "swap_events": [],
        "has_arb_partner": False,
        "arb_partner_count": 0,
        "backrunnable": False,
        "hint_class": "hash_only",
    }

    logs = hint.get("logs") or []
    txs = hint.get("txs") or []
    has_logs = bool(logs)
    has_txs = bool(txs)

    if has_logs and has_txs:
        result["hint_class"] = "full"
    elif has_logs:
        result["hint_class"] = "logs_only"
    elif has_txs:
        result["hint_class"] = "txs_only"

    if not has_logs:
        return result

    for log in logs:
        addr = (log.get("address") or "").lower()
        topics = log.get("topics") or []
        topic0 = topics[0] if topics else ""

        event_name = classify_log_event(topic0)

        if addr in pool_universe:
            pool_info = pool_universe[addr]
            protocol = pool_info.get("protocol", "unknown")
            result["matched"] = True
            result["matched_pools"].append(addr)
            result["matched_protocols"].append(protocol)
            result["swap_events"].append({
                "pool": addr,
                "event": event_name,
                "protocol": protocol,
                "token0": pool_info.get("token0", ""),
                "token1": pool_info.get("token1", ""),
                "symbol0": pool_info.get("symbol0", ""),
                "symbol1": pool_info.get("symbol1", ""),
            })

            if addr in arb_partners:
                result["has_arb_partner"] = True
                result["arb_partner_count"] = max(
                    result["arb_partner_count"],
                    len(arb_partners[addr])
                )

    # Backrunnable = has logs + matches our pool + has a counterpart pool for arb
    if result["matched"] and result["has_arb_partner"]:
        # Only count swap events as backrunnable (not mints/burns)
        swap_events = [e for e in result["swap_events"]
                       if e["event"] in ("SwapV2", "SwapV3")]
        if swap_events:
            result["backrunnable"] = True

    return result


def collect(duration_seconds: int, output_dir: str, pool_universe: dict,
            arb_partners: dict, verbose: bool) -> dict:
    """Stream hints, analyze each against pool universe, write JSONL."""
    try:
        import requests
    except ImportError:
        print("ERROR: pip install requests", file=sys.stderr)
        sys.exit(1)

    os.makedirs(output_dir, exist_ok=True)
    hints_path = os.path.join(output_dir, "mevshare_backrun_hints.jsonl")

    stats = {
        "start_ts": time.time(),
        "total_hints": 0,
        "hints_with_logs": 0,
        "matched_our_pools": 0,
        "has_arb_partner": 0,
        "backrunnable": 0,
        "hint_classes": Counter(),
        "matched_pool_counts": Counter(),
        "matched_protocol_counts": Counter(),
        "swap_event_counts": Counter(),
        "pair_hit_counts": Counter(),  # which token pairs get hit
        "backrunnable_pools": Counter(),
        "errors": 0,
        "reconnects": 0,
    }

    deadline = time.time() + duration_seconds

    print(f"[{_now()}] MEV-Share Backrun Opportunity Probe")
    print(f"[{_now()}] Pool universe: {len(pool_universe)} pools, "
          f"{sum(1 for p in pool_universe if p in arb_partners)} with arb partners")
    print(f"[{_now()}] Collecting for {duration_seconds}s until {_fmt_ts(deadline)}")
    print(f"[{_now()}] Output: {hints_path}")
    print()

    with open(hints_path, "a", encoding="utf-8") as fout:
        while not _stop and time.time() < deadline:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            try:
                headers = {
                    "Accept": "text/event-stream",
                    "Cache-Control": "no-cache",
                }
                with requests.get(
                    MEVSHARE_SSE_URL,
                    headers=headers,
                    stream=True,
                    timeout=(10, 300),
                ) as resp:
                    resp.raise_for_status()
                    for event_type, data_str in iter_sse_events(resp):
                        if _stop or time.time() >= deadline:
                            break

                        data_str = data_str.strip()
                        if not data_str or data_str == "null":
                            continue

                        try:
                            hint = json.loads(data_str)
                        except json.JSONDecodeError:
                            stats["errors"] += 1
                            continue

                        stats["total_hints"] += 1
                        has_logs = bool(hint.get("logs"))
                        if has_logs:
                            stats["hints_with_logs"] += 1

                        # Analyze against our pool universe
                        analysis = analyze_hint(hint, pool_universe, arb_partners)

                        stats["hint_classes"][analysis["hint_class"]] += 1

                        if analysis["matched"]:
                            stats["matched_our_pools"] += 1
                            for pool in analysis["matched_pools"]:
                                stats["matched_pool_counts"][pool] += 1
                                # Track pair
                                info = pool_universe.get(pool, {})
                                s0 = info.get("symbol0", info.get("token0", "?")[:8])
                                s1 = info.get("symbol1", info.get("token1", "?")[:8])
                                stats["pair_hit_counts"][f"{s0}/{s1}"] += 1
                            for proto in analysis["matched_protocols"]:
                                stats["matched_protocol_counts"][proto] += 1
                            for ev in analysis["swap_events"]:
                                stats["swap_event_counts"][ev["event"]] += 1

                        if analysis["has_arb_partner"]:
                            stats["has_arb_partner"] += 1

                        if analysis["backrunnable"]:
                            stats["backrunnable"] += 1
                            for pool in analysis["matched_pools"]:
                                stats["backrunnable_pools"][pool] += 1

                        # Write to JSONL — keep full raw hint for backrunnable
                        record = {
                            "ts": time.time(),
                            "hash": hint.get("hash", ""),
                            "hint_class": analysis["hint_class"],
                            "matched": analysis["matched"],
                            "backrunnable": analysis["backrunnable"],
                            "matched_pools": analysis["matched_pools"],
                            "swap_events": analysis["swap_events"],
                            "arb_partner_count": analysis["arb_partner_count"],
                        }
                        # For matched hints, preserve raw hint data for replay
                        if analysis["matched"]:
                            record["raw_logs"] = hint.get("logs")
                            record["raw_txs"] = hint.get("txs")
                            record["mevGasPrice"] = hint.get("mevGasPrice")
                            record["gasUsed"] = hint.get("gasUsed")
                        fout.write(json.dumps(record) + "\n")

                        # Progress
                        n = stats["total_hints"]
                        if verbose and analysis["backrunnable"]:
                            pairs = [f"{e['symbol0']}/{e['symbol1']}" for e in analysis["swap_events"]]
                            print(f"  #{n:<6} BACKRUNNABLE  pools={analysis['matched_pools'][:2]}  "
                                  f"pairs={pairs}  partners={analysis['arb_partner_count']}")
                        elif n % 100 == 0:
                            elapsed = time.time() - stats["start_ts"]
                            rate = n / elapsed * 60 if elapsed > 0 else 0
                            print(
                                f"[{_now()}] hints={n:>6}  matched={stats['matched_our_pools']:>5}  "
                                f"backrunnable={stats['backrunnable']:>4}  rate={rate:.0f}/min"
                            )
                            fout.flush()

            except Exception as exc:
                stats["reconnects"] += 1
                print(f"[{_now()}] Connection error: {exc} — reconnecting ({stats['reconnects']})...")
                time.sleep(5)

    stats["end_ts"] = time.time()
    stats["elapsed_seconds"] = stats["end_ts"] - stats["start_ts"]
    return stats


def build_summary(stats: dict, output_dir: str) -> dict:
    elapsed = max(stats["elapsed_seconds"], 1)
    total = max(stats["total_hints"], 1)
    hints_per_day = stats["total_hints"] / elapsed * 86400

    matched_rate = stats["matched_our_pools"] / total * 100
    backrunnable_rate = stats["backrunnable"] / total * 100
    backrunnable_of_matched = (
        stats["backrunnable"] / stats["matched_our_pools"] * 100
        if stats["matched_our_pools"] > 0 else 0
    )

    backrunnable_per_day = stats["backrunnable"] / elapsed * 86400

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "collection": {
            "elapsed_seconds": round(elapsed, 1),
            "elapsed_minutes": round(elapsed / 60, 1),
            "reconnects": stats["reconnects"],
            "errors": stats["errors"],
        },
        "funnel": {
            "total_hints": stats["total_hints"],
            "hints_with_logs": stats["hints_with_logs"],
            "matched_our_pools": stats["matched_our_pools"],
            "has_arb_partner": stats["has_arb_partner"],
            "backrunnable": stats["backrunnable"],
        },
        "rates": {
            "hints_per_day": round(hints_per_day),
            "matched_per_day": round(stats["matched_our_pools"] / elapsed * 86400),
            "backrunnable_per_day": round(backrunnable_per_day),
            "pct_matched": round(matched_rate, 2),
            "pct_backrunnable_of_total": round(backrunnable_rate, 2),
            "pct_backrunnable_of_matched": round(backrunnable_of_matched, 2),
        },
        "hint_class_distribution": dict(stats["hint_classes"]),
        "swap_event_types": dict(stats["swap_event_counts"]),
        "top_matched_pools": [
            {"address": a, "count": c}
            for a, c in stats["matched_pool_counts"].most_common(20)
        ],
        "top_pairs_hit": [
            {"pair": p, "count": c}
            for p, c in stats["pair_hit_counts"].most_common(20)
        ],
        "top_backrunnable_pools": [
            {"address": a, "count": c}
            for a, c in stats["backrunnable_pools"].most_common(20)
        ],
        "protocol_distribution": dict(stats["matched_protocol_counts"]),
    }

    summary_path = os.path.join(output_dir, "mevshare_backrun_summary.json")
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)
    return summary


def print_summary(summary: dict):
    sep = "=" * 65
    c = summary["collection"]
    fun = summary["funnel"]
    r = summary["rates"]

    print(f"\n{sep}")
    print("MEV-SHARE BACKRUN OPPORTUNITY PROBE")
    print(sep)
    print(f"  Duration: {c['elapsed_minutes']:.1f} min  "
          f"({c['reconnects']} reconnects, {c['errors']} errors)")

    print(f"\n  FUNNEL:")
    labels = [
        ("Total hints", fun["total_hints"]),
        ("  with logs", fun["hints_with_logs"]),
        ("  matched our pools", fun["matched_our_pools"]),
        ("  has arb partner", fun["has_arb_partner"]),
        ("  BACKRUNNABLE", fun["backrunnable"]),
    ]
    for label, val in labels:
        pct = val / max(fun["total_hints"], 1) * 100
        bar = "#" * int(pct / 2)
        print(f"    {label:<22} {val:>6}  ({pct:>5.1f}%)  {bar}")

    print(f"\n  DAILY PROJECTIONS:")
    print(f"    Hints/day:              {r['hints_per_day']:>8,}")
    print(f"    Matched/day:            {r['matched_per_day']:>8,}")
    print(f"    Backrunnable/day:       {r['backrunnable_per_day']:>8,}")
    print(f"    Match rate:             {r['pct_matched']:>7.1f}%")
    print(f"    Backrunnable (of total):{r['pct_backrunnable_of_total']:>7.1f}%")
    print(f"    Backrunnable (of match):{r['pct_backrunnable_of_matched']:>7.1f}%")

    if summary.get("top_pairs_hit"):
        print(f"\n  TOP PAIRS HIT:")
        for entry in summary["top_pairs_hit"][:10]:
            print(f"    {entry['count']:>5}x  {entry['pair']}")

    if summary.get("top_backrunnable_pools"):
        print(f"\n  TOP BACKRUNNABLE POOLS:")
        for entry in summary["top_backrunnable_pools"][:10]:
            print(f"    {entry['count']:>5}x  {entry['address']}")

    if summary.get("protocol_distribution"):
        print(f"\n  PROTOCOL DISTRIBUTION (matched hints):")
        for proto, count in sorted(summary["protocol_distribution"].items(),
                                   key=lambda x: -x[1]):
            print(f"    {proto:<15} {count:>5}")

    if summary.get("swap_event_types"):
        print(f"\n  SWAP EVENT TYPES:")
        for ev, count in sorted(summary["swap_event_types"].items(),
                                key=lambda x: -x[1]):
            print(f"    {ev:<15} {count:>5}")

    print(sep)


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S")

def _fmt_ts(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M:%S UTC")


def main():
    parser = argparse.ArgumentParser(
        description="Probe MEV-Share for backrun opportunities against our pool universe."
    )
    parser.add_argument("--duration-minutes", type=float, default=60.0,
                        help="Collection duration (default: 60 min)")
    parser.add_argument("--pool-file", default="data/pool_tokens.json",
                        help="Path to pool_tokens JSON (default: data/pool_tokens.json)")
    parser.add_argument("--output-dir", default="/root/mev/research/data",
                        help="Output directory")
    parser.add_argument("--verbose", action="store_true",
                        help="Print every backrunnable hint")
    args = parser.parse_args()

    pool_file = args.pool_file
    if not Path(pool_file).exists():
        # Try relative to script location
        alt = Path(__file__).parent.parent.parent / pool_file
        if alt.exists():
            pool_file = str(alt)
        else:
            print(f"Pool file not found: {pool_file}")
            sys.exit(1)

    print(f"[{_now()}] Loading pool universe from {pool_file}")
    pool_universe = load_pool_universe(pool_file)
    print(f"[{_now()}] Loaded {len(pool_universe)} pools")

    arb_partners = build_arb_index(pool_universe)
    pools_with_partners = sum(1 for p in pool_universe if p in arb_partners)
    print(f"[{_now()}] {pools_with_partners} pools have arb partners "
          f"({pools_with_partners/len(pool_universe)*100:.0f}%)")

    duration_seconds = int(args.duration_minutes * 60)
    stats = collect(duration_seconds, args.output_dir, pool_universe,
                    arb_partners, args.verbose)

    if stats["total_hints"] == 0:
        print("\nNo hints received.")
        sys.exit(1)

    summary = build_summary(stats, args.output_dir)
    print_summary(summary)

    summary_path = os.path.join(args.output_dir, "mevshare_backrun_summary.json")
    print(f"\n[{_now()}] Summary: {summary_path}")
    print(f"[{_now()}] Hints:   {os.path.join(args.output_dir, 'mevshare_backrun_hints.jsonl')}")


if __name__ == "__main__":
    main()
