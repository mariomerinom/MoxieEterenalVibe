#!/usr/bin/env python3
"""
MEV-Share SSE Probe — Phase 4 Step 7

Connects to the Flashbots MEV-Share event stream (mainnet) and collects
hints for a configurable duration.  After collection it prints and saves
a summary that answers:

  • How many backrunnable hints per day?
  • Which pool addresses appear most often?
  • What are the hint types / data richness levels?
  • How many distinct "searcher-visible" bundles are flowing?

Output files (on the droplet):
  /root/mev/research/data/mevshare_hints.jsonl   — one JSON object per hint
  /root/mev/research/data/mevshare_summary.json  — aggregated statistics

Usage:
  python mevshare_probe.py [--duration-minutes 30] [--output-dir /root/mev/research/data]
"""

import argparse
import json
import os
import signal
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# SSE stream URL — mainnet MEV-Share event stream
# ---------------------------------------------------------------------------
MEVSHARE_SSE_URL = "https://mev-share.flashbots.net"


# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------
_stop = False


def _handle_signal(signum, frame):
    global _stop
    _stop = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ---------------------------------------------------------------------------
# SSE parser — works with raw requests (no sseclient dependency required)
# ---------------------------------------------------------------------------

def iter_sse_events(response):
    """
    Yield (event_type, data_str) pairs from a streaming SSE response.
    Handles the 'event:' and 'data:' field lines specified by the SSE spec.
    """
    event_type = "message"
    data_lines = []

    for raw_line in response.iter_lines(decode_unicode=True):
        if raw_line is None:
            continue

        line = raw_line.rstrip("\r\n") if isinstance(raw_line, str) else raw_line.decode("utf-8").rstrip("\r\n")

        if line == "":
            # Blank line = dispatch event
            if data_lines:
                yield event_type, "\n".join(data_lines)
            event_type = "message"
            data_lines = []
            continue

        if line.startswith(":"):
            # Comment / keep-alive — ignore
            continue

        if ":" in line:
            field, _, value = line.partition(":")
            value = value.lstrip(" ")
            if field == "event":
                event_type = value
            elif field == "data":
                data_lines.append(value)
            # id / retry fields are ignored for our purposes
        else:
            # Field with no value
            if line == "data":
                data_lines.append("")


# ---------------------------------------------------------------------------
# Hint parsing helpers
# ---------------------------------------------------------------------------

def extract_hint_features(hint: dict) -> dict:
    """
    Return a flat feature dict from a raw MEV-Share hint object.

    MEV-Share hint schema (approximate):
    {
      "hash": "0x...",               # pending tx / bundle hash
      "logs": [                       # optional — only when revealed
        {
          "address": "0x...",         # emitting contract (pool)
          "topics": ["0x...", ...],
          "data": "0x..."
        }
      ],
      "txs": [                        # optional transaction hashes
        {
          "to": "0x...",
          "functionSelector": "0x...",
          "callData": "0x...",        # may be absent
          "value": "0x..."            # may be absent
        }
      ],
      "mevGasPrice": "0x...",        # optional
      "gasUsed": "0x..."             # optional
    }
    """
    features = {
        "hash": hint.get("hash", ""),
        "has_logs": bool(hint.get("logs")),
        "log_count": len(hint.get("logs") or []),
        "pool_addresses": [],
        "log_topics_0": [],            # first topic = event signature
        "has_txs": bool(hint.get("txs")),
        "tx_count": len(hint.get("txs") or []),
        "to_addresses": [],
        "function_selectors": [],
        "has_mev_gas_price": "mevGasPrice" in hint,
        "has_gas_used": "gasUsed" in hint,
    }

    for log in hint.get("logs") or []:
        addr = (log.get("address") or "").lower()
        if addr:
            features["pool_addresses"].append(addr)
        topics = log.get("topics") or []
        if topics:
            features["log_topics_0"].append(topics[0])

    for tx in hint.get("txs") or []:
        to = (tx.get("to") or "").lower()
        if to:
            features["to_addresses"].append(to)
        sel = tx.get("functionSelector") or ""
        if sel:
            features["function_selectors"].append(sel)

    return features


def classify_hint(features: dict) -> str:
    """
    Return a human-readable richness label.

      full       — logs + txs present (most backrunnable)
      logs_only  — logs present, no tx details
      txs_only   — tx details present, no logs
      hash_only  — just the hash (least information)
    """
    has_logs = features["has_logs"]
    has_txs = features["has_txs"]
    if has_logs and has_txs:
        return "full"
    if has_logs:
        return "logs_only"
    if has_txs:
        return "txs_only"
    return "hash_only"


# ---------------------------------------------------------------------------
# Main collection loop
# ---------------------------------------------------------------------------

def collect(duration_seconds: int, output_dir: str, verbose: bool) -> dict:
    """
    Stream MEV-Share hints and write each to a JSONL file.
    Returns raw statistics used to build the summary.
    """
    try:
        import requests
    except ImportError:
        print("ERROR: 'requests' library not found.  Install it with:  pip install requests", file=sys.stderr)
        sys.exit(1)

    os.makedirs(output_dir, exist_ok=True)
    hints_path = os.path.join(output_dir, "mevshare_hints.jsonl")

    stats = {
        "start_ts": time.time(),
        "total_hints": 0,
        "hints_with_logs": 0,
        "hints_with_txs": 0,
        "hint_types": defaultdict(int),       # full / logs_only / txs_only / hash_only
        "pool_address_counts": defaultdict(int),
        "function_selector_counts": defaultdict(int),
        "log_topic0_counts": defaultdict(int),
        "errors": 0,
        "reconnects": 0,
    }

    deadline = time.time() + duration_seconds
    print(f"[{_now()}] Connecting to MEV-Share SSE stream: {MEVSHARE_SSE_URL}")
    print(f"[{_now()}] Will collect for {duration_seconds}s  →  until {_fmt_ts(deadline)}")
    print(f"[{_now()}] Writing hints to: {hints_path}")
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
                    "X-Client": "mev-research-probe/1.0",
                }
                with requests.get(
                    MEVSHARE_SSE_URL,
                    headers=headers,
                    stream=True,
                    timeout=(10, 300),   # (connect timeout, read timeout)
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

                        # Annotate with receipt timestamp
                        hint["_received_at"] = time.time()
                        hint["_event_type"] = event_type

                        features = extract_hint_features(hint)
                        hint_class = classify_hint(features)

                        # Accumulate stats
                        stats["total_hints"] += 1
                        if features["has_logs"]:
                            stats["hints_with_logs"] += 1
                        if features["has_txs"]:
                            stats["hints_with_txs"] += 1
                        stats["hint_types"][hint_class] += 1
                        for addr in features["pool_addresses"]:
                            stats["pool_address_counts"][addr] += 1
                        for sel in features["function_selectors"]:
                            stats["function_selector_counts"][sel] += 1
                        for t0 in features["log_topics_0"]:
                            stats["log_topic0_counts"][t0] += 1

                        # Write raw hint (with features) to JSONL
                        record = {**hint, "_features": features, "_class": hint_class}
                        fout.write(json.dumps(record) + "\n")

                        if verbose:
                            _print_hint(stats["total_hints"], features, hint_class)
                        elif stats["total_hints"] % 50 == 0:
                            elapsed = time.time() - stats["start_ts"]
                            rate = stats["total_hints"] / elapsed * 60 if elapsed > 0 else 0
                            print(
                                f"[{_now()}] hints={stats['total_hints']:>6}  "
                                f"w/logs={stats['hints_with_logs']:>5}  "
                                f"rate={rate:>6.1f}/min"
                            )
                            fout.flush()

            except requests.exceptions.Timeout:
                stats["reconnects"] += 1
                print(f"[{_now()}] Connection timed out — reconnecting ({stats['reconnects']})...")
                time.sleep(2)
            except requests.exceptions.ConnectionError as exc:
                stats["reconnects"] += 1
                print(f"[{_now()}] Connection error: {exc} — reconnecting ({stats['reconnects']})...")
                time.sleep(5)
            except requests.exceptions.HTTPError as exc:
                print(f"[{_now()}] HTTP error: {exc}", file=sys.stderr)
                stats["errors"] += 1
                break
            except Exception as exc:
                stats["reconnects"] += 1
                print(f"[{_now()}] Unexpected error: {exc} — reconnecting ({stats['reconnects']})...")
                time.sleep(5)

    stats["end_ts"] = time.time()
    stats["elapsed_seconds"] = stats["end_ts"] - stats["start_ts"]
    return stats


# ---------------------------------------------------------------------------
# Summary generation
# ---------------------------------------------------------------------------

def build_summary(stats: dict, output_dir: str) -> dict:
    elapsed = max(stats["elapsed_seconds"], 1)
    hints_per_second = stats["total_hints"] / elapsed
    hints_per_minute = hints_per_second * 60
    hints_per_day = hints_per_second * 86400

    logs_pct = (stats["hints_with_logs"] / stats["total_hints"] * 100) if stats["total_hints"] else 0
    backrunnable_per_day = hints_per_day * (logs_pct / 100)

    # Top pools by mention count
    top_pools = sorted(
        stats["pool_address_counts"].items(),
        key=lambda kv: -kv[1]
    )[:50]

    # Top function selectors
    top_selectors = sorted(
        stats["function_selector_counts"].items(),
        key=lambda kv: -kv[1]
    )[:20]

    # Top log event signatures (topic[0])
    top_topics = sorted(
        stats["log_topic0_counts"].items(),
        key=lambda kv: -kv[1]
    )[:20]

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "stream_url": MEVSHARE_SSE_URL,
        "collection": {
            "start": datetime.fromtimestamp(stats["start_ts"], tz=timezone.utc).isoformat(),
            "end": datetime.fromtimestamp(stats["end_ts"], tz=timezone.utc).isoformat(),
            "elapsed_seconds": round(elapsed, 1),
            "reconnects": stats["reconnects"],
            "parse_errors": stats["errors"],
        },
        "totals": {
            "total_hints": stats["total_hints"],
            "hints_with_logs": stats["hints_with_logs"],
            "hints_with_txs": stats["hints_with_txs"],
            "pct_with_logs": round(logs_pct, 2),
        },
        "rates": {
            "hints_per_minute": round(hints_per_minute, 2),
            "hints_per_day_estimated": round(hints_per_day),
            "backrunnable_per_day_estimated": round(backrunnable_per_day),
        },
        "hint_type_distribution": dict(stats["hint_types"]),
        "unique_pools_seen": len(stats["pool_address_counts"]),
        "top_50_pools": [{"address": a, "count": c} for a, c in top_pools],
        "top_20_function_selectors": [{"selector": s, "count": c} for s, c in top_selectors],
        "top_20_event_topic0": [{"topic0": t, "count": c} for t, c in top_topics],
    }

    summary_path = os.path.join(output_dir, "mevshare_summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    print(f"\n[{_now()}] Summary written to: {summary_path}")
    return summary


def print_summary(summary: dict):
    sep = "=" * 65
    c = summary["collection"]
    t = summary["totals"]
    r = summary["rates"]

    print(f"\n{sep}")
    print("MEV-SHARE PROBE SUMMARY")
    print(sep)
    print(f"  Stream URL     : {summary['stream_url']}")
    print(f"  Duration       : {c['elapsed_seconds']:.0f}s  "
          f"({c['elapsed_seconds']/60:.1f} min)")
    print(f"  Reconnects     : {c['reconnects']}")
    print(f"  Parse errors   : {c['parse_errors']}")
    print()
    print(f"  Total hints        : {t['total_hints']:>8,}")
    print(f"  Hints with logs    : {t['hints_with_logs']:>8,}  ({t['pct_with_logs']:.1f}%)")
    print(f"  Hints with txs     : {t['hints_with_txs']:>8,}")
    print()
    print(f"  Rate  (hints/min)  : {r['hints_per_minute']:>8.1f}")
    print(f"  Est. hints/day     : {r['hints_per_day_estimated']:>8,}")
    print(f"  Est. backrunnable/day : {r['backrunnable_per_day_estimated']:>5,}  "
          f"(hints with logs)")
    print()

    dist = summary["hint_type_distribution"]
    total = t["total_hints"] or 1
    print("  Hint type distribution:")
    for label in ("full", "logs_only", "txs_only", "hash_only"):
        n = dist.get(label, 0)
        bar = "#" * int(n / total * 40)
        print(f"    {label:<12} {n:>6,}  {n/total*100:>5.1f}%  {bar}")

    print()
    print(f"  Unique pool addresses seen : {summary['unique_pools_seen']:,}")
    if summary["top_50_pools"]:
        print("  Top 10 pools by hint count:")
        for entry in summary["top_50_pools"][:10]:
            print(f"    {entry['address']}  ×{entry['count']}")

    if summary["top_20_function_selectors"]:
        print()
        print("  Top function selectors:")
        for entry in summary["top_20_function_selectors"][:10]:
            print(f"    {entry['selector']}  ×{entry['count']}")

    if summary["top_20_event_topic0"]:
        print()
        print("  Top log event signatures (topic[0]):")
        for entry in summary["top_20_event_topic0"][:10]:
            print(f"    {entry['topic0']}  ×{entry['count']}")

    print(sep)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def _fmt_ts(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M:%S UTC")


def _print_hint(n: int, features: dict, hint_class: str):
    pools = ",".join(features["pool_addresses"][:3])
    if len(features["pool_addresses"]) > 3:
        pools += f"+{len(features['pool_addresses'])-3}"
    print(
        f"  #{n:<6} [{hint_class:<10}]  "
        f"logs={features['log_count']}  txs={features['tx_count']}  "
        f"pools=[{pools}]  hash={features['hash'][:12]}…"
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Probe the MEV-Share SSE stream and analyse hints."
    )
    parser.add_argument(
        "--duration-minutes",
        type=float,
        default=30.0,
        help="How long to collect hints (default: 30 minutes)",
    )
    parser.add_argument(
        "--output-dir",
        default="/root/mev/research/data",
        help="Directory for JSONL and summary files (default: /root/mev/research/data)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print one line per hint (instead of every 50)",
    )
    args = parser.parse_args()

    duration_seconds = int(args.duration_minutes * 60)
    stats = collect(duration_seconds, args.output_dir, args.verbose)

    if stats["total_hints"] == 0:
        print("\nNo hints received — check connectivity / stream URL.")
        sys.exit(1)

    summary = build_summary(stats, args.output_dir)
    print_summary(summary)


if __name__ == "__main__":
    main()
