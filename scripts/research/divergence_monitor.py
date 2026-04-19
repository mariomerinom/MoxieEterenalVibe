#!/usr/bin/env python3
"""
Combined CEX-DEX + Cross-Chain Divergence Monitor

Polls:
1. Coinbase Exchange prices (REST API, every poll) for niche tokens
2. On-chain DEX prices (RPC, every 12s) for matching pools

Logs every divergence event >0.1%. Run for 24h.

Answers:
- How often do >0.3% CEX-DEX divergences occur?
- How long do they persist?
- Which tokens have the most opportunities?
- Is $500/day reachable?

Uses Coinbase Exchange (not Binance) because Binance is geo-blocked on some servers.
"""

import json
import os
import signal
import subprocess
import sys
import time
from collections import defaultdict

# ── CONFIG ──
OUTPUT_FILE = "research/data/divergence_events.jsonl"
STATS_INTERVAL = 300  # 5 min
POLL_INTERVAL = 12    # On-chain poll every 12 seconds (1 block)

# Known WETH addresses per chain
WETH = {
    "ethereum": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
    "arbitrum": "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
    "base": "0x4200000000000000000000000000000000000006",
}

CHAIN_RPCS = {}
SHUTDOWN = False


def handle_signal(sig, frame):
    global SHUTDOWN
    print("\nShutting down...")
    SHUTDOWN = True

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


def load_env():
    """Load RPC URLs from .env."""
    rpcs = {}
    for env_path in ["/root/mev/.env", os.path.join(os.path.dirname(__file__), "../../.env")]:
        try:
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if "=" in line and not line.startswith("#"):
                        key, val = line.split("=", 1)
                        rpcs[key.strip()] = val.strip().strip('"').strip("'")
        except FileNotFoundError:
            continue
    return {
        "ethereum": rpcs.get("ETH_RPC_HTTP"),
        "arbitrum": rpcs.get("ARB_RPC_HTTP"),
        "base": rpcs.get("BASE_RPC_HTTP"),
    }


def rpc_call(rpc_url, method, params):
    """Quick RPC call."""
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    try:
        r = subprocess.run(
            ["curl", "-s", "-X", "POST", rpc_url, "-H", "Content-Type: application/json",
             "-d", payload, "--connect-timeout", "5", "--max-time", "10"],
            capture_output=True, text=True, timeout=15
        )
        return json.loads(r.stdout).get("result")
    except Exception:
        return None


def get_v3_price_weth(rpc_url, pool_addr, token0_is_weth, d0, d1):
    """Get price in WETH per token from V3 slot0."""
    result = rpc_call(rpc_url, "eth_call", [
        {"to": pool_addr, "data": "0x3850c7bd"}, "latest"
    ])
    if not result or len(result) < 130:
        return None
    sqp = int(result[2:66], 16)
    if sqp == 0:
        return None
    price_raw = (sqp / (2**96)) ** 2
    price_adj = price_raw * (10**d0) / (10**d1)
    if token0_is_weth:
        return 1.0 / price_adj if price_adj > 0 else None
    else:
        return price_adj


def get_v2_price_weth(rpc_url, pool_addr, token0_is_weth, d0, d1):
    """Get price in WETH per token from V2 reserves."""
    result = rpc_call(rpc_url, "eth_call", [
        {"to": pool_addr, "data": "0x0902f1ac"}, "latest"
    ])
    if not result or len(result) < 130:
        return None
    data = result[2:]
    r0 = int(data[0:64], 16)
    r1 = int(data[64:128], 16)
    if r0 == 0 or r1 == 0:
        return None
    if token0_is_weth:
        return (r0 / 10**d0) / (r1 / 10**d1)
    else:
        return (r1 / 10**d1) / (r0 / 10**d0)


def fetch_coinbase_products():
    """Get all Coinbase Exchange products and current ETH price."""
    try:
        r = subprocess.run(
            ["curl", "-s", "https://api.exchange.coinbase.com/products",
             "--connect-timeout", "10", "--max-time", "15"],
            capture_output=True, text=True, timeout=20
        )
        products = json.loads(r.stdout)
        # Filter to online USD/USDT products
        symbols = {}
        for p in products:
            if p.get("status") == "online" and p["quote_currency"] in ("USD", "USDT"):
                base = p["base_currency"].upper()
                symbols[base] = p["id"]  # e.g., "LINK" -> "LINK-USD"
        return symbols
    except Exception as e:
        print(f"  Error fetching Coinbase products: {e}")
        return {}


def fetch_coinbase_prices(product_ids):
    """Batch fetch current prices from Coinbase for given product IDs.
    Returns {product_id: price_usd}."""
    prices = {}
    # Coinbase doesn't have a batch ticker API, but the exchange-rates endpoint
    # gives us all rates in one call
    try:
        r = subprocess.run(
            ["curl", "-s", "https://api.coinbase.com/v2/exchange-rates?currency=USD",
             "--connect-timeout", "10", "--max-time", "15"],
            capture_output=True, text=True, timeout=20
        )
        data = json.loads(r.stdout)
        rates = data.get("data", {}).get("rates", {})
        # rates[TOKEN] = how many TOKEN per 1 USD → price_usd = 1/rate
        for sym, product_id in product_ids.items():
            if sym in rates:
                rate = float(rates[sym])
                if rate > 0:
                    prices[sym] = 1.0 / rate
        # Also get ETH price
        if "ETH" in rates:
            eth_rate = float(rates["ETH"])
            if eth_rate > 0:
                prices["__ETH_USD__"] = 1.0 / eth_rate
    except Exception as e:
        print(f"  Error fetching Coinbase prices: {e}")
    return prices


def build_monitored_tokens(coinbase_symbols):
    """Build token list from pool data and Coinbase symbols."""
    pool_files = {
        "ethereum": "data/pool_tokens.json",
        "arbitrum": "data/pool_tokens_arbitrum.json",
        "base": "data/pool_tokens_base.json",
    }

    all_pools = {}
    for chain, filepath in pool_files.items():
        try:
            with open(filepath) as f:
                all_pools[chain] = json.load(f)
        except FileNotFoundError:
            all_pools[chain] = {}

    symbol_pools = defaultdict(dict)

    for chain, pools in all_pools.items():
        weth_addr = WETH[chain].lower()
        seen_symbols = {}

        for addr, info in pools.items():
            t0 = info["token0"].lower()
            t1 = info["token1"].lower()

            if t0 != weth_addr and t1 != weth_addr:
                continue

            proto = info.get("protocol", "").lower()
            if "camelot" in proto or "aerodrome" in proto:
                continue

            sym0 = info.get("symbol0", "?")
            sym1 = info.get("symbol1", "?")
            token_sym = sym0 if t0 != weth_addr else sym1
            token_sym = token_sym.upper()

            pool_info = {
                "pool": addr,
                "protocol": proto,
                "decimals0": info.get("decimals0", 18),
                "decimals1": info.get("decimals1", 18),
                "token0_is_weth": t0 == weth_addr,
            }

            if token_sym not in seen_symbols or "v3" in proto:
                seen_symbols[token_sym] = pool_info

        for sym, pool_info in seen_symbols.items():
            symbol_pools[sym][chain] = pool_info

    # Match: tokens on Coinbase AND on at least one DEX chain
    tokens = {}
    for sym, chain_pools in symbol_pools.items():
        if len(chain_pools) == 0:
            continue

        coinbase_id = coinbase_symbols.get(sym)

        if coinbase_id or len(chain_pools) >= 2:
            tokens[sym] = {
                "coinbase_symbol": sym if coinbase_id else None,
                "coinbase_product": coinbase_id,
                "chains": chain_pools,
            }

    return tokens


class DivergenceStats:
    def __init__(self):
        self.start_time = time.time()
        self.polls = 0
        self.cex_dex_events = 0
        self.cex_dex_significant = 0
        self.cex_dex_major = 0
        self.cross_chain_events = 0
        self.cross_chain_significant = 0
        self.by_token = defaultdict(lambda: {"cex_dex": 0, "cross_chain": 0, "max_div": 0.0})
        self.last_print = time.time()

    def print_stats(self):
        elapsed = time.time() - self.start_time
        hours = elapsed / 3600
        daily = 24 / max(hours, 0.001)

        print(f"\n{'='*70}", flush=True)
        print(f"  DIVERGENCE MONITOR — {hours:.2f}h elapsed, {self.polls} poll cycles")
        print(f"{'='*70}")
        print(f"  CEX-DEX divergence events:")
        print(f"    >0.1%: {self.cex_dex_events:,} ({self.cex_dex_events*daily:,.0f}/day)")
        print(f"    >0.3%: {self.cex_dex_significant:,} ({self.cex_dex_significant*daily:,.0f}/day)")
        print(f"    >1.0%: {self.cex_dex_major:,} ({self.cex_dex_major*daily:,.0f}/day)")
        print(f"  Cross-chain divergence events:")
        print(f"    >0.1%: {self.cross_chain_events:,} ({self.cross_chain_events*daily:,.0f}/day)")
        print(f"    >0.3%: {self.cross_chain_significant:,} ({self.cross_chain_significant*daily:,.0f}/day)")

        if self.by_token:
            sorted_tokens = sorted(self.by_token.items(),
                                   key=lambda x: -(x[1]["cex_dex"] + x[1]["cross_chain"]))
            print(f"\n  Top tokens by divergence count:")
            for sym, info in sorted_tokens[:15]:
                total = info["cex_dex"] + info["cross_chain"]
                if total > 0:
                    print(f"    {sym:<10} CEX-DEX: {info['cex_dex']:>4}  cross-chain: {info['cross_chain']:>4}  max: {info['max_div']:.2f}%")

        # Revenue estimate
        if self.cex_dex_significant > 0:
            daily_events = self.cex_dex_significant * daily
            rev = daily_events * 0.003 * 500 * 0.50
            print(f"\n  Revenue estimate (CEX-DEX >0.3%, $500 trades, 50% exec rate):")
            print(f"    ${rev:,.0f}/day")
            print(f"    vs $500/day target: {'VIABLE' if rev >= 500 else 'SHORT'}")

        if self.cross_chain_significant > 0:
            daily_cc = self.cross_chain_significant * daily
            rev_cc = daily_cc * 0.003 * 500 * 0.50
            print(f"\n  Revenue estimate (cross-chain >0.3%, $500 trades, 50% exec rate):")
            print(f"    ${rev_cc:,.0f}/day")

        print(f"{'='*70}\n", flush=True)
        self.last_print = time.time()


def run_monitor():
    global CHAIN_RPCS

    CHAIN_RPCS = load_env()
    active_chains = [c for c, url in CHAIN_RPCS.items() if url]
    print(f"RPC endpoints loaded: {active_chains}")

    # Discover Coinbase products
    print("Fetching Coinbase Exchange products...")
    coinbase_symbols = fetch_coinbase_products()
    print(f"  Coinbase tradeable tokens (USD/USDT): {len(coinbase_symbols)}")

    # Build monitored tokens
    monitored = build_monitored_tokens(coinbase_symbols)
    cex_tokens = [sym for sym, t in monitored.items() if t.get("coinbase_symbol")]
    multi_chain = [sym for sym, t in monitored.items() if len(t["chains"]) >= 2]

    print(f"\nMonitoring {len(monitored)} tokens:")
    print(f"  CEX-DEX pairs (Coinbase): {len(cex_tokens)}")
    print(f"  Cross-chain pairs (2+ chains): {len(multi_chain)}")

    if cex_tokens:
        print(f"  CEX-DEX tokens: {', '.join(sorted(cex_tokens)[:30])}{'...' if len(cex_tokens)>30 else ''}")
    if multi_chain:
        print(f"  Cross-chain tokens: {', '.join(sorted(multi_chain)[:30])}{'...' if len(multi_chain)>30 else ''}")

    if len(monitored) == 0:
        print("ERROR: No tokens to monitor!")
        return

    # Prepare output
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    stats = DivergenceStats()
    outfile = open(OUTPUT_FILE, "a")

    # CEX-DEX product mapping: {symbol: coinbase_product_id}
    cex_product_map = {sym: t["coinbase_symbol"] for sym, t in monitored.items()
                       if t.get("coinbase_symbol")}

    print(f"\nStarting poll loop (every {POLL_INTERVAL}s)...")
    print(f"First stats print in {STATS_INTERVAL}s\n")

    while not SHUTDOWN:
        stats.polls += 1

        # Fetch all Coinbase prices in one call
        cex_prices = fetch_coinbase_prices(cex_product_map)
        eth_price_usd = cex_prices.pop("__ETH_USD__", 2100.0)

        for sym, token_info in monitored.items():
            chains = token_info["chains"]

            # Get DEX prices on each chain
            dex_prices = {}
            for chain, pool_info in chains.items():
                rpc_url = CHAIN_RPCS.get(chain)
                if not rpc_url:
                    continue

                proto = pool_info["protocol"]
                if "v3" in proto:
                    price = get_v3_price_weth(
                        rpc_url, pool_info["pool"],
                        pool_info["token0_is_weth"],
                        pool_info["decimals0"], pool_info["decimals1"]
                    )
                else:
                    price = get_v2_price_weth(
                        rpc_url, pool_info["pool"],
                        pool_info["token0_is_weth"],
                        pool_info["decimals0"], pool_info["decimals1"]
                    )

                if price and price > 0:
                    dex_prices[chain] = price

            # CEX-DEX comparison
            coinbase_sym = token_info.get("coinbase_symbol")
            if coinbase_sym and coinbase_sym in cex_prices:
                cex_price_usd = cex_prices[coinbase_sym]
                cex_price_eth = cex_price_usd / eth_price_usd

                for chain, dex_price_eth in dex_prices.items():
                    if dex_price_eth > 0 and cex_price_eth > 0:
                        div = abs(dex_price_eth - cex_price_eth) / min(dex_price_eth, cex_price_eth) * 100

                        if div > 0.1:
                            stats.cex_dex_events += 1
                            stats.by_token[sym]["cex_dex"] += 1
                            stats.by_token[sym]["max_div"] = max(stats.by_token[sym]["max_div"], div)

                            if div > 0.3:
                                stats.cex_dex_significant += 1
                            if div > 1.0:
                                stats.cex_dex_major += 1

                            record = {
                                "ts": time.time(),
                                "type": "cex_dex",
                                "symbol": sym,
                                "chain": chain,
                                "cex_price_usd": round(cex_price_usd, 6),
                                "cex_price_eth": round(cex_price_eth, 8),
                                "dex_price_eth": round(dex_price_eth, 8),
                                "eth_price_usd": round(eth_price_usd, 2),
                                "divergence_pct": round(div, 4),
                            }
                            outfile.write(json.dumps(record) + "\n")

            # Cross-chain comparison
            chain_list = list(dex_prices.items())
            for i in range(len(chain_list)):
                for j in range(i + 1, len(chain_list)):
                    ca, pa = chain_list[i]
                    cb, pb = chain_list[j]
                    if pa > 0 and pb > 0:
                        div = abs(pa - pb) / min(pa, pb) * 100
                        if div > 0.1:
                            stats.cross_chain_events += 1
                            stats.by_token[sym]["cross_chain"] += 1
                            stats.by_token[sym]["max_div"] = max(stats.by_token[sym]["max_div"], div)
                            if div > 0.3:
                                stats.cross_chain_significant += 1

                            record = {
                                "ts": time.time(),
                                "type": "cross_chain",
                                "symbol": sym,
                                "chain_a": ca,
                                "chain_b": cb,
                                "price_a_eth": round(pa, 8),
                                "price_b_eth": round(pb, 8),
                                "divergence_pct": round(div, 4),
                            }
                            outfile.write(json.dumps(record) + "\n")

        outfile.flush()

        # Stats
        if time.time() - stats.last_print >= STATS_INTERVAL:
            stats.print_stats()

        # Wait for next poll
        time.sleep(POLL_INTERVAL)

    stats.print_stats()
    outfile.close()
    print(f"Data saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    run_monitor()
