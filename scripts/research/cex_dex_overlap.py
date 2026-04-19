#!/usr/bin/env python3
"""
CEX-DEX Overlap Analysis: identify niche arbitrage opportunities between
centralized exchanges (Binance, Coinbase) and on-chain DEXes (Uniswap V2/V3).

Goal: find tokens trading on BOTH CEX and DEX where HFT firms likely
do NOT compete aggressively — i.e. niche, lower-volume tokens that still
have enough liquidity to be tradeable.

Uses only stdlib (urllib, json) — no API keys required.
"""
import json
import os
import ssl
import sys
import urllib.error
import urllib.request
from collections import defaultdict
from pathlib import Path

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"

# Top-50 tokens by market cap — these are where HFT firms dominate.
# We exclude them from "niche" classification.
TOP_50_SYMBOLS = {
    "BTC", "ETH", "USDT", "BNB", "SOL", "USDC", "XRP", "DOGE", "TRX",
    "ADA", "AVAX", "SHIB", "DOT", "LINK", "TON", "BCH", "NEAR", "MATIC",
    "POL", "LTC", "UNI", "ICP", "PEPE", "DAI", "APT", "ETC", "RENDER",
    "STX", "FIL", "HBAR", "ATOM", "MNT", "IMX", "CRO", "MKR", "RNDR",
    "INJ", "OP", "ARB", "GRT", "THETA", "FTM", "RUNE", "AAVE", "SUI",
    "ALGO", "FLOW", "XLM", "VET", "WLD", "WBTC", "WETH", "STETH",
    "LIDO", "LEO", "KAS", "SEI", "TIA", "JUP", "BONK", "WIF", "FLOKI",
}

# Volume filters for "niche" tokens (in USD)
MIN_DAILY_VOLUME = 10_000    # $10K — enough liquidity to trade
MAX_DAILY_VOLUME = 10_000_000  # $10M — above this, HFT likely present

# Divergence assumptions for opportunity sizing
PRICE_DIVERGENCE_PCT = 0.003   # 0.3%
DIVERGENCE_EVENTS_PER_DAY = 10
TRADE_SIZE_USD = 500           # conservative per-trade size
DAILY_TARGET = 500             # revised target

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
def _make_ssl_ctx():
    """Unverified SSL context — needed on some macOS Python installs."""
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx

SSL_CTX = _make_ssl_ctx()


def fetch_json(url: str, label: str = "") -> dict | list | None:
    """Fetch JSON from a URL. Returns None on failure."""
    req = urllib.request.Request(url, headers={"User-Agent": "mev-research/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=30, context=SSL_CTX) as resp:
            data = json.loads(resp.read().decode())
            return data
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, Exception) as e:
        print(f"  [WARN] Failed to fetch {label or url}: {e}")
        return None


# ---------------------------------------------------------------------------
# Step 1: Fetch CEX data
# ---------------------------------------------------------------------------
def fetch_binance_pairs() -> dict[str, float]:
    """
    Returns {symbol: 24h_volume_usd} for all Binance pairs
    with USDT/USDC/ETH quote currencies.
    """
    print("[1/5] Fetching Binance exchange info...")
    info = fetch_json("https://api.binance.com/api/v3/exchangeInfo", "Binance exchangeInfo")
    if not info:
        return {}

    # Build set of valid pairs (USDT/USDC/ETH quote)
    valid_quotes = {"USDT", "USDC", "ETH"}
    pair_to_base = {}  # e.g. "LINKUSDT" -> "LINK"
    for sym in info.get("symbols", []):
        if sym.get("status") != "TRADING":
            continue
        quote = sym.get("quoteAsset", "")
        base = sym.get("baseAsset", "")
        if quote in valid_quotes:
            pair_to_base[sym["symbol"]] = base

    print(f"  Found {len(pair_to_base)} active pairs with USDT/USDC/ETH quote")

    # Fetch 24h volumes
    print("  Fetching Binance 24h ticker...")
    tickers = fetch_json("https://api.binance.com/api/v3/ticker/24hr", "Binance 24hr")
    if not tickers:
        return {}

    result = {}
    for t in tickers:
        sym = t.get("symbol", "")
        if sym in pair_to_base:
            try:
                vol_usd = float(t.get("quoteVolume", 0))
                base = pair_to_base[sym]
                # Aggregate volume by base token across different quote pairs
                result[base] = result.get(base, 0) + vol_usd
            except (ValueError, TypeError):
                pass

    print(f"  Got volume data for {len(result)} base tokens")
    return result


def fetch_coinbase_pairs() -> set[str]:
    """Returns set of base currency symbols listed on Coinbase."""
    print("[2/5] Fetching Coinbase products...")
    products = fetch_json("https://api.exchange.coinbase.com/products", "Coinbase products")
    if not products:
        return set()

    symbols = set()
    for p in products:
        if p.get("status") == "online" or p.get("trading_disabled") is False:
            base = p.get("base_currency", "")
            if base:
                symbols.add(base.upper())

    print(f"  Found {len(symbols)} active base tokens on Coinbase")
    return symbols


# ---------------------------------------------------------------------------
# Step 2: Load on-chain pool data
# ---------------------------------------------------------------------------
def load_onchain_pools() -> dict[str, list[dict]]:
    """
    Returns {symbol_upper: [pool_info, ...]} across all chains.
    Each pool_info has: chain, pool_address, protocol, paired_with.
    """
    print("[3/5] Loading on-chain pool data...")
    all_pools: dict[str, list[dict]] = defaultdict(list)

    # --- Arbitrum ---
    arb_path = DATA_DIR / "pool_tokens_arbitrum.json"
    if arb_path.exists():
        with open(arb_path) as f:
            arb_data = json.load(f)
        tokens = arb_data.get("tokens", {})
        addr_to_sym = {addr.lower(): info.get("symbol", "???").upper()
                       for addr, info in tokens.items()}
        pools = arb_data.get("pools", [])
        for pool in pools:
            t0 = pool.get("token0", "").lower()
            t1 = pool.get("token1", "").lower()
            sym0 = addr_to_sym.get(t0, "???")
            sym1 = addr_to_sym.get(t1, "???")
            protocol = pool.get("protocol", "unknown")
            pool_addr = pool.get("address", "")
            # Add both tokens (skip stables/WETH as the "interesting" token)
            for sym, paired in [(sym0, sym1), (sym1, sym0)]:
                if sym not in ("WETH", "USDC", "USDT", "DAI", "???"):
                    all_pools[sym].append({
                        "chain": "arbitrum",
                        "pool": pool_addr,
                        "protocol": protocol,
                        "paired_with": paired,
                    })
        print(f"  Arbitrum: {len(pools)} pools, {len(tokens)} tokens")
    else:
        print("  [WARN] Arbitrum pool data not found")

    # --- Base ---
    base_path = DATA_DIR / "pool_tokens_base.json"
    if base_path.exists():
        with open(base_path) as f:
            base_data = json.load(f)
        pools_dict = base_data if isinstance(base_data, dict) else {}
        # Base format: {pool_addr: {token0, token1, symbol0, symbol1, ...}}
        pool_count = 0
        for pool_addr, info in pools_dict.items():
            if not isinstance(info, dict):
                continue
            sym0 = info.get("symbol0", "???").upper()
            sym1 = info.get("symbol1", "???").upper()
            protocol = info.get("protocol", "unknown")
            for sym, paired in [(sym0, sym1), (sym1, sym0)]:
                if sym not in ("WETH", "USDC", "USDT", "DAI", "???"):
                    all_pools[sym].append({
                        "chain": "base",
                        "pool": pool_addr,
                        "protocol": protocol,
                        "paired_with": paired,
                    })
            pool_count += 1
        print(f"  Base: {pool_count} pools")
    else:
        print("  [WARN] Base pool data not found")

    # --- Ethereum (pool_tokens.json) ---
    eth_path = DATA_DIR / "pool_tokens.json"
    if eth_path.exists():
        with open(eth_path) as f:
            eth_data = json.load(f)
        # Try both formats
        if isinstance(eth_data, dict) and "pools" in eth_data:
            tokens = eth_data.get("tokens", {})
            addr_to_sym = {addr.lower(): info.get("symbol", "???").upper()
                           for addr, info in tokens.items()}
            pools = eth_data.get("pools", [])
            for pool in pools:
                t0 = pool.get("token0", "").lower()
                t1 = pool.get("token1", "").lower()
                sym0 = addr_to_sym.get(t0, "???")
                sym1 = addr_to_sym.get(t1, "???")
                protocol = pool.get("protocol", "unknown")
                pool_addr = pool.get("address", "")
                for sym, paired in [(sym0, sym1), (sym1, sym0)]:
                    if sym not in ("WETH", "USDC", "USDT", "DAI", "???"):
                        all_pools[sym].append({
                            "chain": "ethereum",
                            "pool": pool_addr,
                            "protocol": protocol,
                            "paired_with": paired,
                        })
            print(f"  Ethereum: {len(pools)} pools, {len(tokens)} tokens")
        elif isinstance(eth_data, dict):
            pool_count = 0
            for pool_addr, info in eth_data.items():
                if not isinstance(info, dict):
                    continue
                sym0 = info.get("symbol0", "???").upper()
                sym1 = info.get("symbol1", "???").upper()
                protocol = info.get("protocol", "unknown")
                for sym, paired in [(sym0, sym1), (sym1, sym0)]:
                    if sym not in ("WETH", "USDC", "USDT", "DAI", "???"):
                        all_pools[sym].append({
                            "chain": "ethereum",
                            "pool": pool_addr,
                            "protocol": protocol,
                            "paired_with": paired,
                        })
                pool_count += 1
            print(f"  Ethereum: {pool_count} pools")
    else:
        print("  [INFO] No Ethereum pool_tokens.json found (expected: data/pool_tokens.json)")

    print(f"  Total unique DEX token symbols: {len(all_pools)}")
    return dict(all_pools)


# ---------------------------------------------------------------------------
# Step 3 & 4: Cross-reference and analyze
# ---------------------------------------------------------------------------
def analyze(
    binance_volumes: dict[str, float],
    coinbase_symbols: set[str],
    dex_pools: dict[str, list[dict]],
):
    """Cross-reference CEX listings with DEX pools, identify niche opportunities."""
    print("\n[4/5] Cross-referencing CEX and DEX tokens...")

    # Normalize: some on-chain symbols have non-standard chars
    # Build a clean lookup: upper symbol -> list of pools
    dex_clean = {}
    for sym, pools in dex_pools.items():
        clean = sym.strip().upper()
        # Handle special chars (e.g. USD₮0 -> USDT)
        clean = clean.replace("₮", "T").replace("0", "") if "₮" in clean else clean
        if clean and clean not in ("WETH", "USDC", "USDT", "DAI"):
            if clean in dex_clean:
                dex_clean[clean].extend(pools)
            else:
                dex_clean[clean] = list(pools)

    cex_symbols = set(binance_volumes.keys()) | coinbase_symbols
    overlapping = set()
    results = []

    for sym in sorted(cex_symbols):
        sym_upper = sym.upper()
        if sym_upper in dex_clean:
            overlapping.add(sym_upper)
            pools = dex_clean[sym_upper]
            vol = binance_volumes.get(sym, 0)
            on_coinbase = sym_upper in coinbase_symbols
            on_binance = sym in binance_volumes
            is_top50 = sym_upper in TOP_50_SYMBOLS
            is_niche = (not is_top50
                        and MIN_DAILY_VOLUME <= vol <= MAX_DAILY_VOLUME)

            chains = set(p["chain"] for p in pools)
            protocols = set(p["protocol"] for p in pools)

            results.append({
                "symbol": sym_upper,
                "cex_volume_usd": vol,
                "on_binance": on_binance,
                "on_coinbase": on_coinbase,
                "is_top50": is_top50,
                "is_niche": is_niche,
                "dex_pool_count": len(pools),
                "dex_chains": sorted(chains),
                "dex_protocols": sorted(protocols),
                "pools": pools,
            })

    # Sort by volume descending
    results.sort(key=lambda r: r["cex_volume_usd"], reverse=True)

    niche = [r for r in results if r["is_niche"]]
    top50_overlap = [r for r in results if r["is_top50"]]
    mid_tier = [r for r in results if not r["is_top50"] and not r["is_niche"]
                and r["cex_volume_usd"] > MAX_DAILY_VOLUME]

    # --------------- Print results ---------------
    print(f"\n{'='*70}")
    print(f"  CEX-DEX OVERLAP ANALYSIS")
    print(f"{'='*70}")
    print(f"  CEX tokens (Binance + Coinbase):   {len(cex_symbols)}")
    print(f"  DEX tokens (on-chain pools):       {len(dex_clean)}")
    print(f"  Overlapping tokens:                {len(overlapping)}")
    print(f"    - Top-50 (HFT-dominated):        {len(top50_overlap)}")
    print(f"    - Mid-tier (>$10M vol):           {len(mid_tier)}")
    print(f"    - Niche ($10K-$10M vol):          {len(niche)}")
    low_vol = [r for r in results if not r["is_top50"]
               and r["cex_volume_usd"] < MIN_DAILY_VOLUME]
    print(f"    - Too low volume (<$10K):         {len(low_vol)}")

    print(f"\n{'='*70}")
    print(f"  TOP-50 TOKENS ON BOTH CEX+DEX (HFT territory - avoid)")
    print(f"{'='*70}")
    for r in top50_overlap[:15]:
        print(f"  {r['symbol']:>8s}  vol=${r['cex_volume_usd']:>14,.0f}  "
              f"pools={r['dex_pool_count']:>3d}  chains={','.join(r['dex_chains'])}")

    print(f"\n{'='*70}")
    print(f"  MID-TIER TOKENS (>$10M vol - likely competitive)")
    print(f"{'='*70}")
    for r in mid_tier[:20]:
        print(f"  {r['symbol']:>8s}  vol=${r['cex_volume_usd']:>14,.0f}  "
              f"pools={r['dex_pool_count']:>3d}  chains={','.join(r['dex_chains'])}")

    print(f"\n{'='*70}")
    print(f"  NICHE TOKENS ($10K-$10M vol) — POTENTIAL OPPORTUNITIES")
    print(f"{'='*70}")
    if niche:
        for r in niche:
            print(f"  {r['symbol']:>8s}  vol=${r['cex_volume_usd']:>12,.0f}  "
                  f"pools={r['dex_pool_count']:>3d}  "
                  f"chains={','.join(r['dex_chains'])}  "
                  f"{'Binance' if r['on_binance'] else '       '} "
                  f"{'Coinbase' if r['on_coinbase'] else '        '}")
    else:
        print("  (none found)")

    # --------------- Opportunity sizing ---------------
    print(f"\n{'='*70}")
    print(f"  OPPORTUNITY SIZING")
    print(f"{'='*70}")

    total_niche_volume = sum(r["cex_volume_usd"] for r in niche)
    total_niche_pools = sum(r["dex_pool_count"] for r in niche)

    print(f"  Niche tokens found:                {len(niche)}")
    print(f"  Total niche CEX volume:            ${total_niche_volume:,.0f}/day")
    print(f"  Total niche DEX pools:             {total_niche_pools}")

    # Estimate: for each niche token, assume price divergence of 0.3%
    # occurs ~10x/day, we can capture ~$500 per trade
    print(f"\n  Assumptions:")
    print(f"    Price divergence:                {PRICE_DIVERGENCE_PCT*100:.1f}%")
    print(f"    Divergence events/day/token:     {DIVERGENCE_EVENTS_PER_DAY}")
    print(f"    Trade size per event:            ${TRADE_SIZE_USD:,.0f}")

    if niche:
        # Per-token daily gross profit estimate
        gross_per_event = TRADE_SIZE_USD * PRICE_DIVERGENCE_PCT
        events_per_day = len(niche) * DIVERGENCE_EVENTS_PER_DAY
        daily_gross = events_per_day * gross_per_event

        # Costs: gas (~$0.10/trade on L2), slippage (~0.1%), CEX fees (~0.1%)
        gas_cost_per_trade = 0.10  # L2
        slippage_cost = TRADE_SIZE_USD * 0.001
        cex_fee = TRADE_SIZE_USD * 0.001
        cost_per_trade = gas_cost_per_trade + slippage_cost + cex_fee
        daily_costs = events_per_day * cost_per_trade
        daily_net = daily_gross - daily_costs

        print(f"\n  Estimates:")
        print(f"    Gross profit per event:          ${gross_per_event:.2f}")
        print(f"    Cost per event (gas+slip+fee):   ${cost_per_trade:.2f}")
        print(f"    Net profit per event:            ${gross_per_event - cost_per_trade:.2f}")
        print(f"    Total events/day:                {events_per_day}")
        print(f"    Daily gross:                     ${daily_gross:,.2f}")
        print(f"    Daily costs:                     ${daily_costs:,.2f}")
        print(f"    Daily net estimate:              ${daily_net:,.2f}")
    else:
        daily_net = 0
        print(f"\n  No niche tokens found — cannot estimate.")

    # --------------- GO/KILL ---------------
    print(f"\n{'='*70}")
    print(f"  GO / KILL ASSESSMENT (target: ${DAILY_TARGET}/day)")
    print(f"{'='*70}")

    if len(niche) == 0:
        verdict = "KILL"
        reasoning = (
            "No niche tokens found trading on both CEX and our tracked DEX pools.\n"
            "  Either our pool universe is too small, or CEX-listed tokens don't\n"
            "  overlap with the long-tail DEX tokens we track."
        )
    elif daily_net >= DAILY_TARGET:
        verdict = "CONDITIONAL GO"
        reasoning = (
            f"  Theoretical daily net: ${daily_net:,.0f} vs ${DAILY_TARGET} target.\n"
            f"  However, this assumes:\n"
            f"    - 0.3% divergences actually occur 10x/day (UNVALIDATED)\n"
            f"    - We can execute before divergence closes (latency?)\n"
            f"    - DEX liquidity supports ${TRADE_SIZE_USD} trades\n"
            f"  NEXT STEP: empirical validation — monitor actual price divergences\n"
            f"  for the {len(niche)} niche tokens over 48 hours."
        )
    elif daily_net >= DAILY_TARGET * 0.3:
        verdict = "NEEDS MORE DATA"
        reasoning = (
            f"  Theoretical daily net: ${daily_net:,.0f} — below ${DAILY_TARGET} target\n"
            f"  but not zero. Could work if divergences are larger/more frequent\n"
            f"  than assumed. Worth a 48hr monitoring probe before killing.\n"
            f"  NEXT STEP: build price-divergence monitor for top niche tokens."
        )
    else:
        verdict = "KILL"
        reasoning = (
            f"  Theoretical daily net: ${daily_net:,.0f} — far below ${DAILY_TARGET} target.\n"
            f"  Even with optimistic assumptions, CEX-DEX arb on niche tokens\n"
            f"  from our current pool universe doesn't reach the target.\n"
            f"  Would need 10x more niche tokens or much larger divergences."
        )

    print(f"\n  Verdict: *** {verdict} ***\n")
    print(f"  Reasoning:")
    print(f"  {reasoning}")
    print()

    # List actionable niche tokens if any
    if niche:
        print(f"{'='*70}")
        print(f"  ACTIONABLE NICHE TOKENS (for further investigation)")
        print(f"{'='*70}")
        for r in niche:
            print(f"\n  {r['symbol']}:")
            print(f"    CEX volume: ${r['cex_volume_usd']:,.0f}/day")
            print(f"    CEX: {'Binance ' if r['on_binance'] else ''}{'Coinbase' if r['on_coinbase'] else ''}")
            print(f"    DEX pools ({r['dex_pool_count']}):")
            for p in r["pools"][:5]:
                print(f"      {p['chain']:>10s} | {p['protocol']:>12s} | paired: {p['paired_with']}")
            if len(r["pools"]) > 5:
                print(f"      ... and {len(r['pools'])-5} more")

    return results, niche


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("CEX-DEX Overlap Analysis")
    print("Finding niche arbitrage opportunities\n")

    binance_volumes = fetch_binance_pairs()
    coinbase_symbols = fetch_coinbase_pairs()
    dex_pools = load_onchain_pools()

    if not binance_volumes and not coinbase_symbols:
        print("\n[ERROR] Could not fetch any CEX data. Check network connection.")
        sys.exit(1)

    if not dex_pools:
        print("\n[ERROR] No on-chain pool data loaded. Check data/ directory.")
        sys.exit(1)

    results, niche = analyze(binance_volumes, coinbase_symbols, dex_pools)

    # Save raw results for further analysis
    out_path = DATA_DIR / "cex_dex_overlap.json"
    serializable = []
    for r in results:
        s = dict(r)
        s["dex_chains"] = list(s["dex_chains"])
        s["dex_protocols"] = list(s["dex_protocols"])
        s.pop("pools", None)  # too verbose for JSON
        serializable.append(s)
    with open(out_path, "w") as f:
        json.dump(serializable, f, indent=2)
    print(f"\n[5/5] Results saved to {out_path}")


if __name__ == "__main__":
    main()
