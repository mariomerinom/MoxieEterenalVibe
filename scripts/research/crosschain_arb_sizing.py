#!/usr/bin/env python3
"""
P7-S1: Cross-Chain Arbitrage Sizing

Checks if the same token pairs have price divergence between L2s
(Arbitrum, Base) and Ethereum mainnet.

For each token that exists on multiple chains:
1. Get current pool prices via on-chain reserves/sqrtPrice
2. Measure divergence between chains
3. Estimate arb opportunity

Cross-chain arb doesn't need mempool access — it detects on-chain state
divergence. The constraint is bridge latency and cost.

Uses Multicall3 for efficient batch price fetching.
"""

import json
import os
import subprocess
import sys
import time

# Known bridge-equivalent tokens across chains
# Format: symbol -> {chain: token_address}
CROSS_CHAIN_TOKENS = {
    "WETH": {
        "ethereum": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
        "arbitrum": "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
        "base": "0x4200000000000000000000000000000000000006",
    },
    "USDC": {
        "ethereum": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        "arbitrum": "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
        "base": "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
    },
    "USDC.e": {
        "arbitrum": "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8",
    },
    "USDT": {
        "ethereum": "0xdac17f958d2ee523a2206206994597c13d831ec7",
        "arbitrum": "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9",
    },
    "DAI": {
        "ethereum": "0x6b175474e89094c44da98b954eedeac495271d0f",
        "arbitrum": "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1",
        "base": "0x50c5725949a6f0c72e6c4a641f24049a917db0cb",
    },
    "WBTC": {
        "ethereum": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
        "arbitrum": "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f",
    },
    "wstETH": {
        "ethereum": "0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0",
        "arbitrum": "0x5979d7b546e38e414f7e9822514be443a4800529",
        "base": "0xc1cba3fcea344f92d9239c08c0568f6f2f0ee452",
    },
    "ARB": {
        "ethereum": "0xb50721bcf8d664c30412cfbc6cf7a15145234ad1",
        "arbitrum": "0x912ce59144191c1204e64559fe8253a0e49e6548",
    },
    "LINK": {
        "ethereum": "0x514910771af9ca656af840dff83e8264ecf986ca",
        "arbitrum": "0xf97f4df75117a78c1a5a0dbb814af92458539fb4",
    },
    "UNI": {
        "ethereum": "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984",
        "arbitrum": "0xfa7f8980b0f1e64a2062791cc3b0871572f1f7f0",
    },
    "AAVE": {
        "ethereum": "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
        "arbitrum": "0xba5ddd1f9d7f570dc94a51479a000e3bce967196",
    },
    "CRV": {
        "ethereum": "0xd533a949740bb3306d119cc777fa900ba034cd52",
        "arbitrum": "0x11cdb42b0eb46d95f990bedd4695a6e3fa034978",
    },
    "GMX": {
        "arbitrum": "0xfc5a1a6eb076a2c7ad06ed22c90d7e710e35ad0a",
    },
    "COMP": {
        "ethereum": "0xc00e94cb662c3520282e6f5717214004a7f26888",
        "arbitrum": "0x354a6da3fcde098f8389cad84b0182725c6c91de",
    },
    "LDO": {
        "ethereum": "0x5a98fcbea516cf06857215779fd812ca3bef1b32",
        "arbitrum": "0x13ad51ed4f1b7e9dc168d8a00cb3f4ddd85efa3e",
    },
    "PENDLE": {
        "ethereum": "0x808507121b80c02388fad14726482e061b8da827",
        "arbitrum": "0x0c880f6761f1af8d9aa9c466984b80dab9a8c9e8",
    },
    "RDNT": {
        "arbitrum": "0x3082cc23568ea640225c2467653db90e9250aaa0",
    },
}

CHAIN_RPCS = {
    "ethereum": "ETH_RPC_HTTP",
    "arbitrum": "ARB_RPC_HTTP",
    "base": "BASE_RPC_HTTP",
}

# V2 getReserves selector
GET_RESERVES = "0x0902f1ac"
# V3 slot0 selector
SLOT0 = "0x3850c7bd"
# V3 liquidity selector
LIQUIDITY = "0x1a686502"


def load_env():
    """Load .env file."""
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
    return rpcs


def rpc_call(rpc_url, method, params):
    """Make an RPC call via curl."""
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    try:
        r = subprocess.run(
            ["curl", "-s", "-X", "POST", rpc_url, "-H", "Content-Type: application/json",
             "-d", payload, "--connect-timeout", "10", "--max-time", "15"],
            capture_output=True, text=True, timeout=20
        )
        result = json.loads(r.stdout)
        return result.get("result")
    except Exception as e:
        return None


def get_v2_price(rpc_url, pool_addr, token0_is_weth, token0_dec, token1_dec):
    """Get price from V2 pool reserves. Returns price of non-WETH token in WETH."""
    result = rpc_call(rpc_url, "eth_call", [
        {"to": pool_addr, "data": GET_RESERVES}, "latest"
    ])
    if not result or len(result) < 130:
        return None

    data = result[2:]
    r0 = int(data[0:64], 16)
    r1 = int(data[64:128], 16)

    if r0 == 0 or r1 == 0:
        return None

    if token0_is_weth:
        # price of token1 in WETH = r0/10^18 / (r1/10^d1)
        return (r0 / 10**token0_dec) / (r1 / 10**token1_dec)
    else:
        return (r1 / 10**token1_dec) / (r0 / 10**token0_dec)


def get_v3_price(rpc_url, pool_addr, token0_is_weth, token0_dec, token1_dec):
    """Get price from V3 slot0. Returns price of non-WETH token in WETH."""
    result = rpc_call(rpc_url, "eth_call", [
        {"to": pool_addr, "data": SLOT0}, "latest"
    ])
    if not result or len(result) < 130:
        return None

    data = result[2:]
    sqrtPriceX96 = int(data[0:64], 16)

    if sqrtPriceX96 == 0:
        return None

    # price = (sqrtPriceX96 / 2^96)^2 = token1/token0 in raw units
    price_raw = (sqrtPriceX96 / (2**96)) ** 2

    # Adjust for decimals: price in token1_units/token0_units
    price_adjusted = price_raw * (10**token0_dec) / (10**token1_dec)

    if token0_is_weth:
        # price_adjusted = token1_per_token0 = tokens_per_weth
        # We want WETH per token = 1/price_adjusted
        return 1 / price_adjusted if price_adjusted > 0 else None
    else:
        # price_adjusted = WETH per token
        return price_adjusted


def load_pool_files():
    """Load all pool universe files."""
    pools_by_chain = {}

    files = {
        "ethereum": "data/pool_tokens.json",
        "arbitrum": "data/pool_tokens_arbitrum.json",
        "base": "data/pool_tokens_base.json",
    }

    for chain, filepath in files.items():
        try:
            with open(filepath) as f:
                pools_by_chain[chain] = json.load(f)
        except FileNotFoundError:
            pools_by_chain[chain] = {}

    return pools_by_chain


def find_cross_chain_pools(pools_by_chain):
    """Find pools for cross-chain tokens on each chain."""
    results = {}  # symbol -> {chain: [pool_info]}

    for symbol, chain_addrs in CROSS_CHAIN_TOKENS.items():
        results[symbol] = {}
        for chain, token_addr in chain_addrs.items():
            if chain not in pools_by_chain:
                continue

            matching_pools = []
            weth_addr = CROSS_CHAIN_TOKENS["WETH"].get(chain, "").lower()

            for pool_addr, info in pools_by_chain[chain].items():
                t0 = info["token0"].lower()
                t1 = info["token1"].lower()
                token_lower = token_addr.lower()

                if token_lower == weth_addr:
                    continue  # Skip WETH itself

                if t0 == token_lower or t1 == token_lower:
                    # Check if paired with WETH
                    other = t1 if t0 == token_lower else t0
                    if other == weth_addr:
                        matching_pools.append({
                            "pool": pool_addr,
                            "protocol": info.get("protocol", "unknown"),
                            "token0": t0,
                            "token1": t1,
                            "decimals0": info.get("decimals0", 18),
                            "decimals1": info.get("decimals1", 18),
                            "token0_is_weth": t0 == weth_addr,
                        })

            if matching_pools:
                results[symbol][chain] = matching_pools

    return results


def main():
    env = load_env()
    pools_by_chain = load_pool_files()

    print(f"{'='*80}")
    print(f"  CROSS-CHAIN ARBITRAGE SIZING")
    print(f"  Target: $500/day")
    print(f"{'='*80}")

    for chain, pools in pools_by_chain.items():
        print(f"  {chain}: {len(pools)} pools loaded")

    # Find cross-chain pools
    cross_pools = find_cross_chain_pools(pools_by_chain)

    # Filter to tokens with pools on 2+ chains
    multi_chain = {sym: chains for sym, chains in cross_pools.items()
                   if len(chains) >= 2}

    print(f"\n  Tokens with WETH pools on 2+ chains: {len(multi_chain)}")
    for sym, chains in sorted(multi_chain.items()):
        chain_list = ", ".join(f"{c}({len(p)})" for c, p in chains.items())
        print(f"    {sym}: {chain_list}")

    # Fetch current prices for each token on each chain
    print(f"\n  Fetching current prices...")
    prices = {}  # symbol -> {chain: price_in_weth}

    for symbol, chains in multi_chain.items():
        prices[symbol] = {}
        for chain, pool_list in chains.items():
            rpc_env = CHAIN_RPCS.get(chain)
            rpc_url = env.get(rpc_env) if rpc_env else None
            if not rpc_url:
                continue

            # Use first pool for price
            pool = pool_list[0]
            proto = pool["protocol"]

            if "v3" in proto.lower() or "uniswapv3" in proto.lower():
                price = get_v3_price(
                    rpc_url, pool["pool"],
                    pool["token0_is_weth"],
                    pool["decimals0"], pool["decimals1"]
                )
            else:
                price = get_v2_price(
                    rpc_url, pool["pool"],
                    pool["token0_is_weth"],
                    pool["decimals0"], pool["decimals1"]
                )

            if price and price > 0:
                prices[symbol][chain] = price

    # Analyze price divergence
    print(f"\n{'='*80}")
    print(f"  CURRENT CROSS-CHAIN PRICE DIVERGENCE")
    print(f"{'='*80}")

    eth_price_usd = 2100  # approximate
    divergences = []

    print(f"\n  {'Token':<10} {'Chain A':<12} {'Price A (WETH)':<16} {'Chain B':<12} {'Price B (WETH)':<16} {'Div %':>8}")
    print(f"  {'-'*76}")

    for symbol, chain_prices in sorted(prices.items()):
        if len(chain_prices) < 2:
            continue

        chains = list(chain_prices.items())
        for i in range(len(chains)):
            for j in range(i + 1, len(chains)):
                chain_a, price_a = chains[i]
                chain_b, price_b = chains[j]

                if price_a > 0 and price_b > 0:
                    div_pct = abs(price_a - price_b) / min(price_a, price_b) * 100
                    divergences.append({
                        "symbol": symbol,
                        "chain_a": chain_a,
                        "chain_b": chain_b,
                        "price_a": price_a,
                        "price_b": price_b,
                        "divergence_pct": div_pct,
                    })
                    flag = " ← OPPORTUNITY" if div_pct > 0.3 else ""
                    print(f"  {symbol:<10} {chain_a:<12} {price_a:<16.8f} {chain_b:<12} {price_b:<16.8f} {div_pct:>7.3f}%{flag}")

    if not divergences:
        print("  No cross-chain price data available")
        return

    # Summary
    divergences.sort(key=lambda x: -x["divergence_pct"])

    print(f"\n{'='*80}")
    print(f"  ANALYSIS")
    print(f"{'='*80}")

    above_03 = [d for d in divergences if d["divergence_pct"] > 0.3]
    above_05 = [d for d in divergences if d["divergence_pct"] > 0.5]
    above_1 = [d for d in divergences if d["divergence_pct"] > 1.0]

    print(f"\n  Pairs with >0.3% divergence: {len(above_03)}/{len(divergences)}")
    print(f"  Pairs with >0.5% divergence: {len(above_05)}/{len(divergences)}")
    print(f"  Pairs with >1.0% divergence: {len(above_1)}/{len(divergences)}")

    if above_03:
        print(f"\n  Top divergences:")
        for d in above_03[:10]:
            # Estimate profit per arb
            # Profit = divergence - bridge_cost - gas_cost
            # Typical bridge cost: ~$2-5 for L2->L1, ~$0.50 for L2->L2
            # Gas: ~$0.10 on L2, ~$5-20 on L1
            arb_size_eth = 1.0  # 1 ETH per trade
            gross_profit = arb_size_eth * d["divergence_pct"] / 100 * eth_price_usd

            # Bridge costs depend on direction
            if "ethereum" in (d["chain_a"], d["chain_b"]):
                bridge_cost = 5.0  # L2 to L1 bridge is expensive and slow
                gas_cost = 15.0    # L1 gas
            else:
                bridge_cost = 0.50  # L2 to L2
                gas_cost = 0.30    # L2 gas

            net = gross_profit - bridge_cost - gas_cost

            print(f"    {d['symbol']} {d['chain_a']}<>{d['chain_b']}: {d['divergence_pct']:.3f}%"
                  f"  gross=${gross_profit:.2f}/ETH  net=${net:.2f}/ETH (1 ETH trade)")

    # Revenue model
    print(f"\n{'='*80}")
    print(f"  REVENUE MODEL")
    print(f"{'='*80}")

    print(f"""
  Cross-chain arb constraints:
  1. Bridge latency: L2→L1 takes 7 days (optimistic rollup finality)
     L2→L2 via fast bridges: ~2-15 minutes
  2. Bridge cost: L2→L1 ~$5-20, L2→L2 ~$0.50-2
  3. Capital lockup: need capital on BOTH chains
  4. Price can move during bridge → directional risk

  CRITICAL: L2→L1 7-day delay makes Arbitrum/Base→Ethereum arb
  impractical for spot arb. Only L2↔L2 via fast bridges is viable.

  For L2↔L2 (Arbitrum↔Base):
    Bridge cost: ~$0.50-2.00 per direction
    Gas cost: ~$0.10-0.30 per swap
    Minimum profitable divergence: ~0.15% on 1 ETH = $3.15
    Breakeven requires: divergence > bridge_cost / trade_size
    At 1 ETH: need >0.1% divergence
    At 10 ETH: need >0.01% divergence
""")

    # Check for L2-L2 opportunities specifically
    l2_divs = [d for d in divergences
                if d["chain_a"] != "ethereum" and d["chain_b"] != "ethereum"
                and d["divergence_pct"] > 0.1]

    l1_l2_divs = [d for d in divergences
                   if ("ethereum" == d["chain_a"] or "ethereum" == d["chain_b"])
                   and d["divergence_pct"] > 1.0]

    print(f"  L2↔L2 opportunities (>0.1% div): {len(l2_divs)}")
    for d in l2_divs[:5]:
        print(f"    {d['symbol']} {d['chain_a']}↔{d['chain_b']}: {d['divergence_pct']:.3f}%")

    print(f"\n  L1↔L2 opportunities (>1.0% div, slow bridge): {len(l1_l2_divs)}")
    for d in l1_l2_divs[:5]:
        print(f"    {d['symbol']} {d['chain_a']}↔{d['chain_b']}: {d['divergence_pct']:.3f}%")

    # GO/KILL
    print(f"\n{'='*80}")
    print(f"  GO/KILL ASSESSMENT (target: $500/day)")
    print(f"{'='*80}")

    print(f"""
  This is a SINGLE SNAPSHOT. Cross-chain arb opportunities are transient.
  What matters is FREQUENCY and MAGNITUDE of divergence events over time.

  To reach $500/day:
    At 0.3% divergence, 10 ETH per trade: ~$63 gross per arb
    After bridge+gas (~$3): ~$60 net per arb
    Need: ~8 arb executions/day

  NEXT STEPS for validation:
  1. Run a 24h divergence monitor: poll prices every 30s, log divergences
  2. Measure: frequency of >0.3% divergence events per pair
  3. Measure: how long divergences persist (seconds? minutes?)
  4. If divergences are frequent and persistent: proceed to execution
  5. If divergences are rare or resolve in <30s: kill (speed competitors)
""")

    # Quick verdict based on snapshot
    significant = [d for d in divergences if d["divergence_pct"] > 0.3]
    if len(significant) >= 3:
        print(f"  SNAPSHOT VERDICT: PROCEED TO DIVERGENCE MONITOR")
        print(f"  {len(significant)} pairs showing >0.3% divergence right now")
    elif len(significant) >= 1:
        print(f"  SNAPSHOT VERDICT: CAUTIOUS PROCEED")
        print(f"  {len(significant)} pair(s) with >0.3% divergence — need time-series data")
    else:
        print(f"  SNAPSHOT VERDICT: LIKELY KILL")
        print(f"  No pairs showing >0.3% divergence — prices are well-arbitraged")


if __name__ == "__main__":
    main()
