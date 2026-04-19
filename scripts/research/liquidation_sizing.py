#!/usr/bin/env python3
"""
Liquidation opportunity sizing across Ethereum, Arbitrum, and Base.

Fetches recent liquidation events from Aave V3 (and Compound V3 on Ethereum)
using eth_getLogs, then computes frequency, size distribution, and top liquidator
market share. Goal: determine if liquidation MEV can reach $1,000/day.
"""

import json
import os
import subprocess
import sys
from collections import defaultdict

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

WETH_PRICE_USD = 2100  # rough approximation for USD conversion

# Aave V3 LiquidationCall event
# event LiquidationCall(
#   address indexed collateralAsset,
#   address indexed debtAsset,
#   address indexed user,
#   uint256 debtToCover,
#   uint256 liquidatedCollateralAmount,
#   address liquidator,
#   bool receiveAToken
# )
AAVE_LIQUIDATION_TOPIC = "0xe413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286"

# Compound V3 AbsorbCollateral event
# event AbsorbCollateral(
#   address indexed absorber,
#   address indexed borrower,
#   address indexed asset,
#   uint256 collateralAbsorbed,
#   uint256 usdValue
# )
COMPOUND_ABSORB_TOPIC = "0x1547a878dc0de1100e8afc65ebcb5959e6f053e8de4df5732572bcd02e3e24a3"

CHAINS = {
    "ethereum": {
        "rpc_env": "ETH_RPC_HTTP",
        "rpc_fallback": "https://eth.llamarpc.com",
        "aave_pool": "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
        "block_time_s": 12,
        "blocks_lookback": 2000,
    },
    "arbitrum": {
        "rpc_env": "ARB_RPC_HTTP",
        "rpc_fallback": "https://arb1.arbitrum.io/rpc",
        "aave_pool": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
        "block_time_s": 0.25,  # Arbitrum ~250ms blocks
        "blocks_lookback": 100000,  # ~7 hours at 0.25s
    },
    "base": {
        "rpc_env": "BASE_RPC_HTTP",
        "rpc_fallback": "https://mainnet.base.org",
        "aave_pool": "0xA238Dd80C259a72e81d7e4664a9801593F98d1c5",
        "block_time_s": 2,
        "blocks_lookback": 12000,  # ~7 hours at 2s
    },
}

# Known Compound V3 Comet contracts on Ethereum
COMPOUND_COMETS = {
    "USDC":  "0xc3d688B66703497DAA19211EEdff47f25384cdc3",
    "WETH":  "0xA17581A9E3356d9A858b789D68B4d866e593aE94",
    "USDT":  "0x3Afdc9BCA9213A35503b077a6072F3D0d5AB0840",
}

# Well-known token decimals for rough USD sizing
# We use 18 as default and adjust for known stables / WBTC
TOKEN_DECIMALS = {
    # Ethereum stablecoins
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": 6,   # USDC
    "0xdac17f958d2ee523a2206206994597c13d831ec7": 6,   # USDT
    "0x6b175474e89094c44da98b954eedeac495271d0f": 18,  # DAI
    "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": 8,   # WBTC
    # Arbitrum
    "0xaf88d065e77c8cc2239327c5edb3a432268e5831": 6,   # USDC (native)
    "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8": 6,   # USDC.e
    "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9": 6,   # USDT
    "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f": 8,   # WBTC
    # Base
    "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913": 6,   # USDC
    "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca": 6,   # USDbC
}

# Rough USD price per token (very approximate, for sizing only)
TOKEN_USD_PRICE = {
    # stablecoins
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": 1.0,
    "0xdac17f958d2ee523a2206206994597c13d831ec7": 1.0,
    "0x6b175474e89094c44da98b954eedeac495271d0f": 1.0,
    "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913": 1.0,
    "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca": 1.0,
    "0xaf88d065e77c8cc2239327c5edb3a432268e5831": 1.0,
    "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8": 1.0,
    "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9": 1.0,
    # WETH variants
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": WETH_PRICE_USD,  # ETH mainnet WETH
    "0x82af49447d8a07e3bd95bd0d56f14dc194175863": WETH_PRICE_USD,  # Arb WETH
    "0x4200000000000000000000000000000000000006": WETH_PRICE_USD,  # Base WETH
    # WBTC
    "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": 65000,
    "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f": 65000,
    # wstETH, cbETH, rETH ~ ETH price
    "0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0": WETH_PRICE_USD,  # wstETH
    "0xbe9895146f7af43049ca1c1ae358b0541ea49704": WETH_PRICE_USD,  # cbETH
    "0xae78736cd615f374d3085123a210448e74fc6393": WETH_PRICE_USD,  # rETH
    "0xec53bf9167f50cdeb3ae105f56099aaab9061f83": WETH_PRICE_USD,  # EIGEN (rough)
}


# ---------------------------------------------------------------------------
# RPC helpers
# ---------------------------------------------------------------------------

def rpc_call(rpc_url, method, params):
    """Make a JSON-RPC call via curl."""
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    try:
        r = subprocess.run(
            ["curl", "-s", "-X", "POST", rpc_url,
             "-H", "Content-Type: application/json",
             "-d", payload],
            capture_output=True, text=True, timeout=30
        )
        resp = json.loads(r.stdout)
        if "error" in resp:
            print(f"  [RPC error] {resp['error']}", file=sys.stderr)
            return None
        return resp.get("result")
    except Exception as e:
        print(f"  [RPC exception] {e}", file=sys.stderr)
        return None


def get_block_number(rpc_url):
    result = rpc_call(rpc_url, "eth_blockNumber", [])
    return int(result, 16) if result else 0


def get_logs(rpc_url, address, topic, from_block, to_block):
    """Fetch logs, chunking if the range is too large."""
    MAX_CHUNK = 10000
    all_logs = []
    start = from_block
    while start <= to_block:
        end = min(start + MAX_CHUNK - 1, to_block)
        result = rpc_call(rpc_url, "eth_getLogs", [{
            "address": address,
            "topics": [topic],
            "fromBlock": hex(start),
            "toBlock": hex(end),
        }])
        if result is None:
            # Try smaller chunk
            if (end - start) > 1000:
                end = start + 999
                result = rpc_call(rpc_url, "eth_getLogs", [{
                    "address": address,
                    "topics": [topic],
                    "fromBlock": hex(start),
                    "toBlock": hex(end),
                }])
            if result is None:
                print(f"    [warn] Failed to fetch logs {start}-{end}", file=sys.stderr)
                start = end + 1
                continue
        all_logs.extend(result)
        start = end + 1
    return all_logs


def get_block_timestamp(rpc_url, block_hex):
    """Get timestamp of a block."""
    result = rpc_call(rpc_url, "eth_getBlockByNumber", [block_hex, False])
    if result and "timestamp" in result:
        return int(result["timestamp"], 16)
    return None


# ---------------------------------------------------------------------------
# Decoding
# ---------------------------------------------------------------------------

def decode_aave_liquidation(log):
    """
    Decode Aave V3 LiquidationCall event.
    Indexed: collateralAsset (topic1), debtAsset (topic2), user (topic3)
    Data: debtToCover (uint256), liquidatedCollateralAmount (uint256),
          liquidator (address), receiveAToken (bool)
    """
    topics = log.get("topics", [])
    data = log.get("data", "0x")[2:]

    if len(topics) < 4 or len(data) < 256:
        return None

    collateral_asset = "0x" + topics[1][-40:]
    debt_asset = "0x" + topics[2][-40:]
    user = "0x" + topics[3][-40:]

    debt_to_cover = int(data[0:64], 16)
    liquidated_collateral = int(data[64:128], 16)
    liquidator = "0x" + data[128+24:192]  # address is right-padded in 32 bytes
    # receiveAToken = bool in data[192:256], not needed

    return {
        "collateral_asset": collateral_asset.lower(),
        "debt_asset": debt_asset.lower(),
        "user": user.lower(),
        "debt_to_cover": debt_to_cover,
        "liquidated_collateral": liquidated_collateral,
        "liquidator": liquidator.lower(),
        "tx_hash": log.get("transactionHash", ""),
        "block": int(log.get("blockNumber", "0x0"), 16),
    }


def decode_compound_absorb(log):
    """
    Decode Compound V3 AbsorbCollateral event.
    Indexed: absorber (topic1), borrower (topic2), asset (topic3)
    Data: collateralAbsorbed (uint256), usdValue (uint256)
    """
    topics = log.get("topics", [])
    data = log.get("data", "0x")[2:]

    if len(topics) < 4 or len(data) < 128:
        return None

    absorber = "0x" + topics[1][-40:]
    borrower = "0x" + topics[2][-40:]
    asset = "0x" + topics[3][-40:]

    collateral_absorbed = int(data[0:64], 16)
    usd_value = int(data[64:128], 16)

    return {
        "absorber": absorber.lower(),
        "borrower": borrower.lower(),
        "asset": asset.lower(),
        "collateral_absorbed": collateral_absorbed,
        "usd_value": usd_value,  # This is already in USD with decimals
        "tx_hash": log.get("transactionHash", ""),
        "block": int(log.get("blockNumber", "0x0"), 16),
    }


def estimate_usd_value(token_addr, raw_amount):
    """Rough USD estimate for a token amount."""
    token_addr = token_addr.lower()
    decimals = TOKEN_DECIMALS.get(token_addr, 18)
    price = TOKEN_USD_PRICE.get(token_addr, None)
    amount = raw_amount / (10 ** decimals)

    if price is not None:
        return amount * price

    # Unknown token: assume it's roughly $1 per unit (conservative for sizing)
    # This will undercount but won't wildly overcount
    return amount * 1.0  # could be very wrong for 18-decimal governance tokens


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

def analyze_liquidations(liquidations, hours_covered, chain_name):
    """Analyze a list of decoded Aave liquidation events."""
    if not liquidations:
        print(f"\n  No liquidations found in the lookback window.")
        return

    n = len(liquidations)
    per_day = n / hours_covered * 24 if hours_covered > 0 else 0

    # Compute USD values
    usd_values = []
    for liq in liquidations:
        # Use debt side for sizing (what the liquidator repays)
        usd = estimate_usd_value(liq["debt_asset"], liq["debt_to_cover"])
        usd_values.append(usd)

    usd_values.sort()
    total_usd = sum(usd_values)
    mean_usd = total_usd / n
    median_usd = usd_values[n // 2]

    print(f"\n  Liquidation count: {n} in {hours_covered:.1f}h => ~{per_day:.0f}/day")
    print(f"  Total volume (debt side): ${total_usd:,.0f}")
    print(f"  Daily volume projection: ${total_usd / hours_covered * 24:,.0f}/day")
    print(f"  Mean size: ${mean_usd:,.0f}")
    print(f"  Median size: ${median_usd:,.0f}")
    if n >= 4:
        print(f"  P25: ${usd_values[n//4]:,.0f}  P75: ${usd_values[3*n//4]:,.0f}")
    print(f"  Min: ${usd_values[0]:,.0f}  Max: ${usd_values[-1]:,.0f}")

    # Size distribution
    buckets = [
        (0, 100),
        (100, 1000),
        (1000, 10000),
        (10000, 50000),
        (50000, 100000),
        (100000, 500000),
        (500000, float('inf')),
    ]
    print(f"\n  Size distribution (debt USD):")
    for lo, hi in buckets:
        count = sum(1 for v in usd_values if lo <= v < hi)
        if count > 0:
            pct = count / n * 100
            vol = sum(v for v in usd_values if lo <= v < hi)
            bar = "#" * int(pct / 2)
            hi_str = f"${hi:>10,.0f}" if hi < float('inf') else "       inf"
            print(f"    ${lo:>10,.0f} - {hi_str}: {count:>4} ({pct:>5.1f}%)  vol=${vol:>12,.0f}  {bar}")

    # Top liquidators
    liquidator_stats = defaultdict(lambda: {"count": 0, "volume": 0.0})
    for liq, usd in zip(liquidations, usd_values):
        addr = liq["liquidator"]
        liquidator_stats[addr]["count"] += 1
        liquidator_stats[addr]["volume"] += usd

    sorted_liquidators = sorted(liquidator_stats.items(), key=lambda x: -x[1]["volume"])
    print(f"\n  Top liquidators (by volume):")
    for i, (addr, stats) in enumerate(sorted_liquidators[:10]):
        share = stats["volume"] / total_usd * 100 if total_usd > 0 else 0
        print(f"    {i+1}. {addr}  txns={stats['count']:>4}  "
              f"vol=${stats['volume']:>12,.0f}  share={share:>5.1f}%")

    # Concentration
    if sorted_liquidators:
        top3_vol = sum(s["volume"] for _, s in sorted_liquidators[:3])
        top3_share = top3_vol / total_usd * 100 if total_usd > 0 else 0
        print(f"\n  Top-3 liquidator concentration: {top3_share:.1f}% of volume")

    return {
        "count": n,
        "per_day": per_day,
        "daily_volume_usd": total_usd / hours_covered * 24 if hours_covered > 0 else 0,
        "mean_usd": mean_usd,
        "median_usd": median_usd,
        "top3_share": top3_share if sorted_liquidators else 0,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def load_env():
    """Load .env file if present."""
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".env")
    if not os.path.exists(env_path):
        # Try /root/mev/.env for server
        env_path = "/root/mev/.env"
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, _, val = line.partition("=")
                    os.environ.setdefault(key.strip(), val.strip())


def main():
    load_env()

    print("=" * 80)
    print("LIQUIDATION OPPORTUNITY SIZING")
    print("Aave V3 on Ethereum, Arbitrum, Base + Compound V3 on Ethereum")
    print(f"WETH price assumption: ${WETH_PRICE_USD}")
    print("=" * 80)

    all_results = {}

    # -----------------------------------------------------------------------
    # Aave V3 across chains
    # -----------------------------------------------------------------------
    for chain_name, cfg in CHAINS.items():
        rpc_url = os.environ.get(cfg["rpc_env"], cfg["rpc_fallback"])
        print(f"\n{'='*80}")
        print(f"AAVE V3 - {chain_name.upper()}")
        print(f"  RPC: {rpc_url[:50]}...")
        print(f"  Pool: {cfg['aave_pool']}")

        current_block = get_block_number(rpc_url)
        if current_block == 0:
            print(f"  [ERROR] Could not get block number. Skipping {chain_name}.")
            continue

        from_block = current_block - cfg["blocks_lookback"]
        hours_covered = cfg["blocks_lookback"] * cfg["block_time_s"] / 3600

        print(f"  Current block: {current_block}")
        print(f"  Looking back {cfg['blocks_lookback']} blocks (~{hours_covered:.1f} hours)")

        # Get timestamps for accurate time range
        from_ts = get_block_timestamp(rpc_url, hex(from_block))
        to_ts = get_block_timestamp(rpc_url, hex(current_block))
        if from_ts and to_ts:
            actual_hours = (to_ts - from_ts) / 3600
            print(f"  Actual time range: {actual_hours:.1f} hours")
            hours_covered = actual_hours

        print(f"\n  Fetching Aave V3 LiquidationCall events...")
        logs = get_logs(rpc_url, cfg["aave_pool"], AAVE_LIQUIDATION_TOPIC, from_block, current_block)
        print(f"  Raw logs fetched: {len(logs)}")

        liquidations = []
        for log in logs:
            decoded = decode_aave_liquidation(log)
            if decoded:
                liquidations.append(decoded)

        print(f"  Decoded liquidations: {len(liquidations)}")
        result = analyze_liquidations(liquidations, hours_covered, chain_name)
        if result:
            all_results[f"aave_{chain_name}"] = result

    # -----------------------------------------------------------------------
    # Compound V3 on Ethereum
    # -----------------------------------------------------------------------
    print(f"\n{'='*80}")
    print(f"COMPOUND V3 (Comet) - ETHEREUM")
    rpc_url = os.environ.get("ETH_RPC_HTTP", "https://eth.llamarpc.com")
    print(f"  RPC: {rpc_url[:50]}...")

    current_block = get_block_number(rpc_url)
    if current_block > 0:
        from_block = current_block - 2000
        hours_covered = 2000 * 12 / 3600

        from_ts = get_block_timestamp(rpc_url, hex(from_block))
        to_ts = get_block_timestamp(rpc_url, hex(current_block))
        if from_ts and to_ts:
            hours_covered = (to_ts - from_ts) / 3600

        total_compound_liqs = 0
        for market_name, comet_addr in COMPOUND_COMETS.items():
            print(f"\n  Comet {market_name} ({comet_addr}):")
            logs = get_logs(rpc_url, comet_addr, COMPOUND_ABSORB_TOPIC, from_block, current_block)
            print(f"    AbsorbCollateral events: {len(logs)}")
            total_compound_liqs += len(logs)

            for log in logs:
                decoded = decode_compound_absorb(log)
                if decoded:
                    # usd_value in Compound is typically 8 decimal (price feed)
                    usd = decoded["usd_value"] / 1e8
                    print(f"    - tx={decoded['tx_hash'][:18]}... "
                          f"absorber={decoded['absorber'][:14]}... "
                          f"usd~${usd:,.0f}")

        if total_compound_liqs == 0:
            print(f"\n  No Compound V3 liquidations in last ~{hours_covered:.1f}h")
            print(f"  (Compound liquidations are rarer - protocol absorbs bad debt)")
    else:
        print(f"  [ERROR] Could not get block number.")

    # -----------------------------------------------------------------------
    # Summary and GO/KILL assessment
    # -----------------------------------------------------------------------
    print(f"\n{'='*80}")
    print("SUMMARY")
    print("=" * 80)

    total_daily_volume = 0
    total_daily_count = 0

    for key, result in all_results.items():
        print(f"\n  {key}:")
        print(f"    ~{result['per_day']:.0f} liquidations/day")
        print(f"    ~${result['daily_volume_usd']:,.0f}/day volume")
        print(f"    Median size: ${result['median_usd']:,.0f}")
        print(f"    Top-3 share: {result['top3_share']:.0f}%")
        total_daily_volume += result["daily_volume_usd"]
        total_daily_count += result["per_day"]

    print(f"\n  TOTAL across chains:")
    print(f"    ~{total_daily_count:.0f} liquidations/day")
    print(f"    ~${total_daily_volume:,.0f}/day total liquidation volume")

    # Profit estimation
    # Liquidation bonus is typically 5-10% on Aave
    # But the liquidator's net profit depends on:
    #  - Gas costs
    #  - Competition (most use flashbots bundles)
    #  - Slippage on selling collateral
    # Realistic net margin for a new entrant: 0.1-0.5% of liquidation volume
    # Top liquidators with optimized infra: maybe 1-2%

    if total_daily_volume > 0:
        for margin_bps in [10, 25, 50, 100]:
            margin_pct = margin_bps / 100
            profit = total_daily_volume * margin_pct / 100
            print(f"    At {margin_bps}bps net margin: ${profit:,.0f}/day")

    print(f"\n{'='*80}")
    print("GO/KILL ASSESSMENT")
    print("=" * 80)

    # Decision logic
    if total_daily_volume == 0:
        print("""
  VERDICT: INSUFFICIENT DATA

  Could not fetch liquidation data. Re-run with working RPC endpoints.
  The script needs ETH_RPC_HTTP, ARB_RPC_HTTP, BASE_RPC_HTTP in .env.
""")
    elif total_daily_volume < 500_000:
        print(f"""
  VERDICT: LIKELY KILL

  Total daily liquidation volume across 3 chains: ~${total_daily_volume:,.0f}
  Even at 100bps net margin (optimistic for a new entrant), that's only
  ${total_daily_volume * 0.01:,.0f}/day.

  To reach $1,000/day you'd need ~{100_000/max(total_daily_volume,1)*100:.0f}% of all volume
  at a generous margin. Top-3 liquidators already control {
  max((r['top3_share'] for r in all_results.values()), default=0):.0f}%+ of volume.

  Additional concerns:
  - Liquidation MEV is highly competitive (specialized bots, flashbots bundles)
  - Requires significant capital or flash loan integration
  - Volume is spiky (correlated with market volatility)
  - Low-volume periods (sideways markets) could mean days with $0 revenue
""")
    else:
        avg_top3 = sum(r["top3_share"] for r in all_results.values()) / max(len(all_results), 1)
        print(f"""
  VERDICT: NEEDS DEEPER ANALYSIS

  Total daily liquidation volume: ~${total_daily_volume:,.0f}
  The TAM exists, but profitability depends on:

  1. COMPETITION: Top-3 liquidators control ~{avg_top3:.0f}% of volume.
     Can we compete with their infrastructure?

  2. MARGIN: Realistic net margin for a new entrant is 10-50bps after gas.
     At 25bps: ${total_daily_volume * 0.0025:,.0f}/day.

  3. VOLATILITY DEPENDENCE: Liquidation volume spikes 10-50x during crashes.
     Daily average understates opportunity during volatile periods but
     overstates during calm markets.

  4. CAPITAL: Need flash loans or ~${max(r['median_usd'] for r in all_results.values()) * 5:,.0f}
     to cover median liquidations.

  NEXT STEPS if pursuing:
  - Check EigenPhi/Dune for longer-term (30d/90d) liquidation volumes
  - Analyze gas costs per liquidation
  - Profile top liquidator strategies (flashbots vs public mempool)
  - Prototype with flash loans on a testnet
""")

    # Note about data window
    print(f"\n  NOTE: This is a ~{max(2000*12/3600, min(cfg['blocks_lookback']*cfg['block_time_s']/3600 for cfg in CHAINS.values())):.0f}h snapshot.")
    print(f"  Liquidation volume is HIGHLY dependent on market conditions.")
    print(f"  A calm market day could show 10x less than a volatile day.")
    print(f"  For reliable sizing, check 30-day data on Dune/EigenPhi.")


if __name__ == "__main__":
    main()
