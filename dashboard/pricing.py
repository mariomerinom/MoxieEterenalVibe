#!/usr/bin/env python3
"""
ETH/USD price engine — derives price from captured WETH/USDC V3 swap data.

The USDC/WETH 0.05% pool (0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640) is the
highest-volume Uniswap V3 pool. Token ordering: token0=USDC, token1=WETH.

For V3 swaps, amount_in/amount_out are the absolute values of the signed
int256 amounts. We use the ratio to derive ETH price in USD.

USDC has 6 decimals, WETH has 18 decimals.
"""

import duckdb
import pandas as pd
import numpy as np
from pathlib import Path

# USDC/WETH 0.05% pool on Ethereum mainnet
WETH_USDC_POOL = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"

# Token decimals
USDC_DECIMALS = 6
WETH_DECIMALS = 18

DATA_DIR = Path(__file__).parent.parent / "data"


def get_connection():
    """Get a DuckDB connection with Parquet views."""
    conn = duckdb.connect(str(DATA_DIR / "mev.duckdb"), read_only=True)
    return conn


def _build_price_table(conn) -> pd.DataFrame:
    """
    Build block_number -> eth_usd price from WETH/USDC swap data.

    In the USDC/WETH pool (token0=USDC, token1=WETH):
    - amount_in and amount_out are stored as string representations of uint256
    - For V3 swaps, the parser stores absolute values

    We look at swaps in this specific pool and compute price from the amounts.
    Since token0=USDC and token1=WETH:
    - If token_in is token0 (USDC going in), WETH comes out: price = usdc_in / weth_out
    - If token_in is token1 (WETH going in), USDC comes out: price = usdc_out / weth_in

    Since we stored token_in/token_out as 0x0 in Phase 1.0, we use a heuristic:
    amount_in and amount_out magnitudes differ by ~12 orders of magnitude (6 vs 18 decimals).
    The smaller raw number is USDC, the larger is WETH.
    """
    df = conn.execute("""
        SELECT
            block_number,
            CAST(amount_in AS DOUBLE) as amount_in,
            CAST(amount_out AS DOUBLE) as amount_out
        FROM read_parquet('data/events/swaps/*/*.parquet', union_by_name=true)
        WHERE lower(pool) = ?
        ORDER BY block_number, log_index
    """, [WETH_USDC_POOL]).fetchdf()

    if df.empty:
        # Try without lowercasing
        df = conn.execute("""
            SELECT
                block_number,
                CAST(amount_in AS DOUBLE) as amount_in,
                CAST(amount_out AS DOUBLE) as amount_out
            FROM read_parquet('data/events/swaps/*/*.parquet', union_by_name=true)
            WHERE pool = ?
            ORDER BY block_number, log_index
        """, [WETH_USDC_POOL]).fetchdf()

    if df.empty:
        return pd.DataFrame(columns=["block_number", "eth_usd"])

    prices = []
    for _, row in df.iterrows():
        amt_in = row["amount_in"]
        amt_out = row["amount_out"]

        if amt_in <= 0 or amt_out <= 0:
            continue

        # Determine which is USDC (6 dec) and which is WETH (18 dec)
        # USDC raw values are much smaller than WETH raw values for same USD amount
        # e.g., $3000 USDC = 3000 * 1e6 = 3e9, $3000 WETH = 1 * 1e18 = 1e18
        # Heuristic: the amount with fewer digits is USDC

        if amt_in < amt_out:
            # amount_in is USDC, amount_out is WETH
            usdc_amount = amt_in / (10 ** USDC_DECIMALS)
            weth_amount = amt_out / (10 ** WETH_DECIMALS)
        else:
            # amount_in is WETH, amount_out is USDC
            weth_amount = amt_in / (10 ** WETH_DECIMALS)
            usdc_amount = amt_out / (10 ** USDC_DECIMALS)

        if weth_amount > 0:
            price = usdc_amount / weth_amount
            # Sanity check: ETH price should be between $500 and $20,000
            if 500 < price < 20000:
                prices.append({
                    "block_number": int(row["block_number"]),
                    "eth_usd": price,
                })

    if not prices:
        return pd.DataFrame(columns=["block_number", "eth_usd"])

    price_df = pd.DataFrame(prices)
    # Average multiple swaps in the same block
    price_df = price_df.groupby("block_number").agg({"eth_usd": "median"}).reset_index()
    return price_df.sort_values("block_number")


class PriceEngine:
    """Cached ETH/USD price lookup."""

    def __init__(self, conn=None):
        self._conn = conn or get_connection()
        self._price_df = None
        self._block_range = None
        self._fallback_price = 3000.0  # Reasonable default

    def _ensure_loaded(self):
        if self._price_df is None:
            self._price_df = _build_price_table(self._conn)
            if not self._price_df.empty:
                self._block_range = (
                    self._price_df["block_number"].min(),
                    self._price_df["block_number"].max(),
                )
                # Use median as fallback
                self._fallback_price = self._price_df["eth_usd"].median()

    def get_eth_price(self, block_number: int) -> float:
        """Get ETH/USD price at a specific block. Uses nearest known price."""
        self._ensure_loaded()

        if self._price_df.empty:
            return self._fallback_price

        # Find nearest block with a price
        idx = np.searchsorted(self._price_df["block_number"].values, block_number)
        if idx >= len(self._price_df):
            idx = len(self._price_df) - 1
        elif idx > 0:
            # Pick closer of the two adjacent entries
            before = self._price_df.iloc[idx - 1]
            after = self._price_df.iloc[idx]
            if abs(block_number - before["block_number"]) < abs(block_number - after["block_number"]):
                idx = idx - 1

        return float(self._price_df.iloc[idx]["eth_usd"])

    def get_price_series(self) -> pd.DataFrame:
        """Get full block_number -> eth_usd DataFrame."""
        self._ensure_loaded()
        return self._price_df.copy()

    def get_average_price(self) -> float:
        """Get average ETH/USD price across the capture window."""
        self._ensure_loaded()
        if self._price_df.empty:
            return self._fallback_price
        return float(self._price_df["eth_usd"].median())

    def get_price_range(self) -> tuple:
        """Get (min_price, max_price, median_price)."""
        self._ensure_loaded()
        if self._price_df.empty:
            return (self._fallback_price, self._fallback_price, self._fallback_price)
        return (
            float(self._price_df["eth_usd"].min()),
            float(self._price_df["eth_usd"].max()),
            float(self._price_df["eth_usd"].median()),
        )


def _try_coingecko_fallback(start_ts: int, end_ts: int) -> float:
    """Fallback: fetch ETH price from CoinGecko if swap data unavailable."""
    try:
        import urllib.request
        import json
        url = f"https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd"
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
            return data["ethereum"]["usd"]
    except Exception:
        return 3000.0  # Hard fallback


if __name__ == "__main__":
    """Quick test: print price stats from captured data."""
    import os
    os.chdir(Path(__file__).parent.parent)  # Run from mev/ dir

    try:
        engine = PriceEngine()
        price_range = engine.get_price_range()
        series = engine.get_price_series()

        print(f"ETH/USD Price Engine")
        print(f"  Data points:  {len(series):,}")
        print(f"  Min price:    ${price_range[0]:,.2f}")
        print(f"  Max price:    ${price_range[1]:,.2f}")
        print(f"  Median price: ${price_range[2]:,.2f}")

        if not series.empty:
            print(f"  Block range:  {series['block_number'].min():,} → {series['block_number'].max():,}")
    except Exception as e:
        print(f"Error: {e}")
        print("(Run this from the mev/ directory with Parquet data present)")
