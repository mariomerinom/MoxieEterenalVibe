#!/usr/bin/env python3
"""
Strategy backtest engine — runs simplified strategy logic against captured data.

Outputs per-strategy results: detected count, estimated profit, gas cost,
competition HHI, and a Go/Investigate/Skip verdict.

Verdict thresholds (from scoring.rs / risk.toml):
  Go:          score > 60 AND monthly_profit > $20K AND HHI < 0.3
  Investigate: score > 40
  Skip:        everything else
"""

import duckdb
import pandas as pd
import numpy as np
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import List

from pricing import PriceEngine

DATA_DIR = Path(__file__).parent.parent / "data"
DUCKDB_PATH = str(DATA_DIR / "mev.duckdb")

from queries import _globs, _actor_col


@dataclass
class StrategyResult:
    strategy: str
    detected_count: int
    estimated_monthly_profit_usd: float
    gas_cost_usd: float
    net_monthly_profit_usd: float
    competition_hhi: float
    unique_competitors: int
    win_rate: float  # 0-1
    sample_tx_hashes: List[str]
    verdict: str  # "Go", "Investigate", "Skip"
    score: float  # 0-100


def _compute_verdict(monthly_profit: float, hhi: float, score: float) -> str:
    if score > 60 and monthly_profit > 20000 and hhi < 0.3:
        return "Go"
    elif score > 40:
        return "Investigate"
    else:
        return "Skip"


def _compute_score(monthly_profit: float, hhi: float, complexity: float, trend: float) -> float:
    """
    Weighted score: 40% market size + 30% competition + 20% complexity + 10% trend.
    All inputs normalized to 0-100.
    """
    # Market size: log scale, $1K=20, $10K=50, $100K=80, $1M=100
    if monthly_profit <= 0:
        size_score = 0
    else:
        size_score = min(100, max(0, 20 * np.log10(monthly_profit / 100)))

    # Competition: inverse HHI (lower concentration = better)
    competition_score = max(0, 100 * (1 - hhi))

    # Complexity: 0=easy, 100=hard (inverted for scoring)
    complexity_score = max(0, 100 - complexity)

    # Trend: positive trend = higher score
    trend_score = max(0, min(100, 50 + trend * 50))

    return (
        0.4 * size_score +
        0.3 * competition_score +
        0.2 * complexity_score +
        0.1 * trend_score
    )


def check_sandwich(conn, price_engine: PriceEngine, days: float, chain: str = "all") -> StrategyResult:
    """
    Detect sandwich patterns: same actor has ≥2 swaps in same block+pool
    with other actors' swaps sandwiched between them.
    """
    bg, sg, _ = _globs(chain)
    actor = _actor_col(conn, sg)

    # Find sandwich patterns
    df = conn.execute(f"""
        WITH block_pool_senders AS (
            SELECT
                block_number, pool, {actor} as actor,
                count(*) as swap_count,
                min(log_index) as first_log,
                max(log_index) as last_log
            FROM read_parquet('{sg}', union_by_name=true)
            WHERE {actor} IS NOT NULL AND {actor} != ''
            GROUP BY block_number, pool, {actor}
            HAVING count(*) >= 2
        ),
        sandwiches AS (
            SELECT
                bps.block_number,
                bps.pool,
                bps.actor as bot,
                bps.first_log,
                bps.last_log,
                count(distinct s.{actor}) as victims
            FROM block_pool_senders bps
            JOIN read_parquet('{sg}', union_by_name=true) s
                ON s.block_number = bps.block_number
                AND s.pool = bps.pool
                AND s.{actor} != bps.actor
                AND s.log_index > bps.first_log
                AND s.log_index < bps.last_log
            GROUP BY bps.block_number, bps.pool, bps.actor, bps.first_log, bps.last_log
        )
        SELECT
            bot,
            count(*) as sandwich_count,
            sum(victims) as total_victims
        FROM sandwiches
        GROUP BY bot
        ORDER BY sandwich_count DESC
    """).fetchdf()

    total_sandwiches = int(df["sandwich_count"].sum()) if not df.empty else 0
    unique_bots = len(df) if not df.empty else 0

    # Get sample tx hashes
    samples = conn.execute(f"""
        WITH block_pool_senders AS (
            SELECT block_number, pool, {actor} as actor, min(log_index) as first_log, max(log_index) as last_log
            FROM read_parquet('{sg}', union_by_name=true)
            WHERE {actor} IS NOT NULL AND {actor} != ''
            GROUP BY block_number, pool, {actor}
            HAVING count(*) >= 2
        )
        SELECT DISTINCT s.tx_hash
        FROM block_pool_senders bps
        JOIN read_parquet('{sg}', union_by_name=true) s
            ON s.block_number = bps.block_number AND s.pool = bps.pool AND s.{actor} = bps.actor
        LIMIT 10
    """).fetchdf()
    sample_hashes = samples["tx_hash"].tolist() if not samples.empty else []

    # HHI among sandwich bots
    if not df.empty and total_sandwiches > 0:
        shares = df["sandwich_count"] / total_sandwiches
        hhi = float((shares ** 2).sum())
    else:
        hhi = 1.0

    # Estimate profit: typical sandwich profit ~$5-50 per sandwich
    # Conservative estimate: $10 average profit per detected pattern
    avg_profit_per_sandwich = 10.0
    monthly_profit = total_sandwiches / max(days, 1) * 30 * avg_profit_per_sandwich

    # Gas cost estimate: ~200K gas per sandwich at median base fee
    gas_stats = conn.execute(f"""
        SELECT avg(base_fee_gwei) FROM read_parquet('{bg}', union_by_name=true)
        WHERE base_fee_gwei IS NOT NULL
    """).fetchone()
    avg_gas_gwei = gas_stats[0] if gas_stats[0] else 20.0
    gas_per_sandwich_eth = 200_000 * avg_gas_gwei / 1e9
    eth_price = price_engine.get_average_price()
    monthly_gas_cost = total_sandwiches / max(days, 1) * 30 * gas_per_sandwich_eth * eth_price

    net_profit = monthly_profit - monthly_gas_cost
    score = _compute_score(net_profit, hhi, complexity=60, trend=0)
    verdict = _compute_verdict(net_profit, hhi, score)

    return StrategyResult(
        strategy="Sandwich",
        detected_count=total_sandwiches,
        estimated_monthly_profit_usd=round(monthly_profit, 2),
        gas_cost_usd=round(monthly_gas_cost, 2),
        net_monthly_profit_usd=round(net_profit, 2),
        competition_hhi=round(hhi, 4),
        unique_competitors=unique_bots,
        win_rate=0.0,  # Can't determine from passive observation
        sample_tx_hashes=sample_hashes[:5],
        verdict=verdict,
        score=round(score, 1),
    )


def check_dex_arb(conn, price_engine: PriceEngine, days: float, chain: str = "all") -> StrategyResult:
    """
    Detect DEX arbitrage: transactions with ≥2 swaps across ≥2 different pools.
    """
    bg, sg, _ = _globs(chain)
    actor = _actor_col(conn, sg)

    # Count arb-shaped transactions
    df = conn.execute(f"""
        WITH tx_swaps AS (
            SELECT
                tx_hash,
                count(*) as swap_count,
                count(distinct pool) as pool_count
            FROM read_parquet('{sg}', union_by_name=true)
            GROUP BY tx_hash
            HAVING count(*) >= 2 AND count(distinct pool) >= 2
        )
        SELECT swap_count, pool_count, count(*) as tx_count
        FROM tx_swaps
        GROUP BY swap_count, pool_count
        ORDER BY swap_count, pool_count
    """).fetchdf()

    total_arb_txs = int(df["tx_count"].sum()) if not df.empty else 0

    # Get actors for HHI calculation
    sender_df = conn.execute(f"""
        WITH arb_txs AS (
            SELECT tx_hash
            FROM read_parquet('{sg}', union_by_name=true)
            GROUP BY tx_hash
            HAVING count(*) >= 2 AND count(distinct pool) >= 2
        )
        SELECT s.{actor} as sender, count(distinct s.tx_hash) as arb_count
        FROM read_parquet('{sg}', union_by_name=true) s
        WHERE s.tx_hash IN (SELECT tx_hash FROM arb_txs)
            AND s.{actor} IS NOT NULL AND s.{actor} != ''
        GROUP BY s.{actor}
        ORDER BY arb_count DESC
    """).fetchdf()

    unique_arbers = len(sender_df) if not sender_df.empty else 0

    # HHI
    if not sender_df.empty and total_arb_txs > 0:
        shares = sender_df["arb_count"] / sender_df["arb_count"].sum()
        hhi = float((shares ** 2).sum())
    else:
        hhi = 1.0

    # Sample hashes
    samples = conn.execute(f"""
        SELECT tx_hash FROM (
            SELECT tx_hash, count(*) as sc, count(distinct pool) as pc
            FROM read_parquet('{sg}', union_by_name=true)
            GROUP BY tx_hash
            HAVING sc >= 3 AND pc >= 2
        )
        LIMIT 10
    """).fetchdf()
    sample_hashes = samples["tx_hash"].tolist() if not samples.empty else []

    # Estimate profit: typical arb $5-100 per tx
    avg_profit_per_arb = 15.0
    monthly_profit = total_arb_txs / max(days, 1) * 30 * avg_profit_per_arb

    # Gas cost: ~300K gas per arb tx
    gas_stats = conn.execute(f"""
        SELECT avg(base_fee_gwei) FROM read_parquet('{bg}', union_by_name=true)
        WHERE base_fee_gwei IS NOT NULL
    """).fetchone()
    avg_gas_gwei = gas_stats[0] if gas_stats[0] else 20.0
    gas_per_arb_eth = 300_000 * avg_gas_gwei / 1e9
    eth_price = price_engine.get_average_price()
    monthly_gas_cost = total_arb_txs / max(days, 1) * 30 * gas_per_arb_eth * eth_price

    # Win rate: not available without transaction parquet
    win_rate = 0.0

    net_profit = monthly_profit - monthly_gas_cost
    score = _compute_score(net_profit, hhi, complexity=40, trend=0)
    verdict = _compute_verdict(net_profit, hhi, score)

    return StrategyResult(
        strategy="DEX Arbitrage",
        detected_count=total_arb_txs,
        estimated_monthly_profit_usd=round(monthly_profit, 2),
        gas_cost_usd=round(monthly_gas_cost, 2),
        net_monthly_profit_usd=round(net_profit, 2),
        competition_hhi=round(hhi, 4),
        unique_competitors=unique_arbers,
        win_rate=round(win_rate, 3),
        sample_tx_hashes=sample_hashes[:5],
        verdict=verdict,
        score=round(score, 1),
    )


def check_liquidation(conn, price_engine: PriceEngine, days: float, chain: str = "all") -> StrategyResult:
    """Analyze liquidation opportunities."""
    _, _, lg = _globs(chain)
    try:
        stats = conn.execute(f"""
            SELECT
                count(*) as total,
                count(distinct liquidator) as unique_liquidators,
                avg(gas_used) as avg_gas,
                avg(gas_price_gwei) as avg_gas_price
            FROM read_parquet('{lg}', union_by_name=true)
        """).fetchone()
    except Exception:
        stats = (0, 0, 500000, 20.0)

    total_liqs = stats[0] if stats[0] else 0
    unique_liquidators = stats[1] if stats[1] else 0
    avg_gas = stats[2] if stats[2] else 500_000
    avg_gas_price = stats[3] if stats[3] else 20.0

    # HHI among liquidators
    try:
        liq_df = conn.execute(f"""
            SELECT liquidator, count(*) as cnt
            FROM read_parquet('{lg}', union_by_name=true)
            GROUP BY liquidator
        """).fetchdf()
    except Exception:
        liq_df = pd.DataFrame()

    if not liq_df.empty and total_liqs > 0:
        shares = liq_df["cnt"] / total_liqs
        hhi = float((shares ** 2).sum())
    else:
        hhi = 1.0

    # Sample hashes
    try:
        samples = conn.execute(f"""
            SELECT tx_hash
            FROM read_parquet('{lg}', union_by_name=true)
            LIMIT 10
        """).fetchdf()
        sample_hashes = samples["tx_hash"].tolist() if not samples.empty else []
    except Exception:
        sample_hashes = []

    # Estimate profit: liquidation bonus is typically 5-10% of collateral
    # Without token prices, estimate ~$50-500 per liquidation
    avg_profit_per_liq = 100.0
    monthly_profit = total_liqs / max(days, 1) * 30 * avg_profit_per_liq

    # Gas cost
    eth_price = price_engine.get_average_price()
    gas_per_liq_eth = avg_gas * avg_gas_price / 1e9
    monthly_gas_cost = total_liqs / max(days, 1) * 30 * gas_per_liq_eth * eth_price

    net_profit = monthly_profit - monthly_gas_cost
    score = _compute_score(net_profit, hhi, complexity=50, trend=0)
    verdict = _compute_verdict(net_profit, hhi, score)

    return StrategyResult(
        strategy="Liquidation",
        detected_count=total_liqs,
        estimated_monthly_profit_usd=round(monthly_profit, 2),
        gas_cost_usd=round(monthly_gas_cost, 2),
        net_monthly_profit_usd=round(net_profit, 2),
        competition_hhi=round(hhi, 4),
        unique_competitors=unique_liquidators,
        win_rate=0.0,
        sample_tx_hashes=sample_hashes[:5],
        verdict=verdict,
        score=round(score, 1),
    )


def check_backrun(conn, price_engine: PriceEngine, days: float, chain: str = "all") -> StrategyResult:
    """
    Detect backrun opportunities: large swaps that could be backrun for profit.
    """
    bg, sg, _ = _globs(chain)

    # Count large single-swap txs (potential backrun targets)
    targets = conn.execute(f"""
        WITH single_swaps AS (
            SELECT tx_hash, count(*) as sc
            FROM read_parquet('{sg}', union_by_name=true)
            GROUP BY tx_hash
            HAVING count(*) = 1
        )
        SELECT count(*) FROM single_swaps
    """).fetchone()
    total_targets = targets[0] if targets else 0

    # Estimate: ~5% of single swaps are profitably backrunnable, ~$5 per backrun
    backrunnable = int(total_targets * 0.05)
    avg_profit_per_backrun = 5.0
    monthly_profit = backrunnable / max(days, 1) * 30 * avg_profit_per_backrun

    # Gas cost: ~150K gas per backrun
    gas_stats = conn.execute(f"""
        SELECT avg(base_fee_gwei) FROM read_parquet('{bg}', union_by_name=true)
        WHERE base_fee_gwei IS NOT NULL
    """).fetchone()
    avg_gas_gwei = gas_stats[0] if gas_stats[0] else 20.0
    eth_price = price_engine.get_average_price()
    gas_per_backrun_eth = 150_000 * avg_gas_gwei / 1e9
    monthly_gas_cost = backrunnable / max(days, 1) * 30 * gas_per_backrun_eth * eth_price

    # HHI: use failed tx senders as proxy
    hhi = 0.15  # Backrunning is generally less concentrated than sandwiching

    net_profit = monthly_profit - monthly_gas_cost
    score = _compute_score(net_profit, hhi, complexity=30, trend=0)
    verdict = _compute_verdict(net_profit, hhi, score)

    return StrategyResult(
        strategy="Backrun",
        detected_count=backrunnable,
        estimated_monthly_profit_usd=round(monthly_profit, 2),
        gas_cost_usd=round(monthly_gas_cost, 2),
        net_monthly_profit_usd=round(net_profit, 2),
        competition_hhi=round(hhi, 4),
        unique_competitors=0,  # Can't determine precisely
        win_rate=0.0,
        sample_tx_hashes=[],
        verdict=verdict,
        score=round(score, 1),
    )


def run_all_checks(conn=None, chain: str = "all") -> List[StrategyResult]:
    """Run all strategy checks and return results."""
    if conn is None:
        conn = duckdb.connect(DUCKDB_PATH, read_only=True)

    price_engine = PriceEngine(conn)
    bg, _, _ = _globs(chain)

    # Determine data window in days
    r = conn.execute(f"""
        SELECT min(timestamp), max(timestamp)
        FROM read_parquet('{bg}', union_by_name=true)
    """).fetchone()
    if r and r[0] and r[1]:
        days = max((r[1] - r[0]) / 86400, 1)
    else:
        days = 7

    results = []
    results.append(check_sandwich(conn, price_engine, days, chain))
    results.append(check_dex_arb(conn, price_engine, days, chain))
    results.append(check_liquidation(conn, price_engine, days, chain))
    results.append(check_backrun(conn, price_engine, days, chain))

    return results


def results_to_df(results: List[StrategyResult]) -> pd.DataFrame:
    """Convert results to a DataFrame for display."""
    rows = []
    for r in results:
        rows.append({
            "Strategy": r.strategy,
            "Detected": r.detected_count,
            "Monthly Profit (USD)": f"${r.estimated_monthly_profit_usd:,.0f}",
            "Gas Cost (USD)": f"${r.gas_cost_usd:,.0f}",
            "Net Profit (USD)": f"${r.net_monthly_profit_usd:,.0f}",
            "HHI": f"{r.competition_hhi:.3f}",
            "Competitors": r.unique_competitors,
            "Score": r.score,
            "Verdict": r.verdict,
        })
    return pd.DataFrame(rows)


if __name__ == "__main__":
    import os
    os.chdir(Path(__file__).parent.parent)

    try:
        results = run_all_checks()
        df = results_to_df(results)
        print("=" * 100)
        print("STRATEGY CHECK RESULTS")
        print("=" * 100)
        print(df.to_string(index=False))
        print()

        for r in results:
            print(f"\n--- {r.strategy} ---")
            print(f"  Detected events:     {r.detected_count:,}")
            print(f"  Est. monthly profit: ${r.estimated_monthly_profit_usd:,.2f}")
            print(f"  Monthly gas cost:    ${r.gas_cost_usd:,.2f}")
            print(f"  Net monthly profit:  ${r.net_monthly_profit_usd:,.2f}")
            print(f"  Competition HHI:     {r.competition_hhi:.4f}")
            print(f"  Unique competitors:  {r.unique_competitors}")
            print(f"  Score:               {r.score}/100")
            print(f"  Verdict:             {r.verdict}")
            if r.sample_tx_hashes:
                print(f"  Sample txs:")
                for h in r.sample_tx_hashes[:3]:
                    print(f"    https://etherscan.io/tx/{h}")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        print("\n(Run from mev/ directory with Parquet data)")
