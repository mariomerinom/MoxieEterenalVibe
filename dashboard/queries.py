#!/usr/bin/env python3
"""
DuckDB query layer — single source of truth for all dashboard SQL.

All queries read from Parquet files via DuckDB glob patterns.
Run from the mev/ directory so relative paths resolve correctly.
"""

import duckdb
import pandas as pd
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"
DUCKDB_PATH = str(DATA_DIR / "mev.duckdb")

# Parquet glob patterns — use {chain} placeholder for per-chain filtering
BLOCKS_GLOB = "data/blocks/{chain}/*.parquet"
TXS_GLOB = "data/transactions/{chain}/*.parquet"
SWAPS_GLOB = "data/events/swaps/{chain}/*.parquet"
LIQUIDATIONS_GLOB = "data/events/liquidations/{chain}/*.parquet"

# All-chain globs (wildcard)
ALL_BLOCKS_GLOB = "data/blocks/*/*.parquet"
ALL_SWAPS_GLOB = "data/events/swaps/*/*.parquet"
ALL_LIQUIDATIONS_GLOB = "data/events/liquidations/*/*.parquet"

SUPPORTED_CHAINS = ["ethereum", "polygon", "blast", "base", "arbitrum"]


def get_connection():
    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    return conn


def _globs(chain: str = "all"):
    """Return (blocks_glob, swaps_glob, liquidations_glob) for the given chain."""
    if chain == "all":
        return ALL_BLOCKS_GLOB, ALL_SWAPS_GLOB, ALL_LIQUIDATIONS_GLOB
    return (
        BLOCKS_GLOB.format(chain=chain),
        SWAPS_GLOB.format(chain=chain),
        LIQUIDATIONS_GLOB.format(chain=chain),
    )


def available_chains(conn) -> list:
    """Return list of chains that have swap data on disk."""
    chains = []
    for c in SUPPORTED_CHAINS:
        try:
            r = conn.execute(f"SELECT count(*) FROM read_parquet('{SWAPS_GLOB.format(chain=c)}', union_by_name=true)").fetchone()
            if r and r[0] > 0:
                chains.append(c)
        except Exception:
            pass
    return chains


def _actor_col(conn, swaps_glob: str) -> str:
    """Return 'tx_from' if the column exists in the parquet, else 'sender'."""
    try:
        cols = conn.execute(f"SELECT * FROM read_parquet('{swaps_glob}', union_by_name=true) LIMIT 0").description
        col_names = [c[0] for c in cols]
        if "tx_from" in col_names:
            return "tx_from"
    except Exception:
        pass
    return "sender"


# ── Overview ──

def overview(conn, chain: str = "all") -> dict:
    """Basic stats: block count, tx count, swap count, liquidation count, time range."""
    bg, sg, lg = _globs(chain)

    r = conn.execute(f"""
        SELECT count(*) as blocks,
               min(block_number) as min_blk,
               max(block_number) as max_blk,
               min(timestamp) as start_ts,
               max(timestamp) as end_ts
        FROM read_parquet('{bg}', union_by_name=true)
    """).fetchone()

    swap_r = conn.execute(f"""
        SELECT count(*), count(distinct pool)
        FROM read_parquet('{sg}', union_by_name=true)
    """).fetchone()

    try:
        liq_count = conn.execute(f"""
            SELECT count(*) FROM read_parquet('{lg}', union_by_name=true)
        """).fetchone()[0]
    except Exception:
        liq_count = 0

    return {
        "blocks": r[0],
        "min_block": r[1],
        "max_block": r[2],
        "start_ts": r[3],
        "end_ts": r[4],
        "tx_count": 0,
        "swap_count": swap_r[0],
        "unique_pools": swap_r[1],
        "liquidation_count": liq_count,
    }


# ── Swap Protocol Breakdown ──

def swap_protocol_breakdown(conn, chain: str = "all") -> pd.DataFrame:
    _, sg, _ = _globs(chain)
    return conn.execute(f"""
        SELECT protocol,
               count(*) as swaps,
               count(distinct pool) as pools,
               round(count(*) * 100.0 / sum(count(*)) over (), 1) as pct
        FROM read_parquet('{sg}', union_by_name=true)
        GROUP BY protocol
        ORDER BY swaps DESC
    """).fetchdf()


# ── Top Pools ──

def top_pools(conn, chain: str = "all", limit=20) -> pd.DataFrame:
    _, sg, _ = _globs(chain)
    return conn.execute(f"""
        SELECT pool, protocol, count(*) as swaps
        FROM read_parquet('{sg}', union_by_name=true)
        GROUP BY pool, protocol
        ORDER BY swaps DESC
        LIMIT {limit}
    """).fetchdf()


# ── Gas Analysis ──

def gas_stats(conn, chain: str = "all") -> dict:
    bg, _, _ = _globs(chain)
    r = conn.execute(f"""
        SELECT
            round(avg(base_fee_gwei), 2) as avg_base_fee,
            round(approx_quantile(base_fee_gwei, 0.5), 2) as median_base_fee,
            round(approx_quantile(base_fee_gwei, 0.95), 2) as p95_base_fee,
            round(max(base_fee_gwei), 2) as max_base_fee,
            round(avg(gas_used * 100.0 / gas_limit), 1) as avg_utilization,
            round(approx_quantile(gas_used * 100.0 / gas_limit, 0.5), 1) as median_utilization
        FROM read_parquet('{bg}', union_by_name=true)
        WHERE base_fee_gwei IS NOT NULL AND gas_limit > 0
    """).fetchone()
    return {
        "avg_base_fee": r[0], "median_base_fee": r[1],
        "p95_base_fee": r[2], "max_base_fee": r[3],
        "avg_utilization": r[4], "median_utilization": r[5],
    }


def gas_time_series(conn, chain: str = "all") -> pd.DataFrame:
    """Hourly average gas price and utilization."""
    bg, _, _ = _globs(chain)
    return conn.execute(f"""
        SELECT
            (timestamp / 3600) * 3600 as hour_ts,
            round(avg(base_fee_gwei), 2) as avg_base_fee,
            round(avg(gas_used * 100.0 / gas_limit), 1) as avg_utilization,
            count(*) as block_count
        FROM read_parquet('{bg}', union_by_name=true)
        WHERE base_fee_gwei IS NOT NULL AND gas_limit > 0
        GROUP BY hour_ts
        ORDER BY hour_ts
    """).fetchdf()


# ── Competition Metrics ──

def top_senders(conn, chain: str = "all", limit=20) -> pd.DataFrame:
    """Top swap actors — uses tx_from when available, falls back to sender."""
    _, sg, _ = _globs(chain)
    actor = _actor_col(conn, sg)
    return conn.execute(f"""
        SELECT
            s.{actor} as sender,
            count(*) as swaps,
            count(distinct s.pool) as pools,
            count(distinct s.protocol) as protocols
        FROM read_parquet('{sg}', union_by_name=true) s
        WHERE s.{actor} IS NOT NULL AND s.{actor} != ''
        GROUP BY s.{actor}
        ORDER BY swaps DESC
        LIMIT {limit}
    """).fetchdf()


def sender_success_rates(conn, chain: str = "all", limit=20) -> pd.DataFrame:
    """Top senders with transaction success rates.
    Note: requires transaction parquet which is now disabled for new data.
    Returns empty DataFrame if unavailable."""
    return pd.DataFrame()  # Transaction parquet disabled — no success rate data


def herfindahl_index(conn, chain: str = "all") -> float:
    """Compute Herfindahl-Hirschman Index for swap actor concentration."""
    _, sg, _ = _globs(chain)
    actor = _actor_col(conn, sg)
    r = conn.execute(f"""
        WITH sender_shares AS (
            SELECT
                {actor},
                count(*) * 1.0 / sum(count(*)) over () as share
            FROM read_parquet('{sg}', union_by_name=true)
            WHERE {actor} IS NOT NULL AND {actor} != ''
            GROUP BY {actor}
        )
        SELECT sum(share * share) as hhi
        FROM sender_shares
    """).fetchone()
    return float(r[0]) if r and r[0] else 0.0


def sender_hourly_activity(conn, chain: str = "all", limit=10) -> pd.DataFrame:
    """Hour-of-day activity for top actors."""
    bg, sg, _ = _globs(chain)
    actor = _actor_col(conn, sg)
    return conn.execute(f"""
        WITH top AS (
            SELECT {actor} as sender FROM (
                SELECT {actor}, count(*) as cnt
                FROM read_parquet('{sg}', union_by_name=true)
                WHERE {actor} IS NOT NULL AND {actor} != ''
                GROUP BY {actor} ORDER BY cnt DESC LIMIT {limit}
            )
        )
        SELECT
            s.{actor} as sender,
            extract(hour from to_timestamp(b.timestamp)) as hour,
            count(*) as swaps
        FROM read_parquet('{sg}', union_by_name=true) s
        JOIN read_parquet('{bg}', union_by_name=true) b
            ON s.block_number = b.block_number
        WHERE s.{actor} IN (SELECT sender FROM top)
        GROUP BY s.{actor}, hour
        ORDER BY s.{actor}, hour
    """).fetchdf()


# ── Multi-swap / Arb Detection ──

def multi_swap_distribution(conn, chain: str = "all") -> pd.DataFrame:
    """Distribution of swaps-per-transaction for multi-swap txs."""
    _, sg, _ = _globs(chain)
    return conn.execute(f"""
        WITH tx_swaps AS (
            SELECT tx_hash, count(*) as swap_count, count(distinct pool) as pool_count
            FROM read_parquet('{sg}', union_by_name=true)
            GROUP BY tx_hash
        )
        SELECT
            swap_count,
            count(*) as tx_count,
            sum(case when pool_count >= 2 then 1 else 0 end) as multi_pool_txs
        FROM tx_swaps
        WHERE swap_count >= 2
        GROUP BY swap_count
        ORDER BY swap_count
        LIMIT 20
    """).fetchdf()


def arb_candidates(conn, chain: str = "all") -> dict:
    """Count strong arb/sandwich candidates (≥3 swaps across ≥2 pools)."""
    _, sg, _ = _globs(chain)
    r = conn.execute(f"""
        WITH tx_swaps AS (
            SELECT tx_hash, count(*) as swap_count, count(distinct pool) as pool_count
            FROM read_parquet('{sg}', union_by_name=true)
            GROUP BY tx_hash
            HAVING count(*) >= 3 AND count(distinct pool) >= 2
        )
        SELECT count(*) as arb_txs FROM tx_swaps
    """).fetchone()

    r2 = conn.execute(f"""
        WITH tx_swaps AS (
            SELECT tx_hash, count(*) as swap_count, count(distinct pool) as pool_count
            FROM read_parquet('{sg}', union_by_name=true)
            GROUP BY tx_hash
            HAVING count(*) = 2 AND count(distinct pool) = 2
        )
        SELECT count(*) FROM tx_swaps
    """).fetchone()

    return {
        "strong_arb_candidates": r[0],
        "simple_arb_candidates": r2[0],
    }


# ── Failed Transactions (Bot Reverts) ──

def failed_tx_stats(conn, chain: str = "all") -> dict:
    """Failed transaction stats. Returns zeros since transaction parquet is disabled."""
    return {
        "total_failed": 0,
        "complex_failed": 0,
        "complex_gas_wasted": 0,
    }


# ── Liquidation Analysis ──

def liquidation_stats(conn, chain: str = "all") -> dict:
    _, _, lg = _globs(chain)
    try:
        r = conn.execute(f"""
            SELECT
                count(*) as total,
                count(distinct liquidator) as unique_liquidators,
                count(distinct borrower) as unique_borrowers
            FROM read_parquet('{lg}', union_by_name=true)
        """).fetchone()
        return {
            "total": r[0],
            "unique_liquidators": r[1],
            "unique_borrowers": r[2],
        }
    except Exception:
        return {"total": 0, "unique_liquidators": 0, "unique_borrowers": 0}


def liquidation_details(conn, chain: str = "all") -> pd.DataFrame:
    _, _, lg = _globs(chain)
    try:
        return conn.execute(f"""
            SELECT
                liquidator,
                count(*) as liquidations,
                count(distinct borrower) as unique_borrowers,
                count(distinct collateral_asset) as collateral_types,
                avg(gas_used) as avg_gas_used,
                avg(gas_price_gwei) as avg_gas_price
            FROM read_parquet('{lg}', union_by_name=true)
            GROUP BY liquidator
            ORDER BY liquidations DESC
        """).fetchdf()
    except Exception:
        return pd.DataFrame()


# ── Hourly Swap Activity ──

def hourly_swap_activity(conn, chain: str = "all") -> pd.DataFrame:
    bg, sg, _ = _globs(chain)
    return conn.execute(f"""
        WITH swap_blocks AS (
            SELECT s.block_number, to_timestamp(b.timestamp) as ts
            FROM read_parquet('{sg}', union_by_name=true) s
            JOIN read_parquet('{bg}', union_by_name=true) b
                ON s.block_number = b.block_number
        )
        SELECT
            extract(hour from ts) as hour,
            count(*) as swaps
        FROM swap_blocks
        GROUP BY hour
        ORDER BY hour
    """).fetchdf()


# ── Daily Activity (for trend analysis) ──

def daily_activity(conn, chain: str = "all") -> pd.DataFrame:
    """Daily swap counts, unique actors, gas stats."""
    bg, sg, _ = _globs(chain)
    actor = _actor_col(conn, sg)
    return conn.execute(f"""
        WITH daily_swaps AS (
            SELECT
                (b.timestamp / 86400) * 86400 as day_ts,
                count(*) as swaps,
                count(distinct s.{actor}) as unique_senders,
                count(distinct s.pool) as active_pools
            FROM read_parquet('{sg}', union_by_name=true) s
            JOIN read_parquet('{bg}', union_by_name=true) b
                ON s.block_number = b.block_number
            GROUP BY day_ts
        ),
        daily_gas AS (
            SELECT
                (timestamp / 86400) * 86400 as day_ts,
                round(avg(base_fee_gwei), 2) as avg_base_fee,
                count(*) as blocks
            FROM read_parquet('{bg}', union_by_name=true)
            WHERE base_fee_gwei IS NOT NULL
            GROUP BY day_ts
        )
        SELECT
            ds.day_ts,
            ds.swaps,
            ds.unique_senders,
            ds.active_pools,
            dg.avg_base_fee,
            dg.blocks
        FROM daily_swaps ds
        LEFT JOIN daily_gas dg ON ds.day_ts = dg.day_ts
        ORDER BY ds.day_ts
    """).fetchdf()


# ── Sandwich Pattern Detection ──

def sandwich_candidates(conn, chain: str = "all") -> pd.DataFrame:
    """
    Find potential sandwich patterns: same actor has multiple swaps in a block
    in the same pool, with other actors' swaps between them.
    """
    _, sg, _ = _globs(chain)
    actor = _actor_col(conn, sg)
    return conn.execute(f"""
        WITH block_pool_senders AS (
            SELECT
                block_number,
                pool,
                {actor} as actor,
                count(*) as swap_count,
                min(log_index) as first_log,
                max(log_index) as last_log
            FROM read_parquet('{sg}', union_by_name=true)
            WHERE {actor} IS NOT NULL AND {actor} != ''
            GROUP BY block_number, pool, {actor}
            HAVING count(*) >= 2
        ),
        victims AS (
            SELECT
                s.block_number,
                s.pool,
                bps.actor as sandwich_bot,
                count(distinct s.{actor}) as victim_count
            FROM read_parquet('{sg}', union_by_name=true) s
            JOIN block_pool_senders bps
                ON s.block_number = bps.block_number
                AND s.pool = bps.pool
                AND s.{actor} != bps.actor
                AND s.log_index > bps.first_log
                AND s.log_index < bps.last_log
            GROUP BY s.block_number, s.pool, bps.actor
        )
        SELECT
            sandwich_bot,
            count(*) as sandwich_count,
            sum(victim_count) as total_victims
        FROM victims
        GROUP BY sandwich_bot
        ORDER BY sandwich_count DESC
        LIMIT 20
    """).fetchdf()


# ── Cross-chain summary ──

def cross_chain_summary(conn) -> pd.DataFrame:
    """Per-chain summary stats for the cross-chain comparison view."""
    rows = []
    for chain in available_chains(conn):
        try:
            _, sg, _ = _globs(chain)
            actor = _actor_col(conn, sg)
            r = conn.execute(f"""
                SELECT
                    count(*) as swaps,
                    count(distinct pool) as pools,
                    count(distinct {actor}) as actors
                FROM read_parquet('{sg}', union_by_name=true)
                WHERE {actor} IS NOT NULL AND {actor} != ''
            """).fetchone()

            # Multi-swap % (arb/sandwich signal)
            ms = conn.execute(f"""
                WITH pool_blocks AS (
                    SELECT block_number, pool, count(*) as n
                    FROM read_parquet('{sg}', union_by_name=true)
                    GROUP BY block_number, pool
                    HAVING count(*) >= 2
                )
                SELECT coalesce(sum(n), 0) FROM pool_blocks
            """).fetchone()

            multi_pct = round(ms[0] / max(r[0], 1) * 100, 1) if ms else 0

            rows.append({
                "chain": chain,
                "swaps": r[0],
                "pools": r[1],
                "actors": r[2],
                "multi_swap_pct": multi_pct,
            })
        except Exception:
            continue

    return pd.DataFrame(rows) if rows else pd.DataFrame()


if __name__ == "__main__":
    """Quick test: run all queries and print summaries."""
    import os
    os.chdir(Path(__file__).parent.parent)

    try:
        conn = get_connection()
        chains = available_chains(conn)
        print(f"Available chains: {chains}")

        for chain in ["all"] + chains:
            print(f"\n{'='*50}")
            print(f"  Chain: {chain}")
            print(f"{'='*50}")

            ov = overview(conn, chain)
            print(f"  Blocks: {ov['blocks']:,}  Swaps: {ov['swap_count']:,}  Pools: {ov['unique_pools']:,}")

            hhi = herfindahl_index(conn, chain)
            print(f"  HHI: {hhi:.4f}")

            arbs = arb_candidates(conn, chain)
            print(f"  Arb candidates: {arbs['strong_arb_candidates']:,}")

        print("\n=== Cross-chain Summary ===")
        print(cross_chain_summary(conn).to_string(index=False))

        conn.close()
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        print("(Run from mev/ directory with Parquet data)")
