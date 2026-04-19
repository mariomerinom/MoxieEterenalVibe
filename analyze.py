#!/usr/bin/env python3
"""Analyze 7-day Ethereum capture data. Run from mev/ directory."""

import duckdb

conn = duckdb.connect("data/mev.duckdb")

# Create views
conn.execute("""
CREATE OR REPLACE VIEW blocks AS SELECT * FROM read_parquet('data/blocks/*/*.parquet', union_by_name=true);
CREATE OR REPLACE VIEW transactions AS SELECT * FROM read_parquet('data/transactions/*/*.parquet', union_by_name=true);
CREATE OR REPLACE VIEW swaps AS SELECT * FROM read_parquet('data/events/swaps/*/*.parquet', union_by_name=true);
CREATE OR REPLACE VIEW liquidations AS SELECT * FROM read_parquet('data/events/liquidations/*/*.parquet', union_by_name=true);
""")

print("=" * 80)
print("MEV CAPTURE — 7-DAY ETHEREUM L1 ANALYSIS")
print("=" * 80)

# ── Overview ──
print("\n── OVERVIEW ──")
r = conn.execute("""
    SELECT count(*) as blocks,
           min(block_number) as min_blk,
           max(block_number) as max_blk,
           min(timestamp) as start_ts,
           max(timestamp) as end_ts
    FROM blocks
""").fetchone()
print(f"  Blocks:     {r[0]:,}  ({r[1]:,} → {r[2]:,})")
from datetime import datetime, timezone
ts_start = datetime.fromtimestamp(r[3], tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
ts_end = datetime.fromtimestamp(r[4], tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
print(f"  Time range: {ts_start} → {ts_end}")

r = conn.execute("SELECT count(*) FROM transactions").fetchone()
print(f"  Transactions: {r[0]:,}")

r = conn.execute("SELECT count(*), count(distinct pool) FROM swaps").fetchone()
print(f"  Swap events:  {r[0]:,}  across {r[1]:,} unique pools")

r = conn.execute("SELECT count(*) FROM liquidations").fetchone()
print(f"  Liquidations: {r[0]:,}")

# ── Swap protocol breakdown ──
print("\n── SWAP VOLUME BY PROTOCOL ──")
rows = conn.execute("""
    SELECT protocol,
           count(*) as swaps,
           count(distinct pool) as pools,
           round(count(*) * 100.0 / sum(count(*)) over (), 1) as pct
    FROM swaps
    GROUP BY protocol
    ORDER BY swaps DESC
""").fetchall()
print(f"  {'Protocol':<15} {'Swaps':>10} {'Pools':>8} {'Share':>8}")
print(f"  {'─'*15} {'─'*10} {'─'*8} {'─'*8}")
for row in rows:
    print(f"  {row[0]:<15} {row[1]:>10,} {row[2]:>8,} {row[3]:>7.1f}%")

# ── Top pools by swap count ──
print("\n── TOP 20 POOLS BY SWAP COUNT ──")
rows = conn.execute("""
    SELECT pool, protocol, count(*) as swaps
    FROM swaps
    GROUP BY pool, protocol
    ORDER BY swaps DESC
    LIMIT 20
""").fetchall()
print(f"  {'Pool':<44} {'Protocol':<12} {'Swaps':>8}")
print(f"  {'─'*44} {'─'*12} {'─'*8}")
for row in rows:
    print(f"  {row[0]:<44} {row[1]:<12} {row[2]:>8,}")

# ── Swaps per block distribution ──
print("\n── SWAPS PER BLOCK DISTRIBUTION ──")
rows = conn.execute("""
    WITH per_block AS (
        SELECT block_number, count(*) as swap_count FROM swaps GROUP BY block_number
    )
    SELECT
        min(swap_count) as min_swaps,
        round(avg(swap_count), 1) as avg_swaps,
        approx_quantile(swap_count, 0.5) as median_swaps,
        approx_quantile(swap_count, 0.95) as p95_swaps,
        max(swap_count) as max_swaps
    FROM per_block
""").fetchone()
print(f"  Min: {rows[0]}  Avg: {rows[1]}  Median: {rows[2]}  P95: {rows[3]}  Max: {rows[4]}")

# ── Gas analysis ──
print("\n── GAS ANALYSIS ──")
rows = conn.execute("""
    SELECT
        round(avg(base_fee_gwei), 2) as avg_base_fee,
        round(approx_quantile(base_fee_gwei, 0.5), 2) as median_base_fee,
        round(approx_quantile(base_fee_gwei, 0.95), 2) as p95_base_fee,
        round(max(base_fee_gwei), 2) as max_base_fee
    FROM blocks
    WHERE base_fee_gwei IS NOT NULL
""").fetchone()
print(f"  Base fee (gwei): avg={rows[0]}  median={rows[1]}  p95={rows[2]}  max={rows[3]}")

rows = conn.execute("""
    SELECT
        round(avg(gas_used * 100.0 / gas_limit), 1) as avg_utilization,
        round(approx_quantile(gas_used * 100.0 / gas_limit, 0.5), 1) as median_util,
        round(approx_quantile(gas_used * 100.0 / gas_limit, 0.95), 1) as p95_util
    FROM blocks
    WHERE gas_limit > 0
""").fetchone()
print(f"  Block utilization: avg={rows[0]}%  median={rows[1]}%  p95={rows[2]}%")

# ── Liquidation analysis ──
print("\n── LIQUIDATION ANALYSIS ──")
r = conn.execute("SELECT count(*) FROM liquidations").fetchone()
if r[0] > 0:
    rows = conn.execute("""
        SELECT
            count(*) as total,
            count(distinct liquidator) as unique_liquidators,
            count(distinct borrower) as unique_borrowers,
            count(distinct collateral_asset) as collateral_assets,
            count(distinct debt_asset) as debt_assets
        FROM liquidations
    """).fetchone()
    print(f"  Total: {rows[0]}")
    print(f"  Unique liquidators: {rows[1]}")
    print(f"  Unique borrowers:   {rows[2]}")
    print(f"  Collateral assets:  {rows[3]}")
    print(f"  Debt assets:        {rows[4]}")

    print("\n  Top liquidators:")
    rows = conn.execute("""
        SELECT liquidator, count(*) as cnt
        FROM liquidations
        GROUP BY liquidator
        ORDER BY cnt DESC
        LIMIT 10
    """).fetchall()
    for row in rows:
        print(f"    {row[0]}  ({row[1]} liquidations)")

    print("\n  Collateral/debt pairs:")
    rows = conn.execute("""
        SELECT collateral_asset, debt_asset, count(*) as cnt
        FROM liquidations
        GROUP BY collateral_asset, debt_asset
        ORDER BY cnt DESC
        LIMIT 10
    """).fetchall()
    for row in rows:
        print(f"    {row[0][:18]}... / {row[1][:18]}...  ({row[2]})")
else:
    print("  No liquidations found in this window")

# ── Hourly swap activity ──
print("\n── SWAP ACTIVITY BY HOUR (UTC) ──")
# Join swaps with blocks to get timestamps
rows = conn.execute("""
    WITH swap_blocks AS (
        SELECT s.block_number, to_timestamp(b.timestamp) as ts
        FROM swaps s
        JOIN blocks b ON s.block_number = b.block_number
    )
    SELECT
        extract(hour from ts) as hour,
        count(*) as swaps,
        round(count(*) * 100.0 / sum(count(*)) over (), 1) as pct
    FROM swap_blocks
    GROUP BY hour
    ORDER BY hour
""").fetchall()
max_swaps = max(r[1] for r in rows) if rows else 1
for row in rows:
    bar_len = int(row[1] / max_swaps * 40)
    print(f"  {int(row[0]):02d}:00  {row[1]:>8,}  {'█' * bar_len}")

# ── Tx success rate ──
print("\n── TRANSACTION SUCCESS RATE ──")
rows = conn.execute("""
    SELECT
        count(*) as total,
        sum(case when success then 1 else 0 end) as succeeded,
        round(sum(case when success then 1 else 0 end) * 100.0 / count(*), 2) as success_rate
    FROM transactions
""").fetchone()
print(f"  Total: {rows[0]:,}  Succeeded: {rows[1]:,}  Rate: {rows[2]}%")

# ── Failed txs with swap-like input (potential MEV failures) ──
print("\n── FAILED TXS WITH CALLDATA >100 BYTES (potential MEV bots) ──")
rows = conn.execute("""
    SELECT count(*) as failed_complex
    FROM transactions
    WHERE NOT success AND input_size > 100
""").fetchone()
rows2 = conn.execute("""
    SELECT count(*) FROM transactions WHERE NOT success
""").fetchone()
print(f"  Failed txs total: {rows2[0]:,}")
print(f"  Failed with >100B calldata: {rows[0]:,}  (likely bot reverts)")

# ── Multi-swap txs (sandwich/arb candidates) ──
print("\n── MULTI-SWAP TRANSACTIONS (arb/sandwich candidates) ──")
rows = conn.execute("""
    WITH tx_swaps AS (
        SELECT tx_hash, count(*) as swap_count, count(distinct pool) as pool_count
        FROM swaps
        GROUP BY tx_hash
    )
    SELECT
        swap_count,
        count(*) as tx_count
    FROM tx_swaps
    WHERE swap_count >= 2
    GROUP BY swap_count
    ORDER BY swap_count
    LIMIT 15
""").fetchall()
print(f"  {'Swaps/tx':>10} {'Tx count':>10}")
print(f"  {'─'*10} {'─'*10}")
for row in rows:
    print(f"  {row[0]:>10} {row[1]:>10,}")

rows = conn.execute("""
    WITH tx_swaps AS (
        SELECT tx_hash, count(*) as swap_count, count(distinct pool) as pool_count
        FROM swaps
        GROUP BY tx_hash
        HAVING count(*) >= 3 AND count(distinct pool) >= 2
    )
    SELECT count(*) as multi_pool_multi_swap
    FROM tx_swaps
""").fetchone()
print(f"\n  Txs with ≥3 swaps across ≥2 pools: {rows[0]:,}  (strong arb/sandwich signal)")

# ── Top senders by swap volume ──
print("\n── TOP 15 SWAP SENDERS (likely MEV bots / routers) ──")
rows = conn.execute("""
    SELECT sender, count(*) as swaps, count(distinct pool) as pools
    FROM swaps
    GROUP BY sender
    ORDER BY swaps DESC
    LIMIT 15
""").fetchall()
print(f"  {'Sender':<44} {'Swaps':>8} {'Pools':>8}")
print(f"  {'─'*44} {'─'*8} {'─'*8}")
for row in rows:
    print(f"  {row[0]:<44} {row[1]:>8,} {row[2]:>8,}")

print("\n" + "=" * 80)
print("END OF ANALYSIS")
print("=" * 80)
