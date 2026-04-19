"""
Capital requirements model for sandwich MEV.
Analyzes position size distributions to determine how much capital
is needed at different capture thresholds.
"""
import json
import duckdb

con = duckdb.connect()

with open("/root/mev/data/pool_tokens.json") as f:
    pool_tokens = json.load(f)

WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
STABLES = {
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": 6,
    "0xdac17f958d2ee523a2206206994597c13d831ec7": 6,
    "0x6b175474e89094c44da98b954eedeac495271d0f": 18,
}

eth_price = 2112.0

pool_pricing = {}
for pool, info in pool_tokens.items():
    t0, t1 = info["token0"], info["token1"]
    if t0 == WETH:
        pool_pricing[pool] = {"quote_side": 0, "decimals": 18, "usd_per_unit": eth_price}
    elif t1 == WETH:
        pool_pricing[pool] = {"quote_side": 1, "decimals": 18, "usd_per_unit": eth_price}
    elif t0 in STABLES:
        pool_pricing[pool] = {"quote_side": 0, "decimals": STABLES[t0], "usd_per_unit": 1.0}
    elif t1 in STABLES:
        pool_pricing[pool] = {"quote_side": 1, "decimals": STABLES[t1], "usd_per_unit": 1.0}

pool_rows = [(p, v["quote_side"], v["decimals"], v["usd_per_unit"]) for p, v in pool_pricing.items()]
con.execute("CREATE TABLE pool_price (pool VARCHAR, quote_side INT, decimals INT, usd_per_unit DOUBLE)")
con.executemany("INSERT INTO pool_price VALUES (?, ?, ?, ?)", pool_rows)

# Step 1: Get individual sandwich records with capital needed and profit
print("Building sandwich dataset with capital and profit...")

con.execute("""
    CREATE TEMP TABLE sandwich_data AS
    WITH swaps AS (
        SELECT s.block_number, s.pool, s.sender, s.log_index,
            CAST(s.amount_in AS DOUBLE) as amt_in,
            CAST(s.amount_out AS DOUBLE) as amt_out,
            pp.decimals, pp.usd_per_unit
        FROM read_parquet('data/events/swaps/ethereum/*.parquet', union_by_name=true) s
        JOIN pool_price pp ON lower(s.pool) = lower(pp.pool)
    ),
    bot_pairs AS (
        SELECT block_number, pool, sender, decimals, usd_per_unit,
            MIN(log_index) as first_log,
            MAX(log_index) as last_log,
            COUNT(*) as swap_count
        FROM swaps
        GROUP BY block_number, pool, sender, decimals, usd_per_unit
        HAVING COUNT(*) >= 2
    ),
    first_swaps AS (
        SELECT s.block_number, s.pool, s.sender,
            s.amt_in / POWER(10, bp.decimals) * bp.usd_per_unit as f_in_usd,
            s.amt_out / POWER(10, bp.decimals) * bp.usd_per_unit as f_out_usd
        FROM swaps s
        JOIN bot_pairs bp ON s.block_number = bp.block_number
            AND s.pool = bp.pool AND s.sender = bp.sender
            AND s.log_index = bp.first_log
    ),
    last_swaps AS (
        SELECT s.block_number, s.pool, s.sender,
            s.amt_in / POWER(10, bp.decimals) * bp.usd_per_unit as l_in_usd,
            s.amt_out / POWER(10, bp.decimals) * bp.usd_per_unit as l_out_usd
        FROM swaps s
        JOIN bot_pairs bp ON s.block_number = bp.block_number
            AND s.pool = bp.pool AND s.sender = bp.sender
            AND s.log_index = bp.last_log
    ),
    assembled AS (
        SELECT bp.*,
            fs.f_in_usd, fs.f_out_usd,
            ls.l_in_usd, ls.l_out_usd,
            GREATEST(fs.f_in_usd, fs.f_out_usd) as capital_needed,
            GREATEST(ls.l_out_usd - fs.f_in_usd, fs.f_out_usd - ls.l_in_usd) as profit,
            (SELECT COUNT(DISTINCT s2.sender)
             FROM swaps s2
             WHERE s2.block_number = bp.block_number
                AND s2.pool = bp.pool
                AND s2.sender != bp.sender
                AND s2.log_index > bp.first_log
                AND s2.log_index < bp.last_log
            ) as victim_count
        FROM bot_pairs bp
        JOIN first_swaps fs ON bp.block_number = fs.block_number
            AND bp.pool = fs.pool AND bp.sender = fs.sender
        JOIN last_swaps ls ON bp.block_number = ls.block_number
            AND bp.pool = ls.pool AND bp.sender = ls.sender
    )
    SELECT capital_needed, profit, victim_count
    FROM assembled
    WHERE victim_count >= 1
        AND profit > 0 AND profit < 10000
        AND capital_needed > 10 AND capital_needed < 10000000
""")

total = con.execute("SELECT COUNT(*) FROM sandwich_data").fetchone()[0]
print(f"Confirmed profitable sandwiches: {total:,}")

# Step 2: Position size distribution
print("\n--- POSITION SIZE DISTRIBUTION ---")
dist = con.execute("""
    SELECT
        APPROX_QUANTILE(capital_needed, 0.10) as p10,
        APPROX_QUANTILE(capital_needed, 0.25) as p25,
        APPROX_QUANTILE(capital_needed, 0.50) as p50,
        APPROX_QUANTILE(capital_needed, 0.75) as p75,
        APPROX_QUANTILE(capital_needed, 0.90) as p90,
        APPROX_QUANTILE(capital_needed, 0.95) as p95,
        APPROX_QUANTILE(capital_needed, 0.99) as p99,
        AVG(capital_needed) as avg_cap
    FROM sandwich_data
""").fetchone()

for label, val in zip(["P10","P25","P50","P75","P90","P95","P99","Avg"], dist):
    print(f"  {label}: ${val:,.0f}")

# Step 3: Profit distribution
print("\n--- PROFIT DISTRIBUTION ---")
pdist = con.execute("""
    SELECT
        APPROX_QUANTILE(profit, 0.25),
        APPROX_QUANTILE(profit, 0.50),
        APPROX_QUANTILE(profit, 0.75),
        APPROX_QUANTILE(profit, 0.90),
        AVG(profit)
    FROM sandwich_data
""").fetchone()

for label, val in zip(["P25","P50","P75","P90","Avg"], pdist):
    print(f"  {label}: ${val:,.2f}")

# Step 4: ROI per sandwich
print("\n--- ROI PER SANDWICH (profit / capital) ---")
roi = con.execute("""
    SELECT
        APPROX_QUANTILE(profit / capital_needed, 0.25),
        APPROX_QUANTILE(profit / capital_needed, 0.50),
        APPROX_QUANTILE(profit / capital_needed, 0.75),
        AVG(profit / capital_needed)
    FROM sandwich_data
""").fetchone()

for label, val in zip(["P25","P50","P75","Avg"], roi):
    print(f"  {label}: {val*100:.3f}%")

# Step 5: Capital threshold analysis - at each capital level, what can you capture?
print("\n--- CAPITAL THRESHOLD ANALYSIS ---")
thresholds = [1000, 5000, 10000, 25000, 50000, 100000, 250000, 500000, 1000000]

print(f"  {'Capital':>12} {'Sandwiches':>12} {'% of Total':>12} {'Total Profit':>14} {'Avg Profit':>12}")
print(f"  {'-'*64}")

for thresh in thresholds:
    row = con.execute("""
        SELECT COUNT(*), SUM(profit), AVG(profit)
        FROM sandwich_data
        WHERE capital_needed <= ?
    """, [thresh]).fetchone()
    pct = row[0] / total * 100 if total > 0 else 0
    tp = row[1] or 0
    ap = row[2] or 0
    print(f"  ${thresh:>11,} {row[0]:>12,} {pct:>11.1f}% ${tp:>13,.0f} ${ap:>11,.2f}")

# Step 6: Full capital model with opportunity cost, turnover, and net ROI
print("\n--- FULL CAPITAL MODEL ---")

DAYS = 14.9  # actual days of data
STAKING_YIELD = 0.035  # 3.5% annual ETH staking APY
BUILDER_TIP_PCT = 0.85  # 85% of profit goes to builder
INFRA_MONTHLY = 330  # minimal infra cost

print(f"\n  Assumptions:")
print(f"    Data window: {DAYS:.1f} days")
print(f"    ETH staking yield (opportunity cost): {STAKING_YIELD*100:.1f}%/yr")
print(f"    Builder tip rate: {BUILDER_TIP_PCT*100:.0f}% of gross profit")
print(f"    Infra cost: ${INFRA_MONTHLY}/month")
print(f"    Capture rate: 1% of addressable sandwiches")

CAPTURE_RATE = 0.01

print(f"\n  {'Capital':>12} {'Addressable':>12} {'Captured/mo':>12} {'Gross/mo':>12} {'Net MEV/mo':>12} {'Opp Cost/mo':>12} {'Final/mo':>12} {'Annual ROI':>12}")
print(f"  {'-'*100}")

for thresh in thresholds:
    row = con.execute("""
        SELECT COUNT(*), SUM(profit)
        FROM sandwich_data
        WHERE capital_needed <= ?
    """, [thresh]).fetchone()

    sandwiches_per_day = row[0] / DAYS
    profit_per_day = (row[1] or 0) / DAYS

    captured_per_mo = sandwiches_per_day * 30 * CAPTURE_RATE
    gross_per_mo = profit_per_day * 30 * CAPTURE_RATE
    net_mev_per_mo = gross_per_mo * (1 - BUILDER_TIP_PCT)  # after builder tips
    opp_cost_per_mo = thresh * STAKING_YIELD / 12  # monthly opportunity cost
    final_per_mo = net_mev_per_mo - opp_cost_per_mo - INFRA_MONTHLY
    annual_roi = (final_per_mo * 12) / thresh * 100 if thresh > 0 else 0

    print(f"  ${thresh:>11,} {row[0]:>12,} {captured_per_mo:>12,.0f} ${gross_per_mo:>11,.0f} ${net_mev_per_mo:>11,.0f} ${opp_cost_per_mo:>11,.0f} ${final_per_mo:>11,.0f} {annual_roi:>11.1f}%")

# Same at 5% capture
print(f"\n  Same table at 5% capture rate:")
CAPTURE_RATE = 0.05
print(f"\n  {'Capital':>12} {'Captured/mo':>12} {'Gross/mo':>12} {'Net MEV/mo':>12} {'Opp Cost/mo':>12} {'Final/mo':>12} {'Annual ROI':>12}")
print(f"  {'-'*88}")

for thresh in thresholds:
    row = con.execute("""
        SELECT COUNT(*), SUM(profit)
        FROM sandwich_data
        WHERE capital_needed <= ?
    """, [thresh]).fetchone()

    profit_per_day = (row[1] or 0) / DAYS
    sandwiches_per_day = row[0] / DAYS

    captured_per_mo = sandwiches_per_day * 30 * CAPTURE_RATE
    gross_per_mo = profit_per_day * 30 * CAPTURE_RATE
    net_mev_per_mo = gross_per_mo * (1 - BUILDER_TIP_PCT)
    opp_cost_per_mo = thresh * STAKING_YIELD / 12
    final_per_mo = net_mev_per_mo - opp_cost_per_mo - INFRA_MONTHLY
    annual_roi = (final_per_mo * 12) / thresh * 100 if thresh > 0 else 0

    print(f"  ${thresh:>11,} {captured_per_mo:>12,.0f} ${gross_per_mo:>11,.0f} ${net_mev_per_mo:>11,.0f} ${opp_cost_per_mo:>11,.0f} ${final_per_mo:>11,.0f} {annual_roi:>11.1f}%")

# Capital turnover analysis
print("\n\n--- CAPITAL TURNOVER ---")
print("  (How many times per day does the capital cycle through sandwiches?)")
for thresh in [10000, 50000, 100000, 250000]:
    row = con.execute("""
        SELECT COUNT(*), SUM(capital_needed), AVG(capital_needed)
        FROM sandwich_data
        WHERE capital_needed <= ?
    """, [thresh]).fetchone()
    daily_sandwiches = row[0] / DAYS
    daily_capital_deployed = (row[1] or 0) / DAYS
    turns = daily_capital_deployed / thresh if thresh > 0 else 0
    print(f"  ${thresh:>11,} capital: {daily_sandwiches:.0f} sandwiches/day, {turns:.1f} capital turns/day")
    print(f"              Each sandwich holds capital for ~12 seconds (1 block)")
    print(f"              Effective utilization: {turns * 12 / 86400 * 100:.2f}% of time")
