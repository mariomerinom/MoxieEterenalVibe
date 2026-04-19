#!/usr/bin/env python3
"""
P1-S2a: Sandwich Opportunity Analysis (Historical)

For every swap in our 15-day dataset, determine:
1. Was it sandwiched by an existing bot?
2. If NOT: what would the theoretical sandwich profit have been?
3. What's the distribution of unsandwiched opportunity?

This answers: "How much sandwich MEV is left uncaptured?"

Uses existing parquet swap data. No new infrastructure needed.
"""

import json
import duckdb

con = duckdb.connect()

# Load pool tokens
with open("data/pool_tokens.json") as f:
    pool_tokens = json.load(f)

WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
STABLES = {
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": 6,
    "0xdac17f958d2ee523a2206206994597c13d831ec7": 6,
    "0x6b175474e89094c44da98b954eedeac495271d0f": 18,
}

eth_price = con.execute("""
    WITH swaps AS (
        SELECT CAST(amount_in AS DOUBLE) as ai, CAST(amount_out AS DOUBLE) as ao
        FROM read_parquet('data/events/swaps/ethereum/*.parquet', union_by_name=true)
        WHERE lower(pool) = '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'
    )
    SELECT APPROX_QUANTILE(
        CASE WHEN ai < ao THEN (ai / 1e6) / (ao / 1e18) END, 0.5
    ) FROM swaps
""").fetchone()[0] or 2500.0
print(f"ETH/USD: ${eth_price:,.0f}")

# Build pool pricing with both token decimals and a flag for how to extract
# the quote-side amount from amt_in/amt_out.
#
# Since token_in/token_out are zeros in parquet, we infer from magnitudes:
# - If quote has MORE raw digits per unit (higher decimals): GREATEST picks it
# - If quote has FEWER raw digits per unit (lower decimals): LEAST picks it
# - If same decimals: quote is more valuable (WETH) → fewer units → LEAST
#                     quote is less valuable (DAI) → more units → GREATEST
#
# use_least=1 means: LEAST(amt_in, amt_out) / 10^quote_dec * usd
# use_least=0 means: GREATEST(amt_in, amt_out) / 10^quote_dec * usd

pool_rows = []
for pool, info in pool_tokens.items():
    t0, t1 = info["token0"], info["token1"]
    d0, d1 = info["decimals0"], info["decimals1"]

    if t0 == WETH:
        quote_dec, nonquote_dec, usd = d0, d1, eth_price
    elif t1 == WETH:
        quote_dec, nonquote_dec, usd = d1, d0, eth_price
    elif t0 in STABLES:
        quote_dec, nonquote_dec, usd = d0, d1, 1.0
    elif t1 in STABLES:
        quote_dec, nonquote_dec, usd = d1, d0, 1.0
    else:
        continue

    if quote_dec > nonquote_dec:
        use_least = 0  # GREATEST
    elif quote_dec < nonquote_dec:
        use_least = 1  # LEAST
    else:
        # Same decimals: more valuable token → fewer units → LEAST
        use_least = 1 if usd > 10 else 0

    pool_rows.append((pool, quote_dec, usd, use_least))

con.execute("""CREATE TABLE pool_price (
    pool VARCHAR, quote_dec INT, usd_per_unit DOUBLE, use_least INT
)""")
con.executemany("INSERT INTO pool_price VALUES (?, ?, ?, ?)", pool_rows)

print(f"Priceable pools: {len(pool_rows)}")

# Verify pricing on known pool
verify = con.execute("""
    WITH swaps AS (
        SELECT CAST(amount_in AS DOUBLE) as ai, CAST(amount_out AS DOUBLE) as ao,
            pp.quote_dec, pp.usd_per_unit, pp.use_least
        FROM read_parquet('data/events/swaps/ethereum/*.parquet', union_by_name=true) s
        JOIN pool_price pp ON lower(s.pool) = lower(pp.pool)
        WHERE lower(s.pool) = '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'
        LIMIT 100
    )
    SELECT
        APPROX_QUANTILE(
            CASE WHEN use_least = 1
                THEN LEAST(ai, ao) / POWER(10, quote_dec) * usd_per_unit
                ELSE GREATEST(ai, ao) / POWER(10, quote_dec) * usd_per_unit
            END, 0.5
        ) as median_usd,
        AVG(
            CASE WHEN use_least = 1
                THEN LEAST(ai, ao) / POWER(10, quote_dec) * usd_per_unit
                ELSE GREATEST(ai, ao) / POWER(10, quote_dec) * usd_per_unit
            END
        ) as mean_usd
    FROM swaps
""").fetchone()
print(f"Verify USDC/WETH pool: median=${verify[0]:,.0f}  mean=${verify[1]:,.0f}")

# Time window
blocks = con.execute("""
    SELECT min(block_number), max(block_number), count(distinct block_number)
    FROM read_parquet('data/blocks/ethereum/*.parquet', union_by_name=true)
""").fetchone()
days = blocks[2] / 7200
print(f"Data: {days:.1f} days ({blocks[2]:,} blocks)")

# Define the swap value SQL expression
SWAP_VALUE_SQL = """
    CASE WHEN pp.use_least = 1
        THEN LEAST(CAST(s.amount_in AS DOUBLE), CAST(s.amount_out AS DOUBLE))
             / POWER(10, pp.quote_dec) * pp.usd_per_unit
        ELSE GREATEST(CAST(s.amount_in AS DOUBLE), CAST(s.amount_out AS DOUBLE))
             / POWER(10, pp.quote_dec) * pp.usd_per_unit
    END
"""

# ── Step 1: Identify sandwiched swaps ──
print("\nStep 1: Identifying sandwiched vs unsandwiched swaps...")

results = con.execute(f"""
    WITH swaps AS (
        SELECT s.block_number, s.pool, s.sender, s.log_index, s.tx_hash,
            {SWAP_VALUE_SQL} as swap_value_usd
        FROM read_parquet('data/events/swaps/ethereum/*.parquet', union_by_name=true) s
        JOIN pool_price pp ON lower(s.pool) = lower(pp.pool)
    ),
    bot_activity AS (
        SELECT block_number, pool, sender,
            MIN(log_index) as first_log,
            MAX(log_index) as last_log
        FROM swaps
        GROUP BY block_number, pool, sender
        HAVING COUNT(*) >= 2
    ),
    swap_status AS (
        SELECT s.*,
            CASE WHEN EXISTS (
                SELECT 1 FROM bot_activity b
                WHERE b.block_number = s.block_number
                AND b.pool = s.pool
                AND b.sender != s.sender
                AND s.log_index > b.first_log
                AND s.log_index < b.last_log
            ) THEN 'sandwiched' ELSE 'unsandwiched' END as status
        FROM swaps s
    )
    SELECT
        status,
        COUNT(*) as swap_count,
        AVG(swap_value_usd) as avg_value,
        APPROX_QUANTILE(swap_value_usd, 0.5) as median_value,
        APPROX_QUANTILE(swap_value_usd, 0.25) as p25_value,
        APPROX_QUANTILE(swap_value_usd, 0.75) as p75_value,
        APPROX_QUANTILE(swap_value_usd, 0.90) as p90_value,
        SUM(swap_value_usd) as total_value,
        COUNT(CASE WHEN swap_value_usd > 100 THEN 1 END) as above_100,
        COUNT(CASE WHEN swap_value_usd > 1000 THEN 1 END) as above_1000,
        COUNT(CASE WHEN swap_value_usd > 10000 THEN 1 END) as above_10000,
        COUNT(CASE WHEN swap_value_usd > 100000 THEN 1 END) as above_100000
    FROM swap_status
    WHERE swap_value_usd > 0 AND swap_value_usd < 1e9
    GROUP BY status
    ORDER BY status
""").fetchall()

print(f"\n{'='*90}")
print(f"  SANDWICH OPPORTUNITY ANALYSIS — ETHEREUM")
print(f"  {days:.1f} days, {len(pool_rows)} priceable pools")
print(f"{'='*90}")

for row in results:
    status, count, avg_val, med_val, p25, p75, p90, total_val, a100, a1000, a10000, a100000 = row
    daily_count = count / max(days, 1)
    print(f"\n  {status.upper()} SWAPS:")
    print(f"    Total: {count:,} ({daily_count:,.0f}/day)")
    print(f"    Value: median=${med_val:,.0f}  mean=${avg_val:,.0f}  p25=${p25:,.0f}  p75=${p75:,.0f}  p90=${p90:,.0f}")
    print(f"    Total volume: ${total_val:,.0f} (${total_val/max(days,1):,.0f}/day)")
    print(f"    Size distribution:")
    print(f"      >$100:    {a100:>8,} ({a100/count*100:.1f}%)")
    print(f"      >$1,000:  {a1000:>8,} ({a1000/count*100:.1f}%)")
    print(f"      >$10,000: {a10000:>8,} ({a10000/count*100:.1f}%)")
    print(f"      >$100K:   {a100000:>8,} ({a100000/count*100:.1f}%)")

# ── Step 2: Estimate sandwich profit on unsandwiched swaps ──
print(f"\n\n  THEORETICAL SANDWICH PROFIT ON UNSANDWICHED SWAPS:")
print(f"  (Using simplified model: profit ≈ 0.3% of swap value)")

profit_dist = con.execute(f"""
    WITH swaps AS (
        SELECT s.block_number, s.pool, s.sender, s.log_index, s.tx_hash,
            {SWAP_VALUE_SQL} as swap_usd
        FROM read_parquet('data/events/swaps/ethereum/*.parquet', union_by_name=true) s
        JOIN pool_price pp ON lower(s.pool) = lower(pp.pool)
    ),
    bot_activity AS (
        SELECT block_number, pool, sender,
            MIN(log_index) as first_log,
            MAX(log_index) as last_log
        FROM swaps
        GROUP BY block_number, pool, sender
        HAVING COUNT(*) >= 2
    ),
    unsandwiched AS (
        SELECT s.*
        FROM swaps s
        WHERE NOT EXISTS (
            SELECT 1 FROM bot_activity b
            WHERE b.block_number = s.block_number
            AND b.pool = s.pool
            AND b.sender != s.sender
            AND s.log_index > b.first_log
            AND s.log_index < b.last_log
        )
        AND swap_usd > 10
        AND swap_usd < 1e9
    )
    SELECT
        COUNT(*) as total,
        COUNT(CASE WHEN swap_usd * 0.003 > 1.0 THEN 1 END) as profitable_at_1usd,
        COUNT(CASE WHEN swap_usd * 0.003 > 5.0 THEN 1 END) as profitable_at_5usd,
        COUNT(CASE WHEN swap_usd * 0.003 > 10.0 THEN 1 END) as profitable_at_10usd,
        COUNT(CASE WHEN swap_usd * 0.003 > 50.0 THEN 1 END) as profitable_at_50usd,
        COUNT(CASE WHEN swap_usd * 0.003 > 100.0 THEN 1 END) as profitable_at_100usd,
        SUM(swap_usd * 0.003) as total_theoretical_profit,
        AVG(swap_usd) as avg_swap,
        APPROX_QUANTILE(swap_usd, 0.5) as median_swap,
        COUNT(CASE WHEN swap_usd BETWEEN 10 AND 100 THEN 1 END) as b_10_100,
        COUNT(CASE WHEN swap_usd BETWEEN 100 AND 1000 THEN 1 END) as b_100_1k,
        COUNT(CASE WHEN swap_usd BETWEEN 1000 AND 10000 THEN 1 END) as b_1k_10k,
        COUNT(CASE WHEN swap_usd BETWEEN 10000 AND 100000 THEN 1 END) as b_10k_100k,
        COUNT(CASE WHEN swap_usd > 100000 THEN 1 END) as b_100k_plus
    FROM unsandwiched
""").fetchone()

total_unsandwiched = profit_dist[0]
daily_unsandwiched = total_unsandwiched / max(days, 1)
total_theo_profit = profit_dist[6] or 0
daily_theo_profit = total_theo_profit / max(days, 1)

print(f"\n    Unsandwiched swaps >$10: {total_unsandwiched:,} ({daily_unsandwiched:,.0f}/day)")
print(f"    Mean swap: ${profit_dist[7]:,.0f}  Median: ${profit_dist[8]:,.0f}")

print(f"\n    Size distribution (unsandwiched):")
print(f"      $10-100:      {profit_dist[9]:>8,} ({profit_dist[9]/total_unsandwiched*100:.1f}%)")
print(f"      $100-1K:      {profit_dist[10]:>8,} ({profit_dist[10]/total_unsandwiched*100:.1f}%)")
print(f"      $1K-10K:      {profit_dist[11]:>8,} ({profit_dist[11]/total_unsandwiched*100:.1f}%)")
print(f"      $10K-100K:    {profit_dist[12]:>8,} ({profit_dist[12]/total_unsandwiched*100:.1f}%)")
print(f"      $100K+:       {profit_dist[13]:>8,} ({profit_dist[13]/total_unsandwiched*100:.1f}%)")

print(f"\n    Profit thresholds (est. 0.3% of swap value):")
print(f"      >$1/swap:   {profit_dist[1]:>8,} ({profit_dist[1]/max(days,1):,.0f}/day)")
print(f"      >$5/swap:   {profit_dist[2]:>8,} ({profit_dist[2]/max(days,1):,.0f}/day)")
print(f"      >$10/swap:  {profit_dist[3]:>8,} ({profit_dist[3]/max(days,1):,.0f}/day)")
print(f"      >$50/swap:  {profit_dist[4]:>8,} ({profit_dist[4]/max(days,1):,.0f}/day)")
print(f"      >$100/swap: {profit_dist[5]:>8,} ({profit_dist[5]/max(days,1):,.0f}/day)")

print(f"\n    Total theoretical profit (0.3% model): ${total_theo_profit:,.0f}")
print(f"    Daily theoretical: ${daily_theo_profit:,.0f}")

# ── Step 3: Mempool vs Private Tx Analysis ──
print(f"\n\n  MEMPOOL EXPOSURE ESTIMATE:")
print(f"  (Large unsandwiched swaps may use Flashbots Protect = invisible to us)")

exposure = con.execute(f"""
    WITH swaps AS (
        SELECT s.block_number, s.pool, s.sender, s.log_index, s.tx_hash,
            {SWAP_VALUE_SQL} as swap_usd
        FROM read_parquet('data/events/swaps/ethereum/*.parquet', union_by_name=true) s
        JOIN pool_price pp ON lower(s.pool) = lower(pp.pool)
    ),
    bot_activity AS (
        SELECT block_number, pool, sender,
            MIN(log_index) as first_log,
            MAX(log_index) as last_log
        FROM swaps
        GROUP BY block_number, pool, sender
        HAVING COUNT(*) >= 2
    ),
    tagged AS (
        SELECT s.*,
            CASE WHEN EXISTS (
                SELECT 1 FROM bot_activity b
                WHERE b.block_number = s.block_number
                AND b.pool = s.pool
                AND b.sender != s.sender
                AND s.log_index > b.first_log
                AND s.log_index < b.last_log
            ) THEN 1 ELSE 0 END as is_sandwiched
        FROM swaps s
    )
    SELECT
        CASE
            WHEN swap_usd < 100 THEN '$10-100'
            WHEN swap_usd < 1000 THEN '$100-1K'
            WHEN swap_usd < 10000 THEN '$1K-10K'
            WHEN swap_usd < 100000 THEN '$10K-100K'
            ELSE '$100K+'
        END as size_bucket,
        COUNT(*) as total,
        SUM(is_sandwiched) as sandwiched,
        COUNT(*) - SUM(is_sandwiched) as unsandwiched,
        ROUND(SUM(is_sandwiched) * 100.0 / COUNT(*), 1) as sandwich_rate_pct
    FROM tagged
    WHERE swap_usd > 10 AND swap_usd < 1e9
    GROUP BY 1
    ORDER BY MIN(swap_usd)
""").fetchall()

print(f"\n    {'Size Bucket':>12}  {'Total':>8}  {'Sandwiched':>10}  {'Not':>8}  {'SW Rate':>8}")
print(f"    {'-'*56}")
for bucket, total, sw, unsw, rate in exposure:
    print(f"    {bucket:>12}  {total:>8,}  {sw:>10,}  {unsw:>8,}  {rate:>7.1f}%")

print(f"\n    Interpretation:")
print(f"    - Low sandwich rate on large swaps (>$10K) suggests Flashbots Protect usage")
print(f"    - High sandwich rate on mid-size ($1K-10K) = mempool exposure = our targets")
print(f"    - Our addressable market = unsandwiched swaps that WERE in the mempool")
print(f"    - Conservative: assume 50% of unsandwiched large swaps are private")

# ── Step 4: Revenue model ──
print(f"\n\n{'='*90}")
print(f"  REVENUE MODEL")
print(f"{'='*90}")

avg_gas = con.execute("""
    SELECT avg(base_fee_gwei) FROM read_parquet('data/blocks/ethereum/*.parquet', union_by_name=true)
    WHERE base_fee_gwei IS NOT NULL
""").fetchone()[0] or 0.3
gas_cost_usd = 300000 * avg_gas / 1e9 * eth_price

print(f"\n  Gas cost per sandwich: 300K gas × {avg_gas:.1f} gwei = ${gas_cost_usd:.2f}")
print(f"  Builder tip: assumed 85% of gross profit")
print(f"  Net to searcher: 15% of gross - gas")

for extraction_rate in [0.002, 0.003, 0.005]:
    pct_label = f"{extraction_rate*100:.1f}%"
    print(f"\n  At {pct_label} extraction rate:")

    model = con.execute(f"""
        WITH swaps AS (
            SELECT s.block_number, s.pool, s.sender, s.log_index,
                {SWAP_VALUE_SQL} as swap_usd
            FROM read_parquet('data/events/swaps/ethereum/*.parquet', union_by_name=true) s
            JOIN pool_price pp ON lower(s.pool) = lower(pp.pool)
        ),
        bot_activity AS (
            SELECT block_number, pool, sender,
                MIN(log_index) as first_log,
                MAX(log_index) as last_log
            FROM swaps
            GROUP BY block_number, pool, sender
            HAVING COUNT(*) >= 2
        ),
        unsandwiched AS (
            SELECT swap_usd
            FROM swaps s
            WHERE NOT EXISTS (
                SELECT 1 FROM bot_activity b
                WHERE b.block_number = s.block_number
                AND b.pool = s.pool
                AND b.sender != s.sender
                AND s.log_index > b.first_log
                AND s.log_index < b.last_log
            )
            AND swap_usd > 1000 AND swap_usd < 1e9
        )
        SELECT
            COUNT(*) as count,
            SUM(swap_usd * {extraction_rate}) as gross_profit,
            SUM(GREATEST(swap_usd * {extraction_rate} * 0.15 - {gas_cost_usd}, 0)) as net_profit
        FROM unsandwiched
    """).fetchone()

    count = model[0]
    gross = model[1] or 0
    net = model[2] or 0
    daily_count = count / max(days, 1)
    daily_gross = gross / max(days, 1)
    daily_net = net / max(days, 1)

    print(f"    Unsandwiched swaps >$1K: {count:,} ({daily_count:,.0f}/day)")
    print(f"    Daily gross: ${daily_gross:,.0f}")
    print(f"    Daily net (after 85% tip + gas): ${daily_net:,.0f}")

    for capture in [0.01, 0.05, 0.10]:
        captured_net = daily_net * capture
        target = "✅" if captured_net >= 1000 else "❌"
        print(f"      At {capture*100:.0f}% capture: ${captured_net:,.0f}/day {target}")

# ── Step 5: Cross-check ──
print(f"\n\n{'='*90}")
print(f"  CROSS-CHECK: KNOWN BENCHMARKS")
print(f"{'='*90}")
print(f"""
  Real Ethereum DEX volume: ~$2-5B/day (Dune, DeFiLlama)
  Our 186-pool universe should capture a fraction of this.
  If daily volume from this analysis >> $5B, pricing is still broken.

  From P1-S1 (sandwich_competition.py):
    ~30K confirmed sandwiches over {days:.0f} days (~2,000/day)
    ~$1.07M/day gross sandwich revenue

  Published data (Flashbots, EigenPhi):
    ~$800K-1.5M/day sandwich MEV on Ethereum
    Our $1.07M/day is within that range ✓
""")

# ── Step 6: Next steps ──
print(f"{'='*90}")
print(f"  NEXT: MEMPOOL PROBE REQUIREMENTS")
print(f"{'='*90}")
print(f"""
  MEV-Share hints do NOT reveal swap amounts (0% have log data fields).
  Sandwich requires seeing pending txs in the PUBLIC MEMPOOL.

  For the live probe (P1-S2b), we need:
  1. WebSocket node that exposes pending transactions
     (Alchemy supports alchemy_pendingTransactions)
  2. Calldata decoder for swap router contracts:
     - Uniswap V2 Router: swapExactTokensForTokens, etc.
     - Uniswap V3 Router: exactInputSingle, multicall
     - 1inch Router
  3. For each decoded swap: compute sandwich profit vs pool state

  Key risk: What % of swaps use Flashbots Protect (invisible to mempool)?
  The sandwich rate analysis above gives a proxy — low sandwich rates on
  large swaps suggest they're already private.
""")
