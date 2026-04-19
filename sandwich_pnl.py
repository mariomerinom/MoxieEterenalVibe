"""
Sandwich P&L: price confirmed sandwiches in USD using resolved token pairs.
"""
import json
import duckdb

con = duckdb.connect()

# Load resolved pool tokens
with open("/root/mev/data/pool_tokens.json") as f:
    pool_tokens = json.load(f)

WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
STABLES = {
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": 6,   # USDC
    "0xdac17f958d2ee523a2206206994597c13d831ec7": 6,   # USDT
    "0x6b175474e89094c44da98b954eedeac495271d0f": 18,  # DAI
}

# Get ETH/USD price from our data
eth_price = con.execute("""
    WITH swaps AS (
        SELECT CAST(amount_in AS DOUBLE) as ai, CAST(amount_out AS DOUBLE) as ao
        FROM read_parquet('data/events/swaps/ethereum/*.parquet', union_by_name=true)
        WHERE lower(pool) = '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'
    )
    SELECT APPROX_QUANTILE(
        CASE WHEN ai < ao THEN (ai / 1e6) / (ao / 1e18) END, 0.5
    ) FROM swaps
""").fetchone()[0]

if not eth_price or eth_price < 500 or eth_price > 20000:
    eth_price = 3000.0
print(f"ETH/USD: ${eth_price:,.0f}")

# Build pool pricing mapping
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

print(f"Priceable pools: {len(pool_pricing)}")

# Create temp table with pool pricing
pool_rows = [(p, v["quote_side"], v["decimals"], v["usd_per_unit"]) for p, v in pool_pricing.items()]
con.execute("CREATE TABLE pool_price (pool VARCHAR, quote_side INT, decimals INT, usd_per_unit DOUBLE)")
con.executemany("INSERT INTO pool_price VALUES (?, ?, ?, ?)", pool_rows)

print("\nFinding confirmed sandwiches on priceable pools...")

# Single query: bot_pairs -> first/last swaps -> victim check -> profit
profits = con.execute("""
    WITH swaps AS (
        SELECT s.block_number, s.pool, s.sender, s.log_index, s.tx_hash,
            CAST(s.amount_in AS DOUBLE) as amt_in,
            CAST(s.amount_out AS DOUBLE) as amt_out,
            pp.quote_side, pp.decimals, pp.usd_per_unit
        FROM read_parquet('data/events/swaps/ethereum/*.parquet', union_by_name=true) s
        JOIN pool_price pp ON lower(s.pool) = lower(pp.pool)
    ),
    -- Step 1: find senders with 2+ swaps on same pool in same block
    bot_pairs AS (
        SELECT block_number, pool, sender, quote_side, decimals, usd_per_unit,
            MIN(log_index) as first_log,
            MAX(log_index) as last_log,
            COUNT(*) as swap_count
        FROM swaps
        GROUP BY block_number, pool, sender, quote_side, decimals, usd_per_unit
        HAVING COUNT(*) >= 2
    ),
    -- Step 2: get the first swap row for each bot pair
    first_swaps AS (
        SELECT s.block_number, s.pool, s.sender, s.amt_in as f_in, s.amt_out as f_out
        FROM swaps s
        JOIN bot_pairs bp ON s.block_number = bp.block_number
            AND s.pool = bp.pool AND s.sender = bp.sender
            AND s.log_index = bp.first_log
    ),
    -- Step 3: get the last swap row for each bot pair
    last_swaps AS (
        SELECT s.block_number, s.pool, s.sender, s.amt_in as l_in, s.amt_out as l_out
        FROM swaps s
        JOIN bot_pairs bp ON s.block_number = bp.block_number
            AND s.pool = bp.pool AND s.sender = bp.sender
            AND s.log_index = bp.last_log
    ),
    -- Step 4: check for victims between first and last
    with_victims AS (
        SELECT bp.*, fs.f_in, fs.f_out, ls.l_in, ls.l_out,
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
    ),
    -- Step 5: compute profit both directions
    sandwiches AS (
        SELECT *,
            (l_out - f_in) / POWER(10, decimals) * usd_per_unit as profit_a_usd,
            (f_out - l_in) / POWER(10, decimals) * usd_per_unit as profit_b_usd,
            f_in / POWER(10, decimals) * usd_per_unit as first_in_usd,
            l_out / POWER(10, decimals) * usd_per_unit as last_out_usd
        FROM with_victims
        WHERE victim_count >= 1
    )
    SELECT
        COUNT(*) as total_sandwiches,

        COUNT(CASE WHEN profit_a_usd > 0 AND profit_a_usd < 10000 THEN 1 END) as profitable_a,
        AVG(CASE WHEN profit_a_usd > 0 AND profit_a_usd < 10000 THEN profit_a_usd END) as avg_profit_a,
        APPROX_QUANTILE(CASE WHEN profit_a_usd > 0 AND profit_a_usd < 10000 THEN profit_a_usd END, 0.5) as median_profit_a,
        SUM(CASE WHEN profit_a_usd > 0 AND profit_a_usd < 10000 THEN profit_a_usd ELSE 0 END) as total_profit_a,

        COUNT(CASE WHEN profit_b_usd > 0 AND profit_b_usd < 10000 THEN 1 END) as profitable_b,
        AVG(CASE WHEN profit_b_usd > 0 AND profit_b_usd < 10000 THEN profit_b_usd END) as avg_profit_b,
        APPROX_QUANTILE(CASE WHEN profit_b_usd > 0 AND profit_b_usd < 10000 THEN profit_b_usd END, 0.5) as median_profit_b,
        SUM(CASE WHEN profit_b_usd > 0 AND profit_b_usd < 10000 THEN profit_b_usd ELSE 0 END) as total_profit_b,

        AVG(CASE WHEN first_in_usd > 0 AND first_in_usd < 1e8 THEN first_in_usd END) as avg_position_size,
        APPROX_QUANTILE(CASE WHEN first_in_usd > 0 AND first_in_usd < 1e8 THEN first_in_usd END, 0.5) as median_position_size,

        AVG(victim_count) as avg_victims

    FROM sandwiches
""").fetchone()

SEP = "=" * 70
DASH = "-" * 76
DASH2 = "-" * 64

print(f"\n{SEP}")
print(f"SANDWICH P&L — ETHEREUM (priceable pools, {len(pool_pricing)} pools)")
print(f"{SEP}")
print(f"\n  Confirmed sandwiches (with victims): {profits[0]:,}")
print(f"  Avg victims per sandwich: {profits[11]:.1f}")

print(f"\n  Direction A (quote: in first -> out last):")
print(f"    Profitable: {profits[1]:,}")
if profits[2]: print(f"    Avg profit: ${profits[2]:,.2f}")
if profits[3]: print(f"    Median profit: ${profits[3]:,.2f}")
if profits[4]: print(f"    Total profit (7d window): ${profits[4]:,.0f}")

print(f"\n  Direction B (quote: out first -> in last):")
print(f"    Profitable: {profits[5]:,}")
if profits[6]: print(f"    Avg profit: ${profits[6]:,.2f}")
if profits[7]: print(f"    Median profit: ${profits[7]:,.2f}")
if profits[8]: print(f"    Total profit (7d window): ${profits[8]:,.0f}")

print(f"\n  Position sizing:")
if profits[9]: print(f"    Avg position: ${profits[9]:,.0f}")
if profits[10]: print(f"    Median position: ${profits[10]:,.0f}")

# Pick the direction with more profitable cases
if (profits[1] or 0) > (profits[5] or 0):
    main_count = profits[1] or 0
    main_total = profits[4] or 0
    main_avg = profits[2] or 0
    main_median = profits[3] or 0
else:
    main_count = profits[5] or 0
    main_total = profits[8] or 0
    main_avg = profits[6] or 0
    main_median = profits[7] or 0

# Time window
blocks = con.execute("""
    SELECT min(block_number), max(block_number), count(distinct block_number)
    FROM read_parquet('data/blocks/ethereum/*.parquet', union_by_name=true)
""").fetchone()

block_range = blocks[1] - blocks[0]
days = block_range / 7200

print(f"\n{SEP}")
print(f"BUSINESS SIZING")
print(f"{SEP}")
print(f"\n  Data window: {days:.1f} days ({blocks[2]:,} blocks)")
print(f"  Priceable sandwiches: {main_count:,}")
print(f"  Total sandwich profit in window: ${main_total:,.0f}")
print(f"  Avg per sandwich: ${main_avg:,.2f}")
print(f"  Median per sandwich: ${main_median:,.2f}")

daily_profit = main_total / max(days, 1)
monthly_profit = daily_profit * 30
annual_profit = daily_profit * 365

print(f"\n  Extrapolated:")
print(f"    Daily:   ${daily_profit:,.0f}")
print(f"    Monthly: ${monthly_profit:,.0f}")
print(f"    Annual:  ${annual_profit:,.0f}")

# Gas cost estimation
avg_gas_gwei = con.execute("""
    SELECT avg(base_fee_gwei) FROM read_parquet('data/blocks/ethereum/*.parquet', union_by_name=true)
    WHERE base_fee_gwei IS NOT NULL
""").fetchone()[0] or 20

gas_per_sandwich = 300000
gas_cost_eth = gas_per_sandwich * avg_gas_gwei / 1e9
gas_cost_usd = gas_cost_eth * eth_price

monthly_sandwiches = main_count / max(days, 1) * 30
monthly_gas = monthly_sandwiches * gas_cost_usd
infra_cost = 50

print(f"\n  Cost structure:")
print(f"    Avg gas price: {avg_gas_gwei:.1f} gwei")
print(f"    Gas per sandwich: {gas_per_sandwich:,} gas = ${gas_cost_usd:.2f}")
print(f"    Monthly gas (at 100% capture): ${monthly_gas:,.0f}")
print(f"    Monthly infra: ${infra_cost}")

print(f"\n  {'Capture Rate':>14} {'Sandwiches/mo':>14} {'Gross':>12} {'Gas':>12} {'Net':>12} {'Annual':>12}")
print(f"  {DASH}")
for rate in [0.01, 0.05, 0.10, 0.25]:
    s_mo = monthly_sandwiches * rate
    gross = monthly_profit * rate
    gas = s_mo * gas_cost_usd
    net = gross - gas - infra_cost
    annual = net * 12
    print(f"  {rate*100:>13.0f}% {s_mo:>14,.0f} ${gross:>10,.0f} ${gas:>10,.0f} ${net:>10,.0f} ${annual:>10,.0f}")

# DEX ARB SIZING
print(f"\n{SEP}")
print(f"DEX ARB SIZING")
print(f"{SEP}")

arb_profits = con.execute("""
    WITH swaps AS (
        SELECT s.block_number, s.pool, s.sender, s.tx_hash, s.log_index,
            CAST(s.amount_in AS DOUBLE) as amt_in,
            CAST(s.amount_out AS DOUBLE) as amt_out,
            pp.quote_side, pp.decimals, pp.usd_per_unit
        FROM read_parquet('data/events/swaps/ethereum/*.parquet', union_by_name=true) s
        JOIN pool_price pp ON lower(s.pool) = lower(pp.pool)
    ),
    arb_txs AS (
        SELECT tx_hash
        FROM swaps
        GROUP BY tx_hash
        HAVING COUNT(*) >= 2 AND COUNT(DISTINCT pool) >= 2
    ),
    arb_flows AS (
        SELECT s.tx_hash, s.sender,
            SUM(s.amt_out / POWER(10, s.decimals) * s.usd_per_unit) as total_out_usd,
            SUM(s.amt_in / POWER(10, s.decimals) * s.usd_per_unit) as total_in_usd,
            COUNT(*) as swap_count
        FROM swaps s
        WHERE s.tx_hash IN (SELECT tx_hash FROM arb_txs)
        GROUP BY s.tx_hash, s.sender
    )
    SELECT
        COUNT(*) as arb_count,
        COUNT(CASE WHEN total_out_usd > total_in_usd
            AND (total_out_usd - total_in_usd) < 50000
            AND total_in_usd > 0 THEN 1 END) as profitable,
        AVG(CASE WHEN total_out_usd > total_in_usd
            AND (total_out_usd - total_in_usd) < 50000
            AND total_in_usd > 0 THEN total_out_usd - total_in_usd END) as avg_profit,
        APPROX_QUANTILE(CASE WHEN total_out_usd > total_in_usd
            AND (total_out_usd - total_in_usd) < 50000
            AND total_in_usd > 0 THEN total_out_usd - total_in_usd END, 0.5) as median_profit,
        SUM(CASE WHEN total_out_usd > total_in_usd
            AND (total_out_usd - total_in_usd) < 50000
            AND total_in_usd > 0 THEN total_out_usd - total_in_usd ELSE 0 END) as total_profit
    FROM arb_flows
""").fetchone()

print(f"\n  Arb txs analyzed: {arb_profits[0]:,}")
print(f"  Profitable (net out > in, <$50K): {arb_profits[1]:,}")
if arb_profits[2]: print(f"  Avg profit: ${arb_profits[2]:,.2f}")
if arb_profits[3]: print(f"  Median profit: ${arb_profits[3]:,.2f}")
if arb_profits[4]: print(f"  Total in window: ${arb_profits[4]:,.0f}")

arb_daily = (arb_profits[4] or 0) / max(days, 1)
arb_monthly = arb_daily * 30

print(f"\n  Extrapolated:")
print(f"    Daily:   ${arb_daily:,.0f}")
print(f"    Monthly: ${arb_monthly:,.0f}")

arb_gas = 350000 * avg_gas_gwei / 1e9 * eth_price
arb_mo_count = (arb_profits[1] or 0) / max(days, 1) * 30
print(f"\n  {'Capture Rate':>14} {'Arbs/mo':>14} {'Gross':>12} {'Gas':>12} {'Net':>12}")
print(f"  {DASH2}")
for rate in [0.01, 0.05, 0.10]:
    a_mo = arb_mo_count * rate
    gross = arb_monthly * rate
    gas = a_mo * arb_gas
    net = gross - gas - infra_cost
    print(f"  {rate*100:>13.0f}% {a_mo:>14,.0f} ${gross:>10,.0f} ${gas:>10,.0f} ${net:>10,.0f}")
