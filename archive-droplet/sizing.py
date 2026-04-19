import duckdb
con = duckdb.connect()

# The WETH/USDC pool we already price from
WETH_USDC = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"

# Known major pools with WETH as one side (Ethereum mainnet)
# We can price any swap on these pools because one leg is WETH
# token0/token1 ordering matters for V3 — but for sizing we just
# need to identify which amount is WETH and which is the other token

# First: what % of ETH swap volume runs through the top N pools?
print("=" * 70)
print("COVERAGE ANALYSIS: How much volume can we price?")
print("=" * 70)

# Top pools by swap count
top = con.execute("""
    SELECT pool, protocol, count(*) as swaps
    FROM read_parquet('data/events/swaps/ethereum/*.parquet', union_by_name=true)
    GROUP BY pool, protocol
    ORDER BY swaps DESC
    LIMIT 50
""").fetchall()

total_swaps = con.execute("""
    SELECT count(*) FROM read_parquet('data/events/swaps/ethereum/*.parquet', union_by_name=true)
""").fetchone()[0]

cumulative = 0
print(f"\n  Total ETH swaps: {total_swaps:,}")
print(f"\n  Top 50 pools cover:")
for i, row in enumerate(top):
    cumulative += row[2]
    if i < 10 or i in [19, 29, 49]:
        print(f"    Top {i+1:>2}: {cumulative:,} swaps ({cumulative*100/total_swaps:.1f}%)")

print(f"\n  Top 50 pools = {cumulative:,} ({cumulative*100/total_swaps:.1f}%)")

# Now the key question: can we identify WETH pairs without factory calls?
# Approach: look at amount magnitudes in the WETH/USDC pool to calibrate,
# then find pools where one side has similar magnitude to WETH amounts
print("\n" + "=" * 70)
print("APPROACH A: Use on-chain pool registry (factory contract)")
print("=" * 70)
print("""
  Uniswap V2 factory: getPool(tokenA, tokenB) -> pool address
  Uniswap V3 factory: getPool(tokenA, tokenB, fee) -> pool address
  
  One RPC call per pool. For top 100 pools = 100 calls.
  Alchemy free tier: 300 CU/sec. eth_call = 26 CU. ~11 calls/sec.
  Time: ~10 seconds for 100 pools.
  
  This gives us token0 and token1 addresses for each pool.
  Then we check if either token is WETH (0xC02aaA39...).
  If yes: we can price the swap in ETH, then in USD.
  If no: we need a second hop (TOKEN_A/WETH pool) to price.
""")

# Approach B: Use amount magnitude heuristic from existing data
print("=" * 70)
print("APPROACH B: Price sandwiches directly via bracketing")
print("=" * 70)
print("""
  For sandwich sizing, we don't need token prices at all.
  
  A sandwich works like:
    1. Bot buys TOKEN on pool (pushes price up)
    2. Victim buys TOKEN at inflated price  
    3. Bot sells TOKEN (captures the spread)
  
  The bot's profit = (sell_amount_out - buy_amount_in) for the quote token.
  
  We have amount_in and amount_out for each swap event.
  For a confirmed sandwich (bot swap 1, victim, bot swap 2 on same pool):
    bot_profit_raw = bot_swap2.amount_out - bot_swap1.amount_in
  
  This gives profit in raw token units (e.g. wei for WETH).
  To convert to USD, we only need to price ONE token — the quote token.
  For WETH pools: profit_usd = bot_profit_raw / 1e18 * eth_price.
""")

# Let's actually try to size sandwiches on ETH using raw amounts
print("=" * 70)
print("SIZING: ETH sandwich profit from raw amounts")
print("=" * 70)

# Find confirmed sandwich patterns: same sender, 2+ swaps bracketing others
r = con.execute("""
    WITH ordered AS (
        SELECT 
            block_number, pool, sender, tx_index, log_index,
            CAST(amount_in AS DOUBLE) as amt_in,
            CAST(amount_out AS DOUBLE) as amt_out,
            ROW_NUMBER() OVER (PARTITION BY block_number, pool, sender ORDER BY log_index) as rn,
            COUNT(*) OVER (PARTITION BY block_number, pool, sender) as sender_swaps
        FROM read_parquet('data/events/swaps/ethereum/*.parquet', union_by_name=true)
    ),
    bot_pairs AS (
        SELECT 
            a.block_number, a.pool, a.sender,
            a.amt_in as buy_in, a.amt_out as buy_out,
            b.amt_in as sell_in, b.amt_out as sell_out,
            a.log_index as first_log, b.log_index as last_log
        FROM ordered a
        JOIN ordered b ON a.block_number = b.block_number 
            AND a.pool = b.pool 
            AND a.sender = b.sender
            AND a.rn = 1 AND b.rn = b.sender_swaps
            AND b.sender_swaps >= 2
            AND b.log_index > a.log_index
    ),
    with_victims AS (
        SELECT bp.*,
            COUNT(DISTINCT s.sender) as victim_count
        FROM bot_pairs bp
        JOIN read_parquet('data/events/swaps/ethereum/*.parquet', union_by_name=true) s
            ON s.block_number = bp.block_number
            AND s.pool = bp.pool
            AND s.sender != bp.sender
            AND s.log_index > bp.first_log
            AND s.log_index < bp.last_log
        GROUP BY bp.block_number, bp.pool, bp.sender,
            bp.buy_in, bp.buy_out, bp.sell_in, bp.sell_out,
            bp.first_log, bp.last_log
    )
    SELECT 
        COUNT(*) as sandwiches,
        -- Profit proxy: compare sell_out to buy_in (same token side)
        -- If bot buys with token0 and sells for token0:
        --   profit = sell_out - buy_in (when amounts are on same side)
        -- We check both directions
        AVG(CASE WHEN sell_out > buy_in AND sell_out < buy_in * 1.1 
            THEN sell_out - buy_in ELSE NULL END) as avg_profit_side_a,
        AVG(CASE WHEN buy_out > sell_in AND buy_out < sell_in * 1.1
            THEN buy_out - sell_in ELSE NULL END) as avg_profit_side_b,
        COUNT(CASE WHEN sell_out > buy_in AND sell_out < buy_in * 1.1 
            THEN 1 END) as profitable_a,
        COUNT(CASE WHEN buy_out > sell_in AND buy_out < sell_in * 1.1
            THEN 1 END) as profitable_b,
        AVG(victim_count) as avg_victims,
        -- Sample some raw numbers to understand magnitudes
        APPROX_QUANTILE(buy_in, 0.5) as median_buy_in,
        APPROX_QUANTILE(sell_out, 0.5) as median_sell_out
    FROM with_victims
""").fetchone()

print(f"\n  Confirmed sandwiches (bot bracket + victim between): {r[0]:,}")
print(f"  Avg victims per sandwich: {r[5]:.1f}")
print(f"  Median buy_in:  {r[6]:.2e}")
print(f"  Median sell_out: {r[7]:.2e}")
print(f"\n  Profit analysis (sell_out > buy_in, <10% spread):")
print(f"    Direction A: {r[3]:,} profitable, avg raw profit: {r[1]:.2e}" if r[1] else f"    Direction A: {r[3]:,} profitable")
print(f"    Direction B: {r[4]:,} profitable, avg raw profit: {r[2]:.2e}" if r[2] else f"    Direction B: {r[4]:,} profitable")

# Now let's try to price these using WETH/USDC reference
print("\n" + "=" * 70)
print("WETH/USDC REFERENCE PRICING")
print("=" * 70)

# Get median ETH price from our data
price = con.execute(f"""
    WITH swaps AS (
        SELECT CAST(amount_in AS DOUBLE) as ai, CAST(amount_out AS DOUBLE) as ao
        FROM read_parquet('data/events/swaps/ethereum/*.parquet', union_by_name=true)
        WHERE lower(pool) = '{WETH_USDC}'
    )
    SELECT 
        APPROX_QUANTILE(CASE WHEN ai < ao THEN (ai / 1e6) / (ao / 1e18) END, 0.5) as price_a,
        APPROX_QUANTILE(CASE WHEN ao < ai THEN (ao / 1e6) / (ai / 1e18) END, 0.5) as price_b,
        COUNT(*) as swap_count
    FROM swaps
""").fetchone()

eth_price = price[0] if price[0] and 500 < price[0] < 20000 else (price[1] if price[1] and 500 < price[1] < 20000 else 3000)
print(f"  Derived ETH/USD: ${eth_price:,.0f} (from {price[2]:,} WETH/USDC swaps)")

# What % of top pools likely have WETH as one token?
# Heuristic: if a pool's median amount_in or amount_out is in the 1e15-1e20 range,
# one side is probably an 18-decimal token (WETH, WBTC, etc.)
print("\n  Estimating WETH-pair coverage among top pools...")
coverage = con.execute("""
    WITH pool_stats AS (
        SELECT pool, 
            APPROX_QUANTILE(CAST(amount_in AS DOUBLE), 0.5) as med_in,
            APPROX_QUANTILE(CAST(amount_out AS DOUBLE), 0.5) as med_out,
            count(*) as swaps
        FROM read_parquet('data/events/swaps/ethereum/*.parquet', union_by_name=true)
        GROUP BY pool
        HAVING count(*) > 100
        ORDER BY swaps DESC
        LIMIT 100
    )
    SELECT 
        count(*) as total_pools,
        sum(swaps) as total_swaps,
        -- Pools where one side looks like 18-decimal and other like 6-decimal
        -- (WETH/USDC pattern: one side ~1e18 per unit, other ~1e6 per unit)
        sum(CASE WHEN (med_in > 1e14 AND med_out < 1e12) OR (med_out > 1e14 AND med_in < 1e12)
            THEN swaps ELSE 0 END) as likely_weth_usdc_pattern,
        sum(CASE WHEN med_in > 1e14 OR med_out > 1e14
            THEN swaps ELSE 0 END) as has_18dec_token
    FROM pool_stats
""").fetchone()

print(f"    Top 100 pools: {coverage[0]} pools, {coverage[1]:,} swaps")
print(f"    Likely WETH/stablecoin pattern: {coverage[2]:,} swaps ({coverage[2]*100//max(coverage[1],1)}%)")
print(f"    Has 18-decimal token: {coverage[3]:,} swaps ({coverage[3]*100//max(coverage[1],1)}%)")

# Bottom line sizing
print("\n" + "=" * 70)
print("BOTTOM LINE: What would it take to get real P&L numbers")
print("=" * 70)
print(f"""
  OPTION 1 — Factory lookup (RECOMMENDED, 1-2 hours of work):
    - Call Uniswap V2/V3 factory contracts for top 100 pools
    - 100 RPC calls = ~10 seconds on Alchemy
    - Maps pool -> (token0, token1)
    - Any pool with WETH: price directly via ETH/USD
    - Any pool with USDC/USDT/DAI: price directly  
    - Coverage: likely 80-90% of swap volume
    - Then: re-run sandwich analysis with real USD profits
    
  OPTION 2 — Amount-ratio heuristic (quick and dirty, 30 min):
    - For confirmed sandwiches, compute sell_out - buy_in ratio
    - If one side looks like WETH (1e15-1e20 range), price it
    - Less accurate, but gives order-of-magnitude sizing today
    
  OPTION 3 — Etherscan/Dexscreener API (minutes, but external):
    - Look up top 20 pool addresses on Dexscreener API
    - Get token pair + current price
    - Quick validation of our data quality
    
  For a real business case you need OPTION 1.
  For a "is this worth pursuing at all" answer, OPTION 2 or 3 today.
""")

