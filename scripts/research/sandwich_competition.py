#!/usr/bin/env python3
"""
P1-S1: Sandwich Competition Census

Identifies top sandwich bot addresses, measures market share concentration
(Herfindahl index), and assesses competitive landscape.

Uses existing parquet swap data and sandwich detection logic from sandwich_pnl.py.

Output: competitive landscape assessment with go/kill signal for sandwich strategy.

Kill if: Top 3 bots capture >95% (Herfindahl >0.5)
Go if: Market is fragmented enough for a new entrant to capture 1%+
"""

import json
import duckdb

con = duckdb.connect()

# Load resolved pool tokens
with open("data/pool_tokens.json") as f:
    pool_tokens = json.load(f)

WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
STABLES = {
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": 6,   # USDC
    "0xdac17f958d2ee523a2206206994597c13d831ec7": 6,   # USDT
    "0x6b175474e89094c44da98b954eedeac495271d0f": 18,  # DAI
}

# Get ETH/USD price
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

# Build pool pricing
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

# Time window
blocks = con.execute("""
    SELECT min(block_number), max(block_number), count(distinct block_number)
    FROM read_parquet('data/blocks/ethereum/*.parquet', union_by_name=true)
""").fetchone()
# Use block COUNT / 7200, not block RANGE / 7200
# Blocks were ingested in batches, not contiguously
block_count = blocks[2]
days = block_count / 7200

print(f"Data window: {days:.1f} days ({blocks[2]:,} blocks)")
print(f"Priceable pools: {len(pool_pricing)}")

# ── SANDWICH BOT IDENTIFICATION ──
# Get per-bot sandwich stats: count, total profit, median profit, avg position size
print("\nIdentifying sandwich bots and computing per-bot stats...")

bot_stats = con.execute("""
    WITH swaps AS (
        SELECT s.block_number, s.pool, s.sender, s.log_index, s.tx_hash,
            CAST(s.amount_in AS DOUBLE) as amt_in,
            CAST(s.amount_out AS DOUBLE) as amt_out,
            pp.quote_side, pp.decimals, pp.usd_per_unit
        FROM read_parquet('data/events/swaps/ethereum/*.parquet', union_by_name=true) s
        JOIN pool_price pp ON lower(s.pool) = lower(pp.pool)
    ),
    bot_pairs AS (
        SELECT block_number, pool, sender, quote_side, decimals, usd_per_unit,
            MIN(log_index) as first_log,
            MAX(log_index) as last_log,
            COUNT(*) as swap_count
        FROM swaps
        GROUP BY block_number, pool, sender, quote_side, decimals, usd_per_unit
        HAVING COUNT(*) >= 2
    ),
    first_swaps AS (
        SELECT s.block_number, s.pool, s.sender, s.amt_in as f_in, s.amt_out as f_out
        FROM swaps s
        JOIN bot_pairs bp ON s.block_number = bp.block_number
            AND s.pool = bp.pool AND s.sender = bp.sender
            AND s.log_index = bp.first_log
    ),
    last_swaps AS (
        SELECT s.block_number, s.pool, s.sender, s.amt_in as l_in, s.amt_out as l_out
        FROM swaps s
        JOIN bot_pairs bp ON s.block_number = bp.block_number
            AND s.pool = bp.pool AND s.sender = bp.sender
            AND s.log_index = bp.last_log
    ),
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
    sandwiches AS (
        SELECT *,
            GREATEST(
                (l_out - f_in) / POWER(10, decimals) * usd_per_unit,
                (f_out - l_in) / POWER(10, decimals) * usd_per_unit
            ) as best_profit_usd,
            f_in / POWER(10, decimals) * usd_per_unit as position_usd
        FROM with_victims
        WHERE victim_count >= 1
    )
    SELECT
        sender,
        COUNT(*) as sandwich_count,
        SUM(CASE WHEN best_profit_usd > 0 AND best_profit_usd < 10000
            THEN best_profit_usd ELSE 0 END) as total_profit_usd,
        AVG(CASE WHEN best_profit_usd > 0 AND best_profit_usd < 10000
            THEN best_profit_usd END) as avg_profit_usd,
        APPROX_QUANTILE(CASE WHEN best_profit_usd > 0 AND best_profit_usd < 10000
            THEN best_profit_usd END, 0.5) as median_profit_usd,
        AVG(CASE WHEN position_usd > 0 AND position_usd < 1e8
            THEN position_usd END) as avg_position_usd,
        APPROX_QUANTILE(CASE WHEN position_usd > 0 AND position_usd < 1e8
            THEN position_usd END, 0.5) as median_position_usd,
        COUNT(DISTINCT pool) as pools_targeted,
        COUNT(DISTINCT block_number) as active_blocks,
        MIN(block_number) as first_block,
        MAX(block_number) as last_block,
        AVG(victim_count) as avg_victims
    FROM sandwiches
    GROUP BY sender
    ORDER BY total_profit_usd DESC
""").fetchall()

total_market_profit = sum(row[2] for row in bot_stats if row[2] and row[2] > 0)
total_sandwiches = sum(row[1] for row in bot_stats)

print(f"\n{'='*90}")
print(f"  SANDWICH COMPETITION CENSUS — ETHEREUM")
print(f"  {days:.1f} days, {len(pool_pricing)} priceable pools")
print(f"{'='*90}")

print(f"\n  Total sandwich bots identified: {len(bot_stats)}")
print(f"  Total confirmed sandwiches: {total_sandwiches:,}")
print(f"  Total market profit: ${total_market_profit:,.0f}")
print(f"  Daily market profit: ${total_market_profit / max(days, 1):,.0f}")

# ── TOP BOTS TABLE ──
print(f"\n  TOP 15 SANDWICH BOTS BY PROFIT:")
print(f"  {'#':>3}  {'Address':>44}  {'Count':>7}  {'Profit':>12}  "
      f"{'Share':>6}  {'Med$/sw':>8}  {'MedPos$':>10}  {'Pools':>5}  Victims")
print(f"  {'-'*115}")

cumulative_share = 0
shares = []

for i, row in enumerate(bot_stats[:15]):
    addr, count, total_p, avg_p, med_p, avg_pos, med_pos, pools, active_blocks, _, _, avg_victims = row
    total_p = total_p or 0
    share = total_p / total_market_profit * 100 if total_market_profit > 0 else 0
    cumulative_share += share
    shares.append(share / 100)
    med_p = med_p or 0
    med_pos = med_pos or 0
    avg_victims = avg_victims or 0
    print(f"  {i+1:>3}  {addr:>44}  {count:>7,}  ${total_p:>10,.0f}  "
          f"{share:>5.1f}%  ${med_p:>7,.0f}  ${med_pos:>9,.0f}  {pools:>5}  {avg_victims:.1f}")

# ── CONCENTRATION ANALYSIS ──
all_shares = [((row[2] or 0) / total_market_profit) for row in bot_stats
              if row[2] and row[2] > 0 and total_market_profit > 0]

herfindahl = sum(s ** 2 for s in all_shares)
top3_share = sum(all_shares[:3]) if len(all_shares) >= 3 else sum(all_shares)
top5_share = sum(all_shares[:5]) if len(all_shares) >= 5 else sum(all_shares)
top10_share = sum(all_shares[:10]) if len(all_shares) >= 10 else sum(all_shares)

print(f"\n  CONCENTRATION ANALYSIS:")
print(f"    Herfindahl-Hirschman Index (HHI): {herfindahl:.4f}")
print(f"    (>0.25 = highly concentrated, >0.15 = moderately concentrated)")
print(f"    Top 3 market share:  {top3_share*100:.1f}%")
print(f"    Top 5 market share:  {top5_share*100:.1f}%")
print(f"    Top 10 market share: {top10_share*100:.1f}%")
print(f"    Total active bots:   {len([s for s in all_shares if s > 0.001])}")
print(f"    Bots with >1% share: {len([s for s in all_shares if s > 0.01])}")

# ── FREQUENCY AND TIMING ──
print(f"\n  ACTIVITY PATTERNS:")
daily_sandwiches = total_sandwiches / max(days, 1)
print(f"    Sandwiches/day (all bots): {daily_sandwiches:,.0f}")

for i, row in enumerate(bot_stats[:5]):
    addr, count, total_p, *_ = row
    daily = count / max(days, 1)
    blocks_active = row[8]
    block_range_bot = (row[10] - row[9]) if row[9] and row[10] else 0
    uptime = blocks_active / max(block_range_bot / 7200, 0.01) if block_range_bot > 0 else 0
    print(f"    Bot #{i+1} ({addr[:10]}...): {daily:.0f}/day, "
          f"{blocks_active:,} active blocks")

# ── POOL OVERLAP ──
print(f"\n  POOL TARGETING (top 5 bots):")
for i, row in enumerate(bot_stats[:5]):
    addr = row[0]
    pools_hit = row[7]
    print(f"    Bot #{i+1} ({addr[:10]}...): targets {pools_hit} pools")

# ── BUILDER TIP ESTIMATION ──
# We can't directly measure tips from swap data, but we can note the gap
print(f"\n  BUILDER TIP ESTIMATE:")
print(f"    Published data suggests 80-90% of gross profit goes to builders")
print(f"    At 85% tip rate:")
net_rate = 0.15
daily_market = total_market_profit / max(days, 1)
print(f"      Daily gross: ${daily_market:,.0f}")
print(f"      Daily net to searchers: ${daily_market * net_rate:,.0f}")
print(f"      Monthly net to all searchers: ${daily_market * net_rate * 30:,.0f}")

# ── GO/KILL ASSESSMENT ──
print(f"\n{'='*90}")
print(f"  GO/KILL ASSESSMENT")
print(f"{'='*90}")

kill_signals = []
go_signals = []

if herfindahl > 0.5:
    kill_signals.append(f"HHI = {herfindahl:.3f} > 0.5 threshold (market is monopolistic)")
elif herfindahl > 0.25:
    kill_signals.append(f"HHI = {herfindahl:.3f} > 0.25 (highly concentrated)")

if top3_share > 0.95:
    kill_signals.append(f"Top 3 capture {top3_share*100:.1f}% > 95% threshold")

if daily_sandwiches < 50:
    kill_signals.append(f"Only {daily_sandwiches:.0f} sandwiches/day (need >50)")

# Go signals
if herfindahl < 0.25:
    go_signals.append(f"HHI = {herfindahl:.3f} < 0.25 (not highly concentrated)")

if top3_share < 0.95:
    go_signals.append(f"Top 3 capture {top3_share*100:.1f}% < 95% (room for new entrant)")

if len([s for s in all_shares if s > 0.01]) >= 5:
    go_signals.append(f"{len([s for s in all_shares if s > 0.01])} bots with >1% share (fragmented)")

daily_net_at_1pct = daily_market * net_rate * 0.01
if daily_net_at_1pct > 100:
    go_signals.append(f"1% capture = ${daily_net_at_1pct:,.0f}/day net (above $100 threshold)")

if kill_signals:
    print(f"\n  KILL SIGNALS:")
    for s in kill_signals:
        print(f"    ❌ {s}")

if go_signals:
    print(f"\n  GO SIGNALS:")
    for s in go_signals:
        print(f"    ✅ {s}")

if kill_signals and not go_signals:
    print(f"\n  VERDICT: KILL — market too concentrated for new entrant")
elif go_signals and not kill_signals:
    print(f"\n  VERDICT: PROCEED TO STAGE 2 — market structure supports entry")
else:
    print(f"\n  VERDICT: MIXED — proceed to Stage 2 with caution")

print(f"\n  Revenue projections at various capture rates (85% builder tip):")
print(f"  {'Capture':>8}  {'Gross/day':>10}  {'Net/day':>10}  {'Net/month':>10}  vs $1K/day target")
for rate in [0.001, 0.005, 0.01, 0.05, 0.10]:
    gross = daily_market * rate
    net = gross * net_rate
    monthly = net * 30
    target = "✅" if net >= 1000 else "❌"
    print(f"  {rate*100:>7.1f}%  ${gross:>9,.0f}  ${net:>9,.0f}  ${monthly:>9,.0f}  {target}")

print()
