import duckdb
con = duckdb.connect()

for chain in ["ethereum", "polygon", "blast"]:
    con.execute(f"""
        CREATE OR REPLACE VIEW {chain}_swaps AS 
        SELECT * FROM read_parquet('/root/mev/data/events/swaps/{chain}/*.parquet', union_by_name=true)
    """)

print("=" * 70)
print("ANALYSIS 6: ETH BOT FINGERPRINTING")
print("Top actors by unique pools touched + swap frequency")
print("=" * 70)

# On ETH, sender is the actual caller (not always a router)
# High swap count + few pools = likely sandwich bot
# High swap count + many pools = likely arb bot
r = con.execute("""
    SELECT 
        sender,
        count(*) as swaps,
        count(distinct pool) as pools,
        count(distinct block_number) as blocks,
        ROUND(count(*)::float / count(distinct block_number), 1) as swaps_per_block,
        ROUND(count(distinct pool)::float / count(*) * 100, 1) as pool_diversity_pct,
        CASE 
            WHEN count(distinct pool)::float / count(*) < 0.005 THEN 'SANDWICH'
            WHEN count(distinct pool)::float / count(*) < 0.05 THEN 'FOCUSED_ARB'
            WHEN count(*)::float / count(distinct block_number) > 3 THEN 'HIGH_FREQ_ARB'
            ELSE 'RETAIL/OTHER'
        END as likely_type
    FROM ethereum_swaps
    GROUP BY sender
    HAVING count(*) > 1000
    ORDER BY swaps DESC
    LIMIT 20
""").fetchall()

print(f"\n  {'Actor':<20} {'Swaps':>10} {'Pools':>7} {'S/Blk':>6} {'Pool%':>6} {'Type':<15}")
print(f"  {'-'*65}")
for row in r:
    addr = row[0][:18] + "..."
    print(f"  {addr:<20} {row[1]:>10,} {row[2]:>7,} {row[4]:>6} {row[5]:>5}% {row[6]:<15}")

print()
print("=" * 70)
print("ANALYSIS 7: CROSS-CHAIN POOL OVERLAP (same pair on multiple chains?)")
print("=" * 70)

# We don't have token symbols, but we can compare pool counts
for chain in ["ethereum", "blast"]:
    r = con.execute(f"""
        SELECT 
            pool,
            count(*) as swaps,
            protocol
        FROM {chain}_swaps
        GROUP BY pool, protocol
        ORDER BY swaps DESC
        LIMIT 10
    """).fetchall()
    
    print(f"\n  {chain.upper()} - Top 10 pools by swap count:")
    print(f"    {'Pool':<46} {'Swaps':>10} {'Protocol'}")
    print(f"    {'-'*70}")
    for row in r:
        print(f"    {row[0]:<46} {row[1]:>10,} {row[2]}")

print()
print("=" * 70)
print("ANALYSIS 8: SANDWICH PROFILING (ETH)")
print("Pattern: A swaps on pool, B swaps same pool same block, A swaps again")
print("=" * 70)

r = con.execute("""
    WITH block_pool_activity AS (
        SELECT 
            block_number, pool,
            count(*) as total_swaps,
            count(distinct sender) as unique_senders,
            min(tx_index) as first_tx,
            max(tx_index) as last_tx
        FROM ethereum_swaps
        GROUP BY block_number, pool
        HAVING count(*) >= 3 AND count(distinct sender) >= 2
    )
    SELECT 
        count(*) as sandwich_candidates,
        avg(total_swaps) as avg_swaps,
        avg(last_tx - first_tx) as avg_tx_spread,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY total_swaps) as median_swaps,
        sum(total_swaps) as total_swaps_involved
    FROM block_pool_activity
""").fetchone()

print(f"\n  ETH Sandwich candidates: {r[0]:,}")
print(f"  Avg swaps per sandwich: {r[1]:.1f}")
print(f"  Avg tx index spread: {r[2]:.1f}")
print(f"  Median swaps: {r[3]:.0f}")
print(f"  Total swaps in sandwiches: {r[4]:,}")

# Top sandwich attackers
print(f"\n  Top potential sandwich attackers (appear in 3+ swap blocks with others):")
r = con.execute("""
    WITH sandwich_blocks AS (
        SELECT block_number, pool
        FROM ethereum_swaps
        GROUP BY block_number, pool
        HAVING count(*) >= 3 AND count(distinct sender) >= 2
    ),
    attackers AS (
        SELECT s.sender, count(*) as appearances,
            count(distinct s.pool) as pools_targeted
        FROM ethereum_swaps s
        JOIN sandwich_blocks sb ON s.block_number = sb.block_number AND s.pool = sb.pool
        GROUP BY s.sender
        HAVING count(*) > 500
        ORDER BY appearances DESC
        LIMIT 10
    )
    SELECT * FROM attackers
""").fetchall()

print(f"    {'Actor':<46} {'Appearances':>12} {'Pools':>8}")
print(f"    {'-'*66}")
for row in r:
    print(f"    {row[0]:<46} {row[1]:>12,} {row[2]:>8,}")

print()
print("=" * 70)
print("ANALYSIS 9: BLAST OPPORTUNITY ASSESSMENT")
print("=" * 70)

r = con.execute("""
    SELECT 
        sender,
        count(*) as swaps,
        count(distinct pool) as pools,
        count(distinct block_number) as blocks,
        ROUND(count(*)::float / count(distinct block_number), 1) as swaps_per_block
    FROM blast_swaps
    GROUP BY sender
    ORDER BY swaps DESC
    LIMIT 5
""").fetchall()

total_blast = con.execute("SELECT count(*), count(distinct sender) FROM blast_swaps").fetchone()

print(f"\n  Total swaps: {total_blast[0]:,}, Unique actors: {total_blast[1]}")
print(f"  Top 2 actors control: {sum(r[0][1] for r in [r[:2]])}...")

# Competition metric: HHI (Herfindahl index)
hhi = con.execute("""
    SELECT sum(share * share) * 10000 as hhi FROM (
        SELECT count(*)::float / (SELECT count(*) FROM blast_swaps) as share
        FROM blast_swaps
        GROUP BY sender
    )
""").fetchone()
print(f"  HHI (market concentration): {hhi[0]:.0f} (>2500 = highly concentrated)")

print(f"\n  Top actors:")
print(f"    {'Actor':<46} {'Swaps':>8} {'Pools':>6} {'S/Blk':>6}")
for row in r:
    print(f"    {row[0]:<46} {row[1]:>8,} {row[2]:>6} {row[4]:>6}")

print()
print("=" * 70)
print("STRATEGY VERDICTS")
print("=" * 70)

# ETH: 70.7% multi-swap = extremely competitive
# Polygon: 87.6% multi-swap = even more competitive (but sender data was routers)
# Blast: 20% multi-swap, 2 actors control 60% = less competitive but dominated

print("""
  ETHEREUM:
    - 70.7% of swaps in multi-swap pool-blocks (very competitive)
    - 150K likely sandwich events detected
    - 6,312 unique actors, top 10 control ~50%
    - Verdict: HIGH competition, requires sophisticated execution
    
  POLYGON:
    - 87.6% multi-swap (highest concentration) 
    - Need re-ingestion with tx_from fix for proper bot analysis
    - V2 dominates (7M vs 900K V3) = different strategy profile
    - Verdict: NEEDS RE-INGEST before strategy conclusions
    
  BLAST:
    - Only 20% multi-swap (least competitive)
    - 57 unique actors, top 2 control 60%
    - HHI very high = oligopoly, but LOW total volume (48K swaps)
    - Verdict: LOW competition but LOW volume — niche opportunity
    
  NEXT STEPS:
    1. Re-ingest Polygon with tx_from fix (running Solana backfill now)
    2. Restart Base + Arbitrum after Solana completes
    3. Deep-dive ETH sandwiches: match attacker addresses to known bots
    4. Price-level arb analysis requires token pair resolution (Phase 1.1)
""")

