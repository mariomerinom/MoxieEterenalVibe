import duckdb
con = duckdb.connect()

# Create views for convenience
for chain in ["ethereum", "polygon", "blast"]:
    con.execute(f"""
        CREATE OR REPLACE VIEW {chain}_swaps AS 
        SELECT * FROM read_parquet('/root/mev/data/events/swaps/{chain}/*.parquet', union_by_name=true)
    """)

print("=" * 70)
print("ANALYSIS 1: CROSS-CHAIN ARB SIZING")
print("Same-block same-pool multi-swap occurrences (potential sandwich/arb)")
print("=" * 70)

for chain in ["ethereum", "polygon", "blast"]:
    try:
        # Pools with 2+ swaps in same block = potential arb/sandwich
        r = con.execute(f"""
            WITH pool_block AS (
                SELECT block_number, pool, count(*) as swap_count
                FROM {chain}_swaps
                GROUP BY block_number, pool
                HAVING count(*) >= 2
            )
            SELECT 
                count(*) as multi_swap_pool_blocks,
                sum(swap_count) as total_swaps_involved,
                avg(swap_count) as avg_swaps_per_occurrence,
                max(swap_count) as max_swaps_in_block
            FROM pool_block
        """).fetchone()
        
        total = con.execute(f"SELECT count(*) FROM {chain}_swaps").fetchone()[0]
        pct = r[1] / total * 100 if total > 0 else 0
        
        print(f"\n  {chain.upper()}:")
        print(f"    Multi-swap pool-blocks: {r[0]:,}")
        print(f"    Swaps involved: {r[1]:,} ({pct:.1f}% of all swaps)")
        print(f"    Avg swaps per occurrence: {r[2]:.1f}")
        print(f"    Max swaps on one pool in one block: {r[3]}")
    except Exception as e:
        print(f"  {chain}: error - {e}")

print()
print("=" * 70)
print("ANALYSIS 2: BOT COMPETITION (sender concentration)")
print("=" * 70)

for chain in ["ethereum", "polygon", "blast"]:
    try:
        # Check if tx_from exists
        cols = con.execute(f"SELECT column_name FROM (DESCRIBE SELECT * FROM {chain}_swaps)").fetchall()
        col_names = [c[0] for c in cols]
        actor_col = "tx_from" if "tx_from" in col_names else "sender"
        
        r = con.execute(f"""
            SELECT 
                {actor_col} as actor,
                count(*) as swap_count,
                count(distinct pool) as unique_pools,
                count(distinct block_number) as active_blocks
            FROM {chain}_swaps
            GROUP BY {actor_col}
            ORDER BY swap_count DESC
            LIMIT 10
        """).fetchall()
        
        total = con.execute(f"SELECT count(*) FROM {chain}_swaps").fetchone()[0]
        unique_actors = con.execute(f"SELECT count(distinct {actor_col}) FROM {chain}_swaps").fetchone()[0]
        
        print(f"\n  {chain.upper()} (using {actor_col}, {unique_actors:,} unique actors):")
        print(f"    {'Actor':<20} {'Swaps':>10} {'% Total':>8} {'Pools':>8} {'Blocks':>8}")
        print(f"    {'-'*54}")
        for row in r[:10]:
            addr = row[0][:18] + "..." if row[0] else "none"
            pct = row[1] / total * 100
            print(f"    {addr:<20} {row[1]:>10,} {pct:>7.1f}% {row[2]:>8,} {row[3]:>8,}")
    except Exception as e:
        print(f"  {chain}: error - {e}")

print()
print("=" * 70)
print("ANALYSIS 3: PROTOCOL DISTRIBUTION")
print("=" * 70)

for chain in ["ethereum", "polygon", "blast"]:
    try:
        r = con.execute(f"""
            SELECT protocol, count(*) as cnt, count(distinct pool) as pools
            FROM {chain}_swaps
            GROUP BY protocol
            ORDER BY cnt DESC
        """).fetchall()
        
        print(f"\n  {chain.upper()}:")
        for row in r:
            print(f"    {row[0]:<20} {row[1]:>10,} swaps  {row[2]:>6,} pools")
    except Exception as e:
        print(f"  {chain}: error - {e}")

print()
print("=" * 70)
print("ANALYSIS 4: SANDWICH DETECTION (3+ swaps same pool same block)")
print("=" * 70)

for chain in ["ethereum", "polygon", "blast"]:
    try:
        r = con.execute(f"""
            WITH sandwich_candidates AS (
                SELECT block_number, pool, count(*) as n,
                    count(distinct sender) as unique_senders
                FROM {chain}_swaps
                GROUP BY block_number, pool
                HAVING count(*) >= 3
            )
            SELECT 
                count(*) as candidate_count,
                sum(n) as total_swaps,
                avg(unique_senders) as avg_unique_senders,
                sum(CASE WHEN unique_senders >= 2 THEN 1 ELSE 0 END) as multi_sender_cases
            FROM sandwich_candidates
        """).fetchone()
        
        print(f"\n  {chain.upper()}:")
        print(f"    Candidate sandwich blocks: {r[0]:,}")
        print(f"    Total swaps in candidates: {r[1]:,}")
        print(f"    Avg unique senders per candidate: {r[2]:.1f}")
        print(f"    Multi-sender cases (likely sandwiches): {r[3]:,}")
    except Exception as e:
        print(f"  {chain}: error - {e}")

print()
print("=" * 70)
print("ANALYSIS 5: SWAP VOLUME OVER TIME (daily)")
print("=" * 70)

for chain in ["ethereum"]:
    try:
        # Use block numbers to approximate time ranges
        r = con.execute(f"""
            WITH block_ranges AS (
                SELECT 
                    (block_number / 7200) * 7200 as block_group,
                    count(*) as swaps,
                    count(distinct pool) as pools,
                    count(distinct sender) as senders
                FROM {chain}_swaps
                GROUP BY block_group
                ORDER BY block_group
            )
            SELECT * FROM block_ranges LIMIT 20
        """).fetchall()
        
        print(f"\n  {chain.upper()} (per ~7200 blocks / ~1 day):")
        print(f"    {'Block Range':<20} {'Swaps':>10} {'Pools':>8} {'Senders':>10}")
        print(f"    {'-'*48}")
        for row in r:
            print(f"    {row[0]:<20} {row[1]:>10,} {row[2]:>8,} {row[3]:>10,}")
    except Exception as e:
        print(f"  {chain}: error - {e}")

