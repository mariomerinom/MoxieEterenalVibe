import duckdb
import sys

con = duckdb.connect()

chains = {}
for c in ["ethereum", "polygon", "blast", "base"]:
    try:
        r = con.execute(f"""
            SELECT count(*), count(distinct pool), min(block_number), max(block_number)
            FROM read_parquet('data/events/swaps/{c}/*.parquet', union_by_name=true)
        """).fetchone()
        if r and r[0] > 0:
            chains[c] = {"swaps": r[0], "pools": r[1], "min_blk": r[2], "max_blk": r[3]}
    except:
        pass

# Check Solana
try:
    r = con.execute("""
        SELECT count(*), count(distinct pool)
        FROM read_parquet('data/events/swaps/solana/*.parquet', union_by_name=true)
    """).fetchone()
    if r and r[0] > 0:
        chains["solana"] = {"swaps": r[0], "pools": r[1]}
except:
    pass

print("=" * 75)
print("MEV OPPORTUNITY SWEEP")
print("=" * 75)

for c, info in chains.items():
    print(f"\n  {c.upper()}: {info['swaps']:,} swaps, {info['pools']:,} pools")

# ─────────────────────────────────────────────────────────────
# 1. SANDWICH OPPORTUNITIES PER CHAIN
# ─────────────────────────────────────────────────────────────
print("\n" + "=" * 75)
print("1. SANDWICH OPPORTUNITY DENSITY")
print("   (blocks with 3+ swaps on same pool, 2+ distinct actors)")
print("=" * 75)

for c in ["ethereum", "polygon", "blast", "base"]:
    if c not in chains:
        continue
    try:
        # Check columns
        cols = [x[0] for x in con.execute(f"SELECT * FROM read_parquet('data/events/swaps/{c}/*.parquet', union_by_name=true) LIMIT 0").description]
        actor = "tx_from" if "tx_from" in cols else "sender"

        r = con.execute(f"""
            WITH candidates AS (
                SELECT block_number, pool, count(*) as n,
                    count(distinct {actor}) as actors
                FROM read_parquet('data/events/swaps/{c}/*.parquet', union_by_name=true)
                WHERE {actor} IS NOT NULL AND {actor} != ''
                GROUP BY block_number, pool
                HAVING count(*) >= 3 AND count(distinct {actor}) >= 2
            )
            SELECT
                count(*) as sandwich_blocks,
                sum(n) as swaps_involved,
                round(avg(n), 1) as avg_swaps,
                round(avg(actors), 1) as avg_actors
            FROM candidates
        """).fetchone()

        blocks = con.execute(f"""
            SELECT count(distinct block_number)
            FROM read_parquet('data/events/swaps/{c}/*.parquet', union_by_name=true)
        """).fetchone()[0]

        density = r[0] / max(blocks, 1) * 100
        print(f"\n  {c.upper()} (using {actor}):")
        print(f"    Sandwich candidates: {r[0]:,} ({density:.1f}% of blocks)")
        print(f"    Swaps involved:      {r[1]:,}")
        print(f"    Avg swaps/sandwich:  {r[2]}")
        print(f"    Avg actors/sandwich: {r[3]}")
    except Exception as e:
        print(f"  {c}: error - {e}")

# ─────────────────────────────────────────────────────────────
# 2. CROSS-DEX ARB OPPORTUNITIES
# ─────────────────────────────────────────────────────────────
print("\n" + "=" * 75)
print("2. CROSS-DEX ARB OPPORTUNITIES")
print("   (txs with 2+ swaps across 2+ pools)")
print("=" * 75)

for c in ["ethereum", "polygon", "blast", "base"]:
    if c not in chains:
        continue
    try:
        r = con.execute(f"""
            WITH arb_txs AS (
                SELECT tx_hash, count(*) as swaps, count(distinct pool) as pools
                FROM read_parquet('data/events/swaps/{c}/*.parquet', union_by_name=true)
                GROUP BY tx_hash
                HAVING count(*) >= 2 AND count(distinct pool) >= 2
            )
            SELECT
                count(*) as arb_txs,
                sum(CASE WHEN swaps >= 3 AND pools >= 3 THEN 1 ELSE 0 END) as complex_arbs,
                round(avg(swaps), 1) as avg_swaps,
                round(avg(pools), 1) as avg_pools
            FROM arb_txs
        """).fetchone()

        total_txs = con.execute(f"""
            SELECT count(distinct tx_hash)
            FROM read_parquet('data/events/swaps/{c}/*.parquet', union_by_name=true)
        """).fetchone()[0]

        pct = r[0] / max(total_txs, 1) * 100
        print(f"\n  {c.upper()}:")
        print(f"    Arb txs:      {r[0]:,} ({pct:.1f}% of swap txs)")
        print(f"    Complex arbs: {r[1]:,} (3+ pools)")
        print(f"    Avg swaps:    {r[2]}, Avg pools: {r[3]}")
    except Exception as e:
        print(f"  {c}: error - {e}")

# ─────────────────────────────────────────────────────────────
# 3. TOP POOLS — HIGHEST ACTIVITY (best targets)
# ─────────────────────────────────────────────────────────────
print("\n" + "=" * 75)
print("3. HIGHEST-ACTIVITY POOLS (best sandwich/arb targets)")
print("=" * 75)

for c in ["ethereum", "blast"]:
    if c not in chains:
        continue
    try:
        r = con.execute(f"""
            SELECT pool, protocol, count(*) as swaps,
                count(distinct block_number) as active_blocks,
                round(count(*) * 1.0 / count(distinct block_number), 1) as swaps_per_block
            FROM read_parquet('data/events/swaps/{c}/*.parquet', union_by_name=true)
            GROUP BY pool, protocol
            HAVING count(*) > 100
            ORDER BY swaps_per_block DESC
            LIMIT 10
        """).fetchall()

        print(f"\n  {c.upper()} — Top pools by swaps-per-block (MEV density):")
        print(f"    {'Pool':<46} {'Proto':<12} {'Swaps':>8} {'S/Blk':>6}")
        print(f"    {'-'*72}")
        for row in r:
            print(f"    {row[0]:<46} {row[1]:<12} {row[2]:>8,} {row[4]:>6}")
    except Exception as e:
        print(f"  {c}: error - {e}")

# ─────────────────────────────────────────────────────────────
# 4. COMPETITION GAPS — underserved chains
# ─────────────────────────────────────────────────────────────
print("\n" + "=" * 75)
print("4. COMPETITION GAPS (where are bots NOT competing?)")
print("=" * 75)

for c in ["ethereum", "polygon", "blast", "base"]:
    if c not in chains:
        continue
    try:
        cols = [x[0] for x in con.execute(f"SELECT * FROM read_parquet('data/events/swaps/{c}/*.parquet', union_by_name=true) LIMIT 0").description]
        actor = "tx_from" if "tx_from" in cols else "sender"

        r = con.execute(f"""
            SELECT
                count(distinct {actor}) as actors,
                count(*) as swaps,
                round(count(*) * 1.0 / count(distinct {actor}), 0) as swaps_per_actor
            FROM read_parquet('data/events/swaps/{c}/*.parquet', union_by_name=true)
            WHERE {actor} IS NOT NULL AND {actor} != ''
        """).fetchone()

        # HHI
        hhi = con.execute(f"""
            WITH shares AS (
                SELECT {actor}, count(*) * 1.0 / sum(count(*)) over () as s
                FROM read_parquet('data/events/swaps/{c}/*.parquet', union_by_name=true)
                WHERE {actor} IS NOT NULL AND {actor} != ''
                GROUP BY {actor}
            )
            SELECT round(sum(s * s) * 10000, 0) FROM shares
        """).fetchone()[0]

        # Single-swap txs (unprotected retail)
        retail = con.execute(f"""
            WITH tx_counts AS (
                SELECT tx_hash, count(*) as n
                FROM read_parquet('data/events/swaps/{c}/*.parquet', union_by_name=true)
                GROUP BY tx_hash
            )
            SELECT count(*) FROM tx_counts WHERE n = 1
        """).fetchone()[0]

        retail_pct = retail / max(r[1], 1) * 100
        level = "LOW" if hhi > 1500 else ("MODERATE" if hhi > 500 else "HIGH")

        print(f"\n  {c.upper()} ({actor}):")
        print(f"    Actors: {r[0]:,}   HHI: {hhi:.0f} ({level} competition)")
        print(f"    Single-swap txs (retail): {retail:,} ({retail_pct:.0f}%)")
        print(f"    Swaps/actor: {r[2]:,.0f}")
    except Exception as e:
        print(f"  {c}: error - {e}")

# ─────────────────────────────────────────────────────────────
# 5. SOLANA — JUPITER ROUTE ANALYSIS
# ─────────────────────────────────────────────────────────────
if "solana" in chains:
    print("\n" + "=" * 75)
    print("5. SOLANA SWAP LANDSCAPE")
    print("=" * 75)
    try:
        r = con.execute("""
            SELECT protocol, count(*) as swaps, count(distinct pool) as pools
            FROM read_parquet('data/events/swaps/solana/*.parquet', union_by_name=true)
            GROUP BY protocol ORDER BY swaps DESC
        """).fetchall()
        print(f"\n  Protocol breakdown:")
        for row in r:
            print(f"    {row[0]:<25} {row[1]:>10,} swaps  {row[2]:>6,} pools")

        jup_agg = con.execute("""
            SELECT count(*) FROM read_parquet('data/events/swaps/solana/*.parquet', union_by_name=true)
            WHERE pool = 'jupiter_aggregated'
        """).fetchone()[0]
        total = chains["solana"]["swaps"]
        print(f"\n  Jupiter aggregated (unresolved routes): {jup_agg:,} ({jup_agg*100//max(total,1)}%)")
        print(f"  Resolved to real pools: {total - jup_agg:,} ({(total-jup_agg)*100//max(total,1)}%)")

        # Top real pools
        top = con.execute("""
            SELECT pool, protocol, count(*) as swaps
            FROM read_parquet('data/events/swaps/solana/*.parquet', union_by_name=true)
            WHERE pool != 'jupiter_aggregated'
            GROUP BY pool, protocol ORDER BY swaps DESC LIMIT 10
        """).fetchall()
        print(f"\n  Top Solana pools (resolved):")
        for row in top:
            print(f"    {row[0][:44]:<46} {row[1]:<16} {row[2]:>8,}")
    except Exception as e:
        print(f"  solana: error - {e}")

# ─────────────────────────────────────────────────────────────
# 6. VERDICT MATRIX
# ─────────────────────────────────────────────────────────────
print("\n" + "=" * 75)
print("6. OPPORTUNITY VERDICT MATRIX")
print("=" * 75)

verdicts = []
for c in ["ethereum", "polygon", "blast", "base"]:
    if c not in chains:
        continue
    try:
        cols = [x[0] for x in con.execute(f"SELECT * FROM read_parquet('data/events/swaps/{c}/*.parquet', union_by_name=true) LIMIT 0").description]
        actor = "tx_from" if "tx_from" in cols else "sender"

        hhi = con.execute(f"""
            WITH shares AS (
                SELECT {actor}, count(*) * 1.0 / sum(count(*)) over () as s
                FROM read_parquet('data/events/swaps/{c}/*.parquet', union_by_name=true)
                WHERE {actor} IS NOT NULL AND {actor} != ''
                GROUP BY {actor}
            )
            SELECT sum(s * s) FROM shares
        """).fetchone()[0]

        sand = con.execute(f"""
            SELECT count(*) FROM (
                SELECT block_number, pool
                FROM read_parquet('data/events/swaps/{c}/*.parquet', union_by_name=true)
                WHERE {actor} IS NOT NULL AND {actor} != ''
                GROUP BY block_number, pool
                HAVING count(*) >= 3 AND count(distinct {actor}) >= 2
            )
        """).fetchone()[0]

        arbs = con.execute(f"""
            SELECT count(*) FROM (
                SELECT tx_hash
                FROM read_parquet('data/events/swaps/{c}/*.parquet', union_by_name=true)
                GROUP BY tx_hash
                HAVING count(*) >= 2 AND count(distinct pool) >= 2
            )
        """).fetchone()[0]

        volume = chains[c]["swaps"]
        comp = "HIGH" if hhi < 0.05 else ("MED" if hhi < 0.15 else "LOW")
        sand_signal = "RICH" if sand > 1000 else ("SOME" if sand > 100 else "THIN")
        arb_signal = "RICH" if arbs > 10000 else ("SOME" if arbs > 1000 else "THIN")

        # Opportunity score: volume * (1/competition) * signal density
        opp_score = (volume / 1000) * (1 - min(hhi, 1)) * (sand + arbs) / max(volume, 1) * 100

        verdicts.append((c, volume, comp, hhi, sand, sand_signal, arbs, arb_signal, opp_score))
    except Exception as e:
        print(f"  {c}: error - {e}")

print(f"\n  {'Chain':<12} {'Volume':>10} {'Comp':>6} {'HHI':>8} {'Sandwich':>10} {'Arb Txs':>10} {'Score':>8}")
print(f"  {'-'*66}")
for v in sorted(verdicts, key=lambda x: -x[8]):
    print(f"  {v[0]:<12} {v[1]:>10,} {v[2]:>6} {v[3]:>8.4f} {v[4]:>10,} {v[6]:>10,} {v[8]:>8.1f}")

print(f"""
  RECOMMENDATIONS:
""")

for v in sorted(verdicts, key=lambda x: -x[8]):
    c = v[0]
    if v[2] == "HIGH" and v[5] == "RICH":
        rec = "SANDWICH: High volume + rich targets, but fierce competition. Need speed edge."
    elif v[2] == "LOW" and v[5] in ("RICH", "SOME"):
        rec = "SANDWICH: Low competition + targets present. BEST ENTRY POINT."
    elif v[2] == "HIGH" and v[7] == "RICH":
        rec = "ARB: Rich arb landscape but crowded. Need latency advantage."
    elif v[2] == "MED" and v[7] in ("RICH", "SOME"):
        rec = "ARB: Moderate competition, good arb targets. INVESTIGATE."
    elif v[1] < 50000:
        rec = "VOLUME TOO LOW for reliable strategy. Monitor for growth."
    else:
        rec = "MIXED: Needs deeper token-pair analysis (Phase 1.1) for sizing."
    print(f"  {c.upper()}: {rec}")

