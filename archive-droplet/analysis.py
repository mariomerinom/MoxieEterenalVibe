import duckdb
con = duckdb.connect()

print("=== DATA INVENTORY ===")
for chain in ["ethereum", "polygon", "blast", "base", "arbitrum"]:
    try:
        r = con.execute(f"SELECT count(*) FROM read_parquet('/root/mev/data/events/swaps/{chain}/*.parquet', union_by_name=true)").fetchone()
        print(f"  {chain}: {r[0]:,} swaps")
    except Exception as e:
        print(f"  {chain}: no data ({e})")

print()
print("=== BLOCK INVENTORY ===")
for chain in ["ethereum", "polygon", "blast", "base", "arbitrum"]:
    try:
        r = con.execute(f"SELECT count(*), min(block_number), max(block_number) FROM read_parquet('/root/mev/data/blocks/{chain}/*.parquet', union_by_name=true)").fetchone()
        print(f"  {chain}: {r[0]:,} blocks [{r[1]:,} - {r[2]:,}]")
    except Exception as e:
        print(f"  {chain}: no data ({e})")

print()
print("=== SOLANA ===")
try:
    r = con.execute("SELECT count(*) FROM read_parquet('/root/mev/data/blocks/solana/*.parquet', union_by_name=true)").fetchone()
    print(f"  blocks: {r[0]:,}")
except Exception as e:
    print(f"  blocks: {e}")
try:
    r = con.execute("SELECT count(*) FROM read_parquet('/root/mev/data/events/swaps/solana/*.parquet', union_by_name=true)").fetchone()
    print(f"  swaps: {r[0]:,}")
except Exception as e:
    print(f"  swaps: {e}")
