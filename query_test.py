import duckdb
conn = duckdb.connect("data/mev.duckdb")

conn.execute("""
CREATE OR REPLACE VIEW blocks AS SELECT * FROM read_parquet('data/blocks/*/*.parquet', union_by_name=true);
CREATE OR REPLACE VIEW transactions AS SELECT * FROM read_parquet('data/transactions/*/*.parquet', union_by_name=true);
CREATE OR REPLACE VIEW swaps AS SELECT * FROM read_parquet('data/events/swaps/*/*.parquet', union_by_name=true);
CREATE OR REPLACE VIEW liquidations AS SELECT * FROM read_parquet('data/events/liquidations/*/*.parquet', union_by_name=true);
""")

print("=== BLOCKS ===")
r = conn.execute("SELECT count(*), min(block_number), max(block_number) FROM blocks").fetchone()
print(f"  count={r[0]}, range={r[1]}..{r[2]}")

print("=== TRANSACTIONS ===")
r = conn.execute("SELECT count(*) FROM transactions").fetchone()
print(f"  count={r[0]}")

print("=== SWAPS ===")
r = conn.execute("SELECT count(*), count(distinct pool) FROM swaps").fetchone()
print(f"  count={r[0]}, unique_pools={r[1]}")
for row in conn.execute("SELECT protocol, count(*) FROM swaps GROUP BY protocol ORDER BY 2 DESC").fetchall():
    print(f"    {row[0]}: {row[1]}")

print("=== LIQUIDATIONS ===")
r = conn.execute("SELECT count(*) FROM liquidations").fetchone()
print(f"  count={r[0]}")

print("=== SAMPLE SWAPS ===")
for row in conn.execute("SELECT block_number, protocol, pool, amount_in, amount_out FROM swaps LIMIT 3").fetchall():
    print(f"  blk={row[0]} proto={row[1]} pool={row[2][:18]}...")
