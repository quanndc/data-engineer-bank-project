"""B1 / A1: create the One Big Table in ClickHouse (database bench), load it from the Parquet
written by spark_build_obt.py, and record time and size of both models.
Output: /lake/bench/results/load_clickhouse.csv"""
import csv, os, re, sys, time
sys.path.insert(0, "/opt/jobs")
from common import LAKE, WAREHOUSE, clickhouse

spark_s = float(sys.argv[1]) if len(sys.argv) > 1 else 0.0
ch = clickhouse()
sql = re.sub(r"--[^\n]*", "", open(f"{WAREHOUSE}/clickhouse/ddl/obt.sql").read())
for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
    ch.command(stmt)

times = {}
for t in ("obt_transactions", "obt_balance_snapshot"):
    cols = ", ".join(r[0] for r in ch.query(
        "SELECT name FROM system.columns WHERE database = 'bench' AND table = %(t)s "
        "ORDER BY position", parameters={"t": t}).result_rows)
    start = time.time()
    ch.command(f"INSERT INTO bench.{t} ({cols}) SELECT {cols} "
               f"FROM file('lake/bench/obt/{t}/*.parquet', Parquet)",
               # small blocks: ClickHouse already holds ~1.1 GB of its 2.2 GB budget at rest
               settings={"max_threads": 1, "max_insert_threads": 1,
                         "max_insert_block_size": 100_000,
                         "min_insert_block_size_rows": 100_000,
                         "min_insert_block_size_bytes": 64 * 2**20,
                         "input_format_parquet_max_block_size": 16_384})
    times[t] = time.time() - start

os.makedirs(f"{LAKE}/bench/results", exist_ok=True)
with open(f"{LAKE}/bench/results/load_clickhouse.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["model", "table", "rows", "load_s", "bytes_on_disk", "bytes_uncompressed"])
    for db, model in (("dwh", "star"), ("bench", "obt")):
        for t, n, disk, raw in ch.query(
                "SELECT table, sum(rows), sum(bytes_on_disk), sum(data_uncompressed_bytes) "
                "FROM system.parts WHERE database = %(db)s AND active "
                "AND table != 'fact_transaction_alert' GROUP BY table ORDER BY table",
                parameters={"db": db}).result_rows:
            w.writerow([model, t, n, round(times.get(t, 0), 2), disk, raw])
            print(f"{model:<4} {t:<32} {n:>10,} rows  disk {disk/2**20:7.1f} MiB  "
                  f"raw {raw/2**20:7.1f} MiB" + (f"  load {times[t]:.2f}s" if t in times else ""))
    w.writerow(["obt", "_spark_build_both", "", round(spark_s, 2), "", ""])
print(f"OBT rows built by Spark in {spark_s:.2f}s")
