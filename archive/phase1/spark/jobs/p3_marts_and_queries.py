"""P3 -- Build the data marts, then run the ten BR queries on ClickHouse.

Each BR result is saved as CSV under /lake/results/<solution>/ so that the same
query can later be compared row by row across solutions (star vs. One Big Table,
ClickHouse vs. PostgreSQL) -- correctness criterion "BR results identical".
"""
import glob
import os
import time

from common import LAKE, WAREHOUSE, clickhouse, ensure_databases, run_sql_file

QUERIES = f"{WAREHOUSE}/queries/clickhouse"
OUT = f"{LAKE}/results/clickhouse_star"


def main():
    ch = clickhouse()
    ensure_databases(ch)
    run_sql_file(ch, f"{WAREHOUSE}/clickhouse/ddl/streaming.sql")   # BR7 table (kept across runs)
    start = time.time()
    run_sql_file(ch, f"{WAREHOUSE}/clickhouse/ddl/marts.sql")
    print(f"mart.rfm built in {time.time() - start:.1f}s")

    os.makedirs(OUT, exist_ok=True)
    for path in sorted(glob.glob(f"{QUERIES}/br*.sql")):
        name = os.path.basename(path)[:-4]
        start = time.time()
        df = ch.query_df(open(path).read())
        ms = (time.time() - start) * 1000
        df.to_csv(f"{OUT}/{name}.csv", index=False)
        print(f"{name:<24} {len(df):>6,} rows  {ms:7.1f} ms")


if __name__ == "__main__":
    main()
