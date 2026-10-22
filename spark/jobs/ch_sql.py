"""Run one SQL statement on the configured ClickHouse (local or cloud) and print the result.

    python3 /opt/jobs/ch_sql.py "SELECT count() FROM dwh.fact_transactions"
"""
import sys

from common import CH_HOST, clickhouse

sql = sys.argv[1]
ch = clickhouse()
if sql.lstrip().upper().startswith(("SELECT", "SHOW", "WITH")):
    for row in ch.query(sql).result_rows:
        print("\t".join(str(v) for v in row))
else:
    ch.command(sql)
    print(f"ok ({CH_HOST})")
