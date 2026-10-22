"""P2 -- Load the core Parquet layer into ClickHouse.

Local container: ClickHouse reads the Parquet files itself (file() table function over
the shared /lake mount), so no data passes through Python.
Remote service (Aiven, ...): each Parquet file is streamed over HTTP(S) as
INSERT ... FORMAT Parquet, since the server cannot see /lake.
"""
import glob
import time

from clickhouse_connect.driver.tools import insert_file

from common import CH_LOCAL, CORE, clickhouse, ensure_databases, run_sql_file

DDL = "/opt/warehouse/clickhouse/ddl/dwh.sql"
TABLES = ["dim_date", "dim_customer", "dim_location", "dim_transaction_type",
          "fact_transactions", "fact_customer_balance_snapshot"]


def main():
    ch = clickhouse()
    ensure_databases(ch)
    run_sql_file(ch, DDL)

    for t in TABLES:
        cols = [r[0] for r in ch.query(
            "SELECT name FROM system.columns WHERE database = 'dwh' AND table = %(t)s "
            "ORDER BY position", parameters={"t": t}).result_rows]
        col_list = ", ".join(cols)
        start = time.time()
        if CH_LOCAL:
            ch.command(f"INSERT INTO dwh.{t} ({col_list}) "
                       f"SELECT {col_list} FROM file('lake/core/{t}/*.parquet', Parquet)")
        else:
            for path in sorted(glob.glob(f"{CORE}/{t}/*.parquet")):
                insert_file(ch, f"dwh.{t}", path, fmt="Parquet", column_names=cols)
        n = ch.query(f"SELECT count() FROM dwh.{t}").result_rows[0][0]
        print(f"dwh.{t}: {n:,} rows ({time.time() - start:.1f}s)")


if __name__ == "__main__":
    main()
