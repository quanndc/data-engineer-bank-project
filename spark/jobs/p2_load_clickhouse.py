"""P2 -- Load the core Parquet layer into ClickHouse.

ClickHouse reads the Parquet files itself (file() table function over the shared
/lake mount), so no data passes through Python.
"""
import time

from common import clickhouse, run_sql_file

DDL = "/opt/warehouse/clickhouse/ddl/dwh.sql"
TABLES = ["dim_date", "dim_customer", "dim_location", "dim_transaction_type",
          "fact_transactions", "fact_customer_balance_snapshot"]


def main():
    ch = clickhouse()
    run_sql_file(ch, DDL)

    for t in TABLES:
        cols = [r[0] for r in ch.query(
            "SELECT name FROM system.columns WHERE database = 'dwh' AND table = %(t)s "
            "ORDER BY position", parameters={"t": t}).result_rows]
        col_list = ", ".join(cols)
        start = time.time()
        ch.command(f"INSERT INTO dwh.{t} ({col_list}) "
                   f"SELECT {col_list} FROM file('lake/core/{t}/*.parquet', Parquet)")
        n = ch.query(f"SELECT count() FROM dwh.{t}").result_rows[0][0]
        print(f"dwh.{t}: {n:,} rows ({time.time() - start:.1f}s)")


if __name__ == "__main__":
    main()
