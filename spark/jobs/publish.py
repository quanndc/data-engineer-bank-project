"""Publish a copy of the warehouse (the 6 dw tables) to Aiven ClickHouse, for the read-only viewer
the members open on Streamlit Community Cloud. The local warehouse stays the system of record:
this copy is a snapshot, refreshed by running `make publish` again.

    python3 publish.py          # needs AIVEN_CLICKHOUSE_* in the environment (from .env)

Each run creates missing tables with warehouse/clickhouse/ddl/dw.sql, empties them, copies the
rows (dimensions with FINAL, the fact one year at a time), and checks rows and money on both sides.
Nothing is written to the local warehouse. Credentials are never printed.
"""
import os
import sys
import time

import clickhouse_connect

from common import WAREHOUSE, clickhouse, run_sql_file

TABLES = ["dim_date", "dim_district", "dim_account", "dim_customer", "dim_transaction_profile",
          "fact_transaction"]
CHECK = """SELECT (SELECT count() FROM dw.fact_transaction), (SELECT sum(amount) FROM dw.fact_transaction),
                  (SELECT count() FROM dw.dim_customer FINAL), (SELECT count() FROM dw.dim_account FINAL)"""


def target():
    env = os.environ
    if not env.get("AIVEN_CLICKHOUSE_HOST"):
        raise SystemExit("AIVEN_CLICKHOUSE_HOST is not set: add the AIVEN_CLICKHOUSE_* lines to .env")
    return clickhouse_connect.get_client(
        host=env["AIVEN_CLICKHOUSE_HOST"], port=int(env.get("AIVEN_CLICKHOUSE_PORT", "8443")),
        username=env.get("AIVEN_CLICKHOUSE_USER", "avnadmin"), password=env.get("AIVEN_CLICKHOUSE_PASSWORD", ""),
        secure=env.get("AIVEN_CLICKHOUSE_SECURE", "true").lower() == "true",
        connect_timeout=30, send_receive_timeout=600)


def main():
    src, dst = clickhouse(), target()
    print(f"target: ClickHouse {dst.server_version} (Aiven)")
    engine = os.environ.get("AIVEN_CLICKHOUSE_DATABASE_ENGINE", "")
    dst.command("CREATE DATABASE IF NOT EXISTS dw" + (f" ENGINE = {engine}" if engine else ""))
    run_sql_file(dst, f"{WAREHOUSE}/clickhouse/ddl/dw.sql")
    total = time.perf_counter()
    for t in TABLES:
        start, rows = time.perf_counter(), 0
        dst.command(f"TRUNCATE TABLE dw.{t}")
        if t == "fact_transaction":
            parts = [f"SELECT * FROM dw.{t} WHERE intDiv(date_key, 10000) = {y}"
                     for (y,) in src.query("SELECT DISTINCT intDiv(date_key, 10000) FROM dw.fact_transaction "
                                           "ORDER BY 1").result_rows]
        else:
            parts = [f"SELECT * FROM dw.{t}{'' if t == 'dim_date' else ' FINAL'}"]
        for q in parts:
            block = src.raw_query(q + " FORMAT Native", settings={"max_threads": 2})
            dst.raw_insert(f"dw.{t}", insert_block=block, fmt="Native")
        rows = dst.query(f"SELECT count() FROM dw.{t}").result_rows[0][0]
        print(f"  dw.{t:<26} {rows:>10,} rows  {time.perf_counter() - start:6.1f} s", flush=True)
    a, b = src.query(CHECK).result_rows[0], dst.query(CHECK).result_rows[0]
    ok = tuple(a) == tuple(b)
    print(f"check (fact rows, money, customer versions, accounts): local {tuple(a)} | Aiven {tuple(b)} -> "
          f"{'EQUAL' if ok else 'DIFFERENT'}")
    print(f"published in {time.perf_counter() - total:.1f} s")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
