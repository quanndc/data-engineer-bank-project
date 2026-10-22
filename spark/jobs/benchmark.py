"""P2-8 Performance: B1 (data model) and B2 (warehouse technology), each changing one factor.

  ch_star  Kimball star schema on ClickHouse          (proposed solution)
  ch_obt   One Big Table on ClickHouse                (B1: same technology, other model)
  pg_star  the same star schema on PostgreSQL         (B2: same model, other technology; decision Q-K)

    python3 benchmark.py load      # copy dw into bench (ClickHouse), build the OBT, copy into dwpg (PostgreSQL)
    python3 benchmark.py run       # 9 BR queries x 3 solutions, result check, storage, pipeline timing
    python3 benchmark.py concurrency   # 1/4/8 concurrent clients on a query mix
    python3 benchmark.py update    # cost of Type 1 corrections on each solution

Scale: BENCH_SCALE=10 (environment) runs everything on a x10 copy instead: databases bench10
(ClickHouse) and dwpg10 (PostgreSQL), results in /lake/bench/p2/x10. The x10 copy is built from
the x1 copy (bench) by repeating every account, client, and transaction 10 times under new keys
(dates, amounts, tiers, and SCD2 history unchanged), so x10 totals are exactly 10 x the x1 totals.
dw itself is never written.

All three solutions are loaded from the same CSV export of the warehouse, so they hold the same
rows. Update tests run on these copies only: dw and ops are never modified.
PostgreSQL is set up as in Phase 1: PK/FK, indexes on the fact's filter and join keys, ANALYZE;
shared_buffers 256 MB (server), work_mem 64 MB, up to 4 parallel workers, jit off (session).

Protocol (as Phase 1): each query runs 6 times; run 1 is "cold" (ClickHouse mark/uncompressed
caches dropped first; the OS page cache cannot be dropped from a container), runs 2-6 give the
"warm" median and p95. Times are client wall-clock including the transfer of the result.
Outputs: /lake/bench/p2/
"""
import csv
import datetime as dt
import io
import json
import os
import random
import re
import statistics
import sys
import threading
import time
from decimal import Decimal

import psycopg2

from common import LAKE, WAREHOUSE, clickhouse, postgres

SCALE = int(os.environ.get("BENCH_SCALE", "1"))
DB = "bench" if SCALE == 1 else f"bench{SCALE}"          # ClickHouse database of this scale
PGDB = "dwpg" if SCALE == 1 else f"dwpg{SCALE}"         # PostgreSQL database of this scale
OUT = f"{LAKE}/bench/p2" + ("" if SCALE == 1 else f"/x{SCALE}")
# key offsets of replica r (larger than every key of the x1 data)
OFFSET = {"account_sk": 10_000, "account_id": 100_000, "customer_sk": 10_000, "client_id": 100_000,
          "transaction_id": 100_000_000}
RUNS = 6
SOLUTIONS = ["ch_star", "ch_obt", "pg_star"]
TABLES = ["dim_date", "dim_district", "dim_account", "dim_customer", "dim_transaction_profile",
          "fact_transaction"]
PG_SESSION = ["SET work_mem = '64MB'", "SET max_parallel_workers_per_gather = 4", "SET jit = off",
              "SET statement_timeout = '300s'"]

PG_DDL = """
CREATE TABLE dim_date (date_key integer, full_date date NOT NULL, day_of_month smallint,
  day_of_week smallint, day_name text, month_number smallint, month_name text, quarter_number smallint,
  year_number smallint, year_month text, is_weekend smallint);
CREATE TABLE dim_district (district_sk smallint, district_id smallint NOT NULL,
  district_name text, region_name text, created_at timestamp, updated_at timestamp);
CREATE TABLE dim_account (account_sk integer, account_id integer NOT NULL,
  account_open_date date, statement_frequency_code text, statement_frequency_name text,
  account_district_sk smallint, created_at timestamp, updated_at timestamp);
CREATE TABLE dim_customer (customer_sk integer, client_id integer NOT NULL, gender text,
  birth_date date, customer_district_sk smallint, membership_tier text,
  valid_from date NOT NULL, valid_to date NOT NULL, is_current smallint NOT NULL,
  created_at timestamp, updated_at timestamp);
CREATE TABLE dim_transaction_profile (transaction_profile_sk smallint, type_code text,
  type_name text, operation_code text, operation_name text, purpose_code text, purpose_name text,
  cash_flow_direction text NOT NULL, created_at timestamp, updated_at timestamp);
CREATE TABLE fact_transaction (transaction_id bigint, date_key integer NOT NULL,
  account_sk integer NOT NULL, customer_sk integer NOT NULL, transaction_profile_sk smallint NOT NULL,
  amount numeric(18, 2) NOT NULL, balance_after numeric(18, 2), counterparty_bank_code text,
  counterparty_account_ref text, load_batch_id text, loaded_at timestamp);
"""
# keys, constraints and indexes after COPY (bulk-load practice, as in Phase 1)
PG_INDEXES = """
ALTER TABLE dim_date ADD PRIMARY KEY (date_key);
ALTER TABLE dim_district ADD PRIMARY KEY (district_sk);
ALTER TABLE dim_account ADD PRIMARY KEY (account_sk), ADD UNIQUE (account_id),
  ADD FOREIGN KEY (account_district_sk) REFERENCES dim_district;
ALTER TABLE dim_customer ADD PRIMARY KEY (customer_sk),
  ADD FOREIGN KEY (customer_district_sk) REFERENCES dim_district;
ALTER TABLE dim_transaction_profile ADD PRIMARY KEY (transaction_profile_sk);
ALTER TABLE fact_transaction ADD PRIMARY KEY (transaction_id),
  ADD FOREIGN KEY (date_key) REFERENCES dim_date, ADD FOREIGN KEY (account_sk) REFERENCES dim_account,
  ADD FOREIGN KEY (customer_sk) REFERENCES dim_customer,
  ADD FOREIGN KEY (transaction_profile_sk) REFERENCES dim_transaction_profile;
CREATE INDEX ON fact_transaction (date_key);
CREATE INDEX ON fact_transaction (account_sk, date_key);
CREATE INDEX ON fact_transaction (customer_sk);
CREATE INDEX ON dim_customer (client_id);
ANALYZE;
"""

# One Big Table: every dimension attribute copied into the fact row (both district roles)
OBT_DDL = """
CREATE TABLE bench.fact_obt (
  transaction_id UInt64, date_key UInt32, full_date Date, year_number UInt16, month_number UInt8,
  year_month LowCardinality(String), day_of_week UInt8, is_weekend UInt8,
  account_id UInt32, account_open_date Date, statement_frequency_name LowCardinality(String),
  branch_district_id UInt16, branch_district_name String, branch_region_name LowCardinality(String),
  client_id UInt32, gender LowCardinality(String), birth_date Date32, membership_tier LowCardinality(String),
  tier_valid_from Date32, tier_valid_to Date32,
  residence_district_id UInt16, residence_district_name String, residence_region_name LowCardinality(String),
  type_code LowCardinality(String), type_name LowCardinality(String),
  operation_code LowCardinality(String), operation_name LowCardinality(String),
  purpose_code LowCardinality(String), purpose_name LowCardinality(String),
  cash_flow_direction LowCardinality(String),
  amount Decimal(18, 2), balance_after Decimal(18, 2),
  counterparty_bank_code Nullable(String), counterparty_account_ref Nullable(String),
  load_batch_id LowCardinality(String), loaded_at DateTime64(3)
) ENGINE = MergeTree
PARTITION BY intDiv(date_key, 10000)
ORDER BY (account_id, date_key, transaction_id)"""
OBT_FILL = """
INSERT INTO bench.fact_obt
SELECT f.transaction_id, f.date_key, d.full_date, d.year_number, d.month_number, d.year_month,
       d.day_of_week, d.is_weekend,
       a.account_id, a.account_open_date, a.statement_frequency_name,
       bd.district_id, bd.district_name, bd.region_name,
       c.client_id, c.gender, c.birth_date, c.membership_tier, c.valid_from, c.valid_to,
       rd.district_id, rd.district_name, rd.region_name,
       p.type_code, p.type_name, p.operation_code, p.operation_name, p.purpose_code, p.purpose_name,
       p.cash_flow_direction,
       f.amount, f.balance_after, f.counterparty_bank_code, f.counterparty_account_ref,
       f.load_batch_id, f.loaded_at
FROM bench.fact_transaction AS f
JOIN bench.dim_date AS d USING (date_key)
JOIN (SELECT * FROM bench.dim_account FINAL) AS a USING (account_sk)
JOIN (SELECT * FROM bench.dim_district FINAL) AS bd ON bd.district_sk = a.account_district_sk
JOIN (SELECT * FROM bench.dim_customer FINAL) AS c USING (customer_sk)
JOIN (SELECT * FROM bench.dim_district FINAL) AS rd ON rd.district_sk = c.customer_district_sk
JOIN (SELECT * FROM bench.dim_transaction_profile FINAL) AS p USING (transaction_profile_sk)"""

# ---------------------------------------------------------------------------------- the queries
# Same question, same result on the three solutions; only model and dialect differ.
P = "(SELECT transaction_profile_sk, cash_flow_direction AS dir FROM bench.dim_transaction_profile FINAL)"
C = "(SELECT customer_sk, client_id, membership_tier FROM bench.dim_customer FINAL)"
A = "(SELECT account_sk, account_id, account_district_sk FROM bench.dim_account FINAL)"
PG_IN = "coalesce(sum(f.amount) FILTER (WHERE p.cash_flow_direction = 'IN'), 0)"   # sumIf gives 0, not NULL
PG_OUT = "coalesce(sum(f.amount) FILTER (WHERE p.cash_flow_direction = 'OUT'), 0)"
ACCOUNT = 2378        # BR02 sample account (437 transactions)
CLIENT = 24           # BR08 sample client (2 tier versions)

QUERIES = {
    "br01_kpi_1998": {
        "title": "BR01 KPI một kỳ (năm 1998)",
        "ch_star": f"""SELECT count(), sumIf(amount, dir = 'IN'), sumIf(amount, dir = 'OUT')
            FROM bench.fact_transaction AS f JOIN {P} AS p USING (transaction_profile_sk)
            WHERE f.date_key BETWEEN 19980101 AND 19981231""",
        "ch_obt": """SELECT count(), sumIf(amount, cash_flow_direction = 'IN'), sumIf(amount, cash_flow_direction = 'OUT')
            FROM bench.fact_obt WHERE date_key BETWEEN 19980101 AND 19981231""",
        "pg_star": f"""SELECT count(*), {PG_IN}, {PG_OUT}
            FROM fact_transaction f JOIN dim_transaction_profile p USING (transaction_profile_sk)
            WHERE f.date_key BETWEEN 19980101 AND 19981231"""},
    "br01_by_month": {
        "title": "BR01 biểu đồ theo tháng, toàn bộ lịch sử",
        "ch_star": f"""SELECT d.year_month, count(), sumIf(amount, dir = 'IN'), sumIf(amount, dir = 'OUT')
            FROM bench.fact_transaction AS f JOIN {P} AS p USING (transaction_profile_sk)
            JOIN bench.dim_date AS d USING (date_key) GROUP BY d.year_month ORDER BY d.year_month""",
        "ch_obt": """SELECT year_month, count(), sumIf(amount, cash_flow_direction = 'IN'),
                   sumIf(amount, cash_flow_direction = 'OUT')
            FROM bench.fact_obt GROUP BY year_month ORDER BY year_month""",
        "pg_star": f"""SELECT d.year_month, count(*), {PG_IN}, {PG_OUT}
            FROM fact_transaction f JOIN dim_transaction_profile p USING (transaction_profile_sk)
            JOIN dim_date d USING (date_key) GROUP BY d.year_month ORDER BY d.year_month"""},
    "br01_by_tier_1998": {
        "title": "BR01 theo hạng thành viên (1998), có đếm khách",
        "ch_star": f"""SELECT c.membership_tier, count(), sumIf(amount, dir = 'IN'), sumIf(amount, dir = 'OUT'),
                   uniqExact(c.client_id)
            FROM bench.fact_transaction AS f JOIN {P} AS p USING (transaction_profile_sk)
            JOIN {C} AS c USING (customer_sk)
            WHERE f.date_key BETWEEN 19980101 AND 19981231 GROUP BY 1 ORDER BY 1""",
        "ch_obt": """SELECT membership_tier, count(), sumIf(amount, cash_flow_direction = 'IN'),
                   sumIf(amount, cash_flow_direction = 'OUT'), uniqExact(client_id)
            FROM bench.fact_obt WHERE date_key BETWEEN 19980101 AND 19981231 GROUP BY 1 ORDER BY 1""",
        "pg_star": f"""SELECT c.membership_tier, count(*), {PG_IN}, {PG_OUT}, count(DISTINCT c.client_id)
            FROM fact_transaction f JOIN dim_transaction_profile p USING (transaction_profile_sk)
            JOIN dim_customer c USING (customer_sk)
            WHERE f.date_key BETWEEN 19980101 AND 19981231 GROUP BY 1 ORDER BY 1"""},
    "br01_by_region": {
        "title": "BR01 theo vùng của chi nhánh (1998), qua dim_district vai trò chi nhánh",
        "ch_star": f"""SELECT r.region_name, count(), sumIf(amount, dir = 'IN'), sumIf(amount, dir = 'OUT')
            FROM bench.fact_transaction AS f JOIN {P} AS p USING (transaction_profile_sk)
            JOIN {A} AS a USING (account_sk)
            JOIN (SELECT district_sk, region_name FROM bench.dim_district FINAL) AS r
                 ON r.district_sk = a.account_district_sk
            WHERE f.date_key BETWEEN 19980101 AND 19981231 GROUP BY 1 ORDER BY 1""",
        "ch_obt": """SELECT branch_region_name, count(), sumIf(amount, cash_flow_direction = 'IN'),
                   sumIf(amount, cash_flow_direction = 'OUT')
            FROM bench.fact_obt WHERE date_key BETWEEN 19980101 AND 19981231 GROUP BY 1 ORDER BY 1""",
        "pg_star": f"""SELECT r.region_name, count(*), {PG_IN}, {PG_OUT}
            FROM fact_transaction f JOIN dim_transaction_profile p USING (transaction_profile_sk)
            JOIN dim_account a USING (account_sk) JOIN dim_district r ON r.district_sk = a.account_district_sk
            WHERE f.date_key BETWEEN 19980101 AND 19981231 GROUP BY 1 ORDER BY 1"""},
    "br02_account_detail": {
        "title": f"BR02 tra cứu tài khoản {ACCOUNT}: toàn bộ giao dịch",
        "ch_star": f"""SELECT f.transaction_id, f.date_key, p.type_name, p.operation_name, p.purpose_name, f.amount,
                   f.balance_after
            FROM bench.fact_transaction AS f JOIN {A} AS a USING (account_sk)
            JOIN (SELECT * FROM bench.dim_transaction_profile FINAL) AS p USING (transaction_profile_sk)
            WHERE a.account_id = {{acc}} ORDER BY f.date_key, f.transaction_id""",
        "ch_obt": """SELECT transaction_id, date_key, type_name, operation_name, purpose_name, amount, balance_after
            FROM bench.fact_obt WHERE account_id = {acc} ORDER BY date_key, transaction_id""",
        "pg_star": """SELECT f.transaction_id, f.date_key, p.type_name, p.operation_name, p.purpose_name, f.amount,
                  f.balance_after
           FROM fact_transaction f JOIN dim_account a USING (account_sk)
           JOIN dim_transaction_profile p USING (transaction_profile_sk)
           WHERE a.account_id = {acc} ORDER BY f.date_key, f.transaction_id"""},
    "br02_account_totals": {
        "title": f"BR02 tổng của tài khoản {ACCOUNT}, năm 1998",
        "ch_star": f"""SELECT count(), sumIf(amount, dir = 'IN'), sumIf(amount, dir = 'OUT')
            FROM bench.fact_transaction AS f JOIN {A} AS a USING (account_sk)
            JOIN {P} AS p USING (transaction_profile_sk)
            WHERE a.account_id = {{acc}} AND f.date_key BETWEEN 19980101 AND 19981231""",
        "ch_obt": """SELECT count(), sumIf(amount, cash_flow_direction = 'IN'), sumIf(amount, cash_flow_direction = 'OUT')
            FROM bench.fact_obt WHERE account_id = {acc} AND date_key BETWEEN 19980101 AND 19981231""",
        "pg_star": f"""SELECT count(*), {PG_IN}, {PG_OUT}
            FROM fact_transaction f JOIN dim_account a USING (account_sk)
            JOIN dim_transaction_profile p USING (transaction_profile_sk)
            WHERE a.account_id = {{acc}} AND f.date_key BETWEEN 19980101 AND 19981231"""},
    "br03_r1_scan": {
        "title": "BR03 quy tắc R1 trên toàn bộ lịch sử (cửa sổ 6 tháng theo tài khoản)",
        "ch_star": f"""SELECT count(), sum(out_m) FROM (
              SELECT account_sk, month, out_m,
                     sum(out_m) OVER (PARTITION BY account_sk ORDER BY month
                                      RANGE BETWEEN 186 PRECEDING AND 1 PRECEDING) / 6 AS avg_out,
                     min(month) OVER (PARTITION BY account_sk) AS first_month
              FROM (SELECT account_sk, toStartOfMonth(toDate(toString(date_key))) AS month,
                           sumIf(amount, dir = 'OUT') AS out_m
                    FROM bench.fact_transaction AS f JOIN {P} AS p USING (transaction_profile_sk)
                    GROUP BY account_sk, month))
            WHERE out_m > 3 * avg_out AND out_m > 50000 AND month >= addMonths(first_month, 6)""",
        "ch_obt": """SELECT count(), sum(out_m) FROM (
              SELECT account_id, month, out_m,
                     sum(out_m) OVER (PARTITION BY account_id ORDER BY month
                                      RANGE BETWEEN 186 PRECEDING AND 1 PRECEDING) / 6 AS avg_out,
                     min(month) OVER (PARTITION BY account_id) AS first_month
              FROM (SELECT account_id, toStartOfMonth(full_date) AS month,
                           sumIf(amount, cash_flow_direction = 'OUT') AS out_m
                    FROM bench.fact_obt GROUP BY account_id, month))
            WHERE out_m > 3 * avg_out AND out_m > 50000 AND month >= addMonths(first_month, 6)""",
        "pg_star": f"""SELECT count(*), coalesce(sum(out_m), 0) FROM (
              SELECT account_sk, month, out_m,
                     coalesce(sum(out_m) OVER (PARTITION BY account_sk ORDER BY month
                              RANGE BETWEEN interval '186 days' PRECEDING AND interval '1 day' PRECEDING), 0)
                       / 6 AS avg_out,
                     min(month) OVER (PARTITION BY account_sk) AS first_month
              FROM (SELECT account_sk, date_trunc('month', to_date(date_key::text, 'YYYYMMDD'))::date AS month,
                           {PG_OUT} AS out_m
                    FROM fact_transaction f JOIN dim_transaction_profile p USING (transaction_profile_sk)
                    GROUP BY account_sk, month) m) w
            WHERE out_m > 3 * avg_out AND out_m > 50000 AND month >= first_month + interval '6 months'"""},
    "br08_tier_old_new": {
        "title": "BR08 theo hạng, kỳ cũ (1993-1998) và kỳ mới (01/1999)",
        "ch_star": f"""SELECT if(f.date_key < 19990101, 'old', 'new') AS period, c.membership_tier,
                   uniqExact(c.client_id), count(), sumIf(amount, dir = 'IN'), sumIf(amount, dir = 'OUT')
            FROM bench.fact_transaction AS f JOIN {P} AS p USING (transaction_profile_sk)
            JOIN {C} AS c USING (customer_sk)
            WHERE f.date_key BETWEEN 19930101 AND 19990131 GROUP BY 1, 2 ORDER BY 1, 2""",
        "ch_obt": """SELECT if(date_key < 19990101, 'old', 'new') AS period, membership_tier,
                   uniqExact(client_id), count(), sumIf(amount, cash_flow_direction = 'IN'),
                   sumIf(amount, cash_flow_direction = 'OUT')
            FROM bench.fact_obt WHERE date_key BETWEEN 19930101 AND 19990131 GROUP BY 1, 2 ORDER BY 1, 2""",
        "pg_star": f"""SELECT CASE WHEN f.date_key < 19990101 THEN 'old' ELSE 'new' END AS period, c.membership_tier,
                   count(DISTINCT c.client_id), count(*), {PG_IN}, {PG_OUT}
            FROM fact_transaction f JOIN dim_transaction_profile p USING (transaction_profile_sk)
            JOIN dim_customer c USING (customer_sk)
            WHERE f.date_key BETWEEN 19930101 AND 19990131 GROUP BY 1, 2 ORDER BY 1, 2"""},
    "br08_client_versions": {
        "title": f"BR08 phiên bản hạng của khách {CLIENT} và giao dịch theo phiên bản",
        "ch_star": f"""SELECT c.membership_tier, toString(c.valid_from), count(), sumIf(amount, dir = 'IN')
            FROM bench.fact_transaction AS f JOIN {P} AS p USING (transaction_profile_sk)
            JOIN (SELECT customer_sk, membership_tier, valid_from FROM bench.dim_customer FINAL
                  WHERE client_id = {CLIENT}) AS c USING (customer_sk)
            GROUP BY 1, 2 ORDER BY 2""",
        "ch_obt": f"""SELECT membership_tier, toString(tier_valid_from), count(), sumIf(amount, cash_flow_direction = 'IN')
            FROM bench.fact_obt WHERE client_id = {CLIENT} GROUP BY 1, 2 ORDER BY 2""",
        "pg_star": f"""SELECT c.membership_tier, c.valid_from::text, count(*), {PG_IN}
            FROM fact_transaction f JOIN dim_transaction_profile p USING (transaction_profile_sk)
            JOIN dim_customer c USING (customer_sk) WHERE c.client_id = {CLIENT}
            GROUP BY 1, 2 ORDER BY 2"""},
}


def sql_for(name, sol, acc=ACCOUNT):
    return QUERIES[name][sol].replace("{acc}", str(acc))


# ------------------------------------------------------------------------------------- engines
class BenchCH:
    """ClickHouse client whose SQL is written against database bench and runs against DB."""
    def __init__(self):
        self.ch = clickhouse()
        self.server_version = self.ch.server_version

    def _sql(self, sql):
        return sql.replace("bench.", f"{DB}.")

    def command(self, sql, **kw):
        return self.ch.command(self._sql(sql), **kw)

    def query(self, sql, **kw):
        return self.ch.query(self._sql(sql), **kw)

    def raw_query(self, sql, **kw):
        return self.ch.raw_query(self._sql(sql), **kw)

    def raw_insert(self, table, **kw):
        return self.ch.raw_insert(self._sql(table), **kw)


def dwpg():
    conn = psycopg2.connect(host=os.environ.get("POSTGRES_HOST", "postgres"), dbname=PGDB,
                            user=os.environ.get("POSTGRES_USER", "bank"),
                            password=os.environ.get("POSTGRES_PASSWORD", "bank"))
    conn.autocommit = True
    with conn.cursor() as cur:
        for s in PG_SESSION:
            cur.execute(s)
    return conn


class Engine:
    """One client per solution (per thread in the concurrency test)."""
    def __init__(self, sol):
        self.sol = sol
        if sol.startswith("ch"):
            self.ch = BenchCH()
        else:
            self.cur = dwpg().cursor()

    def cold(self):
        if self.sol.startswith("ch"):
            for stmt in ("SYSTEM DROP MARK CACHE", "SYSTEM DROP UNCOMPRESSED CACHE"):
                self.ch.command(stmt)

    def run(self, sql):
        if self.sol.startswith("ch"):
            return self.ch.query(sql, settings={"use_query_cache": 0}).result_rows
        self.cur.execute(sql)
        return self.cur.fetchall()


# --------------------------------------------------------------------------------------- load
def load():
    """Same CSV export of dw -> bench star (ClickHouse), OBT (ClickHouse), dwpg (PostgreSQL)."""
    if SCALE != 1:
        return load_scaled()
    ch = BenchCH()
    timings = {"ch_star": {}, "ch_obt": {}, "pg_star": {}}
    exported = {}
    for t in TABLES:
        final = "" if t in ("dim_date", "fact_transaction") else " FINAL"
        exported[t] = ch.raw_query(f"SELECT * FROM dw.{t}{final} FORMAT CSV",
                                   settings={"date_time_output_format": "simple"})
    print("exported dw:", {t: f"{len(b) / 2**20:.1f} MB" for t, b in exported.items()}, flush=True)

    # ClickHouse star: same DDL as dw (warehouse/clickhouse/ddl/dw.sql), database bench
    ch.command("DROP DATABASE IF EXISTS bench SYNC")
    ch.command("CREATE DATABASE bench")
    ddl = re.sub(r"--[^\n]*", "", open(f"{WAREHOUSE}/clickhouse/ddl/dw.sql").read()).replace("dw.", "bench.")
    for stmt in filter(None, (s.strip() for s in ddl.split(";"))):
        ch.command(stmt)
    for t in TABLES:
        start = time.perf_counter()
        ch.raw_insert(f"bench.{t}", insert_block=exported[t], fmt="CSV")
        timings["ch_star"][t] = round(time.perf_counter() - start, 2)
    timings["ch_star"]["total_s"] = round(sum(timings["ch_star"].values()), 2)

    # One Big Table: built by joining the star inside ClickHouse
    ch.command(OBT_DDL)
    start = time.perf_counter()
    ch.command(OBT_FILL, settings={"max_threads": 4})
    timings["ch_obt"]["build_from_star_s"] = round(time.perf_counter() - start, 2)
    ch.command("OPTIMIZE TABLE bench.fact_obt FINAL")
    ch.command("OPTIMIZE TABLE bench.fact_transaction FINAL")

    # PostgreSQL star
    ops = postgres()
    with ops.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = 'dwpg'")
        if cur.fetchone():
            cur.execute("DROP DATABASE dwpg")
        cur.execute("CREATE DATABASE dwpg")
    ops.close()
    pg = dwpg()
    with pg.cursor() as cur:
        cur.execute(PG_DDL)
        for t in TABLES:
            start = time.perf_counter()
            cur.copy_expert(f"COPY {t} FROM STDIN WITH (FORMAT csv, NULL '\\N')", io.BytesIO(exported[t]))
            timings["pg_star"][t] = round(time.perf_counter() - start, 2)
        start = time.perf_counter()
        cur.execute(PG_INDEXES)
        timings["pg_star"]["keys_indexes_analyze_s"] = round(time.perf_counter() - start, 2)
        cur.execute("VACUUM ANALYZE")
    timings["pg_star"]["total_s"] = round(sum(timings["pg_star"].values()), 2)
    counts = {
        "ch_star": ch.query("SELECT count() FROM bench.fact_transaction").result_rows[0][0],
        "ch_obt": ch.query("SELECT count() FROM bench.fact_obt").result_rows[0][0],
        "pg_star": Engine("pg_star").run("SELECT count(*) FROM fact_transaction")[0][0]}
    timings["fact_rows"] = counts
    print(json.dumps(timings, indent=1))
    json.dump(timings, open(f"{OUT}/load.json", "w"), indent=1)


def load_scaled():
    """x SCALE copy of bench: every account, client, and transaction repeated under new keys.
    Dimensions without business keys of their own (date, district, profile) are shared."""
    ch, base = BenchCH(), clickhouse()
    if not base.query("SELECT count() FROM system.tables WHERE database = 'bench' AND name = 'fact_obt'"
                      ).result_rows[0][0]:
        raise SystemExit("run the x1 load first (make bench-load)")
    timings, R, o = {"ch_star": {}, "ch_obt": {}, "pg_star": {}}, SCALE, OFFSET
    assert DB != "bench"                                      # never touch the x1 copy here
    for stmt in ("SYSTEM DROP MARK CACHE", "SYSTEM JEMALLOC PURGE"):
        base.command(stmt)
    base.command(f"DROP DATABASE IF EXISTS {DB} SYNC")
    base.command(f"CREATE DATABASE {DB}")
    ddl = re.sub(r"--[^\n]*", "", open(f"{WAREHOUSE}/clickhouse/ddl/dw.sql").read()).replace("dw.", "bench.")
    for stmt in filter(None, (x.strip() for x in ddl.split(";"))):
        ch.command(stmt)
    # sources are the x1 copy (bench); targets are DB
    fill = {
        "dim_date": "SELECT * FROM bench_x1.dim_date",
        "dim_district": "SELECT * FROM bench_x1.dim_district FINAL",
        "dim_transaction_profile": "SELECT * FROM bench_x1.dim_transaction_profile FINAL",
        "dim_account": f"""SELECT account_sk + r * {o['account_sk']}, account_id + r * {o['account_id']},
                account_open_date, statement_frequency_code, statement_frequency_name, account_district_sk,
                created_at, updated_at
            FROM bench_x1.dim_account FINAL ARRAY JOIN range({R}) AS r""",
        "dim_customer": f"""SELECT customer_sk + r * {o['customer_sk']}, client_id + r * {o['client_id']}, gender,
                birth_date, customer_district_sk, membership_tier, valid_from, valid_to, is_current,
                created_at, updated_at
            FROM bench_x1.dim_customer FINAL ARRAY JOIN range({R}) AS r""",
        # one replica per INSERT: 10 x 1 M rows at once exceeded the 2.2 GiB of ClickHouse (P2-8 x10)
        "fact_transaction": [f"""SELECT transaction_id + {r * o['transaction_id']}, date_key,
                account_sk + {r * o['account_sk']}, customer_sk + {r * o['customer_sk']}, transaction_profile_sk,
                amount, balance_after, counterparty_bank_code, counterparty_account_ref, load_batch_id, loaded_at
            FROM bench_x1.fact_transaction""" for r in range(R)]}
    small = {"max_threads": 2, "max_insert_threads": 1, "max_insert_block_size": 262144}
    for t in TABLES:
        start = time.perf_counter()
        for q in (fill[t] if isinstance(fill[t], list) else [fill[t]]):
            base.command(f"INSERT INTO {DB}.{t} " + q.replace("bench_x1.", "bench."), settings=small)
        timings["ch_star"][t] = round(time.perf_counter() - start, 2)
    timings["ch_star"]["total_s"] = round(sum(timings["ch_star"].values()), 2)
    ch.command("OPTIMIZE TABLE bench.fact_transaction FINAL")
    base.command("SYSTEM DROP MARK CACHE")
    ch.command(OBT_DDL)
    start = time.perf_counter()
    for r in range(R):           # one replica per INSERT, for the same memory reason as the fact
        base.command("SYSTEM JEMALLOC PURGE")
        ch.command(OBT_FILL + f" WHERE intDiv(f.transaction_id, {o['transaction_id']}) = {r}",
                   settings={**small, "max_threads": 2})
    timings["ch_obt"]["build_from_star_s"] = round(time.perf_counter() - start, 2)
    ch.command("OPTIMIZE TABLE bench.fact_obt FINAL")

    # exactly SCALE x the x1 copy: rows, money, distinct accounts and clients
    figures = """SELECT count(), sum(amount), uniqExact(account_sk), uniqExact(customer_sk)
                 FROM {db}.fact_transaction"""
    x1 = base.query(figures.format(db="bench")).result_rows[0]
    xs = base.query(figures.format(db=DB)).result_rows[0]
    timings["check_scaled_equals_x1_times_scale"] = all(b == a * R for a, b in zip(x1, xs))
    timings["x1_figures"], timings["scaled_figures"] = [str(v) for v in x1], [str(v) for v in xs]

    # PostgreSQL: dimensions in one piece, the fact one replica at a time (keeps memory low)
    ops = postgres()
    with ops.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (PGDB,))
        if cur.fetchone():
            cur.execute(f"DROP DATABASE {PGDB}")
        cur.execute(f"CREATE DATABASE {PGDB}")
    ops.close()
    pg = dwpg()
    with pg.cursor() as cur:
        cur.execute(PG_DDL)
        for t in TABLES:
            parts = ([f"SELECT * FROM bench.{t} WHERE intDiv(transaction_id, {o['transaction_id']}) = {r}"
                      for r in range(R)] if t == "fact_transaction" else
                     [f"SELECT * FROM bench.{t}{'' if t == 'dim_date' else ' FINAL'}"])
            start = time.perf_counter()
            for q in parts:
                data = ch.raw_query(q + " FORMAT CSV", settings={"date_time_output_format": "simple"})
                cur.copy_expert(f"COPY {t} FROM STDIN WITH (FORMAT csv, NULL '\\N')", io.BytesIO(data))
            timings["pg_star"][t] = round(time.perf_counter() - start, 2)
        start = time.perf_counter()
        cur.execute(PG_INDEXES)
        timings["pg_star"]["keys_indexes_analyze_s"] = round(time.perf_counter() - start, 2)
        cur.execute("VACUUM ANALYZE")
    timings["pg_star"]["total_s"] = round(sum(timings["pg_star"].values()), 2)
    timings["fact_rows"] = {
        "ch_star": ch.query("SELECT count() FROM bench.fact_transaction").result_rows[0][0],
        "ch_obt": ch.query("SELECT count() FROM bench.fact_obt").result_rows[0][0],
        "pg_star": Engine("pg_star").run("SELECT count(*) FROM fact_transaction")[0][0]}
    print(json.dumps(timings, indent=1))
    json.dump(timings, open(f"{OUT}/load.json", "w"), indent=1)
    if not timings["check_scaled_equals_x1_times_scale"]:
        raise SystemExit(f"x{R} copy is not exactly {R} x the x1 copy")


# ---------------------------------------------------------------------------------------- run
def p95(values):
    s = sorted(values)
    return s[min(len(s) - 1, round(0.95 * (len(s) - 1)))]


def normalize(v):
    if isinstance(v, (Decimal, float, int)) and not isinstance(v, bool):
        return round(float(v), 2)
    if isinstance(v, (dt.date, dt.datetime)):
        return v.isoformat()[:10]
    return str(v)


def write_csv(name, header, rows):
    with open(f"{OUT}/{name}", "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


def storage(ch):
    out = {}
    for t, rows, comp, raw in ch.query("""
            SELECT table, sum(rows), sum(data_compressed_bytes), sum(data_uncompressed_bytes)
            FROM system.parts WHERE database = %(db)s AND active GROUP BY table""",
            parameters={"db": DB}).result_rows:
        sol = "ch_obt" if t == "fact_obt" else "ch_star"
        out.setdefault(sol, {})[t] = {"rows": rows, "mb": round(comp / 2**20, 2),
                                      "uncompressed_mb": round(raw / 2**20, 2)}
    cur = dwpg().cursor()
    for t in TABLES:
        cur.execute("SELECT count(*), pg_table_size(%s), pg_indexes_size(%s) FROM " + t, (t, t))
        n, tbl, idx = cur.fetchone()
        out.setdefault("pg_star", {})[t] = {"rows": n, "mb": round((tbl + idx) / 2**20, 2),
                                            "table_mb": round(tbl / 2**20, 2), "indexes_mb": round(idx / 2**20, 2)}
    for sol in out:
        out[sol]["total_mb"] = round(sum(v["mb"] for v in out[sol].values()), 1)
    return out


def pipeline_timing(cur):
    """Pipeline time per historical batch and per step, from ops.pipeline_step (BR06 data)."""
    cur.execute("""
        WITH last AS (SELECT DISTINCT ON (batch_id) * FROM ops.pipeline_run
                      WHERE status LIKE 'SUCCEEDED%' ORDER BY batch_id, run_id DESC)
        SELECT CASE WHEN l.batch_id LIKE 'HIST_%' THEN 'HIST' ELSE l.batch_id END, s.step_name,
               s.step_order, extract(epoch FROM s.finished_at - s.started_at), l.rows_received, l.run_kind
        FROM last l JOIN ops.pipeline_step s USING (run_id)""")
    steps = cur.fetchall()
    cur.execute("""SELECT extract(epoch FROM finished_at - started_at), rows_received FROM ops.pipeline_run
                   WHERE batch_id LIKE 'HIST_%' AND run_kind = 'RUN' AND status LIKE 'SUCCEEDED%'""")
    first = cur.fetchall()
    secs = [float(r[0]) for r in first]
    write_csv("pipeline_steps.csv", ["batch", "step", "step_order", "seconds", "rows_received", "run_kind"],
              [[b, s, o, round(float(sec), 3), n, k] for b, s, o, sec, n, k in steps])
    return {"historical_batches_first_run": len(secs), "seconds_median": round(statistics.median(secs), 1),
            "seconds_p95": round(p95(secs), 1), "seconds_total": round(sum(secs), 1),
            "rows_per_second_median": round(statistics.median([r[1] / float(r[0]) for r in first if r[0]]))}


def run():
    engines = {sol: Engine(sol) for sol in SOLUTIONS}
    runs, summary, checks = [], [], []
    for name, q in QUERIES.items():
        results = {}
        for sol in SOLUTIONS:
            e, sql = engines[sol], sql_for(name, sol)
            e.cold()
            times = []
            for i in range(RUNS):
                start = time.perf_counter()
                rows = e.run(sql)
                ms = (time.perf_counter() - start) * 1000
                times.append(ms)
                runs.append([name, sol, i + 1, round(ms, 2)])
            results[sol] = [tuple(normalize(v) for v in r) for r in rows]
            warm = times[1:]
            summary.append([name, q["title"], sol, len(rows), round(times[0], 1),
                            round(statistics.median(warm), 1), round(p95(warm), 1)])
            print(f"{name:<22} {sol:<8} {len(rows):>5} rows  cold {times[0]:8.1f}  "
                  f"median {statistics.median(warm):8.1f}  p95 {p95(warm):8.1f} ms", flush=True)
        for sol in ("ch_obt", "pg_star"):
            ok = results[sol] == results["ch_star"]
            checks.append([name, f"ch_star = {sol}", "PASS" if ok else "FAIL", len(results["ch_star"])])
            if not ok:
                print(f"  [FAIL] {name}: ch_star != {sol}", flush=True)
    write_csv("query_runs.csv", ["query", "solution", "run", "ms"], runs)
    write_csv("query_summary.csv", ["query", "title", "solution", "rows", "cold_ms", "warm_median_ms",
                                    "warm_p95_ms"], summary)
    write_csv("result_check.csv", ["query", "comparison", "status", "rows"], checks)

    ch = engines["ch_star"].ch
    pg = dwpg()
    env = {"measured_at": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
           "clickhouse_version": ch.server_version, "postgres_version": pg.server_version,
           "scale": SCALE, "storage": storage(ch), "postgres_settings": {},
           # the pipeline itself only ran at x1
           "pipeline": pipeline_timing(postgres().cursor()) if SCALE == 1 else None}
    with pg.cursor() as cur:
        for s in ("shared_buffers", "work_mem", "max_parallel_workers_per_gather", "jit", "effective_cache_size"):
            cur.execute(f"SHOW {s}")
            env["postgres_settings"][s] = cur.fetchone()[0]
    env["clickhouse_settings"] = {r[0]: r[1] for r in ch.query(
        "SELECT name, value FROM system.settings WHERE name IN ('max_threads', 'max_memory_usage')").result_rows}
    json.dump(env, open(f"{OUT}/env.json", "w"), indent=1, default=str)
    print(f"result checks: {sum(c[2] == 'PASS' for c in checks)}/{len(checks)} PASS")
    print("storage MB:", {s: v["total_mb"] for s, v in env["storage"].items()})


# ------------------------------------------------------------------------------- concurrency
MIX = ["br01_by_tier_1998", "br02_account_detail", "br08_tier_old_new"]   # one per user group


def concurrency(clients=(1, 4, 8), seconds=15):
    """N clients run the mix in a loop for a fixed time; BR02 picks a random account each time."""
    accounts = [r[0] for r in BenchCH().query("SELECT account_id FROM bench.dim_account FINAL").result_rows]
    rows = []
    for sol in SOLUTIONS:
        for n in clients:
            lat, stop = [], time.perf_counter() + seconds
            lock = threading.Lock()

            def worker(seed):
                e, rnd, i = Engine(sol), random.Random(seed), seed
                while time.perf_counter() < stop:
                    name = MIX[i % len(MIX)]
                    i += 1
                    start = time.perf_counter()
                    e.run(sql_for(name, sol, rnd.choice(accounts)))
                    with lock:
                        lat.append((time.perf_counter() - start) * 1000)
            threads = [threading.Thread(target=worker, args=(k,)) for k in range(n)]
            t0 = time.perf_counter()
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            elapsed = time.perf_counter() - t0
            rows.append([sol, n, len(lat), round(len(lat) / elapsed, 2), round(statistics.median(lat), 1),
                         round(p95(lat), 1)])
            print(f"{sol:<8} {n} clients: {len(lat):>5} queries  {len(lat) / elapsed:7.2f} q/s  "
                  f"median {statistics.median(lat):7.1f}  p95 {p95(lat):7.1f} ms", flush=True)
    write_csv("concurrency.csv", ["solution", "clients", "queries", "queries_per_s", "median_ms", "p95_ms"], rows)


# ------------------------------------------------------------------------------------ updates
def update():
    """Type 1 corrections: what each model must rewrite, and how long it takes (bench copies only)."""
    ch, pg = BenchCH(), dwpg().cursor()
    out = []

    def timed(fn):
        start = time.perf_counter()
        fn()
        return round((time.perf_counter() - start) * 1000, 1)

    def obt_mutation(set_sql, where):
        return lambda: ch.command(f"ALTER TABLE bench.fact_obt UPDATE {set_sql} WHERE {where}",
                                  settings={"mutations_sync": 2})

    # U1: correct the name of district 1 (both roles)
    did, = ch.query("SELECT district_sk FROM bench.dim_district FINAL WHERE district_id = 1").result_rows[0]
    old = ch.query(f"SELECT district_name FROM bench.dim_district FINAL WHERE district_sk = {did}").result_rows[0][0]
    for label, name in (("rename", old + " (corrected)"), ("rename back", old)):
        n_obt = ch.query("SELECT count() FROM bench.fact_obt WHERE branch_district_id = 1 OR residence_district_id = 1"
                         ).result_rows[0][0]
        ms_star = timed(lambda: ch.command(f"""INSERT INTO bench.dim_district
            SELECT district_sk, district_id, '{name}', region_name, created_at, now()
            FROM bench.dim_district FINAL WHERE district_sk = {did}"""))
        ms_obt = timed(obt_mutation(
            f"branch_district_name = if(branch_district_id = 1, '{name}', branch_district_name), "
            f"residence_district_name = if(residence_district_id = 1, '{name}', residence_district_name)",
            "branch_district_id = 1 OR residence_district_id = 1"))
        ms_pg = timed(lambda: pg.execute("UPDATE dim_district SET district_name = %s WHERE district_sk = %s",
                                         (name, did)))
        out += [["U1 sửa tên khu vực 1 (" + label + ")", "ch_star", 1, ms_star],
                ["U1 sửa tên khu vực 1 (" + label + ")", "ch_obt", n_obt, ms_obt],
                ["U1 sửa tên khu vực 1 (" + label + ")", "pg_star", 1, ms_pg]]

    # U2: correct the birth date of one client (all tier versions; Type 1 attribute)
    vers = ch.query(f"SELECT count() FROM bench.dim_customer FINAL WHERE client_id = {CLIENT}").result_rows[0][0]
    n_obt = ch.query(f"SELECT count() FROM bench.fact_obt WHERE client_id = {CLIENT}").result_rows[0][0]
    for label, shift in (("correct", 1), ("revert", -1)):
        ms_star = timed(lambda: ch.command(f"""INSERT INTO bench.dim_customer
            SELECT customer_sk, client_id, gender, addDays(birth_date, {shift}), customer_district_sk,
                   membership_tier, valid_from, valid_to, is_current, created_at, now()
            FROM bench.dim_customer FINAL WHERE client_id = {CLIENT}"""))
        ms_obt = timed(obt_mutation(f"birth_date = addDays(birth_date, {shift})", f"client_id = {CLIENT}"))
        ms_pg = timed(lambda: pg.execute(
            f"UPDATE dim_customer SET birth_date = birth_date + {shift} WHERE client_id = %s", (CLIENT,)))
        out += [[f"U2 sửa ngày sinh khách {CLIENT} ({label})", "ch_star", vers, ms_star],
                [f"U2 sửa ngày sinh khách {CLIENT} ({label})", "ch_obt", n_obt, ms_obt],
                [f"U2 sửa ngày sinh khách {CLIENT} ({label})", "pg_star", vers, ms_pg]]

    # after rename + rename back the three solutions must still agree
    same = all(
        [tuple(normalize(v) for v in r) for r in Engine(s).run(sql_for("br01_by_region", s))] ==
        [tuple(normalize(v) for v in r) for r in Engine("ch_star").run(sql_for("br01_by_region", "ch_star"))]
        for s in ("ch_obt", "pg_star"))
    for r in out:
        print(f"{r[0]:<40} {r[1]:<8} rows rewritten {r[2]:>8,}  {r[3]:>9.1f} ms")
    print("results still equal after updates:", same)
    write_csv("update_cost.csv", ["update", "solution", "rows_written", "ms"], out)


if __name__ == "__main__":
    os.makedirs(OUT, exist_ok=True)
    {"load": load, "run": run, "concurrency": concurrency, "update": update}[sys.argv[1]]()
