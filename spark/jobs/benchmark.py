"""P2-8 Performance: the proposed warehouse (Kimball star on ClickHouse) against the alternative
of decision Q-K (the same star schema on PostgreSQL, a row store), plus pipeline timing.

    python3 benchmark.py load      # copy the 6 dw tables from ClickHouse into PostgreSQL database dwpg
    python3 benchmark.py run       # BR01/BR02/BR08 queries on both, compare results, pipeline timing

The PostgreSQL copy has the same tables, columns and rows; it gets what a row store needs to be
fair: primary keys, foreign keys, indexes on the fact's join/filter keys, ANALYZE. It lives in its
own database (dwpg) on the operational PostgreSQL server, so ops is not touched.

Protocol (as Phase 1): each query runs 6 times; run 1 is "cold" (ClickHouse mark/uncompressed
caches dropped first; the OS page cache cannot be dropped from a container), runs 2-6 give the
"warm" median and p95. Times are client wall-clock including the transfer of the result.
Outputs: /lake/bench/p2/*.csv and env.json
"""
import csv
import datetime as dt
import io
import json
import os
import statistics
import sys
import time
from decimal import Decimal

import psycopg2

from common import LAKE, clickhouse, postgres

OUT = f"{LAKE}/bench/p2"
RUNS = 6
TABLES = ["dim_date", "dim_district", "dim_account", "dim_customer", "dim_transaction_profile",
          "fact_transaction"]

PG_DDL = """
CREATE TABLE dim_date (date_key integer PRIMARY KEY, full_date date NOT NULL, day_of_month smallint,
  day_of_week smallint, day_name text, month_number smallint, month_name text, quarter_number smallint,
  year_number smallint, year_month text, is_weekend smallint);
CREATE TABLE dim_district (district_sk smallint PRIMARY KEY, district_id smallint NOT NULL,
  district_name text, region_name text, created_at timestamp, updated_at timestamp);
CREATE TABLE dim_account (account_sk integer PRIMARY KEY, account_id integer NOT NULL UNIQUE,
  account_open_date date, statement_frequency_code text, statement_frequency_name text,
  account_district_sk smallint REFERENCES dim_district, created_at timestamp, updated_at timestamp);
CREATE TABLE dim_customer (customer_sk integer PRIMARY KEY, client_id integer NOT NULL, gender text,
  birth_date date, customer_district_sk smallint REFERENCES dim_district, membership_tier text,
  valid_from date NOT NULL, valid_to date NOT NULL, is_current smallint NOT NULL,
  created_at timestamp, updated_at timestamp);
CREATE TABLE dim_transaction_profile (transaction_profile_sk smallint PRIMARY KEY, type_code text,
  type_name text, operation_code text, operation_name text, purpose_code text, purpose_name text,
  cash_flow_direction text NOT NULL, created_at timestamp, updated_at timestamp);
CREATE TABLE fact_transaction (transaction_id bigint PRIMARY KEY,
  date_key integer NOT NULL REFERENCES dim_date,
  account_sk integer NOT NULL REFERENCES dim_account,
  customer_sk integer NOT NULL REFERENCES dim_customer,
  transaction_profile_sk smallint NOT NULL REFERENCES dim_transaction_profile,
  amount numeric(18, 2) NOT NULL, balance_after numeric(18, 2), counterparty_bank_code text,
  counterparty_account_ref text, load_batch_id text, loaded_at timestamp);
"""
PG_INDEXES = """
CREATE INDEX ON fact_transaction (date_key);
CREATE INDEX ON fact_transaction (account_sk, date_key);
CREATE INDEX ON fact_transaction (customer_sk);
CREATE INDEX ON dim_customer (client_id);
"""

# ---------------------------------------------------------------------------------- the queries
# Same question, same result; only the dialect differs (FINAL / sumIf on ClickHouse).
CH_PROFILE = "(SELECT transaction_profile_sk, cash_flow_direction AS dir FROM dw.dim_transaction_profile FINAL)"
CH_CUSTOMER = "(SELECT customer_sk, client_id, membership_tier FROM dw.dim_customer FINAL)"
CH_ACCOUNT = "(SELECT account_sk, account_id FROM dw.dim_account FINAL)"
PG_IN = "coalesce(sum(f.amount) FILTER (WHERE p.cash_flow_direction = 'IN'), 0)"  # sumIf gives 0, not NULL
PG_OUT = "coalesce(sum(f.amount) FILTER (WHERE p.cash_flow_direction = 'OUT'), 0)"

QUERIES = {
    "br01_kpi_1998": (
        "BR01 KPI một kỳ (năm 1998): số giao dịch, tiền vào, tiền ra",
        f"""SELECT count(), sumIf(amount, dir = 'IN'), sumIf(amount, dir = 'OUT')
            FROM dw.fact_transaction AS f JOIN {CH_PROFILE} AS p USING (transaction_profile_sk)
            WHERE f.date_key BETWEEN 19980101 AND 19981231""",
        f"""SELECT count(*), {PG_IN}, {PG_OUT}
            FROM fact_transaction f JOIN dim_transaction_profile p USING (transaction_profile_sk)
            WHERE f.date_key BETWEEN 19980101 AND 19981231"""),
    "br01_by_month": (
        "BR01 biểu đồ theo tháng, toàn bộ lịch sử",
        f"""SELECT d.year_month, count(), sumIf(amount, dir = 'IN'), sumIf(amount, dir = 'OUT')
            FROM dw.fact_transaction AS f JOIN {CH_PROFILE} AS p USING (transaction_profile_sk)
            JOIN dw.dim_date AS d USING (date_key) GROUP BY d.year_month ORDER BY d.year_month""",
        f"""SELECT d.year_month, count(*), {PG_IN}, {PG_OUT}
            FROM fact_transaction f JOIN dim_transaction_profile p USING (transaction_profile_sk)
            JOIN dim_date d USING (date_key) GROUP BY d.year_month ORDER BY d.year_month"""),
    "br01_by_tier_1998": (
        "BR01 theo hạng thành viên (năm 1998): giao dịch, vào, ra, số khách",
        f"""SELECT c.membership_tier, count(), sumIf(amount, dir = 'IN'), sumIf(amount, dir = 'OUT'),
                   uniqExact(c.client_id)
            FROM dw.fact_transaction AS f JOIN {CH_PROFILE} AS p USING (transaction_profile_sk)
            JOIN {CH_CUSTOMER} AS c USING (customer_sk)
            WHERE f.date_key BETWEEN 19980101 AND 19981231 GROUP BY 1 ORDER BY 1""",
        f"""SELECT c.membership_tier, count(*), {PG_IN}, {PG_OUT}, count(DISTINCT c.client_id)
            FROM fact_transaction f JOIN dim_transaction_profile p USING (transaction_profile_sk)
            JOIN dim_customer c USING (customer_sk)
            WHERE f.date_key BETWEEN 19980101 AND 19981231 GROUP BY 1 ORDER BY 1"""),
    "br02_account_detail": (
        "BR02 tra cứu tài khoản 2378: toàn bộ giao dịch chi tiết",
        f"""SELECT f.transaction_id, f.date_key, p.type_name, p.operation_name, p.purpose_name, f.amount,
                   f.balance_after
            FROM dw.fact_transaction AS f
            JOIN {CH_ACCOUNT} AS a USING (account_sk)
            JOIN (SELECT * FROM dw.dim_transaction_profile FINAL) AS p USING (transaction_profile_sk)
            WHERE a.account_id = 2378 ORDER BY f.date_key, f.transaction_id""",
        """SELECT f.transaction_id, f.date_key, p.type_name, p.operation_name, p.purpose_name, f.amount,
                  f.balance_after
           FROM fact_transaction f JOIN dim_account a USING (account_sk)
           JOIN dim_transaction_profile p USING (transaction_profile_sk)
           WHERE a.account_id = 2378 ORDER BY f.date_key, f.transaction_id"""),
    "br02_account_totals": (
        "BR02 tổng của tài khoản 2378 trong năm 1998",
        f"""SELECT count(), sumIf(amount, dir = 'IN'), sumIf(amount, dir = 'OUT')
            FROM dw.fact_transaction AS f JOIN {CH_ACCOUNT} AS a USING (account_sk)
            JOIN {CH_PROFILE} AS p USING (transaction_profile_sk)
            WHERE a.account_id = 2378 AND f.date_key BETWEEN 19980101 AND 19981231""",
        f"""SELECT count(*), {PG_IN}, {PG_OUT}
            FROM fact_transaction f JOIN dim_account a USING (account_sk)
            JOIN dim_transaction_profile p USING (transaction_profile_sk)
            WHERE a.account_id = 2378 AND f.date_key BETWEEN 19980101 AND 19981231"""),
    "br08_tier_old_new": (
        "BR08 báo cáo theo hạng kỳ cũ (1993-1998) và kỳ mới (01/1999)",
        f"""SELECT if(f.date_key < 19990101, 'old', 'new') AS period, c.membership_tier,
                   uniqExact(c.client_id), count(), sumIf(amount, dir = 'IN'), sumIf(amount, dir = 'OUT')
            FROM dw.fact_transaction AS f JOIN {CH_PROFILE} AS p USING (transaction_profile_sk)
            JOIN {CH_CUSTOMER} AS c USING (customer_sk)
            WHERE f.date_key BETWEEN 19930101 AND 19990131 GROUP BY 1, 2 ORDER BY 1, 2""",
        f"""SELECT CASE WHEN f.date_key < 19990101 THEN 'old' ELSE 'new' END AS period, c.membership_tier,
                   count(DISTINCT c.client_id), count(*), {PG_IN}, {PG_OUT}
            FROM fact_transaction f JOIN dim_transaction_profile p USING (transaction_profile_sk)
            JOIN dim_customer c USING (customer_sk)
            WHERE f.date_key BETWEEN 19930101 AND 19990131 GROUP BY 1, 2 ORDER BY 1, 2"""),
    "br08_client_versions": (
        "BR08 các phiên bản hạng của 1 khách (client 24) và giao dịch theo phiên bản",
        f"""SELECT c.customer_sk, c.membership_tier, toString(c.valid_from), count(), sumIf(amount, dir = 'IN')
            FROM dw.fact_transaction AS f JOIN {CH_PROFILE} AS p USING (transaction_profile_sk)
            JOIN (SELECT customer_sk, client_id, membership_tier, valid_from FROM dw.dim_customer FINAL
                  WHERE client_id = 24) AS c USING (customer_sk)
            GROUP BY 1, 2, 3 ORDER BY 3""",
        f"""SELECT c.customer_sk, c.membership_tier, c.valid_from::text, count(*), {PG_IN}
            FROM fact_transaction f JOIN dim_transaction_profile p USING (transaction_profile_sk)
            JOIN dim_customer c USING (customer_sk) WHERE c.client_id = 24
            GROUP BY 1, 2, 3 ORDER BY 3"""),
}


def dwpg():
    conn = psycopg2.connect(host=os.environ.get("POSTGRES_HOST", "postgres"), dbname="dwpg",
                            user=os.environ.get("POSTGRES_USER", "bank"),
                            password=os.environ.get("POSTGRES_PASSWORD", "bank"))
    conn.autocommit = True
    return conn


def load():
    ops = postgres()
    with ops.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = 'dwpg'")
        if cur.fetchone():
            cur.execute("DROP DATABASE dwpg")
        cur.execute("CREATE DATABASE dwpg")
    ops.close()
    ch, pg = clickhouse(), dwpg()
    timings = {}
    with pg.cursor() as cur:
        cur.execute(PG_DDL)
        for t in TABLES:
            start = time.perf_counter()
            final = "" if t in ("dim_date", "fact_transaction") else " FINAL"
            # CSV out of ClickHouse, straight into COPY (dates up to 2299-12-31 stay text)
            data = ch.raw_query(f"SELECT * FROM dw.{t}{final} FORMAT CSV",
                                settings={"date_time_output_format": "simple"})
            cur.copy_expert(f"COPY {t} FROM STDIN WITH (FORMAT csv, NULL '\\N')", io.BytesIO(data))
            cur.execute(f"SELECT count(*) FROM {t}")
            timings[t] = {"rows": cur.fetchone()[0], "copy_s": round(time.perf_counter() - start, 1)}
            print(f"  {t:<26} {timings[t]['rows']:>9,} rows  {timings[t]['copy_s']} s", flush=True)
        start = time.perf_counter()
        cur.execute(PG_INDEXES)
        cur.execute("ANALYZE")
        timings["indexes_analyze_s"] = round(time.perf_counter() - start, 1)
    json.dump(timings, open(f"{OUT}/pg_load.json", "w"), indent=1)


def p95(values):
    s = sorted(values)
    return s[min(len(s) - 1, round(0.95 * (len(s) - 1)))]


def normalize(v):
    if isinstance(v, (Decimal, float, int)) and not isinstance(v, bool):
        return round(float(v), 2)
    if isinstance(v, (dt.date, dt.datetime)):
        return v.isoformat()[:10]
    return str(v)


def sizes(ch, pg):
    out = {}
    for t, comp, raw in ch.query("""SELECT table, sum(data_compressed_bytes), sum(data_uncompressed_bytes)
                                    FROM system.parts WHERE database = 'dw' AND active GROUP BY table""").result_rows:
        out.setdefault(t, {})["clickhouse_compressed_mb"] = round(comp / 2**20, 1)
        out[t]["clickhouse_uncompressed_mb"] = round(raw / 2**20, 1)
    with pg.cursor() as cur:
        for t in TABLES:
            cur.execute("SELECT pg_table_size(%s), pg_indexes_size(%s)", (t, t))
            tbl, idx = cur.fetchone()
            out.setdefault(t, {})["postgres_table_mb"] = round(tbl / 2**20, 1)
            out[t]["postgres_indexes_mb"] = round(idx / 2**20, 1)
    return out


def pipeline_timing(cur):
    """BR06 data: time per historical batch and per step (latest successful run of each batch)."""
    cur.execute("""
        WITH last AS (SELECT DISTINCT ON (batch_id) * FROM ops.pipeline_run
                      WHERE status LIKE 'SUCCEEDED%' ORDER BY batch_id, run_id DESC)
        SELECT CASE WHEN l.batch_id LIKE 'HIST_%' THEN 'HIST (lô tháng)' ELSE l.batch_id END, s.step_name,
               s.step_order, extract(epoch FROM s.finished_at - s.started_at), l.rows_received, l.run_kind
        FROM last l JOIN ops.pipeline_step s USING (run_id)""")
    rows = cur.fetchall()
    cur.execute("""SELECT extract(epoch FROM finished_at - started_at), rows_received FROM ops.pipeline_run
                   WHERE batch_id LIKE 'HIST_%' AND run_kind = 'RUN' AND status LIKE 'SUCCEEDED%'""")
    first_runs = cur.fetchall()
    return rows, first_runs


def run():
    ch, pg = clickhouse(), dwpg()
    ch_settings = {"use_query_cache": 0}
    runs, summary, checks = [], [], []
    for name, (title, ch_sql, pg_sql) in QUERIES.items():
        results = {}
        for engine in ("clickhouse", "postgres"):
            if engine == "clickhouse":
                for stmt in ("SYSTEM DROP MARK CACHE", "SYSTEM DROP UNCOMPRESSED CACHE"):
                    ch.command(stmt)
                execute = lambda: ch.query(ch_sql, settings=ch_settings).result_rows  # noqa: E731
            else:
                cur = pg.cursor()

                def execute():
                    cur.execute(pg_sql)
                    return cur.fetchall()
            times = []
            for i in range(RUNS):
                start = time.perf_counter()
                rows = execute()
                ms = (time.perf_counter() - start) * 1000
                times.append(ms)
                runs.append([name, engine, i + 1, round(ms, 2)])
            results[engine] = [tuple(normalize(v) for v in r) for r in rows]
            warm = times[1:]
            summary.append([name, title, engine, len(rows), round(times[0], 1),
                            round(statistics.median(warm), 1), round(p95(warm), 1)])
            print(f"{name:<22} {engine:<10} {len(rows):>5} rows  cold {times[0]:8.1f} ms  "
                  f"warm median {statistics.median(warm):8.1f} ms  p95 {p95(warm):8.1f} ms", flush=True)
        ok = results["clickhouse"] == results["postgres"]
        checks.append([name, "PASS" if ok else "FAIL", len(results["clickhouse"])])
        print(f"  [{'PASS' if ok else 'FAIL'}] same result on both", flush=True)

    steps, first_runs = pipeline_timing(postgres().cursor())
    with open(f"{OUT}/pipeline_steps.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["batch", "step", "step_order", "seconds", "rows_received", "run_kind"])
        w.writerows([[b, s, o, round(float(sec), 3), n, k] for b, s, o, sec, n, k in steps])
    secs = [float(r[0]) for r in first_runs]
    rows_per_s = [r[1] / float(r[0]) for r in first_runs if r[0]]
    pipe = {"historical_batches_first_run": len(secs),
            "seconds_median": round(statistics.median(secs), 1), "seconds_p95": round(p95(secs), 1),
            "seconds_total": round(sum(secs), 1), "rows_per_second_median": round(statistics.median(rows_per_s))}
    print("pipeline:", pipe)

    env = {"measured_at": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
           "clickhouse_version": ch.server_version,
           "postgres_version": pg.server_version,
           "postgres_settings": {}, "sizes": sizes(ch, pg), "pipeline": pipe}
    with pg.cursor() as cur:
        for s in ("shared_buffers", "work_mem", "max_parallel_workers_per_gather", "effective_cache_size"):
            cur.execute(f"SHOW {s}")
            env["postgres_settings"][s] = cur.fetchone()[0]
    env["clickhouse_settings"] = {r[0]: r[1] for r in ch.query(
        "SELECT name, value FROM system.settings WHERE name IN ('max_threads', 'max_memory_usage')").result_rows}
    for fname, header, data in (
            ("query_runs.csv", ["query", "engine", "run", "ms"], runs),
            ("query_summary.csv", ["query", "title", "engine", "rows", "cold_ms", "warm_median_ms",
                                   "warm_p95_ms"], summary),
            ("result_check.csv", ["query", "status", "rows"], checks)):
        with open(f"{OUT}/{fname}", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(header)
            w.writerows(data)
    json.dump(env, open(f"{OUT}/env.json", "w"), indent=1, default=str)
    sys.exit(0 if all(c[1] == "PASS" for c in checks) else 1)


if __name__ == "__main__":
    os.makedirs(OUT, exist_ok=True)
    {"load": load, "run": run}[sys.argv[1]]()
