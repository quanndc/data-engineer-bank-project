"""B1 + B2: run the BR queries on the three solutions and compare their results.

  ch_star  Kimball star schema on ClickHouse (proposed solution)
  ch_obt   One Big Table on ClickHouse      (alternative A1)
  pg_star  star schema on PostgreSQL        (alternative A2)

Protocol (Section V): each query runs 6 times; run 1 is reported as "cold" (first execution,
ClickHouse mark/uncompressed caches dropped before it; the OS page cache cannot be dropped
from a container), and the median and 95th percentile of runs 2-6 are reported as "warm".
Times are client wall-clock, including the transfer of the result.

Outputs (/lake/bench/results): query_runs.csv, query_summary.csv, result_check.csv
"""
import csv
import datetime as dt
import glob
import os
import statistics
import sys
import time
from decimal import Decimal

import psycopg2

sys.path.insert(0, "/opt/jobs")
from common import LAKE, clickhouse  # noqa: E402

QUERIES = "/opt/benchmark/queries"
OUT = f"{LAKE}/bench/results"
SOLUTIONS = ["ch_star", "ch_obt", "pg_star"]
RUNS = 6


def p95(values):
    s = sorted(values)
    return s[min(len(s) - 1, round(0.95 * (len(s) - 1)))]


def normalize(value):
    if isinstance(value, (Decimal, float, int)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()[:10]
    return str(value)


def same(a, b):
    if len(a) != len(b):
        return False, f"row count {len(a)} vs {len(b)}"
    for i, (ra, rb) in enumerate(zip(a, b)):
        for x, y in zip(ra, rb):
            if isinstance(x, float) and isinstance(y, float):
                if abs(x - y) > 0.011 + 1e-12 * max(abs(x), abs(y)):
                    return False, f"row {i}: {x} vs {y}"
            elif x != y:
                return False, f"row {i}: {x!r} vs {y!r}"
    return True, ""


class ClickHouse:
    def __init__(self):
        self.ch = clickhouse()

    def cold(self):
        self.ch.command("SYSTEM DROP MARK CACHE")
        self.ch.command("SYSTEM DROP UNCOMPRESSED CACHE")

    def run(self, sql):
        return self.ch.query(sql, settings={"use_query_cache": 0}).result_rows


class Postgres:
    def __init__(self):
        self.conn = psycopg2.connect(host="postgres", user="bank", password="bank", dbname="bank")
        self.conn.autocommit = True
        self.conn.cursor().execute("SET statement_timeout = '300s'")

    def cold(self):
        pass

    def run(self, sql):
        cur = self.conn.cursor()
        cur.execute(sql)
        return cur.fetchall()


def main():
    os.makedirs(OUT, exist_ok=True)
    engines = {"ch_star": ClickHouse(), "ch_obt": ClickHouse(), "pg_star": Postgres()}
    names = sorted(os.path.basename(p)[:-4] for p in glob.glob(f"{QUERIES}/ch_star/br*.sql"))
    runs, summary, results = [], [], {}

    for name in names:
        for sol in SOLUTIONS:
            engine, sql = engines[sol], open(f"{QUERIES}/{sol}/{name}.sql").read()
            engine.cold()
            times = []
            for i in range(RUNS):
                start = time.perf_counter()
                rows = engine.run(sql)
                ms = (time.perf_counter() - start) * 1000
                times.append(ms)
                runs.append([name, sol, i + 1, round(ms, 2)])
            results[(name, sol)] = [tuple(normalize(v) for v in r) for r in rows]
            warm = times[1:]
            summary.append([name, sol, len(rows), round(times[0], 1),
                            round(statistics.median(warm), 1), round(p95(warm), 1)])
            print(f"{name:<22} {sol:<8} {len(rows):>6} rows  cold {times[0]:8.1f} ms  "
                  f"warm median {statistics.median(warm):8.1f} ms  p95 {p95(warm):8.1f} ms", flush=True)

    checks = []
    for name in names:
        for sol in ("ch_obt", "pg_star"):
            ok, why = same(results[(name, "ch_star")], results[(name, sol)])
            checks.append([name, f"ch_star = {sol}", "PASS" if ok else "FAIL", why])
            print(f"[{'PASS' if ok else 'FAIL'}] {name} ch_star = {sol} {why}")

    for fname, header, data in (
            ("query_runs.csv", ["query", "solution", "run", "ms"], runs),
            ("query_summary.csv", ["query", "solution", "rows", "cold_ms", "warm_median_ms",
                                   "warm_p95_ms"], summary),
            ("result_check.csv", ["query", "comparison", "status", "detail"], checks)):
        with open(f"{OUT}/{fname}", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(header)
            w.writerows(data)


if __name__ == "__main__":
    main()
