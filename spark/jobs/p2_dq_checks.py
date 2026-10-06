"""P2 -- Data correctness checks (ISO/IEC 25012 characteristics).

Source-side metrics are computed with Spark from the original CSV files;
warehouse-side metrics are computed in ClickHouse. Every result is appended to
dq.audit (one row per check per run) and printed. Exit code 1 if any check fails.
"""
import datetime as dt
import sys
import uuid
from decimal import Decimal

from pyspark.sql import functions as F

from common import SOURCE, clickhouse, spark_session

AUDIT_DDL = """
CREATE TABLE IF NOT EXISTS dq.audit (
  run_id         String,
  run_ts         DateTime,
  check_id       String,
  characteristic LowCardinality(String),
  metric         String,
  expected       String,
  observed       String,
  passed         Nullable(UInt8)        -- NULL = informational metric
) ENGINE = MergeTree ORDER BY (run_ts, check_id)
"""

# Customer DQ flag bits (see p2_build_core.py)
CUSTOMER_FLAGS = {1: "DOB placeholder 1800-01-01 -> NULL",
                  2: "DOB in the future -> shifted back 100 years",
                  4: "DOB missing in source",
                  8: "DOB outside age 0..100 after correction -> NULL",
                  16: "Gender not M/F -> Unknown",
                  32: "customer_id shared by several individuals"}


def main():
    spark = spark_session("p2_dq_checks")
    src = (spark.read.option("header", True).csv(f"{SOURCE}/fact_transactions.csv")
           .select(F.col("transaction_amount").cast("decimal(18,2)").alias("amt")))
    s = src.agg(F.count("*").alias("n"), F.sum("amt").alias("total")).first()
    src_rows, src_total = s["n"], Decimal(s["total"])
    src_customers = spark.read.option("header", True).csv(f"{SOURCE}/dim_customer.csv").count()
    spark.stop()

    ch = clickhouse()
    q = lambda sql: ch.query(sql).result_rows[0][0]

    checks = [  # (id, characteristic, metric, expected, observed)
        ("C1", "Completeness", "Source transactions vs. rows in fact_transactions",
         src_rows, q("SELECT count() FROM dwh.fact_transactions")),
        ("C2", "Completeness", "Source customer records vs. rows in dim_customer (excl. Unknown)",
         src_customers, q("SELECT count() FROM dwh.dim_customer WHERE customer_key > 0")),
        ("C3", "Completeness", "Fact rows pointing to an Unknown (0) member",
         0, q("SELECT countIf(customer_key = 0 OR location_key = 0 OR txn_type_key = 0) "
              "FROM dwh.fact_transactions")),
        ("U1", "Uniqueness", "Duplicate surrogate keys in dimensions",
         0, q("SELECT (SELECT count() - uniqExact(customer_key) FROM dwh.dim_customer) "
              "+ (SELECT count() - uniqExact(location_key) FROM dwh.dim_location) "
              "+ (SELECT count() - uniqExact(date_key) FROM dwh.dim_date)")),
        ("U2", "Uniqueness", "Duplicate natural keys in dim_customer",
         0, q("SELECT count() - uniqExact(customer_nk) FROM dwh.dim_customer")),
        ("U3", "Uniqueness", "Duplicate transaction_key in fact_transactions",
         0, q("SELECT count() - uniqExact(transaction_key) FROM dwh.fact_transactions")),
        ("A1", "Accuracy", "Non-null DOB outside [1916-10-22, 2016-10-21] (age 0..100)",
         0, q("SELECT countIf(dob IS NOT NULL AND (dob < '1916-10-22' OR dob > '2016-10-21')) "
              "FROM dwh.dim_customer")),
        ("A2", "Accuracy", "Gender outside {M, F, Unknown}",
         0, q("SELECT countIf(gender NOT IN ('M', 'F', 'Unknown')) FROM dwh.dim_customer")),
        ("A3", "Accuracy", "Negative transaction amounts",
         0, q("SELECT countIf(transaction_amount < 0) FROM dwh.fact_transactions")),
        ("K1", "Consistency", "|SUM(amount) source - SUM(amount) warehouse| (INR)",
         Decimal("0.00"), abs(src_total - Decimal(str(q(
             "SELECT sum(transaction_amount) FROM dwh.fact_transactions"))))),
        ("K2", "Consistency", "Orphan foreign keys in fact_transactions",
         0, q("SELECT countIf(customer_key NOT IN (SELECT customer_key FROM dwh.dim_customer)) "
              "+ countIf(location_key NOT IN (SELECT location_key FROM dwh.dim_location)) "
              "+ countIf(date_key NOT IN (SELECT date_key FROM dwh.dim_date)) "
              "+ countIf(txn_type_key NOT IN (SELECT txn_type_key FROM dwh.dim_transaction_type)) "
              "FROM dwh.fact_transactions")),
        ("K3", "Consistency", "Snapshot rows that are not unique per (month end, customer)",
         0, q("SELECT count() - uniqExact(snapshot_date_key, customer_key) "
              "FROM dwh.fact_customer_balance_snapshot")),
    ]
    info = [(f"I{bit}", "Data quality flags", desc, "",
             q(f"SELECT countIf(bitAnd(dq_flags, {bit}) > 0) FROM dwh.dim_customer"))
            for bit, desc in CUSTOMER_FLAGS.items()]
    info.append(("I64", "Data quality flags", "Zero-amount transactions (flagged, kept)", "",
                 q("SELECT countIf(bitAnd(dq_flags, 1) > 0) FROM dwh.fact_transactions")))

    run_id, run_ts = uuid.uuid4().hex[:8], dt.datetime.utcnow().replace(microsecond=0)
    rows, failed = [], 0
    for cid, char, metric, exp, obs in checks:
        ok = int(str(exp) == str(obs))
        failed += 1 - ok
        rows.append([run_id, run_ts, cid, char, metric, str(exp), str(obs), ok])
        print(f"[{'PASS' if ok else 'FAIL'}] {cid} {char:<12} {metric}: expected {exp}, observed {obs}")
    for cid, char, metric, exp, obs in info:
        rows.append([run_id, run_ts, cid, char, metric, exp, str(obs), None])
        print(f"[INFO] {cid:<4} {metric}: {obs:,}")

    ch.command(AUDIT_DDL)
    ch.insert("dq.audit", rows, column_names=["run_id", "run_ts", "check_id", "characteristic",
                                              "metric", "expected", "observed", "passed"])
    print(f"run {run_id}: {len(checks) - failed}/{len(checks)} checks passed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
