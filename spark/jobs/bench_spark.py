"""P2-8 B4 (pipeline C): how the Spark transformation scales with CPU cores, and what splitting the
history into monthly batches costs compared with one batch.

    python3 bench_spark.py            # runs every (cores, mode) in its own process
    python3 bench_spark.py one 4 full # one measurement

Workload = the transformation the pipeline applies to the 72 historical batches, read from their
Data Lake snapshots: parse dates, standardize codes, look up account, profile, owner, and the owner
version valid on the transaction date (SCD2), then write the fact rows as Parquet. It reads the
warehouse dimensions but writes nothing to ClickHouse or PostgreSQL, so it can be repeated.
  full     one Spark job over all 72 snapshots
  monthly  72 jobs, one per batch, in the same Spark session (as the pipeline runs them)
Each run starts a new JVM (local[cores]); the session start-up time is reported separately.
Output: /lake/bench/p2/spark_scaling.csv
"""
import csv
import glob
import json
import os
import subprocess
import sys
import time

OUT = "/lake/bench/p2"
CORES = (1, 2, 4, 8)
MODES = ("full", "monthly")
EXPECTED_ROWS = 1_056_320


def one(cores, mode):
    t0 = time.perf_counter()
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    spark = (SparkSession.builder.master(f"local[{cores}]").appName(f"b4_{cores}_{mode}")
             .config("spark.driver.memory", "2g").config("spark.sql.shuffle.partitions", "8")
             .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    t_session = time.perf_counter() - t0

    import pandas as pd
    from common import STAGING, clickhouse
    from pipeline import standardize_codes, yymmdd
    ch = clickhouse()

    def frame(sql):
        r = ch.query(sql)
        return spark.createDataFrame(pd.DataFrame(r.result_rows, columns=r.column_names))
    t1 = time.perf_counter()
    accounts = F.broadcast(frame("SELECT toString(account_id) AS account_id, account_sk FROM dw.dim_account FINAL"))
    profiles = F.broadcast(frame("SELECT type_code, operation_code, purpose_code, transaction_profile_sk "
                                 "FROM dw.dim_transaction_profile FINAL"))
    versions = F.broadcast(frame("SELECT client_id AS v_client_id, customer_sk, valid_from, valid_to "
                                 "FROM dw.dim_customer FINAL"))
    owner = F.broadcast(spark.read.parquet(f"{STAGING}/stg_account_owner.parquet")
                        .withColumn("account_id", F.col("account_id").cast("string")))

    def transform(t):
        t = standardize_codes(t.withColumn("txn_date", yymmdd(F.col("date"))))
        in_version = ((F.col("client_id") == F.col("v_client_id")) & (F.col("txn_date") >= F.col("valid_from"))
                      & (F.col("txn_date") < F.col("valid_to")))
        return (t.join(accounts, "account_id").join(profiles, ["type_code", "operation_code", "purpose_code"])
                 .join(owner, "account_id", "left").join(versions, in_version, "left")
                 .select(F.col("trans_id").cast("long").alias("transaction_id"),
                         F.date_format("txn_date", "yyyyMMdd").cast("int").alias("date_key"),
                         "account_sk", "customer_sk", "transaction_profile_sk",
                         F.col("amount").cast("decimal(18,2)").alias("amount"),
                         F.col("balance").cast("decimal(18,2)").alias("balance_after")))

    paths = sorted(glob.glob("/lake/batches/HIST_*/trans.parquet"))
    target = f"{OUT}/b4_out/{cores}_{mode}"
    if mode == "full":
        transform(spark.read.parquet(*paths)).write.mode("overwrite").parquet(target)
    else:
        for i, p in enumerate(paths):
            transform(spark.read.parquet(p)).write.mode("overwrite" if i == 0 else "append").parquet(target)
    t_work = time.perf_counter() - t1
    out = spark.read.parquet(target)
    rows, missing = out.count(), out.filter(F.col("customer_sk").isNull()).count()
    spark.stop()
    print(json.dumps({"cores": cores, "mode": mode, "batches": 1 if mode == "full" else len(paths),
                      "session_s": round(t_session, 2), "transform_s": round(t_work, 2),
                      "rows": rows, "rows_without_version": missing,
                      "rows_per_s": round(rows / t_work)}))


def main():
    results = []
    for mode in MODES:
        for cores in CORES:
            start = time.perf_counter()
            p = subprocess.run([sys.executable, __file__, "one", str(cores), mode], capture_output=True, text=True,
                               cwd="/opt/jobs")
            line = [x for x in p.stdout.splitlines() if x.startswith("{")]
            if p.returncode or not line:
                print(p.stdout[-2000:], p.stderr[-2000:])
                raise SystemExit(f"run {cores} {mode} failed")
            r = json.loads(line[-1])
            r["process_s"] = round(time.perf_counter() - start, 2)
            r["correct"] = r["rows"] == EXPECTED_ROWS and r["rows_without_version"] == 0
            results.append(r)
            print(f"{mode:<8} {cores} cores: session {r['session_s']:5.1f} s  transform {r['transform_s']:6.1f} s  "
                  f"{r['rows_per_s']:>8,} rows/s  total process {r['process_s']:6.1f} s  rows {r['rows']:,} "
                  f"{'OK' if r['correct'] else 'WRONG'}", flush=True)
    with open(f"{OUT}/spark_scaling.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(results[0]), lineterminator="\n")
        w.writeheader()
        w.writerows(results)


if __name__ == "__main__":
    os.makedirs(OUT, exist_ok=True)
    if len(sys.argv) > 1 and sys.argv[1] == "one":
        one(int(sys.argv[2]), sys.argv[3])
    else:
        main()
