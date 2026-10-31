"""Benchmark B5 (pipeline engine): Apache Spark vs Apache Flink on the same transformation, the same
input files, and the same machine (both local, each in a 3 GB container). Flink side:
flink/jobs/bench_flink.py.

    python3 bench_engines.py prepare          # shared inputs for x1 and x10 (not timed)
    python3 bench_engines.py spark            # every (scale, cores, mode) in its own JVM

Workload, identical in both engines: read the 72 monthly batch snapshots (Parquet) -> parse dates,
standardize codes -> look up account, transaction profile, owner, and the owner version valid on the
transaction date (SCD2, an interval join) with the four lookup tables broadcast -> write the fact
rows as Parquet. Modes: "full" = one job over all 72 batches; "monthly" = 72 jobs, one per batch.
The lookup tables are exported once from the warehouse to Parquet, so neither engine talks to a
database while it is timed. Each run checks its output (rows, rows without a version, sum of ids,
sum of amounts, distinct versions); both engines must produce the same fingerprint.
Output: /lake/bench/p2/engines/<engine>.csv
"""
import csv
import glob
import json
import os
import shutil
import subprocess
import sys
import time

ROOT = "/lake/bench/p2/engines"
SCALES = {"x1": {"db": "dw", "trans": "/lake/batches/HIST_*/trans.parquet",
                 "owner": "/lake/staging/stg_account_owner.parquet"},
          "x10": {"db": "bench10", "trans": "/lake/bench/p2/x10/input/HIST_*/trans.parquet",
                  "owner": "/lake/bench/p2/x10/input/owner.parquet"}}
CORES = (1, 2, 4, 8)
MODES = ("full", "monthly")


def prepare():
    """Copy the batch snapshots and export the lookup tables to <ROOT>/<scale>/ (plain Parquet files)."""
    import pyarrow.parquet as pq
    from common import clickhouse
    ch = clickhouse()
    for scale, src in SCALES.items():
        base = f"{ROOT}/{scale}"
        shutil.rmtree(base, ignore_errors=True)
        batches = sorted(glob.glob(src["trans"]))
        for b in batches:                                   # HIST_YYYY_MM/part-*.parquet only
            dst = f"{base}/trans/{b.split('/')[-2]}"
            os.makedirs(dst)
            for f in glob.glob(f"{b}/part-*.parquet"):
                shutil.copy(f, dst)
        os.makedirs(f"{base}/dims")
        db = src["db"]
        exports = {
            "accounts": f"SELECT toString(account_id) AS account_id, toInt64(account_sk) AS account_sk "
                        f"FROM {db}.dim_account FINAL",
            "profiles": f"SELECT toString(type_code) AS type_code, toString(operation_code) AS operation_code, "
                        f"toString(purpose_code) AS purpose_code, toInt32(transaction_profile_sk) AS "
                        f"transaction_profile_sk FROM {db}.dim_transaction_profile FINAL",
            "versions": f"SELECT toInt64(client_id) AS v_client_id, toInt64(customer_sk) AS customer_sk, "
                        f"valid_from, valid_to FROM {db}.dim_customer FINAL"}
        for name, sql in exports.items():
            with open(f"{base}/dims/{name}.parquet", "wb") as f:
                f.write(ch.raw_query(sql + " FORMAT Parquet"))
        owner = pq.read_table(src["owner"]).to_pandas()
        owner = owner.assign(account_id=owner.account_id.astype(str), client_id=owner.client_id.astype("int64"))
        pq.write_table(__import__("pyarrow").Table.from_pandas(owner[["account_id", "client_id"]],
                                                               preserve_index=False),
                       f"{base}/dims/owner.parquet")
        rows = sum(pq.read_metadata(f).num_rows for f in glob.glob(f"{base}/trans/*/*.parquet"))
        print(f"{scale}: {len(batches)} batches, {rows:,} transactions, lookup tables "
              + ", ".join(f"{n} {pq.read_metadata(f'{base}/dims/{n}.parquet').num_rows:,}"
                          for n in ("accounts", "profiles", "versions", "owner")), flush=True)


def one(scale, cores, mode):
    t0 = time.perf_counter()
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    spark = (SparkSession.builder.master(f"local[{cores}]").appName(f"b5_{scale}_{cores}_{mode}")
             .config("spark.driver.memory", "2g").config("spark.sql.shuffle.partitions", "8")
             .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false")
             .config("spark.ui.showConsoleProgress", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    t_session = time.perf_counter() - t0
    base, out = f"{ROOT}/{scale}", f"{ROOT}/out/spark_{scale}_{cores}_{mode}"

    t1 = time.perf_counter()
    dims = {n: F.broadcast(spark.read.parquet(f"{base}/dims/{n}.parquet"))
            for n in ("accounts", "profiles", "versions", "owner")}

    def code(c):
        return F.coalesce(F.when(F.trim(F.col(c)) != "", F.upper(F.trim(F.col(c)))), F.lit("NOT_RECORDED"))

    def transform(t):
        t = (t.withColumn("txn_date", F.to_date(F.concat(F.lit("19"), F.col("date")), "yyyyMMdd"))
              .withColumn("type_code", F.upper(F.trim("type")))
              .withColumn("operation_code", code("operation")).withColumn("purpose_code", code("k_symbol")))
        in_version = ((F.col("client_id") == F.col("v_client_id")) & (F.col("txn_date") >= F.col("valid_from"))
                      & (F.col("txn_date") < F.col("valid_to")))
        return (t.join(dims["accounts"], "account_id")
                 .join(dims["profiles"], ["type_code", "operation_code", "purpose_code"])
                 .join(dims["owner"], "account_id", "left").join(dims["versions"], in_version, "left")
                 .select(F.col("trans_id").cast("long").alias("transaction_id"),
                         F.date_format("txn_date", "yyyyMMdd").cast("int").alias("date_key"),
                         "account_sk", "customer_sk", "transaction_profile_sk",
                         F.col("amount").cast("decimal(18,2)").alias("amount"),
                         F.col("balance").cast("decimal(18,2)").alias("balance_after")))

    batches = sorted(glob.glob(f"{base}/trans/HIST_*"))
    if mode == "full":
        transform(spark.read.parquet(*batches)).write.mode("overwrite").parquet(out)
    else:
        for i, b in enumerate(batches):
            transform(spark.read.parquet(b)).write.mode("overwrite" if i == 0 else "append").parquet(out)
    t_work = time.perf_counter() - t1

    r = spark.read.parquet(out).agg(
        F.count("*"), F.sum(F.when(F.col("customer_sk").isNull(), 1).otherwise(0)), F.sum("transaction_id"),
        F.sum("amount"), F.countDistinct("customer_sk")).first()
    spark.stop()
    shutil.rmtree(out, ignore_errors=True)
    print(json.dumps({"engine": "spark", "scale": scale, "cores": cores, "mode": mode, "batches": len(batches),
                      "session_s": round(t_session, 2), "transform_s": round(t_work, 2),
                      "rows": r[0], "rows_without_version": r[1], "sum_transaction_id": str(r[2]),
                      "sum_amount": str(r[3]), "distinct_versions": r[4], "rows_per_s": round(r[0] / t_work)}))


def run_all(engine_cmd, engine):
    """Every (scale, cores, mode) in its own process; results in <ROOT>/<engine>.csv."""
    results = []
    for scale in SCALES:
        for mode in MODES:
            for cores in CORES:
                start = time.perf_counter()
                p = subprocess.run(engine_cmd + ["one", scale, str(cores), mode], capture_output=True, text=True)
                line = [x for x in p.stdout.splitlines() if x.startswith("{")]
                if p.returncode or not line:
                    print(p.stdout[-2000:], p.stderr[-3000:])
                    raise SystemExit(f"{engine} {scale} {cores} {mode} failed")
                r = json.loads(line[-1])
                r["process_s"] = round(time.perf_counter() - start, 2)
                results.append(r)
                print(f"{engine:<6} {scale:<4} {mode:<8} {cores} cores: session {r['session_s']:5.1f} s  "
                      f"transform {r['transform_s']:6.1f} s  {r['rows_per_s']:>9,} rows/s  rows {r['rows']:,}  "
                      f"no-version {r['rows_without_version']}", flush=True)
    with open(f"{ROOT}/{engine}.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(results[0]), lineterminator="\n")
        w.writeheader()
        w.writerows(results)


if __name__ == "__main__":
    os.makedirs(f"{ROOT}/out", exist_ok=True)
    cmd = sys.argv[1]
    if cmd == "prepare":
        prepare()
    elif cmd == "one":
        one(sys.argv[2], int(sys.argv[3]), sys.argv[4])
    else:
        run_all([sys.executable, __file__], "spark")
