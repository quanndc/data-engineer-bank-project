"""Benchmark B5, Flink side: the transformation of spark/jobs/bench_engines.py written as Flink SQL
(Flink 1.20, official image, batch mode), on the same input files. Runs on the host: each run writes a
SQL script into the lake, submits it with the SQL client inside the jobmanager container, and reads
the job start/end times from the Flink REST API.

    make bench-engines                         # or, with the cluster up (--profile flink):
    python3 flink/jobs/bench_flink.py          # Spark and Flink, every (scale, cores, mode), 3 times
    python3 flink/jobs/bench_flink.py one x1 4 full

Parallelism = cores (as local[cores] in Spark). The four lookup tables are broadcast with the
BROADCAST join hint, as Spark broadcasts them. Output fingerprint as in bench_engines.py.
Timing: transform_s = first job start to last job end (Flink's own clock, REST API), the same span
Spark measures around its writes; session_s = the rest of the SQL client process (JVM start, parsing,
planning, submission, the fingerprint query), timed with the VM's monotonic clock. A run during which
the VM wall clock stepped is measured again. The cluster itself is started once, before all runs.
The full run alternates Spark and Flink on each configuration, 3 times each (Spark through
bench_engines.py one ... in the spark container), so both engines meet the same machine conditions.
Output: lake/bench/p2/engines/runs.csv (every run), engines.csv (medians)
"""
import csv
import glob
import json
import os
import re
import shutil
import statistics
import subprocess
import sys
import time
import urllib.request

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
HOST_ROOT = f"{REPO}/lake/bench/p2/engines"        # same folder as /lake/bench/p2/engines in the containers
ROOT = "/lake/bench/p2/engines"
REST = "http://127.0.0.1:18081"
SQL_CLIENT_CMD = ["/opt/flink/bin/sql-client.sh", "-f"]
VM_TIMED = ["docker", "exec", "bankdw-flink-jm", "bash", "-c",
            'echo "__T $(date +%s.%N) $(cut -d\\  -f1 /proc/uptime)"; "$@"; s=$?; '
            'echo "__T $(date +%s.%N) $(cut -d\\  -f1 /proc/uptime)"; exit $s', "timed"]

TRANS_COLS = """trans_id STRING, account_id STRING, `date` STRING, `type` STRING, operation STRING,
                amount STRING, balance STRING, k_symbol STRING, bank STRING, `account` STRING"""
DIMS = {
    "accounts": "account_id STRING, account_sk BIGINT",
    "profiles": "type_code STRING, operation_code STRING, purpose_code STRING, transaction_profile_sk INT",
    "versions": "v_client_id BIGINT, customer_sk BIGINT, valid_from DATE, valid_to DATE",
    "owner": "account_id STRING, client_id BIGINT"}

TRANSFORM = """INSERT INTO fact_out
SELECT /*+ BROADCAST(a, p, o, v) */
       CAST(t.trans_id AS BIGINT),
       CAST(YEAR(t.txn_date) * 10000 + MONTH(t.txn_date) * 100 + DAYOFMONTH(t.txn_date) AS INT),
       a.account_sk, v.customer_sk, p.transaction_profile_sk,
       CAST(t.amount AS DECIMAL(18, 2)), CAST(t.balance AS DECIMAL(18, 2))
FROM (SELECT trans_id, account_id, amount, balance,
             TO_DATE(CONCAT('19', `date`), 'yyyyMMdd') AS txn_date,
             UPPER(TRIM(`type`)) AS type_code,
             COALESCE(CASE WHEN TRIM(operation) <> '' THEN UPPER(TRIM(operation)) END, 'NOT_RECORDED') AS operation_code,
             COALESCE(CASE WHEN TRIM(k_symbol) <> '' THEN UPPER(TRIM(k_symbol)) END, 'NOT_RECORDED') AS purpose_code
      FROM {src}) AS t
JOIN accounts AS a ON t.account_id = a.account_id
JOIN profiles AS p ON t.type_code = p.type_code AND t.operation_code = p.operation_code
                  AND t.purpose_code = p.purpose_code
LEFT JOIN owner AS o ON t.account_id = o.account_id
LEFT JOIN versions AS v ON o.client_id = v.v_client_id AND t.txn_date >= v.valid_from AND t.txn_date < v.valid_to;
"""
FINGERPRINT = """SELECT COUNT(*) AS n, SUM(CASE WHEN customer_sk IS NULL THEN 1 ELSE 0 END) AS no_version,
       SUM(transaction_id) AS sum_id, SUM(amount) AS sum_amount, COUNT(DISTINCT customer_sk) AS versions
FROM fact_out;
"""


def table(name, cols, path):
    return (f"CREATE TEMPORARY TABLE {name} ({cols}) WITH "
            f"('connector' = 'filesystem', 'path' = '{path}', 'format' = 'parquet');\n")


def jobs():
    with urllib.request.urlopen(f"{REST}/jobs/overview", timeout=10) as r:
        return {j["jid"]: j for j in json.load(r)["jobs"]}


def one(scale, cores, mode):
    base, out = f"{ROOT}/{scale}", f"{ROOT}/out/flink_{scale}_{cores}_{mode}"
    shutil.rmtree(f"{HOST_ROOT}/out/flink_{scale}_{cores}_{mode}", ignore_errors=True)
    batches = sorted(os.path.basename(b) for b in glob.glob(f"{HOST_ROOT}/{scale}/trans/HIST_*"))
    sql = ["SET 'execution.runtime-mode' = 'batch';\n", f"SET 'parallelism.default' = '{cores}';\n",
           "SET 'table.dml-sync' = 'true';\n", "SET 'sql-client.execution.result-mode' = 'TABLEAU';\n"]
    sql += [table(n, c, f"{base}/dims/{n}.parquet") for n, c in DIMS.items()]
    sql.append(table("fact_out", """transaction_id BIGINT, date_key INT, account_sk BIGINT, customer_sk BIGINT,
                     transaction_profile_sk INT, amount DECIMAL(18, 2), balance_after DECIMAL(18, 2)""", out))
    if mode == "full":                           # one job; the folder is read recursively (72 batches)
        sql += [table("trans_all", TRANS_COLS, f"{base}/trans"), TRANSFORM.format(src="trans_all")]
    else:                                        # 72 jobs, one per batch
        for i, b in enumerate(batches):
            sql += [table(f"trans_{i}", TRANS_COLS, f"{base}/trans/{b}"), TRANSFORM.format(src=f"trans_{i}")]
    sql.append(FINGERPRINT)
    script = f"run_{scale}_{cores}_{mode}.sql"
    os.makedirs(f"{HOST_ROOT}/sql", exist_ok=True)
    with open(f"{HOST_ROOT}/sql/{script}", "w") as f:
        f.writelines(sql)

    # Timed inside the Docker VM: /proc/uptime is monotonic there, and the VM wall clock (used by the
    # Flink REST times) is compared with it before and after, so a clock step voids the run.
    before = set(jobs())
    p = subprocess.run(VM_TIMED + SQL_CLIENT_CMD + [f"{ROOT}/sql/{script}"],
                       capture_output=True, text=True, cwd=REPO)
    marks = [line.split()[1:] for line in p.stdout.splitlines() if line.startswith("__T ")]
    rows = [line for line in p.stdout.splitlines() if re.match(r"^\|\s*\d", line)]
    if p.returncode or not rows or "[ERROR]" in p.stdout or len(marks) != 2:
        print(p.stdout[-4000:], p.stderr[-2000:])
        raise SystemExit(f"flink {scale} {cores} {mode} failed")
    (wall0, up0), (wall1, up1) = [(float(a), float(b)) for a, b in marks]
    total, step = up1 - up0, abs((wall1 - wall0) - (up1 - up0))
    new = [j for jid, j in jobs().items() if jid not in before]
    inserts = [j for j in new if j["name"].startswith("insert-into")]
    if len(inserts) != (1 if mode == "full" else len(batches)) or any(j["state"] != "FINISHED" for j in inserts):
        raise SystemExit(f"flink {scale} {cores} {mode}: unexpected jobs {[(j['name'], j['state']) for j in new]}")
    t_work = (max(j["end-time"] for j in inserts) - min(j["start-time"] for j in inserts)) / 1000
    r = [c.strip() for c in rows[-1].strip("|").split("|")]
    shutil.rmtree(f"{HOST_ROOT}/out/flink_{scale}_{cores}_{mode}", ignore_errors=True)
    if step > 0.5 or t_work > total:
        return None                              # VM clock stepped during the run: measure again
    return {"engine": "flink", "scale": scale, "cores": cores, "mode": mode, "batches": len(batches),
            "session_s": round(total - t_work, 2), "transform_s": round(t_work, 2),
            "rows": int(r[0]), "rows_without_version": int(r[1]), "sum_transaction_id": r[2],
            "sum_amount": r[3], "distinct_versions": int(r[4]), "rows_per_s": round(int(r[0]) / t_work),
            "process_s": round(total, 2)}


def spark_one(scale, cores, mode):
    p = subprocess.run(["docker", "exec", "bankdw-spark", "bash", "-c",
                        f"cd /opt/jobs && python3 bench_engines.py one {scale} {cores} {mode}"],
                       capture_output=True, text=True)
    line = [x for x in p.stdout.splitlines() if x.startswith("{")]
    if p.returncode or not line:
        print(p.stdout[-2000:], p.stderr[-3000:])
        raise SystemExit(f"spark {scale} {cores} {mode} failed")
    return json.loads(line[-1])


def run_all(reps=3):
    """Spark and Flink alternate on every (scale, mode, cores), reps times each, so both meet the same
    machine conditions. Every run: lake/bench/p2/engines/runs.csv; medians: engines.csv."""
    runs = []
    for scale in ("x1", "x10"):
        for mode in ("full", "monthly"):
            for cores in (1, 2, 4, 8):
                for rep in range(1, reps + 1):
                    for engine in ("spark", "flink"):
                        for _ in range(3):
                            r = spark_one(scale, cores, mode) if engine == "spark" else one(scale, cores, mode)
                            if r:
                                break
                            print(f"flink  {scale} {mode} {cores}: VM clock stepped, measuring again", flush=True)
                        else:
                            raise SystemExit("VM clock keeps stepping: is the machine overloaded?")
                        r["rep"] = rep
                        runs.append(r)
                        print(f"{engine:<6} {scale:<4} {mode:<8} {cores} cores #{rep}: session {r['session_s']:5.1f} s  "
                              f"transform {r['transform_s']:6.1f} s  {r['rows_per_s']:>9,} rows/s  "
                              f"rows {r['rows']:,}  no-version {r['rows_without_version']}  "
                              f"sum {r['sum_amount']}", flush=True)
    cols = ["engine", "scale", "mode", "cores", "rep", "batches", "session_s", "transform_s", "process_s",
            "rows_per_s", "rows", "rows_without_version", "sum_transaction_id", "sum_amount", "distinct_versions"]
    with open(f"{HOST_ROOT}/runs.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore", lineterminator="\n")
        w.writeheader()
        w.writerows(runs)
    summarize(runs)


def summarize(runs):
    """Median of the repetitions per (engine, scale, mode, cores), by transform time."""
    cols = ["engine", "scale", "mode", "cores", "batches", "session_s", "transform_s", "rows_per_s", "rows",
            "rows_without_version", "sum_transaction_id", "sum_amount", "distinct_versions"]
    summary = []
    for key in dict.fromkeys((r["engine"], r["scale"], r["mode"], r["cores"]) for r in runs):
        g = [r for r in runs if (r["engine"], r["scale"], r["mode"], r["cores"]) == key]
        med = sorted(g, key=lambda r: r["transform_s"])[len(g) // 2]
        same = len({(r["rows"], r["rows_without_version"], r["sum_transaction_id"], r["sum_amount"],
                     r["distinct_versions"]) for r in runs if r["scale"] == key[1]}) == 1
        summary.append({**{c: med[c] for c in cols},
                        "session_s": statistics.median(r["session_s"] for r in g),
                        "transform_min_s": min(r["transform_s"] for r in g),
                        "transform_max_s": max(r["transform_s"] for r in g), "same_output_as_other_runs": same})
    with open(f"{HOST_ROOT}/engines.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(summary[0]), lineterminator="\n")
        w.writeheader()
        w.writerows(summary)


if __name__ == "__main__":
    os.makedirs(f"{HOST_ROOT}/out", exist_ok=True)
    if len(sys.argv) > 1 and sys.argv[1] == "one":
        print(json.dumps(one(sys.argv[2], int(sys.argv[3]), sys.argv[4])))
    elif len(sys.argv) > 1 and sys.argv[1] == "summary":     # medians again from runs.csv
        with open(f"{HOST_ROOT}/runs.csv") as f:
            rows = list(csv.DictReader(f))
        for r in rows:
            for c in ("session_s", "transform_s"):
                r[c] = float(r[c])
        summarize(rows)
    else:
        run_all()
