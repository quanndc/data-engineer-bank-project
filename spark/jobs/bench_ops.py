"""P2-8 B3 (architecture of the operational store E): PostgreSQL next to the warehouse, as built,
against keeping the operational data in ClickHouse as well (one database for everything).

    python3 bench_ops.py

Both stores get the same alerts (a copy of ops.alert) and run what the application and the
pipeline actually do, one call at a time:
  W1  change an alert's status and record the action (BR04), 200 alerts, twice
  W2  log a pipeline step: insert as RUNNING, then mark it finished (BR06), 200 steps
  W3  read: the analyst's list of new alerts; one alert with its history (BR03, BR04), 50 times each
and the integrity guarantees the BRs rely on (I1-I4). ClickHouse is tested in two styles:
  ch_versioned  write a new row version (ReplacingMergeTree) and read with FINAL
  ch_mutation   ALTER TABLE ... UPDATE (mutation), waiting for it to finish
Everything runs on copies (schema bench_ops in dwpg, database bench in ClickHouse); ops is only read.
Output: /lake/bench/p2/ops_*.csv, ops_integrity.csv
"""
import csv
import io
import json
import statistics
import time

from benchmark import OUT, dwpg, p95
from common import WAREHOUSE, clickhouse, postgres

N = 200
READS = 50
CH_DDL = [
    "DROP TABLE IF EXISTS bench.ops_alert SYNC",
    "DROP TABLE IF EXISTS bench.ops_alert_action SYNC",
    "DROP TABLE IF EXISTS bench.ops_step SYNC",
    """CREATE TABLE bench.ops_alert (alert_id UInt64, rule_code LowCardinality(String), account_id UInt32,
         period_start Date, period_end Date, reason String, evidence String, transaction_ids Array(UInt64),
         detected_at DateTime64(3), status LowCardinality(String), status_changed_at DateTime64(3))
       ENGINE = ReplacingMergeTree(status_changed_at) ORDER BY alert_id""",
    """CREATE TABLE bench.ops_alert_action (action_id UInt64, alert_id UInt64, from_status String,
         to_status String, note String, acted_by String, acted_at DateTime64(3))
       ENGINE = MergeTree ORDER BY (alert_id, action_id)""",
    """CREATE TABLE bench.ops_step (step_id UInt64, run_id UInt64, step_order UInt8, step_name String,
         status String, started_at DateTime64(3), finished_at Nullable(DateTime64(3)),
         rows_in Nullable(UInt64), rows_out Nullable(UInt64), version UInt8)
       ENGINE = ReplacingMergeTree(version) ORDER BY step_id"""]
ALLOWED = {("NEW", "IN_REVIEW"), ("IN_REVIEW", "RESOLVED"), ("NEW", "RESOLVED"), ("RESOLVED", "IN_REVIEW")}


def setup():
    """Copy ops (schema + batch, pipeline_run, alert) into dwpg.bench_ops and alerts into ClickHouse."""
    pg = dwpg()
    cur = pg.cursor()
    cur.execute("DROP SCHEMA IF EXISTS bench_ops CASCADE")
    cur.execute(open(f"{WAREHOUSE}/postgres/ddl/ops.sql").read().replace("ops.", "bench_ops.")
                .replace("SCHEMA IF NOT EXISTS ops", "SCHEMA bench_ops"))
    src = postgres().cursor()
    for t in ("batch", "pipeline_run", "alert"):
        buf = io.StringIO()
        src.copy_expert(f"COPY ops.{t} TO STDOUT", buf)
        buf.seek(0)
        cur.copy_expert(f"COPY bench_ops.{t} FROM STDIN", buf)
    cur.execute("SELECT setval('bench_ops.alert_alert_id_seq', (SELECT max(alert_id) FROM bench_ops.alert))")
    cur.execute("ANALYZE")
    ch = clickhouse()
    for stmt in CH_DDL:
        ch.command(stmt)
    src.execute("""SELECT alert_id, rule_code, account_id, period_start, period_end, reason, evidence::text,
                          transaction_ids, detected_at, 'NEW', detected_at FROM ops.alert""")
    rows = [list(r) for r in src.fetchall()]
    ch.insert("bench.ops_alert", rows, column_names=[
        "alert_id", "rule_code", "account_id", "period_start", "period_end", "reason", "evidence",
        "transaction_ids", "detected_at", "status", "status_changed_at"])
    cur.execute("UPDATE bench_ops.alert SET status = 'NEW'")
    print(f"copied {len(rows)} alerts to both stores", flush=True)
    return pg, ch


def stats(name, store, lat):
    return [name, store, len(lat), round(statistics.median(lat), 2), round(p95(lat), 2), round(sum(lat), 1)]


def timed_loop(items, fn):
    lat = []
    for x in items:
        start = time.perf_counter()
        fn(x)
        lat.append((time.perf_counter() - start) * 1000)
    return lat


def w1(pg, ch, ids):
    cur, out = pg.cursor(), []
    for frm, to in (("NEW", "IN_REVIEW"), ("IN_REVIEW", "RESOLVED")):
        label = f"W1 {frm} → {to} + ghi lịch sử"
        out.append(stats(label, "postgres", timed_loop(ids, lambda a: cur.execute(
            "SELECT bench_ops.set_alert_status(%s, %s, 'bench', 'bench')", (a, to)))))

        def versioned(a, to=to):
            # read the current version, check the transition in the client, write a new version + action
            cur_status = ch.query(f"SELECT status FROM bench.ops_alert FINAL WHERE alert_id = {a}").result_rows[0][0]
            if (cur_status, to) not in ALLOWED:
                raise ValueError(f"{cur_status} -> {to}")
            ch.command(f"""INSERT INTO bench.ops_alert SELECT alert_id, rule_code, account_id, period_start,
                period_end, reason, evidence, transaction_ids, detected_at, '{to}', now64(3)
                FROM bench.ops_alert FINAL WHERE alert_id = {a}""")
            ch.command(f"""INSERT INTO bench.ops_alert_action VALUES
                (rand64(), {a}, '{cur_status}', '{to}', 'bench', 'bench', now64(3))""")
        if frm == "NEW":
            out.append(stats(label, "ch_versioned", timed_loop(ids, versioned)))
        else:
            # the other ClickHouse style on the same alerts: an in-place mutation
            def mutation(a, to=to):
                cur_status = ch.query(f"SELECT status FROM bench.ops_alert FINAL WHERE alert_id = {a}"
                                      ).result_rows[0][0]
                # the version column (status_changed_at) cannot be mutated: only status changes
                ch.command(f"ALTER TABLE bench.ops_alert UPDATE status = '{to}' WHERE alert_id = {a}",
                           settings={"mutations_sync": 1})
                ch.command(f"""INSERT INTO bench.ops_alert_action VALUES
                    (rand64(), {a}, '{cur_status}', '{to}', 'bench', 'bench', now64(3))""")
            out.append(stats(label, "ch_mutation", timed_loop(ids, mutation)))
    return out


def w2(pg, ch):
    cur, out = pg.cursor(), []

    def pg_step(i):
        cur.execute("""INSERT INTO bench_ops.pipeline_step (run_id, step_order, step_name)
                       VALUES ((SELECT min(run_id) FROM bench_ops.pipeline_run), %s, 'bench') RETURNING step_id""",
                    (i % 30000,))
        sid = cur.fetchone()[0]
        cur.execute("""UPDATE bench_ops.pipeline_step SET status = 'SUCCEEDED', finished_at = now(),
                       rows_in = 100, rows_out = 100 WHERE step_id = %s""", (sid,))
    out.append(stats("W2 ghi 1 bước pipeline (bắt đầu + kết thúc)", "postgres", timed_loop(range(N), pg_step)))
    since = ch.query("SELECT now()").result_rows[0][0]     # part_log keeps earlier runs

    def ch_step(i):
        ch.command(f"INSERT INTO bench.ops_step VALUES ({i}, 1, {i % 255}, 'bench', 'RUNNING', now64(3), NULL, "
                   f"NULL, NULL, 1)")
        ch.command(f"INSERT INTO bench.ops_step VALUES ({i}, 1, {i % 255}, 'bench', 'SUCCEEDED', now64(3), "
                   f"now64(3), 100, 100, 2)")
    out.append(stats("W2 ghi 1 bước pipeline (bắt đầu + kết thúc)", "ch_versioned", timed_loop(range(N), ch_step)))
    ch.command("SYSTEM FLUSH LOGS")
    parts = ch.query("SELECT count() FROM system.part_log WHERE database = 'bench' AND table = 'ops_step' "
                     "AND event_type = 'NewPart' AND event_time >= %(since)s", parameters={"since": since}).result_rows[0][0]
    return out, parts


def w3(pg, ch, ids):
    cur, out = pg.cursor(), []
    reads = {
        "W3 danh sách cảnh báo mới nhất (50)": (
            "SELECT alert_id, rule_code, account_id, period_start, reason FROM bench_ops.alert "
            "WHERE status = 'NEW' ORDER BY detected_at DESC, alert_id LIMIT 50",
            "SELECT alert_id, rule_code, account_id, period_start, reason FROM bench.ops_alert FINAL "
            "WHERE status = 'NEW' ORDER BY detected_at DESC, alert_id LIMIT 50"),
        "W3 1 cảnh báo + lịch sử xử lý": (
            "SELECT a.alert_id, a.status, h.from_status, h.to_status FROM bench_ops.alert a "
            "LEFT JOIN bench_ops.alert_action h USING (alert_id) WHERE a.alert_id = {a} ORDER BY h.action_id",
            "SELECT a.alert_id, a.status, h.from_status, h.to_status FROM (SELECT * FROM bench.ops_alert FINAL "
            "WHERE alert_id = {a}) AS a LEFT JOIN bench.ops_alert_action AS h USING (alert_id) ORDER BY h.acted_at")}
    for label, (pg_sql, ch_sql) in reads.items():
        sample = ids[:READS]
        out.append(stats(label, "postgres", timed_loop(sample, lambda a: (cur.execute(pg_sql.format(a=a)),
                                                                           cur.fetchall()))))
        out.append(stats(label, "ch_versioned", timed_loop(sample, lambda a: ch.query(ch_sql.format(a=a)).result_rows)))
    return out


def integrity(pg, ch, alert_id):
    """What each store guarantees by itself, without help from the application."""
    cur, rows = pg.cursor(), []

    def pg_refuses(sql, params=()):
        try:
            cur.execute("BEGIN")
            cur.execute(sql, params)
            cur.execute("ROLLBACK")
            return False
        except Exception:  # noqa: BLE001 - the database must refuse
            cur.execute("ROLLBACK")
            return True

    def ch_refuses(sql):
        try:
            ch.command(sql)
            return False
        except Exception:  # noqa: BLE001
            return True

    rows.append(["I1 trạng thái không hợp lệ ('DONE')",
                 pg_refuses("UPDATE bench_ops.alert SET status = 'DONE' WHERE alert_id = %s", (alert_id,)),
                 ch_refuses(f"""INSERT INTO bench.ops_alert SELECT alert_id, rule_code, account_id, period_start,
                     period_end, reason, evidence, transaction_ids, detected_at, 'DONE', now64(3)
                     FROM bench.ops_alert FINAL WHERE alert_id = {alert_id}""")])
    rows.append(["I2 chuyển trạng thái sai (RESOLVED → NEW)",
                 pg_refuses("SELECT bench_ops.set_alert_status(%s, 'NEW', 'x', 'bench')", (alert_id,)),
                 ch_refuses(f"""INSERT INTO bench.ops_alert SELECT alert_id, rule_code, account_id, period_start,
                     period_end, reason, evidence, transaction_ids, detected_at, 'NEW', now64(3)
                     FROM bench.ops_alert FINAL WHERE alert_id = {alert_id}""")])
    rows.append(["I3 cảnh báo trùng (cùng tài khoản, quy tắc, kỳ)",
                 pg_refuses("""INSERT INTO bench_ops.alert (rule_code, account_id, period_start, period_end, reason,
                     evidence) SELECT rule_code, account_id, period_start, period_end, reason, evidence
                     FROM bench_ops.alert WHERE alert_id = %s""", (alert_id,)),
                 ch_refuses(f"""INSERT INTO bench.ops_alert SELECT alert_id + 1000000, rule_code, account_id,
                     period_start, period_end, reason, evidence, transaction_ids, detected_at, 'NEW', now64(3)
                     FROM bench.ops_alert FINAL WHERE alert_id = {alert_id}""")])
    rows.append(["I4 lịch sử cho cảnh báo không tồn tại (khóa ngoại)",
                 pg_refuses("""INSERT INTO bench_ops.alert_action (alert_id, from_status, to_status)
                               VALUES (99999999, 'NEW', 'IN_REVIEW')"""),
                 ch_refuses("""INSERT INTO bench.ops_alert_action VALUES
                               (rand64(), 99999999, 'NEW', 'IN_REVIEW', '', 'bench', now64(3))""")])
    # I5 atomicity: status change succeeds, recording the action fails -> is the status rolled back?
    cur.execute("SELECT status FROM bench_ops.alert WHERE alert_id = %s", (alert_id,))
    before = cur.fetchone()[0]
    try:
        cur.execute("BEGIN")
        cur.execute("UPDATE bench_ops.alert SET status = 'IN_REVIEW' WHERE alert_id = %s", (alert_id,))
        cur.execute("INSERT INTO bench_ops.alert_action (alert_id, from_status, to_status) VALUES (%s, %s, 'BAD')",
                    (alert_id, before))
        cur.execute("COMMIT")
    except Exception:  # noqa: BLE001 - the action insert violates the CHECK
        cur.execute("ROLLBACK")
    cur.execute("SELECT status FROM bench_ops.alert WHERE alert_id = %s", (alert_id,))
    pg_atomic = cur.fetchone()[0] == before
    ch_before = ch.query(f"SELECT status FROM bench.ops_alert FINAL WHERE alert_id = {alert_id}").result_rows[0][0]
    target = "IN_REVIEW" if ch_before != "IN_REVIEW" else "RESOLVED"
    ch.command(f"""INSERT INTO bench.ops_alert SELECT alert_id, rule_code, account_id, period_start, period_end,
        reason, evidence, transaction_ids, detected_at, '{target}', now64(3)
        FROM bench.ops_alert FINAL WHERE alert_id = {alert_id}""")
    ch_refuses("INSERT INTO bench.ops_alert_action VALUES (rand64(), 'not-a-number', 'x', 'y', '', '', now64(3))")
    ch_atomic = ch.query(f"SELECT status FROM bench.ops_alert FINAL WHERE alert_id = {alert_id}"
                         ).result_rows[0][0] == ch_before
    rows.append(["I5 đổi trạng thái + ghi lịch sử là một giao dịch (lỗi ở bước 2 → hoàn tác bước 1)",
                 pg_atomic, ch_atomic])
    return rows


def main():
    pg, ch = setup()
    cur = pg.cursor()
    cur.execute(f"SELECT alert_id FROM bench_ops.alert ORDER BY alert_id LIMIT {N}")
    ids = [r[0] for r in cur.fetchall()]
    lat = w1(pg, ch, ids)
    steps, parts = w2(pg, ch)
    lat += steps + w3(pg, ch, ids)
    for r in lat:
        print(f"{r[0]:<45} {r[1]:<13} n={r[2]:<4} median {r[3]:8.2f}  p95 {r[4]:8.2f}  total {r[5]:9.1f} ms")
    print(f"ClickHouse parts created by W2: {parts} (one per INSERT)")
    integ = integrity(pg, ch, ids[0])
    for r in integ:
        print(f"{r[0]:<80} postgres {'chặn' if r[1] else 'KHÔNG chặn'} | clickhouse {'chặn' if r[2] else 'KHÔNG chặn'}")
    with open(f"{OUT}/ops_latency.csv", "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["workload", "store", "n", "median_ms", "p95_ms", "total_ms"])
        w.writerows(lat)
    with open(f"{OUT}/ops_integrity.csv", "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["guarantee", "postgres_enforced", "clickhouse_enforced"])
        w.writerows(integ)
    json.dump({"clickhouse_parts_created_by_w2": parts, "n": N, "reads": READS},
              open(f"{OUT}/ops_meta.json", "w"), indent=1)


if __name__ == "__main__":
    main()
