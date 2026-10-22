"""BR07: back up and restore the whole warehouse (sheet flow L06).

    python3 backup.py backup  [name]      # BACKUP DATABASE dw -> ClickHouse disk 'backups' (./backups)
    python3 backup.py restore <name>      # RESTORE the 6 tables to that point
    python3 backup.py list

The backup holds structure and data of the 6 warehouse tables, SCD2 history included. It only runs
while the pipeline is not writing (no RUNNING run in ops.pipeline_run). The catalog, the figures
used for reconciliation and the result of every operation are kept in PostgreSQL (ops.backup_*);
PostgreSQL itself is outside the backup scope (sheet T02).
"""
import datetime as dt
import json
import sys

from common import clickhouse, postgres

TABLES = ["dim_date", "dim_district", "dim_account", "dim_customer", "dim_transaction_profile",
          "fact_transaction"]


FIGURE_SETTINGS = {"max_threads": 2}


def free_memory(ch):
    """A restore leaves ClickHouse close to its 2.2 GB limit on this machine: drop caches first."""
    for stmt in ("SYSTEM DROP MARK CACHE", "SYSTEM DROP UNCOMPRESSED CACHE", "SYSTEM JEMALLOC PURGE"):
        try:
            ch.command(stmt)
        except Exception:  # noqa: BLE001 - best effort, not available on every server
            pass


def snapshot_figures(ch):
    """Figures compared before/after (sheet: số dòng, tiền vào/ra, báo cáo theo hạng, phiên bản SCD)."""
    free_memory(ch)
    q = lambda sql: ch.query(sql, settings=FIGURE_SETTINGS).result_rows  # noqa: E731
    # dim_date is a plain MergeTree (no FINAL); the other tables are ReplacingMergeTree
    rows = {t: int(q(f"SELECT count() FROM dw.{t}{'' if t == 'dim_date' else ' FINAL'}")[0][0])
            for t in TABLES}
    money = q("""
        SELECT sumIf(f.amount, p.cash_flow_direction = 'IN'), sumIf(f.amount, p.cash_flow_direction = 'OUT')
        FROM dw.fact_transaction AS f FINAL
        JOIN (SELECT * FROM dw.dim_transaction_profile FINAL) AS p USING (transaction_profile_sk)""")[0]
    by_tier = {r[0]: {"transactions": int(r[1]), "money_in": str(r[2]), "money_out": str(r[3])}
               for r in q("""
        SELECT c.membership_tier, count(), sumIf(f.amount, p.cash_flow_direction = 'IN'),
               sumIf(f.amount, p.cash_flow_direction = 'OUT')
        FROM dw.fact_transaction AS f FINAL
        JOIN (SELECT customer_sk, membership_tier FROM dw.dim_customer FINAL) AS c USING (customer_sk)
        JOIN (SELECT * FROM dw.dim_transaction_profile FINAL) AS p USING (transaction_profile_sk)
        GROUP BY c.membership_tier ORDER BY 1""")}
    scd = q("SELECT count(), countIf(is_current = 1), countIf(is_current = 0) FROM dw.dim_customer FINAL")[0]
    batches = [r[0] for r in q("SELECT DISTINCT load_batch_id FROM dw.fact_transaction "
                               "WHERE load_batch_id NOT LIKE 'HIST_%' ORDER BY 1")]
    return {"rows": rows, "money_in": str(money[0]), "money_out": str(money[1]), "by_tier": by_tier,
            "scd_versions": {"total": int(scd[0]), "current": int(scd[1]), "closed": int(scd[2])},
            "non_history_batches": batches}


def pipeline_idle(cur):
    cur.execute("SELECT count(*) FROM ops.pipeline_run WHERE status = 'RUNNING' "
                "AND started_at > now() - interval '1 hour'")
    return cur.fetchone()[0] == 0


def backup(name=None):
    name = name or "dw_" + dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    ch, pg = clickhouse(), postgres()
    with pg.cursor() as cur:
        if not pipeline_idle(cur):
            raise SystemExit("pipeline is writing: backup refused (sheet L06)")
        figures = snapshot_figures(ch)
        cur.execute("""INSERT INTO ops.backup_catalog (backup_id, status, tables, row_counts, totals)
                       VALUES (%s, 'CREATING', %s, %s, %s)""",
                    (name, TABLES, json.dumps(figures["rows"]), json.dumps(figures)))
        cur.execute("INSERT INTO ops.backup_operation (backup_id, op_type, status) "
                    "VALUES (%s, 'BACKUP', 'RUNNING') RETURNING op_id", (name,))
        op = cur.fetchone()[0]
        try:
            ch.command(f"BACKUP DATABASE dw TO Disk('backups', '{name}')")
        except Exception as e:
            cur.execute("UPDATE ops.backup_catalog SET status = 'FAILED' WHERE backup_id = %s", (name,))
            cur.execute("UPDATE ops.backup_operation SET status = 'FAILED', finished_at = now(), "
                        "error_message = %s WHERE op_id = %s", (str(e)[:2000], op))
            raise
        cur.execute("UPDATE ops.backup_catalog SET status = 'AVAILABLE' WHERE backup_id = %s", (name,))
        cur.execute("UPDATE ops.backup_operation SET status = 'SUCCEEDED', finished_at = now(), "
                    "result = %s WHERE op_id = %s", (json.dumps(figures), op))
    print(f"backup {name}: AVAILABLE | rows {figures['rows']}")
    return name, figures


def restore(name):
    ch, pg = clickhouse(), postgres()
    with pg.cursor() as cur:
        if not pipeline_idle(cur):
            raise SystemExit("pipeline is writing: restore refused (sheet L06)")
        cur.execute("SELECT totals FROM ops.backup_catalog WHERE backup_id = %s AND status = 'AVAILABLE'",
                    (name,))
        found = cur.fetchone()
        if not found:
            raise SystemExit(f"backup {name} is not available")
        expected = found[0]
        before = snapshot_figures(ch)
        cur.execute("INSERT INTO ops.backup_operation (backup_id, op_type, status) "
                    "VALUES (%s, 'RESTORE', 'RUNNING') RETURNING op_id", (name,))
        op = cur.fetchone()[0]
        try:
            # the restored tables replace the current ones entirely (no mixing of two states)
            for t in TABLES:
                ch.command(f"DROP TABLE IF EXISTS dw.{t} SYNC")
            tables = ", ".join(f"TABLE dw.{t}" for t in TABLES)
            ch.command(f"RESTORE {tables} FROM Disk('backups', '{name}')")
            after = snapshot_figures(ch)
        except Exception as e:
            cur.execute("UPDATE ops.backup_operation SET status = 'FAILED', finished_at = now(), "
                        "error_message = %s WHERE op_id = %s", (str(e)[:2000], op))
            raise
        match = {k: after[k] == expected[k] for k in ("rows", "money_in", "money_out", "by_tier",
                                                      "scd_versions")}
        result = {"before_restore": before, "after_restore": after, "backup_figures": expected,
                  "matches_backup": match}
        status = "SUCCEEDED" if all(match.values()) else "FAILED"
        cur.execute("UPDATE ops.backup_operation SET status = %s, finished_at = now(), result = %s, "
                    "error_message = %s WHERE op_id = %s",
                    (status, json.dumps(result), None if status == "SUCCEEDED" else "figures differ", op))
    print(f"restore {name}: {status} | matches backup {match}")
    print(f"  rows before {before['rows']['fact_transaction']:,} -> after {after['rows']['fact_transaction']:,} "
          f"(backup {expected['rows']['fact_transaction']:,})")
    return status == "SUCCEEDED"


def list_backups():
    with postgres().cursor() as cur:
        cur.execute("SELECT backup_id, status, created_at, row_counts->>'fact_transaction' "
                    "FROM ops.backup_catalog ORDER BY created_at")
        for r in cur.fetchall():
            print(*r, sep="\t")


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "backup":
        backup(sys.argv[2] if len(sys.argv) > 2 else None)
    elif cmd == "restore":
        sys.exit(0 if restore(sys.argv[2]) else 1)
    else:
        list_backups()
