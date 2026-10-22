"""E. Operational store writer: batches, runs, steps and rejected records (BR05, BR06)."""
import json
import os
from contextlib import contextmanager

from psycopg2.extras import execute_values

from common import postgres


class RunLog:
    """One pipeline run of one batch. Use `with log.step("name") as s:` around every step and
    fill s["rows_in"], s["rows_out"], s["rows_rejected"]; a failing step marks the run FAILED."""

    def __init__(self, batch_id, batch_type, description, period_from=None, period_to=None,
                 lake_path=None, scenario_code=None, triggered_by=None):
        # "app" when the runner started the job from the application (F -> C), else "cli"
        triggered_by = triggered_by or os.environ.get("BANKDW_TRIGGERED_BY", "cli")
        self.batch_id = batch_id
        self.conn = postgres()
        with self.conn.cursor() as cur:
            cur.execute("""INSERT INTO ops.batch (batch_id, batch_type, scenario_code, description,
                                                  period_from, period_to, lake_path)
                           VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (batch_id) DO NOTHING""",
                        (batch_id, batch_type, scenario_code, description, period_from, period_to,
                         lake_path))
            cur.execute("SELECT count(*) FROM ops.pipeline_run WHERE batch_id = %s", (batch_id,))
            kind = "RERUN" if cur.fetchone()[0] else "RUN"
            cur.execute("""INSERT INTO ops.pipeline_run (batch_id, run_kind, triggered_by)
                           VALUES (%s, %s, %s) RETURNING run_id""", (batch_id, kind, triggered_by))
            self.run_id = cur.fetchone()[0]
        self.kind = kind
        self.order = 0
        self.rows_received = self.rows_loaded = self.rows_rejected = 0

    @contextmanager
    def step(self, name):
        self.order += 1
        with self.conn.cursor() as cur:
            cur.execute("""INSERT INTO ops.pipeline_step (run_id, step_order, step_name)
                           VALUES (%s, %s, %s) RETURNING step_id""", (self.run_id, self.order, name))
            step_id = cur.fetchone()[0]
        stats = {"rows_in": None, "rows_out": None, "rows_rejected": None}
        try:
            yield stats
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"[:2000]
            with self.conn.cursor() as cur:
                cur.execute("""UPDATE ops.pipeline_step SET status = 'FAILED', finished_at = now(),
                               rows_in = %s, error_message = %s WHERE step_id = %s""",
                            (stats["rows_in"], msg, step_id))
                cur.execute("""UPDATE ops.pipeline_run SET status = 'FAILED', finished_at = now(),
                               failed_step = %s, error_message = %s, rows_received = %s,
                               rows_loaded = %s, rows_rejected = %s WHERE run_id = %s""",
                            (name, msg, self.rows_received, self.rows_loaded, self.rows_rejected,
                             self.run_id))
            raise
        with self.conn.cursor() as cur:
            cur.execute("""UPDATE ops.pipeline_step SET status = 'SUCCEEDED', finished_at = now(),
                           rows_in = %s, rows_out = %s, rows_rejected = %s WHERE step_id = %s""",
                        (stats["rows_in"], stats["rows_out"], stats["rows_rejected"], step_id))

    def reject(self, records):
        """records: iterable of (source_table, source_key, rule_code, reason, record_dict)."""
        rows = [(self.run_id, self.batch_id, t, k, r, why, json.dumps(rec, default=str))
                for t, k, r, why, rec in records]
        if rows:
            with self.conn.cursor() as cur:
                execute_values(cur, """INSERT INTO ops.rejected_record
                    (run_id, batch_id, source_table, source_key, rule_code, reason, record)
                    VALUES %s""", rows)
        self.rows_rejected += len(rows)
        return len(rows)

    def finish(self):
        status = "SUCCEEDED_WITH_REJECTS" if self.rows_rejected else "SUCCEEDED"
        with self.conn.cursor() as cur:
            cur.execute("""UPDATE ops.pipeline_run SET status = %s, finished_at = now(),
                           rows_received = %s, rows_loaded = %s, rows_rejected = %s
                           WHERE run_id = %s""",
                        (status, self.rows_received, self.rows_loaded, self.rows_rejected,
                         self.run_id))
        self.conn.close()
        return status
