"""BR03: rule-based alerts, computed from the warehouse after a load (sheet flow L03: D -> C -> E).

Rules and parameters live in ops.alert_rule (decision Q-F), so thresholds can be tuned without a
code change:
  R1  outflow of an account in a month > multiplier x its average monthly outflow of the previous
      lookback_months months, and > min_outflow_czk; needs lookback_months of history
  R2  an outgoing transaction after >= dormant_days without any transaction on the account
Alerts are unique per (account, rule, period): a re-run or an overlapping batch adds nothing.
"""
import json

from psycopg2.extras import execute_values

from common import clickhouse, postgres

FACT = """(SELECT f.transaction_id, f.account_sk, f.date_key, toDate(toString(f.date_key)) AS d,
                  f.amount, p.cash_flow_direction AS dir, f.load_batch_id
           FROM dw.fact_transaction AS f FINAL
           JOIN (SELECT transaction_profile_sk, cash_flow_direction
                 FROM dw.dim_transaction_profile FINAL) AS p USING (transaction_profile_sk)
           WHERE {accounts})"""
# Window functions over the whole fact table exceed ClickHouse's 2.2 GB on this machine: a batch
# only reads the accounts it touched, and a full back-fill may sort on disk.
SETTINGS = {"max_threads": 2, "max_bytes_before_external_sort": 300_000_000,
            "max_bytes_before_external_group_by": 300_000_000}


def _fact(batch_id):
    if batch_id is None:
        return FACT.format(accounts="1")
    return FACT.format(accounts=f"""f.account_sk IN (SELECT account_sk FROM dw.fact_transaction
                                                     WHERE load_batch_id = '{batch_id}')""")
ACCOUNTS = "(SELECT account_sk, account_id FROM dw.dim_account FINAL)"


def _in_batch(batch_id, what):
    """SQL filter limiting a rule to what the batch touched (None = the whole warehouse)."""
    if batch_id is None:
        return "1"
    if what == "month":     # R1: the account-months that received transactions from the batch
        return (f"(w.account_sk, w.month) IN (SELECT account_sk, toStartOfMonth(d) FROM {_fact(batch_id)} "
                f"WHERE load_batch_id = '{batch_id}')")
    return (f"t.transaction_id IN (SELECT transaction_id FROM {_fact(batch_id)} "   # R2: the batch's transactions
            f"WHERE load_batch_id = '{batch_id}')")


def r1(ch, p, batch_id):
    return ch.query(f"""
        WITH m AS (
          SELECT account_sk, toStartOfMonth(d) AS month, sumIf(amount, dir = 'OUT') AS out_m,
                 groupArrayIf(transaction_id, dir = 'OUT') AS out_ids
          FROM {_fact(batch_id)} GROUP BY account_sk, month),
        w AS (
          SELECT account_sk, month, out_m, out_ids,
                 sum(out_m) OVER (PARTITION BY account_sk ORDER BY month
                                  RANGE BETWEEN {p['lookback_months'] * 31} PRECEDING AND 1 PRECEDING)
                   / {p['lookback_months']} AS avg_out,
                 min(month) OVER (PARTITION BY account_sk) AS first_month
          FROM m)
        SELECT a.account_id, w.month, toLastDayOfMonth(w.month), w.out_m, w.avg_out, w.out_ids
        FROM w JOIN {ACCOUNTS} AS a USING (account_sk)
        WHERE w.out_m > {p['multiplier']} * w.avg_out AND w.out_m > {p['min_outflow_czk']}
          AND w.month >= addMonths(w.first_month, {p['lookback_months']})
          AND {_in_batch(batch_id, "month")}
    """, settings=SETTINGS).result_rows


def r2(ch, p, batch_id):
    return ch.query(f"""
        SELECT a.account_id, toStartOfMonth(t.d), toLastDayOfMonth(t.d), t.transaction_id, t.d,
               t.prev_d, t.gap, t.amount
        FROM (SELECT *, lagInFrame(d) OVER w AS prev_d, dateDiff('day', lagInFrame(d) OVER w, d) AS gap,
                     row_number() OVER w AS rn
              FROM {_fact(batch_id)}
              WINDOW w AS (PARTITION BY account_sk ORDER BY date_key, transaction_id
                           ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)) AS t
        JOIN {ACCOUNTS} AS a USING (account_sk)
        WHERE t.rn > 1 AND t.dir = 'OUT' AND t.gap >= {p['dormant_days']} AND {_in_batch(batch_id, "txn")}
    """, settings=SETTINGS).result_rows


def detect(batch_id=None, run_id=None):
    """Write new alerts to ops.alert; returns {rule: new alerts}."""
    ch, pg = clickhouse(), postgres()
    with pg.cursor() as cur:
        cur.execute("SELECT rule_code, parameters FROM ops.alert_rule")
        params = dict(cur.fetchall())
    rows = []
    for acc, start, end, out_m, avg_out, ids in r1(ch, params["R1"], batch_id):
        ratio = float(out_m) / float(avg_out) if avg_out else None
        reason = (f"Tiền ra tháng {start:%m/%Y}: {float(out_m):,.0f} CZK, gấp "
                  f"{ratio:.1f} lần trung bình {params['R1']['lookback_months']} tháng trước "
                  f"({float(avg_out):,.0f} CZK)" if ratio else
                  f"Tiền ra tháng {start:%m/%Y}: {float(out_m):,.0f} CZK, 6 tháng trước không có tiền ra")
        evidence = {"outflow_month": float(out_m), "avg_outflow_previous": float(avg_out),
                    "ratio": round(ratio, 2) if ratio else None, **params["R1"]}
        rows.append(("R1", acc, start, end, reason, json.dumps(evidence), list(ids)))
    for acc, start, end, tid, d, prev_d, gap, amount in r2(ch, params["R2"], batch_id):
        reason = (f"Tài khoản không giao dịch {gap} ngày (từ {prev_d:%d/%m/%Y}) rồi có giao dịch ra "
                  f"{float(amount):,.0f} CZK ngày {d:%d/%m/%Y}")
        evidence = {"previous_transaction": str(prev_d), "transaction_date": str(d), "gap_days": gap,
                    "amount": float(amount), **params["R2"]}
        rows.append(("R2", acc, start, end, reason, json.dumps(evidence), [tid]))
    with pg.cursor() as cur:
        inserted = execute_values(cur, """
            INSERT INTO ops.alert (rule_code, account_id, period_start, period_end, reason, evidence,
                                   transaction_ids, run_id, batch_id)
            VALUES %s ON CONFLICT (account_id, rule_code, period_start) DO NOTHING
            RETURNING rule_code""",
            [r + (run_id, batch_id) for r in rows], fetch=True)
    pg.close()
    counts = {}
    for (rule,) in inserted:
        counts[rule] = counts.get(rule, 0) + 1
    return counts, len(rows)
