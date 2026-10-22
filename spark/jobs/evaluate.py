"""P2-8 Data correctness: the 8 evaluation scenarios KB01-KB08 and the data-quality checks.

Every KB is checked against something independent of the warehouse:
  * the SOURCE: Berka trans.csv + the scenario input files, recomputed here with pandas (not Spark,
    not ClickHouse), owner and tier taken from disp.csv and the tier files (sheet: "đối chiếu tại nguồn");
  * the scenario's expected.json, written by tools/make_scenarios.py before anything was loaded;
  * the operational store (ops.*) for alerts, rejects, runs and backups.
Data-quality dimensions reuse the Phase 1 frame (ISO/IEC 25012): completeness, uniqueness,
accuracy (= source reconciliation), consistency (foreign keys, SCD2).

    python3 evaluate.py            # writes /lake/eval/kb_results.json and prints one line per check
Read-only on ClickHouse; KB04 runs its status changes inside a transaction that is rolled back.
"""
import datetime as dt
import json
import os
import sys

import pandas as pd

from backup import snapshot_figures
from common import SOURCE, SOURCE_RAW, clickhouse, postgres

OUT = "/lake/eval"
SCEN = f"{SOURCE}/scenarios"
OPEN_END = dt.date(2299, 12, 31)
NEW_PERIOD = 19990101          # tier changes of KB08 take effect on this date
CHECKS = []


def check(kb, name, ok, expected=None, actual=None, detail=""):
    CHECKS.append({"kb": kb, "check": name, "status": "PASS" if ok else "FAIL",
                   "expected": expected, "actual": actual, "detail": detail})
    print(f"[{'PASS' if ok else 'FAIL'}] {kb:<5} {name}" + (f" | {detail}" if detail else ""), flush=True)


def expected(code):
    return json.load(open(f"{SCEN}/{code}/expected.json"))


def cents(series):
    return (series.astype(float) * 100).round().astype("int64")


# ------------------------------------------------------------------------------------------ source
def source_transactions():
    """Berka + the scenario rows that should be in the warehouse now, with owner tier at each date."""
    cols = ["trans_id", "account_id", "date", "type", "amount"]
    raw = pd.read_csv(f"{SOURCE_RAW}/trans.csv", sep=";", usecols=cols, low_memory=False)
    raw["date_key"] = 19000000 + raw["date"].astype(int)
    parts = [raw]
    keep = {"KB03": None, "KB05": set(expected("KB05")["loaded_transaction_ids"]), "KB08": None}
    for code, ids in keep.items():       # KB07 rows were removed by the restore (BR07)
        s = pd.read_csv(f"{SCEN}/{code}/trans.csv", sep=";", dtype=str, keep_default_na=False)
        if ids:   # KB05 repeats id 90000001 on purpose (V6): the pipeline keeps the first row
            s = s[s.trans_id.isin({str(i) for i in ids})].drop_duplicates("trans_id", keep="first")
        s = s.assign(trans_id=s.trans_id.astype(int), account_id=s.account_id.astype(int),
                     date_key=19000000 + s.date.astype(int))      # YYMMDD, all scenario dates are 199x
        parts.append(s[["trans_id", "account_id", "date_key", "type", "amount"]])
    t = pd.concat([p[["trans_id", "account_id", "date_key", "type", "amount"]] for p in parts],
                  ignore_index=True)
    t["type"] = t["type"].str.strip().str.upper()
    t["cents"] = cents(t["amount"])
    t["dir"] = t["type"].map(lambda x: "IN" if x == "PRIJEM" else "OUT")

    disp = pd.read_csv(f"{SOURCE_RAW}/disp.csv", sep=";")
    owner = disp[disp.type == "OWNER"][["account_id", "client_id"]]
    tier0 = pd.read_csv(f"{SOURCE}/customer_profile_simulated.csv")[["client_id", "membership_tier"]]
    tier1 = pd.read_csv(f"{SCEN}/KB08/customer_profile_changes.csv")[["client_id", "membership_tier"]]
    t = t.merge(owner, on="account_id", how="left")
    t = t.merge(tier0, on="client_id", how="left")
    t = t.merge(tier1.rename(columns={"membership_tier": "tier_new"}), on="client_id", how="left")
    t["tier"] = t["membership_tier"].where(t.tier_new.isna() | (t.date_key < NEW_PERIOD), t.tier_new)
    return t


def src_totals(t, lo, hi, by=None):
    p = t[(t.date_key >= lo) & (t.date_key <= hi)]
    if by is None:
        return {"transactions": len(p), "in": int(p.cents[p.dir == "IN"].sum()),
                "out": int(p.cents[p.dir == "OUT"].sum())}
    out = {}
    for k, g in p.groupby(by):
        out[k] = {"transactions": len(g), "in": int(g.cents[g.dir == "IN"].sum()),
                  "out": int(g.cents[g.dir == "OUT"].sum()), "clients": int(g.client_id.nunique())}
    return out


# --------------------------------------------------------------------------------------- warehouse
BASE = """FROM dw.fact_transaction AS f
          JOIN (SELECT transaction_profile_sk, cash_flow_direction AS dir
                FROM dw.dim_transaction_profile FINAL) AS p USING (transaction_profile_sk)
          JOIN (SELECT customer_sk, client_id, membership_tier FROM dw.dim_customer FINAL) AS c
               USING (customer_sk)"""


def dw_totals(ch, lo, hi, by=None):
    cols = ("count(), toInt64(sumIf(amount, dir = 'IN') * 100), toInt64(sumIf(amount, dir = 'OUT') * 100), "
            "uniqExact(client_id)")
    where = f"WHERE f.date_key BETWEEN {lo} AND {hi}"
    if by is None:
        n, i, o, _ = ch.query(f"SELECT {cols} {BASE} {where}").result_rows[0]
        return {"transactions": n, "in": i, "out": o}
    return {r[0]: {"transactions": r[1], "in": r[2], "out": r[3], "clients": r[4]}
            for r in ch.query(f"SELECT membership_tier, {cols} {BASE} {where} GROUP BY 1").result_rows}


# ----------------------------------------------------------------------------------------- the KBs
def kb01(ch, src):
    for label, lo, hi in (("1993-1998 (lịch sử)", 19930101, 19981231), ("năm 1998", 19980101, 19981231),
                          ("01/1999 (kỳ sau KB08)", 19990101, 19990131), ("toàn bộ", 19000101, 22991231)):
        s, d = src_totals(src, lo, hi), dw_totals(ch, lo, hi)
        check("KB01", f"BR01 {label}: số giao dịch, tiền vào, tiền ra = nguồn", s == d, s, d)
        s, d = src_totals(src, lo, hi, "tier"), dw_totals(ch, lo, hi, "tier")
        check("KB01", f"BR01 {label}: theo hạng (giao dịch, vào, ra, số khách) = nguồn", s == d, s, d)


def kb02(ch, src):
    accounts = [11, 14, 2378] + sorted(src.account_id.drop_duplicates().sample(20, random_state=27))
    bad = []
    for a in accounts:
        s = src[src.account_id == a]
        rows = ch.query(f"""SELECT f.transaction_id, toInt64(f.amount * 100), p.dir
            FROM dw.fact_transaction AS f
            JOIN (SELECT account_sk FROM dw.dim_account FINAL WHERE account_id = {a}) AS a USING (account_sk)
            JOIN (SELECT transaction_profile_sk, cash_flow_direction AS dir FROM dw.dim_transaction_profile FINAL)
                 AS p USING (transaction_profile_sk)""").result_rows
        if sorted(rows) != sorted(zip(s.trans_id, s.cents, s.dir)):
            bad.append(a)
    check("KB02", f"BR02 tra cứu {len(accounts)} tài khoản: đúng từng giao dịch (id, số tiền, chiều) = nguồn",
          not bad, 0, len(bad), f"tài khoản lệch: {bad}" if bad else f"tài khoản: {accounts[:3]} + 20 ngẫu nhiên")
    # BR02 period filter on the KB03 account
    s = src[(src.account_id == 11) & (src.date_key.between(19990101, 19991231))]
    n = ch.query("""SELECT count() FROM dw.fact_transaction AS f
        JOIN (SELECT account_sk FROM dw.dim_account FINAL WHERE account_id = 11) AS a USING (account_sk)
        WHERE date_key BETWEEN 19990101 AND 19991231""").result_rows[0][0]
    check("KB02", "BR02 lọc theo kỳ (tài khoản 11, năm 1999) = nguồn", n == len(s), len(s), n)


def kb03(ch, cur):
    exp = expected("KB03")
    cur.execute("""SELECT rule_code, account_id, period_start::text, evidence FROM ops.alert
                   WHERE batch_id = 'KB03_FRAUD' ORDER BY account_id""")
    got = cur.fetchall()
    want = sorted((a["rule"], a["account_id"], a["period_start"]) for a in exp["alerts"])
    check("KB03", "BR03 cảnh báo sinh ra = mong đợi (quy tắc, tài khoản, kỳ)",
          sorted(r[:3] for r in got) == want, len(want), len(got))
    ev = {r[1]: r[3] for r in got}
    diffs = []
    for a in exp["alerts"]:
        e = ev.get(a["account_id"], {})
        if a["rule"] == "R1" and (abs(e.get("outflow_month", 0) - a["outflow"]) > 0.02 or
                                  abs(e.get("avg_outflow_previous", 0) - a["avg_outflow_previous"]) > 0.02):  # rounded to cents
            diffs.append(a["account_id"])
        if a["rule"] == "R2" and e.get("gap_days") != a["gap_days"]:
            diffs.append(a["account_id"])
    check("KB03", "BR03 căn cứ (tiền ra tháng, trung bình 6 tháng, số ngày ngủ đông) = mong đợi", not diffs,
          detail=f"lệch: {diffs}" if diffs else "")
    cur.execute("SELECT count(*) FROM ops.pipeline_run WHERE batch_id = 'KB03_FRAUD'")
    runs = cur.fetchone()[0]
    check("KB03", f"Chạy lại không sinh cảnh báo trùng ({runs} lần chạy, vẫn {len(got)} cảnh báo)",
          len(got) == len(want) and runs > 1, len(want), len(got))
    cur.execute("SELECT unnest(transaction_ids) FROM ops.alert WHERE batch_id = 'KB03_FRAUD'")
    tids = [r[0] for r in cur.fetchall()]
    n = ch.query(f"SELECT count() FROM dw.fact_transaction WHERE transaction_id IN ({','.join(map(str, tids))})"
                 ).result_rows[0][0] if tids else 0
    check("KB03", "Mở được giao dịch liên quan của mọi cảnh báo trong kho", n == len(tids), len(tids), n)


def kb04(pg):
    """Status flow on a real alert, in a transaction that is rolled back (nothing is kept)."""
    pg.autocommit = False
    cur = pg.cursor()
    try:
        cur.execute("SELECT alert_id FROM ops.alert WHERE status = 'NEW' ORDER BY alert_id LIMIT 1")
        aid = cur.fetchone()[0]
        for to in ("IN_REVIEW", "RESOLVED"):
            cur.execute("SELECT ops.set_alert_status(%s, %s, %s, 'evaluate')", (aid, to, f"KB04 -> {to}"))
        cur.execute("SELECT status FROM ops.alert WHERE alert_id = %s", (aid,))
        final = cur.fetchone()[0]
        cur.execute("SELECT from_status || '->' || to_status FROM ops.alert_action "
                    "WHERE alert_id = %s AND acted_by = 'evaluate' ORDER BY action_id", (aid,))
        history = [r[0] for r in cur.fetchall()]
        check("KB04", "BR04 NEW → IN_REVIEW → RESOLVED, mỗi bước có ghi chú trong lịch sử",
              final == "RESOLVED" and history == ["NEW->IN_REVIEW", "IN_REVIEW->RESOLVED"],
              ["NEW->IN_REVIEW", "IN_REVIEW->RESOLVED"], history)
        cur.execute("SAVEPOINT bad")
        try:
            cur.execute("SELECT ops.set_alert_status(%s, 'NEW', 'không hợp lệ', 'evaluate')", (aid,))
            refused = False
        except Exception:  # noqa: BLE001 - the database must refuse
            refused = True
            cur.execute("ROLLBACK TO SAVEPOINT bad")
        check("KB04", "BR04 chuyển trạng thái không hợp lệ (RESOLVED → NEW) bị từ chối", refused)
    finally:
        pg.rollback()
        pg.autocommit = True
    check("KB04", "BR04 qua giao diện: đã thử trên trình duyệt ở P2-6 (alert 1699, dữ liệu thử đã xoá)", True,
          detail="kiểm tra tay, xem CHANGELOG P2-6")


def kb05(ch, cur):
    exp = expected("KB05")
    cur.execute("""SELECT rule_code, count(*) FROM ops.rejected_record WHERE run_id =
                     (SELECT min(run_id) FROM ops.pipeline_run WHERE batch_id = 'KB05_DATA_ERRORS')
                   GROUP BY 1""")
    got = dict(cur.fetchall())
    check("KB05", "BR05 số bản ghi bị tách theo quy tắc = mong đợi", got == exp["rejected_by_rule"],
          exp["rejected_by_rule"], got)
    ids = sorted(r[0] for r in ch.query(
        "SELECT transaction_id FROM dw.fact_transaction WHERE load_batch_id = 'KB05_DATA_ERRORS'").result_rows)
    check("KB05", "Chỉ 10 giao dịch hợp lệ vào kho, đúng id", ids == exp["loaded_transaction_ids"],
          len(exp["loaded_transaction_ids"]), len(ids))
    cur.execute("""SELECT rows_received, rows_loaded, rows_rejected, status FROM ops.pipeline_run
                   WHERE batch_id = 'KB05_DATA_ERRORS' ORDER BY run_id LIMIT 1""")
    r = cur.fetchone()
    check("KB05", "Lô không hỏng cả lô: nhận 20, nạp 10, tách 10, SUCCEEDED_WITH_REJECTS",
          r == (20, 10, 10, "SUCCEEDED_WITH_REJECTS"), [20, 10, 10, "SUCCEEDED_WITH_REJECTS"], list(r))


def kb06(cur):
    cur.execute("""SELECT count(*), count(*) FILTER (WHERE s.n = 0) FROM ops.pipeline_run r
                   CROSS JOIN LATERAL (SELECT count(*) n FROM ops.pipeline_step WHERE run_id = r.run_id) s
                   WHERE r.status <> 'RUNNING'""")
    runs, no_steps = cur.fetchone()
    check("KB06", f"BR06 mọi lần chạy ({runs}) có bước, trạng thái, thời gian", no_steps == 0, 0, no_steps)
    cur.execute("""SELECT count(*) FROM ops.pipeline_step WHERE status = 'SUCCEEDED'
                   AND (finished_at IS NULL OR rows_out IS NULL)""")
    check("KB06", "Bước thành công có thời gian kết thúc và số đạt", cur.fetchone()[0] == 0)
    cur.execute("""SELECT r.run_id, r.batch_id, r.failed_step, left(r.error_message, 80) FROM ops.pipeline_run r
                   JOIN ops.pipeline_step s ON s.run_id = r.run_id AND s.status = 'FAILED'
                   WHERE r.status = 'FAILED' AND r.error_message IS NOT NULL ORDER BY r.run_id LIMIT 1""")
    f = cur.fetchone()
    check("KB06", "Lần chạy lỗi thấy bước thất bại + nguyên nhân", f is not None,
          detail=f"run {f[0]} {f[1]} bước {f[2]}: {f[3]}" if f else "")
    cur.execute("SELECT count(*) FROM ops.pipeline_run WHERE run_kind = 'RERUN'")
    check("KB06", "Có chạy lại lô (RERUN) và được ghi nhận", cur.fetchone()[0] > 0)


def kb07(ch, cur):
    cur.execute("""SELECT o.status, o.result->'matches_backup', c.totals FROM ops.backup_operation o
                   JOIN ops.backup_catalog c USING (backup_id)
                   WHERE o.op_type = 'RESTORE' ORDER BY o.op_id DESC LIMIT 1""")
    status, match, totals = cur.fetchone()
    check("KB07", "BR07 khôi phục SUCCEEDED, 5 nhóm số liệu khớp bản sao lưu",
          status == "SUCCEEDED" and all(match.values()), True, match)
    n = ch.query("SELECT count() FROM dw.fact_transaction WHERE load_batch_id = 'KB07_AFTER_BACKUP'"
                 ).result_rows[0][0]
    check("KB07", "Lô nạp sau sao lưu (KB07) không còn trong kho", n == 0, 0, n)
    now = snapshot_figures(ch)
    same = {k: now[k] == totals[k] for k in ("rows", "money_in", "money_out", "by_tier", "scd_versions")}
    check("KB07", "Kho hiện tại vẫn bằng số liệu bản sao lưu (sau đó chỉ có chạy lại không thêm dòng)",
          all(same.values()), True, same)


def kb08(ch, src):
    exp = expected("KB08")
    v = ch.query("SELECT count(), countIf(is_current = 1), countIf(is_current = 0) FROM dw.dim_customer FINAL"
                 ).result_rows[0]
    want = (exp["versions_total_after"], exp["current_versions_after"], exp["versions_closed"])
    check("KB08", "SCD2: tổng / hiện tại / đã đóng = mong đợi", tuple(v) == want, list(want), list(v))
    new = dw_totals(ch, 19990101, 19990131, "tier")
    new_kb08 = ch.query(f"""SELECT membership_tier, uniqExact(client_id), sumIf(amount, dir = 'IN'),
                                   sumIf(amount, dir = 'OUT') {BASE}
                            WHERE load_batch_id = 'KB08_TIER_CHANGE' GROUP BY 1""").result_rows
    got = {r[0]: {"clients": r[1], "money_in": float(r[2]), "money_out": float(r[3])} for r in new_kb08}
    check("KB08", "Kỳ mới (giao dịch KB08) tính theo hạng mới = mong đợi", got == exp["new_period_by_new_tier"],
          exp["new_period_by_new_tier"], got)
    s, d = src_totals(src, 19930101, 19981231, "membership_tier"), dw_totals(ch, 19930101, 19981231, "tier")
    check("KB08", "Kỳ cũ 1993-1998 vẫn theo hạng cũ (= nguồn với hạng ban đầu)", s == d, s, d)
    changed = ch.query("""SELECT count() FROM (SELECT client_id FROM dw.dim_customer FINAL
                          GROUP BY client_id HAVING count() = 2 AND groupArray(membership_tier)[1] !=
                          groupArray(membership_tier)[2])""").result_rows[0][0]
    check("KB08", "618 khách có 2 phiên bản với hạng khác nhau", changed == exp["tier_changes"],
          exp["tier_changes"], changed)


# ------------------------------------------------------------------------------ data quality frame
def quality(ch):
    q = lambda sql: ch.query(sql).result_rows[0][0]  # noqa: E731
    n = q("SELECT count() FROM dw.fact_transaction")
    check("DQ", "Uniqueness: transaction_id không trùng trong fact",
          q("SELECT uniqExact(transaction_id) FROM dw.fact_transaction") == n, n,
          q("SELECT uniqExact(transaction_id) FROM dw.fact_transaction"))
    check("DQ", "Completeness: khóa và số tiền của fact không thiếu", q(
        "SELECT countIf(date_key = 0 OR account_sk = 0 OR customer_sk = 0 OR transaction_profile_sk = 0) "
        "FROM dw.fact_transaction") == 0)
    orphans = {
        "date": "SELECT count() FROM dw.fact_transaction WHERE date_key NOT IN (SELECT date_key FROM dw.dim_date)",
        "account": "SELECT count() FROM dw.fact_transaction WHERE account_sk NOT IN "
                   "(SELECT account_sk FROM dw.dim_account FINAL)",
        "customer": "SELECT count() FROM dw.fact_transaction WHERE customer_sk NOT IN "
                    "(SELECT customer_sk FROM dw.dim_customer FINAL)",
        "profile": "SELECT count() FROM dw.fact_transaction WHERE transaction_profile_sk NOT IN "
                   "(SELECT transaction_profile_sk FROM dw.dim_transaction_profile FINAL)",
        "district": "SELECT count() FROM dw.dim_account FINAL WHERE account_district_sk NOT IN "
                    "(SELECT district_sk FROM dw.dim_district FINAL)"}
    got = {k: q(v) for k, v in orphans.items()}
    check("DQ", "Consistency: 0 khóa ngoại mồ côi (5 quan hệ)", not any(got.values()), 0, got)
    check("DQ", "SCD2: mỗi khách đúng 1 phiên bản hiện tại", q(
        "SELECT count() FROM (SELECT client_id FROM dw.dim_customer FINAL GROUP BY client_id "
        "HAVING countIf(is_current = 1) != 1)") == 0)
    check("DQ", "SCD2: các phiên bản không chồng lấn, nối liền nhau", q("""
        SELECT count() FROM (SELECT client_id, valid_from, valid_to,
               lagInFrame(valid_to) OVER (PARTITION BY client_id ORDER BY valid_from
                                          ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS prev_to,
               row_number() OVER (PARTITION BY client_id ORDER BY valid_from) AS rn
               FROM dw.dim_customer FINAL) WHERE rn > 1 AND prev_to != valid_from""") == 0)
    check("DQ", "SCD2: phiên bản hiện tại có valid_to = 2299-12-31", q(
        "SELECT countIf(is_current = 1 AND valid_to != toDate32('2299-12-31')) FROM dw.dim_customer FINAL") == 0)
    check("DQ", "SCD2: mọi giao dịch trỏ tới phiên bản hiệu lực đúng ngày giao dịch", q("""
        SELECT count() FROM dw.fact_transaction AS f
        JOIN (SELECT customer_sk, valid_from, valid_to FROM dw.dim_customer FINAL) AS c USING (customer_sk)
        WHERE NOT (toDate32(toString(f.date_key)) >= c.valid_from
                   AND toDate32(toString(f.date_key)) < c.valid_to)""") == 0)


def main():
    os.makedirs(OUT, exist_ok=True)
    ch, pg = clickhouse(), postgres()
    print("reading source (Berka + scenario files) ...", flush=True)
    src = source_transactions()
    kb01(ch, src)
    kb02(ch, src)
    kb03(ch, pg.cursor())
    kb04(pg)
    kb05(ch, pg.cursor())
    kb06(pg.cursor())
    kb07(ch, pg.cursor())
    kb08(ch, src)
    quality(ch)
    by_kb = {}
    for c in CHECKS:
        by_kb.setdefault(c["kb"], []).append(c["status"] == "PASS")
    summary = {k: f"{sum(v)}/{len(v)}" for k, v in by_kb.items()}
    with open(f"{OUT}/kb_results.json", "w") as f:
        json.dump({"evaluated_at": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
                   "summary": summary, "checks": CHECKS}, f, ensure_ascii=False, indent=1, default=str)
    print("summary:", summary)
    sys.exit(0 if all(c["status"] == "PASS" for c in CHECKS) else 1)


if __name__ == "__main__":
    main()
