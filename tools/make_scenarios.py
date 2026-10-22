"""Generate the scenario input files (the sheet: "bổ sung dữ liệu cho KB05 và KB08; KB03 tùy quy tắc").

    python3 tools/make_scenarios.py            # -> data/berka/scenarios/<KBxx>/...

Files use the Berka format (';'-separated, same columns as trans.csv) so that the pipeline treats
them exactly like a source batch. Everything is deterministic. The expected results are written
next to the inputs (expected.json) and used by the evaluation (P2-8).
"""
import csv
import json
import os

import duckdb

RAW = "data/berka/raw"
OUT = "data/berka/scenarios"
COLS = ["trans_id", "account_id", "date", "type", "operation", "amount", "balance", "k_symbol",
        "bank", "account"]

c = duckdb.connect()
for t in ("account", "disp", "trans"):
    c.execute(f"CREATE VIEW {t} AS SELECT * FROM read_csv('{RAW}/{t}.csv', delim=';', header=true, "
              f"all_varchar=true)")


def write(code, name, rows, cols=COLS, delim=";"):
    os.makedirs(f"{OUT}/{code}", exist_ok=True)
    with open(f"{OUT}/{code}/{name}", "w", newline="") as f:
        w = csv.writer(f, delimiter=delim, quoting=csv.QUOTE_MINIMAL)
        w.writerow(cols)
        w.writerows(rows)


def last_balances(n):
    """n accounts with their last balance at the end of 1998 (deterministic: lowest account_id)."""
    return c.sql(f"""
        SELECT account_id, arg_max(balance::DECIMAL(18, 2), date || lpad(trans_id, 10, '0')) AS bal
        FROM trans GROUP BY account_id ORDER BY account_id::INT LIMIT {n}""").fetchall()


# ---------------------------------------------------------------- KB05: data errors (BR05)
def kb05():
    accts = last_balances(10)
    rows, expected_ok = [], []
    for i, (acc, bal) in enumerate(accts):
        tid, amount = 90_000_001 + i, 1000 + 100 * i
        deposit = i % 2 == 0
        new_bal = bal + amount if deposit else bal - amount
        rows.append([tid, acc, "990105", "PRIJEM" if deposit else "VYDAJ",
                     "VKLAD" if deposit else "VYBER", f"{amount:.2f}", f"{new_bal:.2f}", "", "", ""])
        expected_ok.append(tid)
    a0, b0 = accts[0]
    existing = c.sql("SELECT trans_id, account_id, date, type, operation, amount, balance FROM trans "
                     "ORDER BY trans_id::INT LIMIT 1").fetchone()
    errors = [  # (row, rule)
        ([90_000_101, a0, "990106", "PRIJEM", "VKLAD", "", f"{b0:.2f}", "", "", ""], "V1"),     # no amount
        ([90_000_102, "", "990106", "PRIJEM", "VKLAD", "500.00", "500.00", "", "", ""], "V1"),   # no account
        ([90_000_103, "99999", "990106", "PRIJEM", "VKLAD", "500.00", "500.00", "", "", ""], "V2"),
        ([90_000_104, a0, "990106", "VYDAJ", "VYBER", "-500.00", f"{b0:.2f}", "", "", ""], "V3"),
        ([90_000_105, a0, "990106", "VYDAJ", "VYBER", "abc", f"{b0:.2f}", "", "", ""], "V3"),
        ([90_000_106, a0, "990231", "PRIJEM", "VKLAD", "500.00", f"{b0:.2f}", "", "", ""], "V4"),  # 31 Feb
        ([90_000_107, a0, "010115", "PRIJEM", "VKLAD", "500.00", f"{b0:.2f}", "", "", ""], "V4"),  # 2001: outside calendar
        ([90_000_108, a0, "990106", "XYZ", "VKLAD", "500.00", f"{b0:.2f}", "", "", ""], "V5"),
        (rows[0][:], "V6"),                                                      # duplicate inside the batch
        (list(existing) + ["", "", ""], "V6"),                                   # already loaded by HIST_1993_01
    ]
    all_rows = rows + [r for r, _ in errors]
    write("KB05", "trans.csv", all_rows)
    rules = {}
    for _, rule in errors:
        rules[rule] = rules.get(rule, 0) + 1
    expected = {"scenario": "KB05", "batch_id": "KB05_DATA_ERRORS", "rows_received": len(all_rows),
                "rows_loaded": len(rows), "rows_rejected": len(errors), "rejected_by_rule": rules,
                "loaded_transaction_ids": expected_ok}
    json.dump(expected, open(f"{OUT}/KB05/expected.json", "w"), indent=2, default=str)
    print(f"KB05: {len(all_rows)} rows -> expect {len(rows)} loaded, {len(errors)} rejected {rules}")


# ---------------------------------------------------------------- KB08: tier change + next period (BR08)
TIERS = [(550_000, "Platinum"), (300_000, "Gold"), (150_000, "Silver"), (0, "Standard")]
KB05_ACCOUNTS = 10          # kb05() uses the 10 lowest account ids


def tier_of(amount):
    return next(name for limit, name in TIERS if amount >= limit)


def kb08():
    """Year-end re-evaluation (decision 2026-10-22): tier recomputed on 1998 inflow with the same
    thresholds; the new tier is effective on 1999-01-01. Next period = January 1999: two
    transactions (deposit, withdrawal) for every account whose owner changed tier."""
    c.execute("CREATE OR REPLACE VIEW prof AS SELECT * FROM read_csv('data/berka/customer_profile_simulated.csv', header=true)")
    rows = c.sql("""
        SELECT p.client_id, p.membership_tier AS old_tier, d.account_id,
               coalesce(sum(t.amount::DECIMAL(18, 2)) FILTER (WHERE trim(t.type) = 'PRIJEM' AND t.date LIKE '98%'), 0) AS in98,
               arg_max(t.balance::DECIMAL(18, 2), t.date || lpad(t.trans_id, 10, '0')) AS last_balance
        FROM prof AS p
        JOIN disp AS d ON d.client_id::INT = p.client_id AND d.type = 'OWNER'
        LEFT JOIN trans AS t ON t.account_id = d.account_id
        GROUP BY p.client_id, p.membership_tier, d.account_id
        ORDER BY p.client_id""").fetchall()
    changes, trans, by_tier = [], [], {}
    tid = 91_000_001
    for client_id, old_tier, account_id, in98, last_balance in rows:
        new_tier = tier_of(in98)
        if new_tier == old_tier:
            continue
        assert int(account_id) > KB05_ACCOUNTS, "KB08 must not touch the KB05 accounts"
        changes.append([client_id, new_tier, "1999-01-01", "1999-01-04 09:00:00"])
        deposit = max(round(float(in98) / 12, -2), 100.0)
        withdrawal = round(deposit * 0.4, -1)
        bal = float(last_balance) + deposit
        trans.append([tid, account_id, "990115", "PRIJEM", "VKLAD", f"{deposit:.2f}", f"{bal:.2f}", "", "", ""])
        bal -= withdrawal
        trans.append([tid + 1, account_id, "990120", "VYDAJ", "VYBER", f"{withdrawal:.2f}", f"{bal:.2f}", "", "", ""])
        tid += 2
        t = by_tier.setdefault(new_tier, {"clients": 0, "money_in": 0.0, "money_out": 0.0})
        t["clients"] += 1
        t["money_in"] += deposit
        t["money_out"] += withdrawal
    write("KB08", "customer_profile_changes.csv", changes,
          cols=["client_id", "membership_tier", "tier_effective_from", "updated_at"], delim=",")
    write("KB08", "trans.csv", trans)
    expected = {"scenario": "KB08", "batch_id": "KB08_TIER_CHANGE", "tier_changes": len(changes),
                "versions_closed": len(changes), "versions_opened": len(changes),
                "versions_total_after": 4500 + len(changes), "current_versions_after": 4500,
                "rows_loaded": len(trans),
                "new_period_by_new_tier": {k: {kk: round(vv, 2) for kk, vv in v.items()} for k, v in by_tier.items()},
                "old_period_rule": "totals by tier for 1993-01-01..1998-12-31 must be identical before and after KB08"}
    json.dump(expected, open(f"{OUT}/KB08/expected.json", "w"), indent=2)
    print(f"KB08: {len(changes)} tier changes effective 1999-01-01, {len(trans)} transactions in January 1999")


# ---------------------------------------------------------------- KB03: fraud patterns (BR03)
def kb03():
    """Transactions built to meet the alert rules (ops.alert_rule, decision Q-F):
    R1 -- 3 accounts withdraw 4x their average monthly outflow of Jul-Dec 1998 (>= 60,000 CZK) in Feb 1999;
    R2 -- 2 accounts with no transaction after 1998-12-31 withdraw 5,000 CZK on 1999-07-15 (196 days).
    Accounts are disjoint from KB05 (ids 1-10) and KB08 (owners who changed tier)."""
    kb08_accounts = {r[1] for r in csv.reader(open(f"{OUT}/KB08/trans.csv"), delimiter=";") if r[0] != "trans_id"}
    cand = c.sql("""
        SELECT account_id,
               sum(amount::DECIMAL(18, 2)) FILTER (WHERE trim(type) <> 'PRIJEM' AND date BETWEEN '980701' AND '981231') / 6 AS avg_out,
               arg_max(balance::DECIMAL(18, 2), date || lpad(trans_id, 10, '0')) AS last_balance,
               max(date) AS last_date, min(date) AS first_date
        FROM trans GROUP BY account_id
        HAVING min(date) <= '980101' AND max(date) = '981231'
        ORDER BY account_id::INT""").fetchall()
    cand = [r for r in cand if int(r[0]) > KB05_ACCOUNTS and r[0] not in kb08_accounts]
    rows, alerts = [], []
    tid = 92_000_001
    for acc, avg_out, bal, _, _ in cand[:3]:
        amount = max(60_000, int(-(-float(avg_out) * 4 // 1000)) * 1000)
        rows.append([tid, acc, "990210", "VYDAJ", "VYBER", f"{amount:.2f}", f"{float(bal) - amount:.2f}", "", "", ""])
        alerts.append({"rule": "R1", "account_id": int(acc), "period_start": "1999-02-01",
                       "outflow": amount, "avg_outflow_previous": round(float(avg_out), 2)})
        tid += 1
    for acc, _, bal, _, _ in cand[3:5]:
        rows.append([tid, acc, "990715", "VYDAJ", "VYBER", "5000.00", f"{float(bal) - 5000:.2f}", "", "", ""])
        alerts.append({"rule": "R2", "account_id": int(acc), "period_start": "1999-07-01", "gap_days": 196})
        tid += 1
    write("KB03", "trans.csv", rows)
    expected = {"scenario": "KB03", "batch_id": "KB03_FRAUD", "rows_loaded": len(rows),
                "new_alerts": len(alerts), "alerts": alerts}
    json.dump(expected, open(f"{OUT}/KB03/expected.json", "w"), indent=2)
    print(f"KB03: {len(rows)} transactions -> expect {len(alerts)} new alerts "
          f"({sum(a['rule'] == 'R1' for a in alerts)} R1, {sum(a['rule'] == 'R2' for a in alerts)} R2)")


if __name__ == "__main__":
    kb05()
    kb08()
    kb03()
