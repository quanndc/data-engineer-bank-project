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


if __name__ == "__main__":
    kb05()
