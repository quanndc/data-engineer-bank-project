"""P2-2: create the warehouse (ClickHouse, dw) and the operational store (PostgreSQL, ops), then
check every warehouse table against the column list of the group sheet's Data Dictionary.

    make schema
"""
import sys

from common import WAREHOUSE, clickhouse, ensure_databases, postgres, run_sql_file

# Column lists exactly as in the sheet "Data Dictionary" (2026-10-18)
DATA_DICTIONARY = {
    "fact_transaction": ["transaction_id", "date_key", "account_sk", "customer_sk",
                         "transaction_profile_sk", "amount", "balance_after",
                         "counterparty_bank_code", "counterparty_account_ref", "load_batch_id",
                         "loaded_at"],
    "dim_customer": ["customer_sk", "client_id", "gender", "birth_date", "customer_district_sk",
                     "membership_tier", "valid_from", "valid_to", "is_current", "created_at",
                     "updated_at"],
    "dim_account": ["account_sk", "account_id", "account_open_date", "statement_frequency_code",
                    "statement_frequency_name", "account_district_sk", "created_at", "updated_at"],
    "dim_date": ["date_key", "full_date", "day_of_month", "day_of_week", "day_name",
                 "month_number", "month_name", "quarter_number", "year_number", "year_month",
                 "is_weekend"],
    "dim_district": ["district_sk", "district_id", "district_name", "region_name", "created_at",
                     "updated_at"],
    "dim_transaction_profile": ["transaction_profile_sk", "type_code", "type_name",
                                "operation_code", "operation_name", "purpose_code", "purpose_name",
                                "cash_flow_direction", "created_at", "updated_at"],
}
OPS_TABLES = ["batch", "pipeline_run", "pipeline_step", "dq_rule", "rejected_record",
              "alert_rule", "alert", "alert_action", "backup_catalog", "backup_operation"]


def main():
    ok = True
    ch = clickhouse()
    ensure_databases(ch)
    run_sql_file(ch, f"{WAREHOUSE}/clickhouse/ddl/dw.sql")
    print("D ClickHouse dw:")
    for table, expected in DATA_DICTIONARY.items():
        rows = ch.query("SELECT name, type FROM system.columns WHERE database = 'dw' AND table = %(t)s "
                        "ORDER BY position", parameters={"t": table}).result_rows
        actual = [r[0] for r in rows]
        match = actual == expected
        ok &= match
        print(f"  [{'OK' if match else 'MISMATCH'}] {table:<24} {len(actual)} columns"
              + ("" if match else f"\n      expected {expected}\n      actual   {actual}"))

    pg = postgres()
    with pg.cursor() as cur:
        cur.execute(open(f"{WAREHOUSE}/postgres/ddl/ops.sql").read())
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'ops'")
        present = {r[0] for r in cur.fetchall()}
        cur.execute("SELECT (SELECT count(*) FROM ops.dq_rule), (SELECT count(*) FROM ops.alert_rule)")
        dq_rules, alert_rules = cur.fetchone()
    missing = [t for t in OPS_TABLES if t not in present]
    ok &= not missing
    print(f"E PostgreSQL ops: {len(OPS_TABLES) - len(missing)}/{len(OPS_TABLES)} tables"
          + (f", missing {missing}" if missing else "")
          + f"; reference rows: {dq_rules} data quality rules, {alert_rules} alert rules")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
