"""P2-0: re-check the source survey recorded in the group sheet (K01-K05) on the Berka files.

    python3 tools/profile_berka.py            # reads data/berka/raw/*.csv

Prints each figure next to the value written in the sheet so that differences are visible.
"""
import duckdb

RAW = "data/berka/raw"
c = duckdb.connect()
for t in ("account", "client", "disp", "district", "trans"):
    # all_varchar keeps "" (empty string) distinct from a missing value (NULL)
    c.execute(f"CREATE VIEW {t} AS SELECT * FROM read_csv('{RAW}/{t}.csv', delim=';', header=true, "
              f"all_varchar=true, quote='\"')")

def one(sql):
    return c.sql(sql).fetchone()[0]

checks = [
    ("K01", "Accounts", "4.500", one("SELECT count(*) FROM account")),
    ("K01", "Clients", "5.369", one("SELECT count(*) FROM client")),
    ("K01", "Transactions", "1.056.320", one("SELECT count(*) FROM trans")),
    ("K01", "Districts", "77", one("SELECT count(*) FROM district")),
    ("K02", "Accounts with exactly one OWNER", "4.500",
     one("SELECT count(*) FROM (SELECT account_id FROM disp WHERE type = 'OWNER' "
         "GROUP BY account_id HAVING count(*) = 1)")),
    ("K02", "Accounts without an OWNER", "0",
     one("SELECT count(*) FROM account a WHERE NOT EXISTS "
         "(SELECT 1 FROM disp d WHERE d.account_id = a.account_id AND d.type = 'OWNER')")),
    ("K03", "Accounts in the same district as their owner", "4.091",
     one("SELECT count(*) FROM account a JOIN disp d ON d.account_id = a.account_id AND d.type = 'OWNER' "
         "JOIN client c ON c.client_id = d.client_id WHERE a.district_id = c.district_id")),
    ("K03", "Accounts in a different district", "409",
     one("SELECT count(*) FROM account a JOIN disp d ON d.account_id = a.account_id AND d.type = 'OWNER' "
         "JOIN client c ON c.client_id = d.client_id WHERE a.district_id <> c.district_id")),
    ("K04", "Raw (type, operation, k_symbol) combinations", "17",
     one("SELECT count(*) FROM (SELECT DISTINCT type, operation, k_symbol FROM trans)")),
    ("K04", "Combinations after standardization (trim; NULL/blank -> NOT_RECORDED)", "15",
     one("SELECT count(*) FROM (SELECT DISTINCT upper(trim(type)), "
         "coalesce(nullif(upper(trim(operation)), ''), 'NOT_RECORDED'), "
         "coalesce(nullif(upper(trim(k_symbol)), ''), 'NOT_RECORDED') FROM trans)")),
    ("K05", "operation missing (NULL or blank)", "183.114",
     one("SELECT count(*) FROM trans WHERE operation IS NULL OR trim(operation) = ''")),
    ("K05", "... of which k_symbol = UROK", "183.114",
     one("SELECT count(*) FROM trans WHERE (operation IS NULL OR trim(operation) = '') "
         "AND trim(k_symbol) = 'UROK'")),
    ("K05", "k_symbol NULL", "481.881", one("SELECT count(*) FROM trans WHERE k_symbol IS NULL")),
    ("K05", "k_symbol blank string ('' or spaces)", "53.433",
     one("SELECT count(*) FROM trans WHERE k_symbol IS NOT NULL AND trim(k_symbol) = ''")),
    ("K05", "type = VYBER (direction checked on balances)", "9.597",
     one("SELECT count(*) FROM trans WHERE trim(type) = 'VYBER'")),
    ("—", "Negative amounts", "0 (sheet: none)",
     one("SELECT count(*) FROM trans WHERE CAST(amount AS DOUBLE) < 0")),
    ("—", "Zero amounts", "—", one("SELECT count(*) FROM trans WHERE CAST(amount AS DOUBLE) = 0")),
    ("—", "First / last transaction date", "1993–1998",
     one("SELECT min(date) || ' / ' || max(date) FROM trans")),
    ("—", "Owner clients (clients with an OWNER disposition)", "—",
     one("SELECT count(DISTINCT client_id) FROM disp WHERE type = 'OWNER'")),
]
print(f"{'ID':<4} {'Check':<70} {'Sheet':>16} {'Measured':>16}")
for cid, name, sheet, value in checks:
    v = f"{value:,}".replace(",", ".") if isinstance(value, int) else str(value)
    print(f"{cid:<4} {name:<70} {sheet:>16} {v:>16}")

print("\nRaw (type, operation, k_symbol) combinations:")
print(c.sql("SELECT type, operation, '[' || coalesce(k_symbol, 'NULL') || ']' AS k_symbol, count(*) AS n "
            "FROM trans GROUP BY ALL ORDER BY n DESC"))
