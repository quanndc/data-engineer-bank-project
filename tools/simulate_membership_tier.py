"""P2-0: simulate the membership tier source file that goes with the Berka data.

    python3 tools/simulate_membership_tier.py
    -> data/berka/customer_profile_simulated.csv

One row per OWNER client (the sheet: "mỗi client_id một dòng hạng hiện tại"), with the columns of
the Data Dictionary: client_id, membership_tier, tier_effective_from, updated_at.

Rule (decision Q-E, proposed; easy to explain in the report):
  * first-year inflow = sum of PRIJEM (incoming) amounts on the client's own account during the
    12 months after the account was opened;
  * tier by fixed CZK thresholds: Standard < 150k <= Silver < 300k <= Gold < 550k <= Platinum;
  * tier_effective_from = account opening date, so the first version covers the whole loaded
    history (the rule for valid_from in the sheet);
  * updated_at = when the profile system exported the file (1999-01-04 08:00), deliberately
    different from the effective date (the sheet: "Ngày hiệu lực khác thời điểm cập nhật").
The rule is deterministic: running the script again produces the same file. The tiers are
simulated data and are reported as such.
"""
import duckdb

RAW = "data/berka/raw"
OUT = "data/berka/customer_profile_simulated.csv"
UPDATED_AT = "1999-01-04 08:00:00"
THRESHOLDS = [(550_000, "Platinum"), (300_000, "Gold"), (150_000, "Silver"), (0, "Standard")]

c = duckdb.connect()
for t in ("account", "client", "disp", "trans"):
    c.execute(f"CREATE VIEW {t} AS SELECT * FROM read_csv('{RAW}/{t}.csv', delim=';', "
              f"header=true, all_varchar=true)")

tier_case = "CASE " + " ".join(f"WHEN first_year_in >= {v} THEN '{n}'" for v, n in THRESHOLDS) + " END"
c.execute(f"""
CREATE TABLE profile AS
WITH own AS (
  SELECT d.client_id::INT AS client_id, a.account_id, strptime(a.date, '%y%m%d')::DATE AS opened
  FROM disp AS d JOIN account AS a USING (account_id)
  WHERE d.type = 'OWNER'),
inflow AS (
  SELECT o.client_id, o.opened,
         coalesce(sum(t.amount::DECIMAL(18, 2)) FILTER (
           WHERE trim(t.type) = 'PRIJEM'
             AND strptime(t.date, '%y%m%d')::DATE < o.opened + INTERVAL 12 MONTH), 0) AS first_year_in
  FROM own AS o LEFT JOIN trans AS t USING (account_id)
  GROUP BY o.client_id, o.opened)
SELECT client_id, {tier_case} AS membership_tier, opened AS tier_effective_from,
       TIMESTAMP '{UPDATED_AT}' AS updated_at, first_year_in
FROM inflow
ORDER BY client_id""")

c.execute(f"COPY (SELECT client_id, membership_tier, tier_effective_from, updated_at FROM profile) "
          f"TO '{OUT}' (HEADER, DELIMITER ',')")

# ---- checks (the "done when" of the plan)
owners = c.sql("SELECT count(DISTINCT client_id) FROM disp WHERE type = 'OWNER'").fetchone()[0]
rows, dup = c.sql("SELECT count(*), count(*) - count(DISTINCT client_id) FROM profile").fetchone()
unknown = c.sql("SELECT count(*) FROM profile p WHERE NOT EXISTS "
                "(SELECT 1 FROM client c WHERE c.client_id::INT = p.client_id)").fetchone()[0]
late = c.sql("""SELECT count(*) FROM profile p JOIN disp d ON d.client_id::INT = p.client_id AND d.type = 'OWNER'
                JOIN trans t ON t.account_id = d.account_id
                WHERE strptime(t.date, '%y%m%d')::DATE < p.tier_effective_from""").fetchone()[0]
print(f"{OUT}: {rows:,} rows | OWNER clients {owners:,} | duplicates {dup} | unknown client_id {unknown} | "
      f"transactions before tier_effective_from {late}")
print(c.sql("""SELECT membership_tier, count(*) AS clients, round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct,
                      round(min(first_year_in)) AS min_inflow, round(max(first_year_in)) AS max_inflow
               FROM profile GROUP BY membership_tier ORDER BY min_inflow"""))
assert rows == owners and dup == 0 and unknown == 0 and late == 0, "membership tier file failed its checks"
