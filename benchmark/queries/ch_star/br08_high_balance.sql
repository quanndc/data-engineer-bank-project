-- BR8 (Wealth management): customers in the top 1% of balances at the latest month end.
-- Balance is semi-additive, so it is read from the month-end snapshot, never summed over time.
WITH (SELECT max(snapshot_date_key) FROM dwh.fact_customer_balance_snapshot) AS latest,
     (SELECT quantileExact(0.99)(balance) FROM dwh.fact_customer_balance_snapshot
      WHERE snapshot_date_key = latest) AS threshold
SELECT s.customer_key,
       c.customer_id,
       c.age_group,
       c.gender,
       s.balance,
       s.days_since_last_txn,
       threshold AS top1pct_threshold
FROM dwh.fact_customer_balance_snapshot AS s
INNER JOIN dwh.dim_customer AS c ON s.customer_key = c.customer_key
WHERE s.snapshot_date_key = latest AND s.balance >= threshold
ORDER BY s.balance DESC, s.customer_key
