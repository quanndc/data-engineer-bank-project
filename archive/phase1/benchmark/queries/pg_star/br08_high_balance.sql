WITH latest AS (SELECT max(snapshot_date_key) AS k FROM fact_customer_balance_snapshot),
     thr AS (SELECT percentile_disc(0.99) WITHIN GROUP (ORDER BY balance) AS t
             FROM fact_customer_balance_snapshot WHERE snapshot_date_key = (SELECT k FROM latest))
SELECT s.customer_key, c.customer_id, c.age_group, c.gender, s.balance, s.days_since_last_txn,
       thr.t AS top1pct_threshold
FROM fact_customer_balance_snapshot AS s
JOIN dim_customer AS c ON s.customer_key = c.customer_key
CROSS JOIN thr
WHERE s.snapshot_date_key = (SELECT k FROM latest) AND s.balance >= thr.t
ORDER BY s.balance DESC, s.customer_key
