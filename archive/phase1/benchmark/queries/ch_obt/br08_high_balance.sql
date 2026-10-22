WITH (SELECT max(snapshot_date_key) FROM bench.obt_balance_snapshot) AS latest,
     (SELECT quantileExact(0.99)(balance) FROM bench.obt_balance_snapshot
      WHERE snapshot_date_key = latest) AS threshold
SELECT customer_key, customer_id, age_group, gender, balance, days_since_last_txn,
       threshold AS top1pct_threshold
FROM bench.obt_balance_snapshot
WHERE snapshot_date_key = latest AND balance >= threshold
ORDER BY balance DESC, customer_key
