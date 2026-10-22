-- BR7 (Risk management): alerts raised by the streaming job, per day and rule.
-- The live page reads the latest rows; this query summarizes them.
SELECT d.full_date,
       a.rule,
       count()                               AS alerts,
       sum(a.transaction_amount)             AS flagged_value,
       round(avg(dateDiff('millisecond', a.produced_at, a.inserted_at)), 0) AS avg_latency_ms
FROM dwh.fact_transaction_alert AS a
INNER JOIN dwh.dim_date AS d ON a.date_key = d.date_key
GROUP BY d.full_date, a.rule
ORDER BY d.full_date, a.rule
