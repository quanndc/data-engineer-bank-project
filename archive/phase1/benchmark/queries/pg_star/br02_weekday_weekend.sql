SELECT d.day_of_week, d.day_name, d.is_weekend, count(*) AS transactions,
       count(DISTINCT d.date_key) AS days_observed,
       round(count(*)::numeric / count(DISTINCT d.date_key), 1) AS avg_transactions_per_day,
       round(sum(f.transaction_amount) / count(DISTINCT d.date_key), 2) AS avg_value_per_day,
       round(avg(f.transaction_amount), 2) AS avg_transaction_amount
FROM fact_transactions AS f
JOIN dim_date AS d ON f.date_key = d.date_key
GROUP BY d.day_of_week, d.day_name, d.is_weekend
ORDER BY d.day_of_week
