SELECT day_of_week, day_name, is_weekend, count() AS transactions,
       uniqExact(date_key) AS days_observed,
       round(count() / uniqExact(date_key), 1) AS avg_transactions_per_day,
       round(sum(transaction_amount) / uniqExact(date_key), 2) AS avg_value_per_day,
       round(avg(transaction_amount), 2) AS avg_transaction_amount
FROM bench.obt_transactions
GROUP BY day_of_week, day_name, is_weekend
ORDER BY day_of_week
