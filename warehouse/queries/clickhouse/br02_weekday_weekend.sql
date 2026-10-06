-- BR2 (Executive board): behavior by day of week, weekday vs. weekend.
-- Averages are per calendar day so that weekdays with more dates are not favored.
SELECT d.day_of_week,
       d.day_name,
       d.is_weekend,
       count()                                        AS transactions,
       uniqExact(d.date_key)                          AS days_observed,
       round(count() / uniqExact(d.date_key), 1)      AS avg_transactions_per_day,
       round(sum(f.transaction_amount) / uniqExact(d.date_key), 2) AS avg_value_per_day,
       round(avg(f.transaction_amount), 2)            AS avg_transaction_amount
FROM dwh.fact_transactions AS f
INNER JOIN dwh.dim_date AS d ON f.date_key = d.date_key
GROUP BY d.day_of_week, d.day_name, d.is_weekend
ORDER BY d.day_of_week
