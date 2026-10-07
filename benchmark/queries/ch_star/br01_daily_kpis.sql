-- BR1 (Executive board): transaction count, value, and active customers per day.
-- Weekly and monthly views roll the same query up on dim_date (week_of_year, month).
SELECT d.full_date,
       d.week_of_year,
       d.month_name,
       count()                    AS transactions,
       sum(f.transaction_amount)  AS transaction_value,
       uniqExact(f.customer_key)  AS active_customers
FROM dwh.fact_transactions AS f
INNER JOIN dwh.dim_date AS d ON f.date_key = d.date_key
GROUP BY d.full_date, d.week_of_year, d.month_name
ORDER BY d.full_date
