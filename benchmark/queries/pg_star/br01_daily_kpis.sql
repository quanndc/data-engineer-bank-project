SELECT d.full_date, d.week_of_year, d.month_name, count(*) AS transactions,
       sum(f.transaction_amount) AS transaction_value, count(DISTINCT f.customer_key) AS active_customers
FROM fact_transactions AS f
JOIN dim_date AS d ON f.date_key = d.date_key
GROUP BY d.full_date, d.week_of_year, d.month_name
ORDER BY d.full_date
