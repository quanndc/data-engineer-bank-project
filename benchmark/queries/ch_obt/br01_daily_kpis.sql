SELECT full_date, week_of_year, month_name, count() AS transactions,
       sum(transaction_amount) AS transaction_value, uniqExact(customer_key) AS active_customers
FROM bench.obt_transactions
GROUP BY full_date, week_of_year, month_name
ORDER BY full_date
