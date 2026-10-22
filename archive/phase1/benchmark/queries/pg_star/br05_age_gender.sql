SELECT c.age_group, c.gender, count(DISTINCT f.customer_key) AS customers, count(*) AS transactions,
       sum(f.transaction_amount) AS transaction_value, round(avg(f.transaction_amount), 2) AS avg_transaction_amount
FROM fact_transactions AS f
JOIN dim_customer AS c ON f.customer_key = c.customer_key
GROUP BY c.age_group, c.gender
ORDER BY c.age_group, c.gender
