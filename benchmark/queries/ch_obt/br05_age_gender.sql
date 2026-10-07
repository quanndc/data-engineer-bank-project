SELECT age_group, gender, uniqExact(customer_key) AS customers, count() AS transactions,
       sum(transaction_amount) AS transaction_value, round(avg(transaction_amount), 2) AS avg_transaction_amount
FROM bench.obt_transactions
GROUP BY age_group, gender
ORDER BY age_group, gender
