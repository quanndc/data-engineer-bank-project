SELECT city, state, count() AS transactions, sum(transaction_amount) AS transaction_value,
       uniqExact(customer_key) AS customers, round(avg(transaction_amount), 2) AS avg_transaction_amount,
       rank() OVER (ORDER BY sum(transaction_amount) DESC) AS value_rank
FROM bench.obt_transactions
GROUP BY city, state
ORDER BY value_rank, city
