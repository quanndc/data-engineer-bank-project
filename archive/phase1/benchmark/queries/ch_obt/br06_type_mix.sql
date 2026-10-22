SELECT city, type_name, count() AS transactions, sum(transaction_amount) AS transaction_value,
       round(count() / sum(count()) OVER (PARTITION BY city) * 100, 2) AS pct_of_city_transactions
FROM bench.obt_transactions
GROUP BY city, type_name
ORDER BY city, type_name
