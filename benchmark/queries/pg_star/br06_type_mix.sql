SELECT l.city, t.type_name, count(*) AS transactions, sum(f.transaction_amount) AS transaction_value,
       round(count(*)::numeric / sum(count(*)) OVER (PARTITION BY l.city) * 100, 2) AS pct_of_city_transactions
FROM fact_transactions AS f
JOIN dim_location AS l ON f.location_key = l.location_key
JOIN dim_transaction_type AS t ON f.txn_type_key = t.txn_type_key
GROUP BY l.city, t.type_name
ORDER BY l.city, t.type_name
