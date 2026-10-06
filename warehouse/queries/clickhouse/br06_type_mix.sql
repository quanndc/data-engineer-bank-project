-- BR6 (Product development): transaction type mix by city (share within each city).
SELECT l.city,
       t.type_name,
       count()                                  AS transactions,
       sum(f.transaction_amount)                AS transaction_value,
       round(count() / sum(count()) OVER (PARTITION BY l.city) * 100, 2) AS pct_of_city_transactions
FROM dwh.fact_transactions AS f
INNER JOIN dwh.dim_location AS l ON f.location_key = l.location_key
INNER JOIN dwh.dim_transaction_type AS t ON f.txn_type_key = t.txn_type_key
GROUP BY l.city, t.type_name
ORDER BY l.city, t.type_name
