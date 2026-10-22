-- BR3 (Regional management): rank cities by transaction value, customers, average value.
SELECT l.city,
       l.state,
       count()                               AS transactions,
       sum(f.transaction_amount)             AS transaction_value,
       uniqExact(f.customer_key)             AS customers,
       round(avg(f.transaction_amount), 2)   AS avg_transaction_amount,
       rank() OVER (ORDER BY sum(f.transaction_amount) DESC) AS value_rank
FROM dwh.fact_transactions AS f
INNER JOIN dwh.dim_location AS l ON f.location_key = l.location_key
GROUP BY l.city, l.state
ORDER BY value_rank, l.city
