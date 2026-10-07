SELECT l.city,
       CASE WHEN f.transaction_amount < 100 THEN '1: <100' WHEN f.transaction_amount < 500 THEN '2: 100-500'
            WHEN f.transaction_amount < 2000 THEN '3: 500-2k' WHEN f.transaction_amount < 10000 THEN '4: 2k-10k'
            WHEN f.transaction_amount < 50000 THEN '5: 10k-50k' ELSE '6: >=50k' END AS amount_band,
       count(*) AS transactions, sum(f.transaction_amount) AS transaction_value
FROM fact_transactions AS f
JOIN dim_location AS l ON f.location_key = l.location_key
GROUP BY l.city, amount_band
ORDER BY l.city, amount_band
