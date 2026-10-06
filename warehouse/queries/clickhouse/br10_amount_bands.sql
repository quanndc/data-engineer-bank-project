-- BR10 (Regional management): distribution of transaction values by amount band and city.
SELECT l.city,
       multiIf(f.transaction_amount < 100,   '1: <100',
               f.transaction_amount < 500,   '2: 100-500',
               f.transaction_amount < 2000,  '3: 500-2k',
               f.transaction_amount < 10000, '4: 2k-10k',
               f.transaction_amount < 50000, '5: 10k-50k',
                                             '6: >=50k') AS amount_band,
       count()                    AS transactions,
       sum(f.transaction_amount)  AS transaction_value
FROM dwh.fact_transactions AS f
INNER JOIN dwh.dim_location AS l ON f.location_key = l.location_key
GROUP BY l.city, amount_band
ORDER BY l.city, amount_band
