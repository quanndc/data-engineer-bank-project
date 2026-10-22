SELECT city,
       multiIf(transaction_amount < 100, '1: <100', transaction_amount < 500, '2: 100-500',
               transaction_amount < 2000, '3: 500-2k', transaction_amount < 10000, '4: 2k-10k',
               transaction_amount < 50000, '5: 10k-50k', '6: >=50k') AS amount_band,
       count() AS transactions, sum(transaction_amount) AS transaction_value
FROM bench.obt_transactions
GROUP BY city, amount_band
ORDER BY city, amount_band
