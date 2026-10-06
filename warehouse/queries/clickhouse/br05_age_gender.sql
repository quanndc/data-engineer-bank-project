-- BR5 (Marketing / CRM): spending behavior by age group and gender.
SELECT c.age_group,
       c.gender,
       uniqExact(f.customer_key)            AS customers,
       count()                              AS transactions,
       sum(f.transaction_amount)            AS transaction_value,
       round(avg(f.transaction_amount), 2)  AS avg_transaction_amount
FROM dwh.fact_transactions AS f
INNER JOIN dwh.dim_customer AS c ON f.customer_key = c.customer_key
GROUP BY c.age_group, c.gender
ORDER BY c.age_group, c.gender
