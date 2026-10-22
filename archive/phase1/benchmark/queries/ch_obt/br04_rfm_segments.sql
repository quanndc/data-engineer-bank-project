-- BR4 benchmark form: RFM computed from the fact table (not read from the pre-built mart)
SELECT segment, count() AS customers, round(avg(recency), 1) AS avg_recency_days,
       round(avg(frequency), 2) AS avg_frequency, round(avg(monetary), 2) AS avg_monetary,
       sum(monetary) AS total_monetary
FROM (
  SELECT recency, frequency, monetary,
         multiIf(r >= 4 AND f >= 3, 'Champions', f >= 3, 'Loyal', r >= 4, 'Recent',
                 r <= 2 AND m >= 4, 'At Risk (High Value)', 'Hibernating') AS segment
  FROM (
    SELECT recency, frequency, monetary,
           multiIf(recency <= 7, 5, recency <= 21, 4, recency <= 42, 3, recency <= 63, 2, 1) AS r,
           least(frequency, 5) AS f,
           multiIf(monetary < 500, 1, monetary < 2000, 2, monetary < 10000, 3, monetary < 50000, 4, 5) AS m
    FROM (
      SELECT f.customer_key,
             dateDiff('day', max(f.full_date), toDate('2016-10-21')) AS recency,
             count() AS frequency, sum(f.transaction_amount) AS monetary
      FROM bench.obt_transactions AS f
      GROUP BY f.customer_key)))
GROUP BY segment
ORDER BY customers DESC, segment
