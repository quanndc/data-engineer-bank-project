SELECT segment, count(*) AS customers, round(avg(recency), 1) AS avg_recency_days,
       round(avg(frequency), 2) AS avg_frequency, round(avg(monetary), 2) AS avg_monetary,
       sum(monetary) AS total_monetary
FROM (
  SELECT recency, frequency, monetary,
         CASE WHEN r >= 4 AND f >= 3 THEN 'Champions' WHEN f >= 3 THEN 'Loyal'
              WHEN r >= 4 THEN 'Recent' WHEN r <= 2 AND m >= 4 THEN 'At Risk (High Value)'
              ELSE 'Hibernating' END AS segment
  FROM (
    SELECT recency, frequency, monetary,
           CASE WHEN recency <= 7 THEN 5 WHEN recency <= 21 THEN 4 WHEN recency <= 42 THEN 3
                WHEN recency <= 63 THEN 2 ELSE 1 END AS r,
           least(frequency, 5) AS f,
           CASE WHEN monetary < 500 THEN 1 WHEN monetary < 2000 THEN 2 WHEN monetary < 10000 THEN 3
                WHEN monetary < 50000 THEN 4 ELSE 5 END AS m
    FROM (
      SELECT f.customer_key, DATE '2016-10-21' - max(d.full_date) AS recency,
             count(*) AS frequency, sum(f.transaction_amount) AS monetary
      FROM fact_transactions AS f
      JOIN dim_date AS d ON f.date_key = d.date_key
      GROUP BY f.customer_key) a) b) c
GROUP BY segment
ORDER BY customers DESC, segment
