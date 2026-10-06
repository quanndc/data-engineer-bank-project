-- BR4 (Marketing / CRM): RFM segment distribution (campaign target selection).
SELECT segment,
       count()                          AS customers,
       round(count() / (SELECT count() FROM mart.rfm) * 100, 2) AS pct_customers,
       round(avg(recency), 1)           AS avg_recency_days,
       round(avg(frequency), 2)         AS avg_frequency,
       round(avg(monetary), 2)          AS avg_monetary,
       sum(monetary)                    AS total_monetary
FROM mart.rfm
GROUP BY segment
ORDER BY customers DESC, segment
