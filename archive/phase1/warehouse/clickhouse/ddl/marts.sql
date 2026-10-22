-- Data marts built from the star schema. Recreated on every run.

-- BR4: RFM segmentation with the correct reference date (end of data, 2016-10-21)
-- and FIXED score thresholds. Equal-size quantiles (ntile) are not used: 70.8% of
-- customers have one transaction, so quantiles would give identical customers
-- different scores -- the flaw found in the pre-built source mart.
DROP TABLE IF EXISTS mart.rfm;
CREATE TABLE mart.rfm (
  customer_key UInt32,
  recency      UInt16,                 -- days since last transaction
  frequency    UInt16,                 -- number of transactions
  monetary     Decimal(38, 2),         -- total transaction value (INR)
  r_score      UInt8,
  f_score      UInt8,
  m_score      UInt8,
  rfm_code     String,                 -- e.g. '513'
  segment      LowCardinality(String)
) ENGINE = MergeTree ORDER BY customer_key;

INSERT INTO mart.rfm
SELECT customer_key, recency, frequency, monetary, r_score, f_score, m_score,
       concat(toString(r_score), toString(f_score), toString(m_score)) AS rfm_code,
       multiIf(r_score >= 4 AND f_score >= 3, 'Champions',
               f_score >= 3,                  'Loyal',
               r_score >= 4,                  'Recent',
               r_score <= 2 AND m_score >= 4, 'At Risk (High Value)',
                                              'Hibernating') AS segment
FROM (
  SELECT customer_key, recency, frequency, monetary,
         multiIf(recency <= 7, 5, recency <= 21, 4,
                 recency <= 42, 3, recency <= 63, 2, 1)          AS r_score,
         least(frequency, 5)                                     AS f_score,
         multiIf(monetary < 500, 1, monetary < 2000, 2,
                 monetary < 10000, 3, monetary < 50000, 4, 5)    AS m_score
  FROM (
    SELECT f.customer_key,
           dateDiff('day', max(d.full_date), toDate('2016-10-21')) AS recency,
           count()                                                 AS frequency,
           sum(f.transaction_amount)                               AS monetary
    FROM dwh.fact_transactions AS f
    INNER JOIN dwh.dim_date AS d ON f.date_key = d.date_key
    GROUP BY f.customer_key
  )
);
