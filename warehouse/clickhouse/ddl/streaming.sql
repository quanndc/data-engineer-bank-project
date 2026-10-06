-- BR7: alerts written by the streaming job (P4). Never dropped by batch reloads.
CREATE TABLE IF NOT EXISTS dwh.fact_transaction_alert (
  transaction_key    UInt64,
  date_key           UInt32,
  customer_key       UInt32,             -- conformed with dwh.dim_customer
  location_key       UInt32,             -- conformed with dwh.dim_location
  transaction_amount Decimal(18, 2),
  account_balance    Decimal(18, 2),
  rule               LowCardinality(String),   -- HIGH_AMOUNT | HIGH_BALANCE_RATIO
  produced_at        DateTime64(3),      -- event sent by the producer
  detected_at        DateTime64(3),      -- rule evaluated by the stream processor
  inserted_at        DateTime64(3) DEFAULT now64(3)   -- row visible in ClickHouse
) ENGINE = MergeTree
ORDER BY (detected_at, transaction_key);
