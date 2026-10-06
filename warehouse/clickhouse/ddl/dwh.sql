-- Kimball star schema for the customer-transaction business process.
-- Recreated on every full load (tables are dropped first).

DROP TABLE IF EXISTS dwh.dim_date;
CREATE TABLE dwh.dim_date (
  date_key       UInt32,
  full_date      Date,
  year           UInt16,
  quarter        UInt8,
  month          UInt8,
  month_name     LowCardinality(String),
  week_of_year   UInt8,
  day_of_month   UInt8,
  day_of_week    UInt8,                 -- ISO: 1 = Monday ... 7 = Sunday
  day_name       LowCardinality(String),
  is_weekend     UInt8,
  is_holiday     UInt8,
  holiday_name   String
) ENGINE = MergeTree ORDER BY date_key;

DROP TABLE IF EXISTS dwh.dim_customer;
CREATE TABLE dwh.dim_customer (
  customer_key   UInt32,                -- surrogate key; 0 = Unknown member
  customer_nk    String,                -- sha256(customer_id | original DOB | original gender)
  customer_id    String,
  dob            Nullable(Date32),      -- Date32: DOBs before 1970 are valid
  age            Nullable(UInt8),       -- at the reference date 2016-10-21
  age_group      LowCardinality(String),
  gender         LowCardinality(String),
  marital_status LowCardinality(String),
  account_status LowCardinality(String),
  household_id   UInt64,
  dq_flags       UInt8
) ENGINE = MergeTree ORDER BY customer_key;

DROP TABLE IF EXISTS dwh.dim_location;
CREATE TABLE dwh.dim_location (
  location_key   UInt32,                -- surrogate key; 0 = Unknown member
  location_name  String,
  city           LowCardinality(String),
  state          LowCardinality(String),
  is_top_city    UInt8
) ENGINE = MergeTree ORDER BY location_key;

DROP TABLE IF EXISTS dwh.dim_transaction_type;
CREATE TABLE dwh.dim_transaction_type (
  txn_type_key     UInt8,
  type_name        LowCardinality(String),
  is_rule_inferred UInt8
) ENGINE = MergeTree ORDER BY txn_type_key;

DROP TABLE IF EXISTS dwh.fact_transactions;
CREATE TABLE dwh.fact_transactions (
  transaction_key    UInt64,            -- degenerate dimension (source transaction id)
  date_key           UInt32,
  customer_key       UInt32,
  location_key       UInt32,
  txn_type_key       UInt8,
  transaction_amount Decimal(18, 2),    -- additive
  account_balance    Decimal(18, 2),    -- semi-additive: never SUM over time
  dq_flags           UInt8
) ENGINE = MergeTree
PARTITION BY intDiv(date_key, 100)      -- one partition per month
ORDER BY (date_key, location_key, customer_key);

DROP TABLE IF EXISTS dwh.fact_customer_balance_snapshot;
CREATE TABLE dwh.fact_customer_balance_snapshot (
  snapshot_date_key   UInt32,           -- month end
  customer_key        UInt32,
  balance             Decimal(18, 2),   -- last known balance at the month end
  last_txn_date_key   UInt32,
  days_since_last_txn UInt16,
  is_partial_period   UInt8             -- 1 when the month end is after the data ends
) ENGINE = MergeTree ORDER BY (snapshot_date_key, customer_key);
