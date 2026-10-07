-- Alternative solution A1: One Big Table (denormalized). Every dimension attribute is copied
-- into every fact row, so BR queries need no joins. The rows are produced by Spark
-- (benchmark/spark_build_obt.py, same ETL as the star schema) and loaded from Parquet.
CREATE DATABASE IF NOT EXISTS bench;

DROP TABLE IF EXISTS bench.obt_transactions;
CREATE TABLE bench.obt_transactions (
  transaction_key UInt64, date_key UInt32, full_date Date, year UInt16, quarter UInt8,
  month UInt8, month_name LowCardinality(String), week_of_year UInt8, day_of_month UInt8,
  day_of_week UInt8, day_name LowCardinality(String), is_weekend UInt8, is_holiday UInt8,
  holiday_name String,
  customer_key UInt32, customer_nk String, customer_id String, dob Nullable(Date32),
  age Nullable(UInt8), age_group LowCardinality(String), gender LowCardinality(String),
  marital_status LowCardinality(String), account_status LowCardinality(String),
  household_id UInt64, customer_dq_flags UInt8,
  location_key UInt32, location_name String, city LowCardinality(String),
  state LowCardinality(String), is_top_city UInt8,
  txn_type_key UInt8, type_name LowCardinality(String), is_rule_inferred UInt8,
  transaction_amount Decimal(18, 2), account_balance Decimal(18, 2), dq_flags UInt8
) ENGINE = MergeTree
PARTITION BY intDiv(date_key, 100)
ORDER BY (date_key, location_key, customer_key);      -- same sort key as the star fact


DROP TABLE IF EXISTS bench.obt_balance_snapshot;
CREATE TABLE bench.obt_balance_snapshot (
  snapshot_date_key UInt32, customer_key UInt32, customer_nk String, customer_id String,
  dob Nullable(Date32), age Nullable(UInt8), age_group LowCardinality(String),
  gender LowCardinality(String), marital_status LowCardinality(String),
  account_status LowCardinality(String), household_id UInt64, customer_dq_flags UInt8,
  balance Decimal(18, 2), last_txn_date_key UInt32, days_since_last_txn UInt16,
  is_partial_period UInt8
) ENGINE = MergeTree ORDER BY (snapshot_date_key, customer_key);

