-- Alternative solution A2: the same Kimball star schema in PostgreSQL (row store).
DROP TABLE IF EXISTS fact_customer_balance_snapshot, fact_transactions,
                     dim_transaction_type, dim_location, dim_customer, dim_date;

CREATE TABLE dim_date (
  date_key integer, full_date date, year smallint, quarter smallint, month smallint,
  month_name text, week_of_year smallint, day_of_month smallint, day_of_week smallint,
  day_name text, is_weekend smallint, is_holiday smallint, holiday_name text);
CREATE TABLE dim_customer (
  customer_key integer, customer_nk text, customer_id text, dob date, age smallint,
  age_group text, gender text, marital_status text, account_status text,
  household_id bigint, dq_flags smallint);
CREATE TABLE dim_location (
  location_key integer, location_name text, city text, state text, is_top_city smallint);
CREATE TABLE dim_transaction_type (
  txn_type_key smallint, type_name text, is_rule_inferred smallint);
CREATE TABLE fact_transactions (
  transaction_key bigint, date_key integer, customer_key integer, location_key integer,
  txn_type_key smallint, transaction_amount numeric(18,2), account_balance numeric(18,2),
  dq_flags smallint);
CREATE TABLE fact_customer_balance_snapshot (
  snapshot_date_key integer, customer_key integer, balance numeric(18,2),
  last_txn_date_key integer, days_since_last_txn integer, is_partial_period smallint);
