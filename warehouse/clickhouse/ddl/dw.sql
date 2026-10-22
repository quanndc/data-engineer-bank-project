-- D. Data Warehouse (ClickHouse), database dw: 1 fact + 5 dimensions.
-- Columns, grain and rules follow the group sheet "Data Dictionary" (2026-10-18); this file adds
-- the physical types (decision Q-J and docs/data_model.md).
--
-- Physical conventions
--   * ClickHouse has no enforced PK/FK; keys are logical and checked by the pipeline (sheet, ERD note).
--   * Dimensions use ReplacingMergeTree(updated_at): a Type 1 correction or the closing of an SCD2
--     version is written as a NEW row with the same key and a later updated_at; the latest row wins.
--     Readers use FINAL (dimensions are small).
--   * fact_transaction uses ReplacingMergeTree(loaded_at) keyed by transaction_id as a safety net:
--     the pipeline already skips transaction_ids that are loaded, so a re-run cannot add duplicates.
--   * "Open" end of an SCD2 version: valid_to = 2299-12-31 (largest Date32; the sheet's logical
--     9999-12-31 cannot be stored in ClickHouse).
-- Tables are created only if missing: this file never drops data.

CREATE TABLE IF NOT EXISTS dw.dim_date (
  date_key        UInt32,                     -- YYYYMMDD, e.g. 19980701
  full_date       Date,
  day_of_month    UInt8,
  day_of_week     UInt8,                      -- 1 = Monday ... 7 = Sunday
  day_name        LowCardinality(String),
  month_number    UInt8,
  month_name      LowCardinality(String),
  quarter_number  UInt8,
  year_number     UInt16,
  year_month      LowCardinality(String),     -- 'YYYY-MM'
  is_weekend      UInt8                       -- Saturday/Sunday; not a holiday flag
) ENGINE = MergeTree
ORDER BY date_key;

CREATE TABLE IF NOT EXISTS dw.dim_district (
  district_sk     UInt16,
  district_id     UInt16,                     -- district.A1 (77 districts)
  district_name   String,                     -- district.A2
  region_name     LowCardinality(String),     -- district.A3 (8 regions)
  created_at      DateTime,
  updated_at      DateTime                    -- Type 1: corrections keep district_sk
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY district_sk;

CREATE TABLE IF NOT EXISTS dw.dim_account (
  account_sk                UInt32,
  account_id                UInt32,           -- account.account_id (4,500 accounts)
  account_open_date         Date,
  statement_frequency_code  LowCardinality(String),   -- account.frequency, kept as in the source
  statement_frequency_name  LowCardinality(String),   -- monthly / weekly / after transaction
  account_district_sk       UInt16,           -- -> dim_district (branch role)
  created_at                DateTime,
  updated_at                DateTime          -- Type 1; no account history in the demo
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY account_sk;

CREATE TABLE IF NOT EXISTS dw.dim_customer (
  customer_sk           UInt32,               -- one surrogate key per tier VERSION
  client_id             UInt32,               -- stable across versions; not unique in this table
  gender                LowCardinality(String),
  birth_date            Date32,               -- births before 1970
  customer_district_sk  UInt16,               -- -> dim_district (residence role)
  membership_tier       LowCardinality(String),
  valid_from            Date32,               -- inclusive
  valid_to              Date32,               -- exclusive; 2299-12-31 = current version
  is_current            UInt8,                -- exactly one current version per client_id
  created_at            DateTime,
  updated_at            DateTime              -- changes when the version is closed
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY customer_sk;

CREATE TABLE IF NOT EXISTS dw.dim_transaction_profile (
  transaction_profile_sk  UInt16,
  type_code               LowCardinality(String),   -- trans.type, standardized
  type_name               LowCardinality(String),
  operation_code          LowCardinality(String),   -- NULL/blank -> NOT_RECORDED
  operation_name          LowCardinality(String),
  purpose_code            LowCardinality(String),   -- trans.k_symbol; NULL/blank -> NOT_RECORDED
  purpose_name            LowCardinality(String),
  cash_flow_direction     LowCardinality(String),   -- IN (PRIJEM) / OUT (VYDAJ, VYBER)
  created_at              DateTime,
  updated_at              DateTime                  -- labels are Type 1; codes never change
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY transaction_profile_sk;

CREATE TABLE IF NOT EXISTS dw.fact_transaction (
  transaction_id            UInt64,           -- trans.trans_id, or the id in a scenario CSV
  date_key                  UInt32,           -- -> dim_date
  account_sk                UInt32,           -- -> dim_account
  customer_sk               UInt32,           -- -> dim_customer: OWNER version valid on the transaction date
  transaction_profile_sk    UInt16,           -- -> dim_transaction_profile
  amount                    Decimal(18, 2),   -- unsigned source amount; direction comes from the profile
  balance_after             Decimal(18, 2),   -- may be negative; never summed across transactions
  counterparty_bank_code    Nullable(String),
  counterparty_account_ref  Nullable(String),
  load_batch_id             LowCardinality(String),
  loaded_at                 DateTime64(3)
) ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY intDiv(date_key, 10000)          -- one partition per year (1993-1998)
ORDER BY (account_sk, date_key, transaction_id);
