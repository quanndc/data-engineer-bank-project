-- Runs once, when the ClickHouse volume is first created (local container only).
-- dw : Kimball data warehouse of Phase 2 (fact_transaction + 5 dimensions)
CREATE DATABASE IF NOT EXISTS dw;
