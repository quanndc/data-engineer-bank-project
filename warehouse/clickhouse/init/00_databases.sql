-- Runs once, when the ClickHouse volume is first created.
-- dwh  : Kimball star schema (dimensions, facts, alerts)
-- mart : data marts built from the star schema (e.g., RFM)
-- dq   : data quality audit results
CREATE DATABASE IF NOT EXISTS dwh;
CREATE DATABASE IF NOT EXISTS mart;
CREATE DATABASE IF NOT EXISTS dq;
