-- Created after the bulk load (faster than maintaining them row by row).
ALTER TABLE dim_date             ADD PRIMARY KEY (date_key);
ALTER TABLE dim_customer         ADD PRIMARY KEY (customer_key);
ALTER TABLE dim_location         ADD PRIMARY KEY (location_key);
ALTER TABLE dim_transaction_type ADD PRIMARY KEY (txn_type_key);
ALTER TABLE fact_transactions    ADD PRIMARY KEY (transaction_key);
ALTER TABLE fact_customer_balance_snapshot ADD PRIMARY KEY (snapshot_date_key, customer_key);
-- Enforced foreign keys: what a row store offers and ClickHouse does not
ALTER TABLE fact_transactions ADD FOREIGN KEY (date_key)     REFERENCES dim_date;
ALTER TABLE fact_transactions ADD FOREIGN KEY (customer_key) REFERENCES dim_customer;
ALTER TABLE fact_transactions ADD FOREIGN KEY (location_key) REFERENCES dim_location;
ALTER TABLE fact_transactions ADD FOREIGN KEY (txn_type_key) REFERENCES dim_transaction_type;
ALTER TABLE fact_customer_balance_snapshot ADD FOREIGN KEY (customer_key) REFERENCES dim_customer;
CREATE INDEX ON fact_transactions (date_key);
CREATE INDEX ON fact_transactions (customer_key);
CREATE INDEX ON fact_transactions (location_key);
ANALYZE;
