#!/usr/bin/env bash
# B2 / A2: copy the x1 star schema from ClickHouse (dwh) into PostgreSQL and time each step.
# Output: lake/bench/results/load_postgres.csv
set -euo pipefail
cd "$(dirname "$0")/.."
CH="docker compose exec -T clickhouse clickhouse-client -u bank --password bank"
PG="docker compose exec -T postgres psql -U bank -d bank -v ON_ERROR_STOP=1 -q"
OUT=lake/bench/results; mkdir -p $OUT; CSV=$OUT/load_postgres.csv
echo "step,table,rows,seconds" > $CSV
now() { python3 -c 'import time; print(time.time())'; }

$PG < warehouse/postgres/schema.sql
for t in dim_date dim_customer dim_location dim_transaction_type fact_transactions fact_customer_balance_snapshot; do
  s=$(now)
  $CH -q "SELECT * FROM dwh.$t FORMAT CSV" | $PG -c "\\copy $t FROM STDIN WITH (FORMAT csv, NULL '\\N')"
  e=$(now); n=$($PG -At -c "SELECT count(*) FROM $t")
  echo "copy,$t,$n,$(python3 -c "print(round($e-$s,2))")" | tee -a $CSV
done
s=$(now); $PG < warehouse/postgres/constraints.sql; e=$(now)
echo "constraints+indexes+analyze,all,,$(python3 -c "print(round($e-$s,2))")" | tee -a $CSV
$PG -At -F, -c "SELECT 'size', relname, pg_total_relation_size(relid) FROM pg_stat_user_tables ORDER BY 3 DESC" | tee -a $CSV
