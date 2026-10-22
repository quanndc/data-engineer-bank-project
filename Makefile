# Banking Data Warehouse (Kimball) -- CO5173 Group 27
TOPIC   := transactions

# ClickHouse target comes from .env: CLICKHOUSE_HOST unset or "clickhouse" = local container,
# anything else = cloud service (Aiven, ...), and the local container is not started.
CLICKHOUSE_HOST := $(or $(shell sed -n 's/^CLICKHOUSE_HOST=//p' .env 2>/dev/null),clickhouse)
ifeq ($(CLICKHOUSE_HOST),clickhouse)
COMPOSE := docker compose --profile local-ch
else
COMPOSE := docker compose
endif

.PHONY: sql up down ps logs topic smoke clean produce ingest core load dq pipeline marts stream stream-stop stream-reset stream-logs produce-live app-up app-down app-bootstrap

up:            ## Build and start Kafka, Spark and (local mode) ClickHouse
	@echo "ClickHouse: $(CLICKHOUSE_HOST)"
	mkdir -p lake
	$(COMPOSE) up -d --build --wait
	$(MAKE) topic

down:          ## Stop the stack (keeps data volumes)
	$(COMPOSE) down

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=100

topic:         ## Create the transactions topic (3 partitions) if missing
	$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
	  --create --if-not-exists --topic $(TOPIC) --partitions 3 --replication-factor 1

smoke:         ## Check that every service answers
	@echo "== Kafka topics";      $(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
	@echo "== ClickHouse ($(CLICKHOUSE_HOST))"; $(COMPOSE) exec -T spark python3 /opt/jobs/ch_sql.py "SELECT version(), arrayStringConcat(groupArray(name), ', ') FROM system.databases WHERE name IN ('dwh','mart','dq')"
	@echo "== Spark";             $(COMPOSE) exec -T spark spark-submit --version 2>&1 | grep -m1 -o "version [0-9.]*"
	@echo "== Spark -> Kafka/ClickHouse libs"; $(COMPOSE) exec -T spark python3 -c "import importlib.metadata, confluent_kafka, clickhouse_connect, pyspark; print('confluent-kafka', confluent_kafka.version()[0], '| clickhouse-connect', importlib.metadata.version('clickhouse-connect'), '| pyspark', pyspark.__version__)"
	@echo "== Source data mounted"; $(COMPOSE) exec -T spark ls /data/source/dwh

# ---------------------------------------------------------------- pipeline (P1-P2)
SPARK := $(COMPOSE) exec -T spark

sql:           ## Run one statement on the configured ClickHouse: make sql Q="SELECT count() FROM dwh.fact_transactions"
	$(SPARK) python3 /opt/jobs/ch_sql.py "$(Q)"

produce:       ## P1: replay all source transactions into Kafka (unthrottled)
	$(SPARK) python3 /opt/ingestion/producer.py

ingest:        ## P1: Kafka + CSV -> raw Parquet
	$(SPARK) spark-submit /opt/jobs/p1_ingest_raw.py

core:          ## P2: raw -> staging -> core (star schema) Parquet
	$(SPARK) spark-submit /opt/jobs/p2_build_core.py

load:          ## P2: core Parquet -> ClickHouse dwh.*
	$(SPARK) python3 /opt/jobs/p2_load_clickhouse.py

dq:            ## P2: data correctness checks -> dq.audit
	$(SPARK) spark-submit /opt/jobs/p2_dq_checks.py

marts:         ## P3: build data marts and run the 10 BR queries (results -> lake/results)
	$(SPARK) python3 /opt/jobs/p3_marts_and_queries.py

# ---------------------------------------------------------------- streaming (P4, BR7)
RATE  ?= 2000
LIMIT ?= 40000
SKIP  ?= 0

stream:        ## P4: start the alert stream in the background (new events only)
	$(SPARK) mkdir -p /lake/logs
	$(COMPOSE) exec -d spark sh -c "spark-submit /opt/jobs/p4_stream_alerts.py > /lake/logs/stream_alerts.log 2>&1"
	@echo "stream started; follow it with: make stream-logs"

stream-stop:   ## P4: stop the alert stream
	-$(SPARK) pkill -f p4_stream_alerts.py

stream-reset:  ## P4: stop the stream, empty the alert table and drop the stream checkpoint
	-$(SPARK) pkill -f p4_stream_alerts.py
	$(SPARK) python3 /opt/jobs/ch_sql.py "TRUNCATE TABLE IF EXISTS dwh.fact_transaction_alert"
	$(SPARK) rm -rf /lake/checkpoints/alerts

stream-logs:
	$(SPARK) tail -f /lake/logs/stream_alerts.log

produce-live:  ## P4: replay LIMIT events at RATE events/s from SKIP (e.g. make produce-live RATE=2000 LIMIT=40000 SKIP=100000)
	$(SPARK) python3 /opt/ingestion/producer.py --rate $(RATE) --limit $(LIMIT) --skip $(SKIP)

# ---------------------------------------------------------------- application (P5)
app-up:        ## P5: start Superset (http://localhost:8088) and Streamlit (http://localhost:8501)
	$(COMPOSE) --profile app up -d --build --wait superset streamlit

app-down:
	$(COMPOSE) --profile app stop superset streamlit

app-bootstrap: ## P5: (re)create the ClickHouse connection, datasets, charts, dashboards
	$(COMPOSE) exec -T superset python /opt/bootstrap/bootstrap.py

pipeline: ingest core load dq marts   ## P1-P3 end to end (run `make produce` once before)

clean:         ## Stop the stack AND delete Kafka/ClickHouse volumes and the lake
	$(COMPOSE) down -v
	rm -rf lake
