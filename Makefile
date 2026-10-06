# Banking Data Warehouse (Kimball) -- CO5173 Group 27
COMPOSE := docker compose
TOPIC   := transactions

.PHONY: up down ps logs topic smoke clean produce ingest core load dq pipeline marts

up:            ## Build and start Kafka, ClickHouse, Spark
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
	@echo "== ClickHouse";        $(COMPOSE) exec -T clickhouse clickhouse-client -u bank --password bank -q "SELECT version(), arrayStringConcat(groupArray(name), ', ') FROM system.databases WHERE name IN ('dwh','mart','dq')"
	@echo "== Spark";             $(COMPOSE) exec -T spark spark-submit --version 2>&1 | grep -m1 -o "version [0-9.]*"
	@echo "== Spark -> Kafka/ClickHouse libs"; $(COMPOSE) exec -T spark python3 -c "import importlib.metadata, confluent_kafka, clickhouse_connect, pyspark; print('confluent-kafka', confluent_kafka.version()[0], '| clickhouse-connect', importlib.metadata.version('clickhouse-connect'), '| pyspark', pyspark.__version__)"
	@echo "== Source data mounted"; $(COMPOSE) exec -T spark ls /data/source/dwh

# ---------------------------------------------------------------- pipeline (P1-P2)
SPARK := $(COMPOSE) exec -T spark

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

pipeline: ingest core load dq marts   ## P1-P3 end to end (run `make produce` once before)

clean:         ## Stop the stack AND delete Kafka/ClickHouse volumes and the lake
	$(COMPOSE) down -v
	rm -rf lake
