# Banking Data Warehouse (Kimball) -- CO5173 Group 27 -- Phase 2 (Berka)
# Phase 1 (Kaggle, Kafka, Superset) is archived in archive/phase1 and in the git tag phase1-kaggle.

# ClickHouse target comes from .env: CLICKHOUSE_HOST unset or "clickhouse" = local container,
# anything else = cloud service (Aiven, ...), and the local container is not started.
CLICKHOUSE_HOST := $(or $(shell sed -n 's/^CLICKHOUSE_HOST=//p' .env 2>/dev/null),clickhouse)
ifeq ($(CLICKHOUSE_HOST),clickhouse)
COMPOSE := docker compose --profile local-ch
else
COMPOSE := docker compose
endif
SPARK := $(COMPOSE) exec -T spark
SHELL := /bin/bash   # PIPESTATUS in the benchmark targets

.PHONY: up down ps logs smoke sql psql clean source-check tiers schema load-ref load-history rerun reconcile scenarios scenario alerts backup restore backups evaluate bench-load bench bench-concurrency bench-update bench-ops bench-spark bench-engines-prepare bench-engines bench-all bench-clean publish viewer

up:            ## Start D ClickHouse, E PostgreSQL, C Spark, F app (http://localhost:8501)
	@echo "ClickHouse: $(CLICKHOUSE_HOST)"
	mkdir -p lake backups
	$(COMPOSE) up -d --build --wait

down:          ## Stop the stack (keeps data volumes)
	$(COMPOSE) down

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=100

smoke:         ## Check that every block of the architecture answers
	$(SPARK) python3 /opt/jobs/smoke.py
	@curl -fs http://localhost:8501/_stcore/health >/dev/null && echo "F App        : http://localhost:8501 ok"

schema:        ## P2-2: create dw (ClickHouse) and ops (PostgreSQL); check columns against the Data Dictionary
	$(SPARK) python3 /opt/jobs/apply_schema.py

# ---------------------------------------------------------------- batch pipeline (P2-3)
FROM ?= 1993-01
TO   ?= 1998-12
PIPELINE := $(SPARK) spark-submit /opt/jobs/pipeline.py

load-ref:      ## REF_INITIAL: district, account, client + OWNER + tier -> dimensions (SCD2 first versions)
	$(PIPELINE) ref

load-history:  ## HIST_YYYY_MM batches from FROM to TO (default 1993-01 .. 1998-12)
	$(PIPELINE) history $(FROM) $(TO)

rerun:         ## Re-run one batch from its Data Lake snapshot: make rerun BATCH=HIST_1995_03
	$(PIPELINE) rerun $(BATCH)

scenarios:     ## Generate the scenario input files (data/berka/scenarios/KBxx) on the host
	python3 tools/make_scenarios.py

scenario:      ## Run one scenario batch: make scenario KB=KB05
	$(PIPELINE) scenario $(KB)

alerts:        ## Evaluate the alert rules on loaded data (back-fill): make alerts [ON=all|BATCH_ID]
	$(PIPELINE) alerts $(or $(ON),all)

# ---------------------------------------------------------------- backup and restore (P2-7, BR07)
backup:        ## Back up the 6 warehouse tables: make backup [NAME=...]
	$(SPARK) python3 /opt/jobs/backup.py backup $(NAME)

restore:       ## Restore the warehouse to a backup: make restore NAME=...
	$(SPARK) python3 /opt/jobs/backup.py restore $(NAME)

backups:       ## List the backups in the catalog
	$(SPARK) python3 /opt/jobs/backup.py list

reconcile:     ## Whole Berka source vs. warehouse (count, distinct ids, money in/out)
	$(PIPELINE) reconcile

# ---------------------------------------------------------------- viewer copy for the members
AIVEN_VARS := AIVEN_CLICKHOUSE_HOST AIVEN_CLICKHOUSE_PORT AIVEN_CLICKHOUSE_USER AIVEN_CLICKHOUSE_PASSWORD \
              AIVEN_CLICKHOUSE_SECURE AIVEN_CLICKHOUSE_DATABASE_ENGINE

publish:       ## Copy the 6 dw tables to Aiven ClickHouse (AIVEN_CLICKHOUSE_* in .env) for the viewer app
	@set -a; . ./.env; set +a; \
	$(COMPOSE) exec -T $(foreach v,$(AIVEN_VARS),-e $(v)) spark python3 /opt/jobs/publish.py

viewer:        ## Read-only app on http://localhost:8502 (BR01, BR02, BR08, SQL; ClickHouse only)
	$(COMPOSE) --profile viewer up -d app-viewer

# ---------------------------------------------------------------- evaluation (P2-8)
RESULTS := docs/results/p2

evaluate:      ## KB01-KB08 + data-quality checks against the source and expected.json
	$(SPARK) bash -c "cd /opt/jobs && python3 evaluate.py"; s=$$?; mkdir -p $(RESULTS); \
	cp lake/eval/kb_results.json $(RESULTS)/; exit $$s

# every benchmark step keeps its console log and copies the results to docs.
# SCALE=10 runs on the x10 copy (bench10 / dwpg10); results in .../x10. dw is never written.
SCALE ?= 1
BDIR := lake/bench/p2$(if $(filter-out 1,$(SCALE)),/x$(SCALE))
BRES := $(RESULTS)$(if $(filter-out 1,$(SCALE)),/x$(SCALE))
BENCH = mkdir -p $(BDIR)/logs $(BRES)/logs; \
	$(COMPOSE) exec -T -e BENCH_SCALE=$(SCALE) spark bash -c "cd /opt/jobs && python3 $(1)" 2>&1 \
	| grep -v -E " (INFO|WARN) |^\[Stage" | tee $(BDIR)/logs/$(2).log; s=$${PIPESTATUS[0]}; \
	cp $(BDIR)/*.csv $(BDIR)/*.json $(BRES)/ 2>/dev/null; cp $(BDIR)/logs/*.log $(BRES)/logs/; \
	exit $$s

bench-load:    ## B1/B2: copies for the benchmark (x1 from dw; SCALE=10 from the x1 copy)
	@$(call BENCH,benchmark.py load,load)

bench:         ## B1/B2: 9 BR queries x 3 solutions (6 runs each), result check, storage
	@$(call BENCH,benchmark.py run,run)

bench-concurrency: ## B2: 1/4/8 concurrent clients on a query mix
	@$(call BENCH,benchmark.py concurrency,concurrency)

bench-update:  ## B1: cost of Type 1 corrections (star vs One Big Table vs PostgreSQL)
	@$(call BENCH,benchmark.py update,update)

bench-ops:     ## B3: operational store on PostgreSQL vs ClickHouse (x1 only: it does not grow with dw)
	@$(call BENCH,bench_ops.py,ops)

bench-spark:   ## B4: Spark transformation with 1/2/4/8 cores, one batch vs 72 monthly batches
	@$(call BENCH,bench_spark.py,spark)

# B5 runs both scales in one go (SCALE is not used). Spark and Flink alternate on each configuration
# (3 times each), never at the same time; the Flink cluster runs only during B5.
ENG := lake/bench/p2/engines
HADOOP_JARS := hadoop-client-api-3.3.4.jar hadoop-client-runtime-3.3.4.jar commons-logging-1.1.3.jar zstd-jni-1.5.5-4.jar

bench-engines-prepare: ## B5: shared Parquet inputs for Spark and Flink (x1 from dw, x10 from bench10)
	$(COMPOSE) exec -T spark bash -c "cd /opt/jobs && python3 bench_engines.py prepare"

bench-engines: ## B5: Spark vs Flink, same transformation, x1 and x10, 1/2/4/8 cores, 1 vs 72 batches
	@test -f flink/lib/flink-sql-parquet-1.20.1.jar || { mkdir -p flink/lib && curl -fsSL -o flink/lib/flink-sql-parquet-1.20.1.jar \
	  https://repo1.maven.org/maven2/org/apache/flink/flink-sql-parquet/1.20.1/flink-sql-parquet-1.20.1.jar; }
	@mkdir -p flink/lib/hadoop; for j in $(HADOOP_JARS); do test -f flink/lib/hadoop/$$j || \
	  docker cp bankdw-spark:/opt/spark/jars/$$j flink/lib/hadoop/; done
	$(COMPOSE) --profile flink up -d flink-jobmanager flink-taskmanager
	@until curl -sf http://127.0.0.1:18081/taskmanagers | grep -q '"slotsNumber":8'; do sleep 2; done
	@mkdir -p $(ENG)/logs $(RESULTS)/engines/logs; python3 flink/jobs/bench_flink.py 2>&1 | tee $(ENG)/logs/engines.log; \
	s=$${PIPESTATUS[0]}; cp $(ENG)/*.csv $(RESULTS)/engines/; cp $(ENG)/logs/engines.log $(RESULTS)/engines/logs/; \
	$(COMPOSE) --profile flink stop flink-jobmanager flink-taskmanager; exit $$s

bench-all: bench-load bench bench-concurrency bench-update $(if $(filter 1,$(SCALE)),bench-ops) bench-spark ## All benchmarks (make bench-all SCALE=10)

bench-clean:   ## Drop every benchmark copy (bench*, dwpg*, bench_ops) and the x10 Spark input; dw and ops untouched
	$(SPARK) python3 -c "import sys; sys.path.insert(0, '/opt/jobs'); from common import clickhouse, postgres; \
	ch = clickhouse(); [ch.command(f'DROP DATABASE IF EXISTS {d} SYNC') for d in ('bench', 'bench10', 'bench50')]; \
	c = postgres().cursor(); [c.execute(f'DROP DATABASE IF EXISTS {d}') for d in ('dwpg', 'dwpg10', 'dwpg50')]"
	rm -rf lake/bench/p2/x*/input lake/bench/p2/b4_out lake/bench/p2/x*/b4_out lake/bench/p2/engines

sql:           ## One statement on ClickHouse: make sql Q="SELECT count() FROM dw.fact_transaction"
	$(SPARK) python3 /opt/jobs/ch_sql.py "$(Q)"

psql:          ## Shell on the operational store
	$(COMPOSE) exec postgres psql -U bank -d bankops

# ---------------------------------------------------------------- source (P2-0, run on the host)
source-check:  ## Re-check the sheet survey K01-K05 on data/berka/raw (needs tools/requirements.txt)
	python3 tools/profile_berka.py

tiers:         ## Regenerate data/berka/customer_profile_simulated.csv
	python3 tools/simulate_membership_tier.py

clean:         ## Stop the stack AND delete ClickHouse/PostgreSQL volumes, the lake and backups
	$(COMPOSE) down -v
	rm -rf lake backups/*
