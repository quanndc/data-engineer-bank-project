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

.PHONY: up down ps logs smoke sql psql clean source-check tiers schema load-ref load-history rerun reconcile scenarios scenario alerts backup restore backups evaluate bench-load bench bench-concurrency bench-update bench-ops bench-spark bench-all

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

# ---------------------------------------------------------------- evaluation (P2-8)
RESULTS := docs/results/p2

evaluate:      ## KB01-KB08 + data-quality checks against the source and expected.json
	$(SPARK) bash -c "cd /opt/jobs && python3 evaluate.py"; s=$$?; mkdir -p $(RESULTS); \
	cp lake/eval/kb_results.json $(RESULTS)/; exit $$s

# every benchmark step keeps its console log in lake/bench/p2/logs and copies the results to docs
BENCH = mkdir -p lake/bench/p2/logs $(RESULTS)/logs; \
	$(SPARK) bash -c "cd /opt/jobs && python3 $(1)" 2>&1 | grep -v -E " (INFO|WARN) |^\[Stage" \
	| tee lake/bench/p2/logs/$(2).log; s=$${PIPESTATUS[0]}; \
	cp lake/bench/p2/*.csv lake/bench/p2/*.json $(RESULTS)/ 2>/dev/null; cp lake/bench/p2/logs/*.log $(RESULTS)/logs/; \
	exit $$s

bench-load:    ## B1/B2: copy dw into bench (ClickHouse star + One Big Table) and dwpg (PostgreSQL)
	@$(call BENCH,benchmark.py load,load)

bench:         ## B1/B2: 9 BR queries x 3 solutions (6 runs each), result check, storage
	@$(call BENCH,benchmark.py run,run)

bench-concurrency: ## B2: 1/4/8 concurrent clients on a query mix
	@$(call BENCH,benchmark.py concurrency,concurrency)

bench-update:  ## B1: cost of Type 1 corrections (star vs One Big Table vs PostgreSQL)
	@$(call BENCH,benchmark.py update,update)

bench-ops:     ## B3: operational store on PostgreSQL vs ClickHouse (writes, reads, integrity)
	@$(call BENCH,bench_ops.py,ops)

bench-spark:   ## B4: Spark transformation with 1/2/4/8 cores, one batch vs 72 monthly batches
	@$(call BENCH,bench_spark.py,spark)

bench-all: bench-load bench bench-concurrency bench-update bench-ops bench-spark ## All benchmarks, in order

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
