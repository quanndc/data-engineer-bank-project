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

.PHONY: up down ps logs smoke sql psql clean source-check tiers schema load-ref load-history rerun reconcile scenarios scenario alerts backup restore backups

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
