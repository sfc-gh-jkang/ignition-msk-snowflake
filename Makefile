SHELL := /bin/bash
ENV ?= local/.env
COMPOSE := docker compose -f local/docker-compose.yml --env-file $(ENV)
COMPOSE81 := docker compose -f local/docker-compose.81.yml --env-file $(ENV)
SNOW = snow sql -c $$SNOWFLAKE_CONNECTION \
  -D "db=$$SNOWFLAKE_DATABASE" -D "schema=$$SNOWFLAKE_SCHEMA" -D "wh=$$SNOWFLAKE_WAREHOUSE" \
  -D "role=$$SNOWFLAKE_ROLE" -D "user=$$SNOWFLAKE_USER"
LOAD := set -a; source $(ENV); set +a

.PHONY: help keys snowflake-setup batch-setup batch-teardown snowflake-reset local-up local-connector local-verify local-down snowflake-teardown lint rest-setup rest-up rest-verify rest-down dcp-token dcp-up dcp-up-gateway dcp-verify dcp-down dcp-teardown

help:
	@echo "keys               generate the connector key pair in .secrets/"
	@echo "snowflake-setup    create the table, Dynamic Table, role, service user and network policy"
	@echo "local-up           start Kafka, Ignition and Kafka Connect, then register the connector"
	@echo "local-connector    (re)register the Kafka connector on a running local stack"
	@echo "local-verify       row counts, freshness and an offset gap check in Snowflake"
	@echo "local-down         stop the local stack and delete its volumes"
	@echo "snowflake-teardown drop everything snowflake-setup created"
	@echo "rest-setup         Ignition 8.1 mode: REST landing table + dedup Dynamic Table"
	@echo "rest-up            Ignition 8.1 mode: build the gateway backup and start Ignition 8.1"
	@echo "rest-verify        Ignition 8.1 mode: rows, duplicates, freshness, gaps"
	@echo "rest-down          Ignition 8.1 mode: stop"
	@echo "snowflake-reset    teardown + setup; needed after recreating Kafka (see README gotcha 9)"
	@echo "batch-setup        path 0: batched-JDBC test tables and warehouses (after snowflake-setup)"
	@echo "batch-teardown     path 0: drop what batch-setup created"
	@echo "dcp-token          path 2: write the DCP bootstrap token to .secrets/ (0600, not printed)"
	@echo "dcp-up             path 2: plant SQL Server, simulator and DCP agent"
	@echo "dcp-up-gateway     path 2: same, with a real Ignition 8.1.42 gateway instead of the simulator"
	@echo "dcp-verify         path 2: contiguity and freshness of the replicated table"
	@echo "dcp-down           path 2: stop the plant stack, including the gateway"
	@echo "dcp-teardown       path 2: terminate and drop the Openflow runtime, deployment and DCP objects"
	@echo "lint               shellcheck the scripts and validate the compose files"

keys:
	@scripts/gen_keypair.sh

snowflake-setup: keys
	@$(LOAD); $(SNOW) -D "rsa_public_key=$$(scripts/pubkey_body.sh)" -D "allowed_ip=$$SNOWFLAKE_ALLOWED_IP" -f snowflake/setup.sql

local-up:
	@$(LOAD); $(COMPOSE) up -d --build --wait
	@$(MAKE) --no-print-directory local-connector

local-connector:
	@$(LOAD); scripts/register_connector.sh http://localhost:8083

local-verify:
	@$(LOAD); $(SNOW) -f snowflake/verify.sql

local-down:
	$(COMPOSE) down -v

snowflake-teardown:
	@$(LOAD); $(SNOW) -f snowflake/teardown.sql

snowflake-reset: snowflake-teardown snowflake-setup

# Path 0 (batched JDBC) test tables and warehouses; run after snowflake-setup.
batch-setup:
	@$(LOAD); $(SNOW) -f snowflake/setup_batch.sql

batch-teardown:
	@$(LOAD); $(SNOW) -f snowflake/teardown_batch.sql

rest-setup:
	@$(LOAD); $(SNOW) -f snowflake/setup_rest.sql

rest-up:
	ignition81/make_gwbk.sh
	@# Source the env file so it wins over any SNOWFLAKE_* already exported in the shell;
	@# compose otherwise prefers shell variables to --env-file.
	@$(LOAD); $(COMPOSE81) up -d --wait --force-recreate

rest-verify:
	@$(LOAD); $(SNOW) -f snowflake/verify_rest.sql

rest-down:
	$(COMPOSE81) down -v

COMPOSEDCP := docker compose -f openflow-dcp/docker-compose.yml --env-file openflow-dcp/.env
COMPOSEDCP_EXAMPLE := docker compose -f openflow-dcp/docker-compose.yml --env-file openflow-dcp/.env.example

# DCP bootstrap JWT -> .secrets/dcp-bootstrap-token (0600), never printed. Needs dcp_setup.sql section 3.
dcp-token:
	@$(LOAD); mkdir -p .secrets && chmod 700 .secrets && umask 077 && \
	  snow sql -c $$SNOWFLAKE_CONNECTION --role OPENFLOW_ADMIN --format json \
	    -q "SELECT SYSTEM\$$GENERATE_DATA_CONNECTIVITY_PROXY_BOOTSTRAP_TOKEN('IGNITION_PLANT_DCP', $${DCP_TOKEN_DAYS:-7}) AS T" \
	  | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["T"], end="")' > .secrets/dcp-bootstrap-token && \
	  test -s .secrets/dcp-bootstrap-token && echo "wrote .secrets/dcp-bootstrap-token"

dcp-up:
	$(COMPOSEDCP) up -d --wait sqlserver
	$(COMPOSEDCP) up -d simulator dcp-agent

# Option B with a real Ignition 8.1.42 gateway in place of the simulator.
dcp-up-gateway:
	@set -a; source openflow-dcp/.env; set +a; \
	  PLANT_SQL_PASSWORD="$$IGNITION_SQL_PASSWORD" GWBK_NAME=plant.gwbk ignition81/make_gwbk.sh
	SIMULATE=0 $(COMPOSEDCP) up -d --wait sqlserver
	SIMULATE=0 $(COMPOSEDCP) up simulator
	SIMULATE=0 $(COMPOSEDCP) --profile gateway up -d --wait ignition81
	$(COMPOSEDCP) up -d dcp-agent

dcp-verify:
	@$(LOAD); snow sql -c $$SNOWFLAKE_CONNECTION --role OPENFLOW_ADMIN -f openflow-dcp/snowflake/verify.sql

dcp-down:
	$(COMPOSEDCP) --profile gateway down -v

# Snowflake side of path 2: waits for each TERMINATE before its DROP (see scripts/dcp_teardown.sh).
dcp-teardown:
	@$(LOAD); scripts/dcp_teardown.sh $$SNOWFLAKE_CONNECTION

lint:
	shellcheck scripts/*.sh connect/*.sh ignition/*.sh ignition81/*.sh aws/msk-connect/*.sh openflow-dcp/simulator/*.sh
	$(COMPOSE) config -q
	$(COMPOSE81) config -q
	$(COMPOSEDCP_EXAMPLE) config -q
