SHELL := bash
.ONESHELL:

API_BASE ?= http://localhost:4999
DFUI_DIR := ./dataframe-api

.PHONY: help up down restart restart-x logs logs-x wait test select select-exclude groupby filter merge pivot compare-identical compare-schema mutate datetime rename rename-columns pivot-longer mutate-row datetime-derive filter-advanced filter-null merge-left merge-outer select-spark select-exclude-spark filter-spark groupby-spark merge-spark pivot-spark compare-identical-spark compare-schema-spark mutate-spark datetime-spark rename-columns-spark pipeline-preview pipeline-run pipeline-save pipeline-load pipeline-run-saved pipeline-list pipeline-export-yaml pipeline-import-yaml chained-pipelines chained-operations dataframe-profile dataframe-download-csv dataframe-download-json api-stats all prepare build build-ui build-ui-x generate-account init-admin flush-redis flush-users list-users

help:
	@echo "Targets:"
	@echo "  up         - start redis, spark, spark-worker, dataframe-api, dataframe-ui-x"
	@echo "  down       - stop all services"
	@echo "  build      - build dataframe-api and dataframe-ui-x images"
	@echo "  build-ui   - build dataframe-api image"
	@echo "  build-ui-x - build dataframe-ui-x image"
	@echo "  restart    - restart dataframe-api"
	@echo "  restart-x  - restart dataframe-ui-x"
	@echo "  logs       - tail dataframe-api logs"
	@echo "  logs-x     - tail dataframe-ui-x logs"
	@echo "  wait       - wait for API readiness"
	@echo "  test       - run all curl tests"
	@echo ""
	@echo "Basic Operation Tests:"
	@echo "  select     - run SELECT test"
	@echo "  select-exclude - run SELECT exclude test"
	@echo "  groupby    - run GROUPBY test"
	@echo "  filter     - run FILTER test"
	@echo "  merge      - run MERGE test"
	@echo "  pivot      - run PIVOT test"
	@echo "  compare-identical - run COMPARE identical test"
	@echo "  compare-schema    - run COMPARE schema mismatch test"
	@echo "  mutate     - run MUTATE test (total_value)"
	@echo "  datetime   - run DATETIME parse test"
	@echo "  rename     - run DataFrame RENAME test"
	@echo ""
	@echo "Advanced Operation Tests:"
	@echo "  rename-columns - run RENAME columns test"
	@echo "  pivot-longer   - run PIVOT longer test"
	@echo "  mutate-row     - run MUTATE row mode test"
	@echo "  datetime-derive - run DATETIME derive test"
	@echo "  filter-advanced - run advanced FILTER test"
	@echo "  filter-null    - run null check FILTER test"
	@echo "  merge-left     - run LEFT MERGE test"
	@echo "  merge-outer    - run OUTER MERGE test"
	@echo ""
	@echo "Spark Engine Tests:"
	@echo "  select-spark     - run SELECT test with Spark engine"
	@echo "  select-exclude-spark - run SELECT exclude test with Spark engine"
	@echo "  filter-spark     - run FILTER test with Spark engine"
	@echo "  groupby-spark    - run GROUPBY test with Spark engine"
	@echo "  merge-spark      - run MERGE test with Spark engine"
	@echo "  pivot-spark      - run PIVOT test with Spark engine"
	@echo "  compare-identical-spark - run COMPARE identical test with Spark engine"
	@echo "  compare-schema-spark    - run COMPARE schema mismatch test with Spark engine"
	@echo "  mutate-spark     - run MUTATE test with Spark engine"
	@echo "  datetime-spark   - run DATETIME parse test with Spark engine"
	@echo "  rename-columns-spark - run RENAME columns test with Spark engine"
	@echo ""
	@echo "Pipeline Tests:"
	@echo "  pipeline-preview      - run PIPELINE preview test"
	@echo "  pipeline-run          - run PIPELINE run test"
	@echo "  pipeline-save         - run PIPELINE save test"
	@echo "  pipeline-load         - run PIPELINE load test"
	@echo "  pipeline-run-saved    - run saved PIPELINE run test"
	@echo "  pipeline-list         - run PIPELINE list test"
	@echo "  pipeline-export-yaml  - run PIPELINE YAML export test"
	@echo "  pipeline-import-yaml  - run PIPELINE YAML import test"
	@echo "  chained-pipelines     - run chained PIPELINE test"
	@echo ""
	@echo "Advanced Tests:"
	@echo "  chained-operations    - run chained operations test"
	@echo "  dataframe-profile     - run DataFrame profile test"
	@echo "  dataframe-download-csv - run DataFrame CSV download test"
	@echo "  dataframe-download-json - run DataFrame JSON download test"
	@echo "  api-stats             - run API stats test"
	@echo ""
	@echo "Database Management:"
	@echo "  generate-account      - create new user account interactively"
	@echo "  init-admin            - initialize admin account"
	@echo "  flush-redis           - clear Redis cache"
	@echo "  flush-users           - remove all users from PostgreSQL"
	@echo "  list-users            - list all users in PostgreSQL"
	@echo ""
	@echo "Visual Testing (Playwright):"
	@echo "  test-visual-build     - build Playwright test container"
	@echo "  test-visual           - run visual tests (headless)"
	@echo "  test-visual-dev       - run visual tests in development mode (with browser)"
	@echo "  test-visual-screenshots - run tests and capture screenshots on failure"
	@echo "  test-visual-clean     - clean up visual test artifacts"

prepare:
	chmod +x $(DFUI_DIR)/test.sh || true

up:
	docker compose -f ./docker-compose.yml up -d redis spark spark-worker dataframe-api story-mode dataframe-ui-x

build:
	docker compose -f ./docker-compose.yml build dataframe-api story-mode dataframe-ui-x

build-ui:
	docker compose -f ./docker-compose.yml build dataframe-api

build-ui-x:
	docker compose -f ./docker-compose.yml build dataframe-ui-x

build-story:
	docker compose -f ./docker-compose.yml build story-mode

restart:
	docker compose -f ./docker-compose.yml restart dataframe-api

restart-x:
	docker compose -f ./docker-compose.yml restart dataframe-ui-x

restart-story:
	docker compose -f ./docker-compose.yml restart story-mode

logs:
	docker compose -f ./docker-compose.yml logs -f dataframe-api

logs-x:
	docker compose -f ./docker-compose.yml logs -f dataframe-ui-x

logs-story:
	docker compose -f ./docker-compose.yml logs -f story-mode

wait: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh wait

test: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh all

select: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh select

select-exclude: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh select-exclude

groupby: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh groupby

filter: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh filter

merge: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh merge

pivot: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh pivot

compare-identical: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh compare-identical

compare-schema: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh compare-schema

mutate: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh mutate

datetime: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh datetime

rename: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh rename

# Additional operation tests

rename-columns: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh rename-columns

pivot-longer: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh pivot-longer

mutate-row: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh mutate-row

datetime-derive: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh datetime-derive

filter-advanced: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh filter-advanced

filter-null: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh filter-null

merge-left: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh merge-left

merge-outer: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh merge-outer

# Spark engine tests

select-spark: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh select-spark

select-exclude-spark: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh select-exclude-spark

filter-spark: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh filter-spark

groupby-spark: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh groupby-spark

merge-spark: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh merge-spark

pivot-spark: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh pivot-spark

compare-identical-spark: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh compare-identical-spark

compare-schema-spark: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh compare-schema-spark

mutate-spark: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh mutate-spark

datetime-spark: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh datetime-spark

rename-columns-spark: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh rename-columns-spark

# Pipeline tests

pipeline-preview: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh pipeline-preview

pipeline-run: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh pipeline-run

pipeline-save: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh pipeline-save

pipeline-load: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh pipeline-load

pipeline-run-saved: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh pipeline-run-saved

pipeline-list: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh pipeline-list

pipeline-export-yaml: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh pipeline-export-yaml

pipeline-import-yaml: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh pipeline-import-yaml

chained-pipelines: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh chained-pipelines

# Advanced tests

chained-operations: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh chained-operations

dataframe-profile: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh dataframe-profile

dataframe-download-csv: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh dataframe-download-csv

dataframe-download-json: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh dataframe-download-json

api-stats: prepare
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh api-stats

down:
	docker compose -f ./docker-compose.yml down

# Authentication and database management commands

generate-account:
	@echo "Creating new user account..."
	./generate-credentials.sh generate

init-admin:
	@echo "Initializing admin account..."
	./generate-credentials.sh init

flush-redis:
	@echo "Flushing Redis cache..."
	docker compose -f ./docker-compose.yml exec redis redis-cli FLUSHALL
	@echo "Redis cache cleared successfully"

flush-users:
	@echo "Flushing all users from PostgreSQL..."
	docker compose -f ./docker-compose.yml exec postgres psql -U dataframe_user -d dataframe_ui -c "DELETE FROM users;"
	@echo "All users cleared from database"

list-users:
	@echo "Listing all users in PostgreSQL..."
	docker compose -f ./docker-compose.yml exec postgres psql -U dataframe_user -d dataframe_ui -c "SELECT username, created_at FROM users ORDER BY created_at;"

# Visual testing with Playwright

test-visual-build:
	@echo "Building Playwright test container..."
	docker compose -f ./docker-compose.yml build playwright-tests

test-visual: prepare
	@echo "Running Playwright visual tests..."
	@echo "Starting required services..."
	docker compose -f ./docker-compose.yml up -d redis dataframe-api dataframe-ui-x
	@echo "Waiting for services to be ready..."
	sleep 15
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh wait
	@echo "Running visual tests..."
	docker compose -f ./docker-compose.yml --profile testing run --rm playwright-tests
	@echo "Visual tests completed."

test-visual-dev:
	@echo "Running Playwright visual tests in development mode (headless with verbose output)..."
	docker compose -f ./docker-compose.yml up -d redis dataframe-api dataframe-ui-x
	sleep 15
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh wait
	docker compose -f ./docker-compose.yml --profile testing run --rm -e HEADLESS=true playwright-tests python -m pytest tests/ -v -s --tb=long

test-visual-screenshots:
	@echo "Running visual tests and capturing screenshots..."
	docker compose -f ./docker-compose.yml up -d redis dataframe-api dataframe-ui-x
	sleep 15
	API_BASE=$(API_BASE) $(DFUI_DIR)/test.sh wait
	docker compose -f ./docker-compose.yml --profile testing run --rm -e SCREENSHOT_ON_FAILURE=true playwright-tests
	@echo "Screenshots saved to ./playwright/screenshots/"

test-visual-clean:
	@echo "Cleaning up visual test artifacts..."
	docker compose -f ./docker-compose.yml --profile testing down
	rm -rf ./playwright/screenshots/*

.PHONY: test-visual-build test-visual test-visual-dev test-visual-screenshots test-visual-clean
