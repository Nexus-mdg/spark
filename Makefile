SHELL := bash
.ONESHELL:

API_BASE ?= http://localhost:4999
DFUI_DIR := ./dataframe-api

.PHONY: help up down restart restart-x logs logs-x wait test select select-exclude groupby filter merge pivot compare-identical compare-schema mutate datetime rename all prepare build build-ui build-ui-x

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

prepare:
	chmod +x $(DFUI_DIR)/test.sh || true

up:
	docker compose -f ./docker-compose.yml up -d redis spark spark-worker dataframe-api dataframe-ui-x

build:
	docker compose -f ./docker-compose.yml build dataframe-api dataframe-ui-x

build-ui:
	docker compose -f ./docker-compose.yml build dataframe-api

build-ui-x:
	docker compose -f ./docker-compose.yml build dataframe-ui-x

restart:
	docker compose -f ./docker-compose.yml restart dataframe-api

restart-x:
	docker compose -f ./docker-compose.yml restart dataframe-ui-x

logs:
	docker compose -f ./docker-compose.yml logs -f dataframe-api

logs-x:
	docker compose -f ./docker-compose.yml logs -f dataframe-ui-x

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

down:
	docker compose -f ./docker-compose.yml down
