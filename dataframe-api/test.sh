#!/usr/bin/env bash
set -euo pipefail

API_BASE="${API_BASE:-http://localhost:4999}"
SAMPLES_DIR="$(cd "$(dirname "$0")" && pwd)/data/sample"

wait_api() {
  echo -n "Waiting for API at ${API_BASE} ..."
  for _ in {1..60}; do
    if curl -fsS "$API_BASE/api/stats" >/dev/null; then
      echo " ready."
      return 0
    fi
    echo -n "."
    sleep 1
  done
  echo "\nAPI not reachable after timeout" >&2
  return 1
}

ensure_upload() {
  local name="$1"; shift
  local file="$1"; shift
  echo "Uploading ${name} from ${file} (if not exists) ..."
  # Try upload; ignore conflict 409
  http_code=$(curl -sS -o /dev/null -w "%{http_code}" -F "file=@${file}" -F "name=${name}" "${API_BASE}/api/dataframes/upload" || true)
  if [[ "$http_code" == "201" || "$http_code" == "200" ]]; then
    echo "Uploaded ${name}."
  elif [[ "$http_code" == "409" ]]; then
    echo "${name} already exists, continuing."
  else
    echo "Upload returned HTTP ${http_code} (continuing anyway)."
  fi
}

# Individual tests (use cached names, not URLs)

test_select() {
  echo "\n[TEST] SELECT: people columns=id,name"
  curl -sS "${API_BASE}/api/ops/select/get?name=people&columns=id,name" | python3 -m json.tool || true
}

# New: select exclude

test_select_exclude() {
  echo "\n[TEST] SELECT (exclude): people drop id,name"
  curl -sS "${API_BASE}/api/ops/select/get?name=people&columns=id,name&exclude=true" | python3 -m json.tool || true
}

test_groupby() {
  echo "\n[TEST] GROUPBY: purchases by product sum(quantity)"
  AGGS='%7B%22quantity%22%3A%22sum%22%7D'
  curl -sS "${API_BASE}/api/ops/groupby/get?name=purchases&by=product&aggs=${AGGS}" | python3 -m json.tool || true
}

test_filter() {
  echo "\n[TEST] FILTER: people age>=30 AND city==New York"
  FILTERS='%5B%7B%22col%22%3A%22age%22%2C%22op%22%3A%22gte%22%2C%22value%22%3A30%7D%2C%7B%22col%22%3A%22city%22%2C%22op%22%3A%22eq%22%2C%22value%22%3A%22New%20York%22%7D%5D'
  curl -sS "${API_BASE}/api/ops/filter/get?name=people&combine=and&filters=${FILTERS}" | python3 -m json.tool || true
}

test_merge() {
  echo "\n[TEST] MERGE: people with purchases on id (inner)"
  curl -sS "${API_BASE}/api/ops/merge/get?names=people&names=purchases&keys=id&how=inner" | python3 -m json.tool || true
}

test_pivot() {
  echo "\n[TEST] PIVOT (wider): purchases index=city names_from=product values_from=quantity agg=sum"
  curl -sS "${API_BASE}/api/ops/pivot/get?name=purchases&mode=wider&index=city&names_from=product&values_from=quantity&aggfunc=sum" | python3 -m json.tool || true
}

test_compare_identical() {
  echo "\n[TEST] COMPARE: people vs people (identical)"
  curl -sS "${API_BASE}/api/ops/compare/get?name1=people&name2=people" | python3 -m json.tool || true
}

test_compare_schema() {
  echo "\n[TEST] COMPARE: people vs purchases (schema mismatch)"
  curl -sS "${API_BASE}/api/ops/compare/get?name1=people&name2=purchases" | python3 -m json.tool || true
}

# New: mutate tests (POST JSON)

test_mutate_total_value() {
  echo "\n[TEST] MUTATE (vector): purchases total_value = quantity * price"
  curl -sS -X POST "${API_BASE}/api/ops/mutate" \
    -H 'Content-Type: application/json' \
    -d '{"name":"purchases","target":"total_value","mode":"vector","expr":"col('\''quantity'\'') * col('\''price'\'')"}' | python3 -m json.tool || true
}

# New: datetime test (POST JSON)

test_datetime_parse() {
  echo "\n[TEST] DATETIME parse: purchases date -> date_dt"
  curl -sS -X POST "${API_BASE}/api/ops/datetime" \
    -H 'Content-Type: application/json' \
    -d '{"name":"purchases","action":"parse","source":"date","target":"date_dt","overwrite":true}' | python3 -m json.tool || true
}

# New: rename dataframe name and description

test_rename_dataframe() {
  echo "\n[TEST] RENAME DataFrame: people -> temp name, update description, then revert"
  local tmp_name="people_renamed_$(date +%s)"
  # Rename to temporary name with new description
  curl -sS -X POST "${API_BASE}/api/dataframes/people/rename" \
    -H 'Content-Type: application/json' \
    -d "{\"new_name\":\"${tmp_name}\",\"description\":\"Renamed via test\"}" | python3 -m json.tool || true
  # Verify new name exists
  curl -sS "${API_BASE}/api/dataframes/${tmp_name}" | python3 -m json.tool || true
  # Attempt to rename back to 'people'; if conflict, delete and retry
  http_code=$(curl -sS -o /dev/null -w "%{http_code}" -X POST "${API_BASE}/api/dataframes/${tmp_name}/rename" \
    -H 'Content-Type: application/json' \
    -d '{"new_name":"people"}') || http_code=000
  if [[ "$http_code" == "409" ]]; then
    echo "Conflict renaming back to 'people', deleting existing and retrying..."
    curl -sS -X DELETE "${API_BASE}/api/dataframes/people" | python3 -m json.tool || true
    curl -sS -X POST "${API_BASE}/api/dataframes/${tmp_name}/rename" \
      -H 'Content-Type: application/json' \
      -d '{"new_name":"people"}' | python3 -m json.tool || true
  elif [[ "$http_code" == "200" ]]; then
    echo "Renamed back to people."
  else
    echo "Unexpected HTTP ${http_code} while renaming back (continuing)."
  fi
  # Final check
  curl -sS "${API_BASE}/api/dataframes/people" | python3 -m json.tool || true
}

# Additional operation tests

test_rename_columns() {
  echo "\n[TEST] RENAME columns: people id->person_id, name->full_name"
  curl -sS -X POST "${API_BASE}/api/ops/rename" \
    -H 'Content-Type: application/json' \
    -d '{"name":"people","map":{"id":"person_id","name":"full_name"}}' | python3 -m json.tool || true
}

test_pivot_longer() {
  echo "\n[TEST] PIVOT (longer): purchases melt product,quantity into variable,value"
  curl -sS -X POST "${API_BASE}/api/ops/pivot" \
    -H 'Content-Type: application/json' \
    -d '{"name":"purchases","mode":"longer","id_vars":["id","city","date"],"value_vars":["product","quantity"],"var_name":"attribute","value_name":"value"}' | python3 -m json.tool || true
}

test_mutate_row_mode() {
  echo "\n[TEST] MUTATE (row): purchases full_description from row context"
  curl -sS -X POST "${API_BASE}/api/ops/mutate" \
    -H 'Content-Type: application/json' \
    -d '{"name":"purchases","target":"full_description","mode":"row","expr":"str(r[\"product\"]) + \" in \" + str(r[\"city\"])","overwrite":true}' | python3 -m json.tool || true
}

test_datetime_derive() {
  echo "\n[TEST] DATETIME derive: purchases extract year,month,day from date"
  curl -sS -X POST "${API_BASE}/api/ops/datetime" \
    -H 'Content-Type: application/json' \
    -d '{"name":"purchases","action":"derive","source":"date","outputs":{"year":true,"month":true,"day":true},"names":{"year":"purchase_year","month":"purchase_month","day":"purchase_day"},"month_style":"short","overwrite":true}' | python3 -m json.tool || true
}

test_filter_advanced() {
  echo "\n[TEST] FILTER advanced: people age in [25,30,35] OR city contains 'New'"
  FILTERS_IN='%5B%7B%22col%22%3A%22age%22%2C%22op%22%3A%22in%22%2C%22value%22%3A%5B25%2C30%2C35%5D%7D%2C%7B%22col%22%3A%22city%22%2C%22op%22%3A%22contains%22%2C%22value%22%3A%22New%22%7D%5D'
  curl -sS "${API_BASE}/api/ops/filter/get?name=people&combine=or&filters=${FILTERS_IN}" | python3 -m json.tool || true
}

test_filter_null_checks() {
  echo "\n[TEST] FILTER null checks: purchases quantity not null"
  curl -sS -X POST "${API_BASE}/api/ops/filter" \
    -H 'Content-Type: application/json' \
    -d '{"name":"purchases","filters":[{"col":"quantity","op":"notnull"}],"combine":"and"}' | python3 -m json.tool || true
}

test_merge_left() {
  echo "\n[TEST] MERGE (left): people with purchases on id"
  curl -sS -X POST "${API_BASE}/api/ops/merge" \
    -H 'Content-Type: application/json' \
    -d '{"names":["people","purchases"],"keys":["id"],"how":"left"}' | python3 -m json.tool || true
}

test_merge_outer() {
  echo "\n[TEST] MERGE (outer): people with purchases on id"  
  curl -sS -X POST "${API_BASE}/api/ops/merge" \
    -H 'Content-Type: application/json' \
    -d '{"names":["people","purchases"],"keys":["id"],"how":"outer"}' | python3 -m json.tool || true
}

# Pipeline operation tests

test_pipeline_preview() {
  echo "\n[TEST] PIPELINE preview: select + filter on people"
  curl -sS -X POST "${API_BASE}/api/pipeline/preview" \
    -H 'Content-Type: application/json' \
    -d '{"start":"people","steps":[{"op":"select","params":{"columns":["id","name","age"]}},{"op":"filter","params":{"filters":[{"col":"age","op":"gte","value":25}],"combine":"and"}}],"preview_rows":5}' | python3 -m json.tool || true
}

test_pipeline_run() {
  echo "\n[TEST] PIPELINE run: select + groupby on purchases"
  curl -sS -X POST "${API_BASE}/api/pipeline/run" \
    -H 'Content-Type: application/json' \
    -d '{"start":"purchases","steps":[{"op":"select","params":{"columns":["city","quantity"]}},{"op":"groupby","params":{"by":["city"],"aggs":{"quantity":"sum"}}}],"materialize":true}' | python3 -m json.tool || true
}

test_pipeline_save() {
  echo "\n[TEST] PIPELINE save: create a test pipeline"
  curl -sS -X POST "${API_BASE}/api/pipelines" \
    -H 'Content-Type: application/json' \
    -d '{"name":"test_pipeline","description":"Test pipeline for validation","start":"people","steps":[{"op":"select","params":{"columns":["id","name"]}},{"op":"filter","params":{"filters":[{"col":"id","op":"lte","value":3}],"combine":"and"}}],"overwrite":true}' | python3 -m json.tool || true
}

test_pipeline_load() {
  echo "\n[TEST] PIPELINE load: get test pipeline"
  curl -sS "${API_BASE}/api/pipelines/test_pipeline" | python3 -m json.tool || true
}

test_pipeline_run_saved() {
  echo "\n[TEST] PIPELINE run saved: execute test_pipeline"
  curl -sS -X POST "${API_BASE}/api/pipelines/test_pipeline/run" \
    -H 'Content-Type: application/json' \
    -d '{"materialize":true}' | python3 -m json.tool || true
}

test_pipeline_list() {
  echo "\n[TEST] PIPELINE list: get all pipelines"
  curl -sS "${API_BASE}/api/pipelines" | python3 -m json.tool || true
}

test_pipeline_export_yaml() {
  echo "\n[TEST] PIPELINE export YAML: export test_pipeline"
  curl -sS "${API_BASE}/api/pipelines/test_pipeline/export.yml" || true
}

test_pipeline_import_yaml() {
  echo "\n[TEST] PIPELINE import YAML: import a simple pipeline"
  local yaml_content='name: imported_test_pipeline
description: Imported test pipeline
start: people
steps:
- op: select
  params:
    columns: [id, name, age]
- op: filter
  params:
    filters:
    - col: age
      op: gte
      value: 30
    combine: and'
  
  curl -sS -X POST "${API_BASE}/api/pipelines/import" \
    -H 'Content-Type: application/json' \
    -d "{\"yaml\":$(echo "$yaml_content" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))'),\"overwrite\":true}" | python3 -m json.tool || true
}

test_chained_pipelines() {
  echo "\n[TEST] CHAINED pipelines: create and execute chained pipeline operations"
  
  # First, create a simple processing pipeline
  echo "Creating base processing pipeline..."
  curl -sS -X POST "${API_BASE}/api/pipelines" \
    -H 'Content-Type: application/json' \
    -d '{"name":"base_processor","description":"Base processing pipeline","start":null,"steps":[{"op":"select","params":{"columns":["id","name","age"]}},{"op":"filter","params":{"filters":[{"col":"age","op":"gte","value":25}],"combine":"and"}}],"overwrite":true}' | python3 -m json.tool || true
  
  # Now create a pipeline that uses chain_pipeline to execute the base processor
  echo "Creating chained pipeline..."
  curl -sS -X POST "${API_BASE}/api/pipelines" \
    -H 'Content-Type: application/json' \
    -d '{"name":"main_with_chain","description":"Main pipeline with chained execution","start":"people","steps":[{"op":"chain_pipeline","params":{"pipeline":"base_processor"}},{"op":"groupby","params":{"by":["name"],"aggs":{"age":"mean"}}}],"overwrite":true}' | python3 -m json.tool || true
  
  # Execute the chained pipeline
  echo "Executing chained pipeline..."
  curl -sS -X POST "${API_BASE}/api/pipelines/main_with_chain/run" \
    -H 'Content-Type: application/json' \
    -d '{"materialize":true}' | python3 -m json.tool || true
}

# Chained operations tests

test_chained_operations() {
  echo "\n[TEST] CHAINED operations: multi-step data processing"
  curl -sS -X POST "${API_BASE}/api/pipeline/run" \
    -H 'Content-Type: application/json' \
    -d '{"start":"purchases","steps":[{"op":"select","params":{"columns":["id","city","product","quantity","price"]}},{"op":"mutate","params":{"target":"total_value","expr":"col(\"quantity\") * col(\"price\")","mode":"vector","overwrite":true}},{"op":"filter","params":{"filters":[{"col":"total_value","op":"gt","value":10}],"combine":"and"}},{"op":"groupby","params":{"by":["city"],"aggs":{"total_value":"sum","quantity":"count"}}}],"materialize":true}' | python3 -m json.tool || true
}

# Additional dataframe operations

test_dataframe_profile() {
  echo "\n[TEST] DATAFRAME profile: get people profile stats"
  curl -sS "${API_BASE}/api/dataframes/people/profile" | python3 -m json.tool || true
}

test_dataframe_download_csv() {
  echo "\n[TEST] DATAFRAME download CSV: download people as CSV"
  curl -sS "${API_BASE}/api/dataframes/people/download.csv" | head -5 || true
}

test_dataframe_download_json() {
  echo "\n[TEST] DATAFRAME download JSON: download people as JSON"
  curl -sS "${API_BASE}/api/dataframes/people/download.json" | python3 -m json.tool | head -20 || true
}

test_api_stats() {
  echo "\n[TEST] API stats: get system statistics"
  curl -sS "${API_BASE}/api/stats" | python3 -m json.tool || true
}

run_all() {
  # Basic operation tests
  test_select
  test_select_exclude
  test_groupby
  test_filter
  test_merge
  test_pivot
  test_compare_identical
  test_compare_schema
  test_mutate_total_value
  test_datetime_parse
  test_rename_dataframe
  
  # Additional operation tests
  test_rename_columns
  test_pivot_longer
  test_mutate_row_mode
  test_datetime_derive
  test_filter_advanced
  test_filter_null_checks
  test_merge_left
  test_merge_outer
  
  # Pipeline operation tests
  test_pipeline_preview
  test_pipeline_run
  test_pipeline_save
  test_pipeline_load
  test_pipeline_run_saved
  test_pipeline_list
  test_pipeline_export_yaml
  test_pipeline_import_yaml
  test_chained_pipelines
  
  # Chained operations
  test_chained_operations
  
  # Additional dataframe operations
  test_dataframe_profile
  test_dataframe_download_csv
  test_dataframe_download_json
  test_api_stats
}

main() {
  local cmd="${1:-all}"
  wait_api
  # Ensure sample data uploaded
  ensure_upload people "${SAMPLES_DIR}/people.csv"
  ensure_upload purchases "${SAMPLES_DIR}/purchases.csv"
  case "$cmd" in
    wait) exit 0 ;;
    select) test_select ;;
    select-exclude) test_select_exclude ;;
    groupby) test_groupby ;;
    filter) test_filter ;;
    merge) test_merge ;;
    pivot) test_pivot ;;
    compare-identical) test_compare_identical ;;
    compare-schema) test_compare_schema ;;
    mutate) test_mutate_total_value ;;
    datetime) test_datetime_parse ;;
    rename) test_rename_dataframe ;;
    
    # Additional operation tests
    rename-columns) test_rename_columns ;;
    pivot-longer) test_pivot_longer ;;
    mutate-row) test_mutate_row_mode ;;
    datetime-derive) test_datetime_derive ;;
    filter-advanced) test_filter_advanced ;;
    filter-null) test_filter_null_checks ;;
    merge-left) test_merge_left ;;
    merge-outer) test_merge_outer ;;
    
    # Pipeline tests
    pipeline-preview) test_pipeline_preview ;;
    pipeline-run) test_pipeline_run ;;
    pipeline-save) test_pipeline_save ;;
    pipeline-load) test_pipeline_load ;;
    pipeline-run-saved) test_pipeline_run_saved ;;
    pipeline-list) test_pipeline_list ;;
    pipeline-export-yaml) test_pipeline_export_yaml ;;
    pipeline-import-yaml) test_pipeline_import_yaml ;;
    chained-pipelines) test_chained_pipelines ;;
    
    # Chained operations
    chained-operations) test_chained_operations ;;
    
    # Dataframe operations
    dataframe-profile) test_dataframe_profile ;;
    dataframe-download-csv) test_dataframe_download_csv ;;
    dataframe-download-json) test_dataframe_download_json ;;
    api-stats) test_api_stats ;;
    
    all) run_all ;;
    *) echo "Unknown command: $cmd" >&2; exit 2 ;;
  esac
}

main "$@"
