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

run_all() {
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
    all) run_all ;;
    *) echo "Unknown command: $cmd" >&2; exit 2 ;;
  esac
}

main "$@"
