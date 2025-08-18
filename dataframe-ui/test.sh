#!/usr/bin/env bash
set -euo pipefail

API_BASE="${API_BASE:-http://localhost:4999}"
SAMPLES_DIR="$(cd "$(dirname "$0")" && pwd)/data/sample"
SAMPLE_HOST="http://localhost:8000"
HTTP_PID=""

start_samples_server() {
  echo "Starting sample file server from $SAMPLES_DIR at $SAMPLE_HOST ..."
  (cd "$SAMPLES_DIR" && python3 -m http.server 8000 >/dev/null 2>&1 & echo $! > /tmp/sample_server.pid)
  HTTP_PID=$(cat /tmp/sample_server.pid)
  sleep 0.5
}

stop_samples_server() {
  if [[ -n "${HTTP_PID}" ]] && kill -0 "$HTTP_PID" >/dev/null 2>&1; then
    echo "Stopping sample file server (pid=$HTTP_PID)"
    kill "$HTTP_PID" || true
  fi
  rm -f /tmp/sample_server.pid || true
}

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

# Individual tests

test_select() {
  echo "\n[TEST] SELECT: people.csv columns=id,name"
  curl -fsS "${API_BASE}/api/ops/select/get?url=${SAMPLE_HOST}/people.csv&columns=id,name" | python3 -m json.tool || true
}

test_groupby() {
  echo "\n[TEST] GROUPBY: purchases.csv by product sum(quantity)"
  AGGS='%7B%22quantity%22%3A%22sum%22%7D'
  curl -fsS "${API_BASE}/api/ops/groupby/get?url=${SAMPLE_HOST}/purchases.csv&by=product&aggs=${AGGS}" | python3 -m json.tool || true
}

test_filter() {
  echo "\n[TEST] FILTER: people.csv age>=30 AND city==New York"
  FILTERS='%5B%7B%22col%22%3A%22age%22%2C%22op%22%3A%22gte%22%2C%22value%22%3A30%7D%2C%7B%22col%22%3A%22city%22%2C%22op%22%3A%22eq%22%2C%22value%22%3A%22New%20York%22%7D%5D'
  curl -fsS "${API_BASE}/api/ops/filter/get?url=${SAMPLE_HOST}/people.csv&combine=and&filters=${FILTERS}" | python3 -m json.tool || true
}

test_merge() {
  echo "\n[TEST] MERGE: people.csv with purchases.csv on id (inner)"
  curl -fsS "${API_BASE}/api/ops/merge/get?urls=${SAMPLE_HOST}/people.csv,${SAMPLE_HOST}/purchases.csv&keys=id&how=inner" | python3 -m json.tool || true
}

test_pivot() {
  echo "\n[TEST] PIVOT (wider): purchases.csv index=city names_from=product values_from=quantity agg=sum"
  curl -fsS "${API_BASE}/api/ops/pivot/get?url=${SAMPLE_HOST}/purchases.csv&mode=wider&index=city&names_from=product&values_from=quantity&aggfunc=sum" | python3 -m json.tool || true
}

test_compare_identical() {
  echo "\n[TEST] COMPARE: people.csv vs people.csv (identical)"
  curl -fsS "${API_BASE}/api/ops/compare/get?url1=${SAMPLE_HOST}/people.csv&url2=${SAMPLE_HOST}/people.csv" | python3 -m json.tool || true
}

test_compare_schema() {
  echo "\n[TEST] COMPARE: people.csv vs purchases.csv (schema mismatch)"
  curl -fsS "${API_BASE}/api/ops/compare/get?url1=${SAMPLE_HOST}/people.csv&url2=${SAMPLE_HOST}/purchases.csv" | python3 -m json.tool || true
}

run_all() {
  test_select
  test_groupby
  test_filter
  test_merge
  test_pivot
  test_compare_identical
  test_compare_schema
}

main() {
  trap stop_samples_server EXIT
  local cmd="${1:-all}"
  if [[ "$cmd" == "wait" ]]; then
    wait_api
    exit $?
  fi
  start_samples_server
  wait_api
  case "$cmd" in
    select) test_select ;;
    groupby) test_groupby ;;
    filter) test_filter ;;
    merge) test_merge ;;
    pivot) test_pivot ;;
    compare-identical) test_compare_identical ;;
    compare-schema) test_compare_schema ;;
    all) run_all ;;
    *) echo "Unknown command: $cmd" >&2; exit 2 ;;
  esac
}

main "$@"
