DataFrame UI Backend (Flask)

Overview
- A Flask REST API for uploading, caching, exploring, and transforming tabular datasets (CSV/Excel/JSON) using Redis for storage.
- Includes GET-based operation endpoints that accept file URLs, cache them, then delegate to existing POST operations.
- Optional Spark integration (via docker-compose) accelerates compare.

Services (via docker-compose at project root)
- Redis (port 6379)
- Spark master/worker (7077, 8081/8082)
- Flask API (dataframe-ui, port 4999)

Quick start (docker compose)
- From the project root:
  - docker compose up -d redis spark spark-worker dataframe-ui
  - API: http://localhost:4999
  - UI (simple index): http://localhost:4999/
- Tear down: docker compose down

Quick start (local Python, optional)
- cd dataframe-ui
- python3 -m venv .venv && . .venv/bin/activate
- pip install -r requirements.txt
- export PORT=4999
- python app.py
- Requires a running Redis at localhost:6379.

New GET bridge endpoints
These endpoints accept URLs to data files. They first download and cache the datasets, then call the corresponding POST operations and return their JSON.
- Compare: /api/ops/compare/get?url1=<urlA>&url2=<urlB>
  - Or use cached names: ?name1=A&name2=B
- Merge: /api/ops/merge/get?urls=<urlA>,<urlB>[,<urlC>]&keys=key1[,key2]&how=inner|left|right|outer
  - Also supports repeated params: ?url=<urlA>&url=<urlB>&key=key1&key=key2
  - Or use cached names: names=A,B
- Pivot: /api/ops/pivot/get?url=<url>&mode=wider|longer&...
  - Wider: index=idx1,idx2&names_from=category&values_from=val1,val2&aggfunc=first|sum|mean|...
  - Longer: id_vars=idx1,idx2&value_vars=col1,col2&var_name=variable&value_name=value
- Filter: /api/ops/filter/get?url=<url>&combine=and|or&filters=<JSON-encoded array>
  - filters example (JSON): [{"col":"city","op":"eq","value":"NY"}]
- GroupBy: /api/ops/groupby/get?url=<url>&by=colA,colB&aggs=<JSON-encoded object>
  - aggs example (JSON): {"amount":"sum","price":"mean"}
- Select: /api/ops/select/get?url=<url>&columns=col1,col2

Notes
- Supported formats: CSV (.csv), Excel (.xlsx/.xls), JSON (.json)
- Download timeout: 30s (DOWNLOAD_TIMEOUT_SECONDS)
- Max file size: 100MB (MAX_DOWNLOAD_MB)
- When using URL parameters that contain JSON, ensure they are URL-encoded.

Sample data & tests
- Sample CSVs live under data/sample.
- test.sh starts a simple HTTP file server to host those CSVs at http://localhost:8000, then exercises the GET endpoints with curl.
- Run via Makefile from the repository root:
  - make up       # brings up dependent services via docker compose
  - make wait     # waits for API to become ready
  - make test     # runs curl tests in dataframe-ui/test.sh
  - make logs     # tails API logs
  - make down     # tears services down

Manual curl examples
- Select
  - curl "http://localhost:4999/api/ops/select/get?url=https://example.com/data.csv&columns=id,name"
- GroupBy (URL-encode JSON)
  - AGGS=%7B%22amount%22:%22sum%22%7D
  - curl "http://localhost:4999/api/ops/groupby/get?url=https://example.com/orders.csv&by=product&aggs=$AGGS"

Troubleshooting
- Compare may try Spark first; if Spark isn’t reachable, it falls back to pandas automatically.
- If downloads fail, verify the file URLs are reachable from the API container host.
- For large files, increase MAX_DOWNLOAD_MB (env var) and ensure Redis has enough RAM.
