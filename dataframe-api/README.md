DataFrame API Backend (Flask)

Overview
- A Flask REST API for uploading, caching, exploring, and transforming tabular datasets (CSV/Excel/JSON) using Redis for storage.
- Provides REST API endpoints for dataframe-ui-x frontend consumption.
- Optional Spark integration (via docker-compose) accelerates compare operations.
- **NEW**: DataFrame lifecycle management with automatic cleanup based on configurable expiration times.

DataFrame Types and Lifecycle Management
- **Static**: Never deleted automatically, suitable for reference data
- **Ephemeral**: Deleted after a specified duration (default: 1 hour), suitable for temporary analysis
- **Sub**: Auto-created from pipeline operations, deleted after 10 minutes
- Background cleanup daemon automatically removes expired dataframes
- Manual cleanup available via API endpoints

Environment Variables
- `ENABLE_CLEANUP_DAEMON`: Enable/disable cleanup daemon (default: true)
- `CLEANUP_INTERVAL`: Cleanup check interval in seconds (default: 60)
- `ENABLE_API_PROTECTION`: Enable/disable API protection (default: true)

Services (via docker-compose at project root)
- Redis (port 6379)
- Spark master/worker (7077, 8081/8082)
- Flask API (dataframe-api, port 4999)

Quick start (docker compose)
- From the project root:
  - docker compose up -d redis spark spark-worker dataframe-api
  - API: http://localhost:4999
- Tear down: docker compose down

Quick start (local Python, optional)
- cd dataframe-api
- python3 -m venv .venv && . .venv/bin/activate
- pip install -r requirements.txt
- export PORT=4999
- python app.py
- Requires a running Redis at localhost:6379.

REST API Endpoints
- DataFrames: 
  - GET /api/dataframes - List cached dataframes
  - GET /api/dataframes/<name> - Get dataframe data
  - POST /api/dataframes/upload - Upload new dataframe (supports type, duration parameters)
  - DELETE /api/dataframes/<name> - Delete dataframe
- Operations (all POST with JSON payload):
  - /api/ops/select - Select columns
  - /api/ops/filter - Filter rows
  - /api/ops/groupby - Group and aggregate
  - /api/ops/merge - Join dataframes
  - /api/ops/pivot - Pivot/melt operations
  - /api/ops/compare - Compare two dataframes
  - /api/ops/mutate - Add/modify columns
  - /api/ops/datetime - Parse/derive date columns
- Pipelines:
  - GET /api/pipelines - List saved pipelines
  - POST /api/pipelines - Save pipeline
  - POST /api/pipelines/<name>/run - Execute pipeline
- **NEW** Cleanup Management:
  - GET /api/cleanup/status - Get cleanup daemon status and expiring dataframes
  - POST /api/cleanup/manual - Manually trigger cleanup

Upload API Parameters
- `file`: CSV/Excel/JSON file (required)
- `name`: DataFrame name (optional, defaults to filename)
- `description`: Description (optional)
- `type`: DataFrame type - "static", "ephemeral", "sub" (optional, default: "ephemeral")
- `duration`: Expiration duration in seconds (optional, only for ephemeral type, default: 3600)

Notes
- Supported formats: CSV (.csv), Excel (.xlsx/.xls), JSON (.json)
- All operation endpoints return JSON responses
- DataFrames are cached in Redis for fast access
- Operations inherit type and expiration from source dataframes
- Static dataframes never expire and are preserved across system restarts
- Ephemeral and sub dataframes are automatically cleaned up when expired

Sample data & tests
- Sample CSVs live under data/sample.
- test.sh exercises the POST API endpoints with curl.
- Run via Makefile from the repository root:
  - make up       # brings up dependent services via docker compose
  - make wait     # waits for API to become ready
  - make test     # runs curl tests in dataframe-api/test.sh
  - make logs     # tails API logs
  - make down     # tears services down

Manual curl examples
- Upload a file with type:
  - curl -F "file=@data.csv" -F "name=mydata" -F "type=static" http://localhost:4999/api/dataframes/upload
- Upload ephemeral with custom duration:
  - curl -F "file=@data.csv" -F "name=temp_data" -F "type=ephemeral" -F "duration=1800" http://localhost:4999/api/dataframes/upload
- Select columns:
  - curl -X POST -H "Content-Type: application/json" -d '{"name":"mydata","columns":["id","name"],"target":"mydata_selected"}' http://localhost:4999/api/ops/select
- Check cleanup status:
  - curl http://localhost:4999/api/cleanup/status
- Manual cleanup:
  - curl -X POST http://localhost:4999/api/cleanup/manual

Troubleshooting
- Compare may try Spark first; if Spark isn’t reachable, it falls back to pandas automatically.
- If downloads fail, verify the file URLs are reachable from the API container host.
- For large files, increase MAX_DOWNLOAD_MB (env var) and ensure Redis has enough RAM.
