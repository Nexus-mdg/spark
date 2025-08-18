# Spark test visualizer

A lightweight Flask-based web UI to upload, cache, browse, and preview tabular test data (CSV/Excel/JSON) using Redis as a backing store. The UI is built with Bootstrap 5, DataTables, and jQuery (all served locally, no external CDNs).

## Features
- Upload CSV, Excel (.xlsx/.xls), and JSON files
- Cached DataFrame listing with size, dimensions, and timestamp
- Preview large datasets safely (first 20 rows)
- Paginated fetch for moderate datasets
- View metadata and columns
- Delete individual datasets or clear the entire cache
- Toast notifications and modal confirmations (Bootstrap 5)

## Components
- Backend: Flask (Python), Redis
- Frontend: Bootstrap 5, DataTables, jQuery
- Containerization: Docker/Docker Compose (optional)

## Quick start (local dev)

Prereqs:
- Python 3.10+
- Redis running locally on 6379

Install backend deps:
```bash
cd dataframe-ui
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Run Flask backend:
```bash
cd dataframe-ui
python3 app.py
```

Open the UI:
- http://127.0.0.1:5000/

## Docker / Compose (optional)
A basic Dockerfile is provided under `dataframe-ui/`. If you have a compose stack for Redis + Web, wire it up accordingly. Example snippet:
```yaml
services:
  redis:
    image: redis:7
    ports: ["6379:6379"]
  ui:
    build: ./dataframe-ui
    ports: ["5000:5000"]
    depends_on: [redis]
```

## Jenkins pipeline
A Kotlin-based Jenkins pipeline script is provided at `jenkins.kt`.
- In your Jenkins instance, create a new Pipeline job.
- Set "Pipeline script from SCM" pointing to this repository.
- Configure the job to use `jenkins.kt` as the pipeline script.
  - If your Jenkins doesn’t support Kotlin natively, use a shared library or the Kotlin pipeline runner plugin per your CI setup.

At minimum, ensure the pipeline stages cover:
- Checkout
- Install backend dependencies (pip)
- Lint/build (optional)
- Run tests (if present)
- Build and push container (optional)
- Deploy (optional)

## Configuration
- Redis host/port are currently hardcoded in `dataframe-ui/app.py` as `localhost:6379`.
- Adjust or parameterize via env vars if needed for CI/CD environments.

## Static assets
All third-party assets are local under `dataframe-ui/static/vendor/`:
- Bootstrap 5.3.0 (CSS/JS)
- jQuery 3.7.1
- DataTables 1.13.6 (core + Bootstrap5 integration)
- Font Awesome 6.0.0 (CSS + webfonts)

## License
This project is licensed under the GNU General Public License v3.0 (GPL-3.0). See [LICENSE](LICENSE).

