DataFrame UI (Tailwind + React)

Overview
- Separate UI that consumes the existing dataframe-ui Flask REST API (port 4999 by default).
- Built with React (Vite) and Tailwind CSS; served by a tiny Flask server.
- Does not modify the original dataframe-ui service.
- Expressions Guide for mutate and pipeline steps: see EXPRESSIONS.md

Local development (frontend only)
1) Install Node 18+.
2) In web/: npm install
3) Start dev server: npm run dev
4) Ensure the original API is running at http://localhost:4999 (docker-compose up dataframe-ui).
5) Open the Vite dev URL and the app will fetch /config.js from the Flask server only in production; in dev it calls http://localhost:4999 directly via api.js fallback.

Build + serve locally with Flask
1) From web/: npm install && npm run build
2) From project root (this folder):
   - python3 -m venv .venv && source .venv/bin/activate
   - pip install -r requirements.txt
   - python app.py
3) Visit http://localhost:5001 and the SPA is served from dist/.

With Docker Compose
- docker compose build dataframe-ui-x
- docker compose up -d dataframe-ui dataframe-ui-x
- Visit http://localhost:5001

Configuration
- API_BASE_URL env controls which REST API the UI uses (default http://localhost:4999). The Flask server exposes /config.js to inject this at runtime.

Notes
- CORS is enabled in the original API.
- This service is read-only with respect to the API—it uses the existing endpoints for list/upload/delete/clear.
