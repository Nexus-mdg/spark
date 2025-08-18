#!/usr/bin/env python3
from flask import Flask, send_from_directory, Response
import os

# Serve built assets from the dist directory (created by Vite build)
app = Flask(__name__, static_folder='dist', static_url_path='')

# Configurable API base URL (default points to the existing dataframe-ui API on 4999)
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:4999")
PORT = int(os.getenv("PORT", "5001"))

@app.route('/config.js')
def config_js():
    js = f"window.APP_CONFIG = {{ API_BASE_URL: '{API_BASE_URL}' }};"
    return Response(js, mimetype='application/javascript')

# Root serves the SPA index.html
@app.route('/')
def index():
    return send_from_directory(app.static_folder, 'index.html')

# Fallback for client-side routing (any unknown path returns index.html)
@app.errorhandler(404)
def spa_fallback(_):
    try:
        return send_from_directory(app.static_folder, 'index.html')
    except Exception:
        return "Build not found. Please build the frontend.", 404

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=True)
