#!/usr/bin/env python3
"""
Spark test visualizer - Flask Backend
=====================================
REST API for managing DataFrames stored in Redis
"""

import os
from flask import Flask, render_template, send_from_directory, jsonify
from flask_cors import CORS

# Import blueprints
from routes.dataframes import dataframes_bp
from routes.operations import operations_bp
from routes.pipelines import pipelines_bp

app = Flask(__name__)
CORS(app)

# Ensure upload directory exists
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Feature flags / env toggles
ENABLE_WEB_UI = str(os.getenv('ENABLE_WEB_UI', 'true')).lower() in ('1', 'true', 'yes', 'on')

# Register blueprints
app.register_blueprint(dataframes_bp)
app.register_blueprint(operations_bp)
app.register_blueprint(pipelines_bp)


@app.route('/')
def index():
    """Serve the main web interface"""
    if not ENABLE_WEB_UI:
        return jsonify({'success': False, 'error': 'Web UI is disabled', 'hint': 'Set ENABLE_WEB_UI=true to enable'}), 404
    return render_template('index.html')


# When UI is disabled, prevent serving static assets too
@app.route('/static/<path:filename>')
def static_files(filename):
    if not ENABLE_WEB_UI:
        return jsonify({'success': False, 'error': 'Web UI is disabled'}), 404
    return send_from_directory('static', filename)


if __name__ == '__main__':
    PORT = int(os.getenv('PORT', '4999'))
    app.run(host='0.0.0.0', port=PORT, debug=True)