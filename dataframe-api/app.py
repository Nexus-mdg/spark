#!/usr/bin/env python3
"""
Spark test visualizer - Flask Backend
=====================================
REST API for managing DataFrames stored in Redis
"""

import os
from flask import Flask, jsonify
from flask_cors import CORS

# Import blueprints
from routes.dataframes import dataframes_bp
from routes.operations import operations_bp
from routes.pipelines import pipelines_bp

app = Flask(__name__, static_folder=None)
CORS(app)

# Ensure upload directory exists
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Register blueprints
app.register_blueprint(dataframes_bp)
app.register_blueprint(operations_bp)
app.register_blueprint(pipelines_bp)

if __name__ == '__main__':
    PORT = int(os.getenv('PORT', '4999'))
    app.run(host='0.0.0.0', port=PORT, debug=True)