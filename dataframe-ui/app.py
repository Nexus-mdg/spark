#!/usr/bin/env python3
"""
Redis DataFrame Web UI - Flask Backend
=====================================
REST API for managing DataFrames stored in Redis
"""

from flask import Flask, request, jsonify, render_template, send_from_directory
from flask_cors import CORS
import redis
import pandas as pd
import json
import io
import base64
from datetime import datetime
import os

app = Flask(__name__)
CORS(app)

# Redis connection
redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Ensure upload directory exists
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@app.route('/')
def index():
    """Serve the main web interface"""
    return render_template('index.html')

@app.route('/api/dataframes', methods=['GET'])
def list_dataframes():
    """Get list of all cached DataFrames"""
    try:
        names = redis_client.smembers("dataframe_index")
        dataframes = []

        for name in names:
            meta_key = f"meta:{name}"
            if redis_client.exists(meta_key):
                meta_json = redis_client.get(meta_key)
                metadata = json.loads(meta_json)
                dataframes.append(metadata)

        return jsonify({
            'success': True,
            'dataframes': dataframes,
            'count': len(dataframes)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/dataframes/<name>', methods=['GET'])
def get_dataframe(name):
    """Get a specific DataFrame with its data"""
    try:
        # Check if exists
        df_key = f"df:{name}"
        meta_key = f"meta:{name}"

        if not redis_client.exists(df_key):
            return jsonify({'success': False, 'error': 'DataFrame not found'}), 404

        # Load CSV data
        csv_string = redis_client.get(df_key)
        df = pd.read_csv(io.StringIO(csv_string))

        # Load metadata
        meta_json = redis_client.get(meta_key)
        metadata = json.loads(meta_json)

        # Convert DataFrame to JSON for frontend
        df_json = df.to_dict('records')
        columns = df.columns.tolist()

        return jsonify({
            'success': True,
            'metadata': metadata,
            'data': df_json,
            'columns': columns,
            'preview': df.head(10).to_dict('records')  # First 10 rows for preview
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/dataframes/<name>', methods=['DELETE'])
def delete_dataframe(name):
    """Delete a DataFrame from Redis cache"""
    try:
        df_key = f"df:{name}"
        meta_key = f"meta:{name}"

        if not redis_client.exists(df_key):
            return jsonify({'success': False, 'error': 'DataFrame not found'}), 404

        # Delete from Redis
        redis_client.delete(df_key)
        redis_client.delete(meta_key)
        redis_client.srem("dataframe_index", name)

        return jsonify({
            'success': True,
            'message': f'DataFrame {name} deleted successfully'
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/dataframes/upload', methods=['POST'])
def upload_dataframe():
    """Upload and cache a new DataFrame"""
    try:
        # Check if file is present
        if 'file' not in request.files:
            return jsonify({'success': False, 'error': 'No file provided'}), 400

        file = request.files['file']
        name = request.form.get('name', '')
        description = request.form.get('description', '')

        if file.filename == '':
            return jsonify({'success': False, 'error': 'No file selected'}), 400

        if not name:
            # Use filename without extension as default name
            name = os.path.splitext(file.filename)[0]

        # Check if name already exists
        if redis_client.exists(f"df:{name}"):
            return jsonify({'success': False, 'error': f'DataFrame with name "{name}" already exists'}), 409

        # Read file based on extension
        file_ext = os.path.splitext(file.filename)[1].lower()

        if file_ext == '.csv':
            df = pd.read_csv(file)
        elif file_ext in ['.xlsx', '.xls']:
            df = pd.read_excel(file)
        elif file_ext == '.json':
            df = pd.read_json(file)
        else:
            return jsonify({'success': False, 'error': 'Unsupported file format. Use CSV, Excel, or JSON'}), 400

        # Convert DataFrame to CSV string for storage
        csv_string = df.to_csv(index=False)

        # Store in Redis
        redis_client.set(f"df:{name}", csv_string)

        # Create metadata
        size_mb = len(csv_string.encode('utf-8')) / (1024 * 1024)
        metadata = {
            'name': name,
            'rows': len(df),
            'cols': len(df.columns),
            'columns': df.columns.tolist(),
            'description': description,
            'timestamp': datetime.now().isoformat(),
            'size_mb': round(size_mb, 2),
            'format': 'csv',
            'original_filename': file.filename
        }

        # Store metadata
        redis_client.set(f"meta:{name}", json.dumps(metadata))
        redis_client.sadd("dataframe_index", name)

        return jsonify({
            'success': True,
            'message': f'DataFrame {name} uploaded successfully',
            'metadata': metadata
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/stats', methods=['GET'])
def get_cache_stats():
    """Get cache statistics"""
    try:
        names = redis_client.smembers("dataframe_index")
        total_size = 0

        for name in names:
            meta_key = f"meta:{name}"
            if redis_client.exists(meta_key):
                meta_json = redis_client.get(meta_key)
                metadata = json.loads(meta_json)
                total_size += metadata.get('size_mb', 0)

        return jsonify({
            'success': True,
            'stats': {
                'dataframe_count': len(names),
                'total_size_mb': round(total_size, 2)
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/cache/clear', methods=['DELETE'])
def clear_cache():
    """Clear all cached DataFrames"""
    try:
        names = redis_client.smembers("dataframe_index")

        # Delete all dataframes and metadata
        for name in names:
            redis_client.delete(f"df:{name}")
            redis_client.delete(f"meta:{name}")

        # Clear the index
        redis_client.delete("dataframe_index")

        return jsonify({
            'success': True,
            'message': f'Cleared {len(names)} DataFrames from cache'
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
