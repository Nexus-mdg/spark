#!/usr/bin/env python3
"""
Spark test visualizer - Flask Backend
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
import numpy as np

app = Flask(__name__)
CORS(app)

# Redis connection
redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Ensure upload directory exists
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# --- Helpers ---

def df_to_records_json_safe(df: pd.DataFrame):
    """Convert a DataFrame to JSON-serializable records with nulls instead of NaN/NaT/Inf.
    Uses pandas to_json to coerce numpy types into Python-native types, then loads back.
    """
    # Replace NaN/NaT with None and +/-inf with None
    safe_df = df.replace([np.inf, -np.inf], None).where(pd.notnull(df), None)
    return json.loads(safe_df.to_json(orient='records'))

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
        # Get query parameters for pagination
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 100))
        preview_only = request.args.get('preview', 'false').lower() == 'true'

        # Check if exists
        df_key = f"df:{name}"
        meta_key = f"meta:{name}"

        if not redis_client.exists(df_key):
            return jsonify({'success': False, 'error': 'DataFrame not found'}), 404

        # Load metadata first (lightweight)
        meta_json = redis_client.get(meta_key)
        metadata = json.loads(meta_json)

        # For large dataframes, only load preview or paginated data
        csv_string = redis_client.get(df_key)
        df = pd.read_csv(io.StringIO(csv_string))

        columns = df.columns.tolist()
        total_rows = len(df)

        if preview_only:
            # Only return preview (JSON-safe) for large datasets or when preview requested
            preview_data = df_to_records_json_safe(df.head(20))
            return jsonify({
                'success': True,
                'metadata': metadata,
                'columns': columns,
                'preview': preview_data,
                'total_rows': total_rows,
                'is_preview_only': True
            })

        # For smaller datasets (< 1000 rows) or when specifically requested, return paginated data
        if total_rows <= 1000 or not preview_only:
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size

            paginated_df = df.iloc[start_idx:end_idx]
            df_json = df_to_records_json_safe(paginated_df)

            return jsonify({
                'success': True,
                'metadata': metadata,
                'data': df_json,
                'columns': columns,
                'preview': df_to_records_json_safe(df.head(10)),
                'pagination': {
                    'page': page,
                    'page_size': page_size,
                    'total_rows': total_rows,
                    'total_pages': (total_rows + page_size - 1) // page_size
                }
            })
        else:
            # For very large datasets, return only metadata and preview
            preview_data = df_to_records_json_safe(df.head(20))
            return jsonify({
                'success': True,
                'metadata': metadata,
                'columns': columns,
                'preview': preview_data,
                'total_rows': total_rows,
                'is_large_dataset': True,
                'message': 'Dataset is too large to display fully. Showing preview only.'
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

# --- New: direct download/share endpoints ---
@app.route('/api/dataframes/<name>/download.csv', methods=['GET'])
def download_dataframe_csv(name):
    """Return the full DataFrame as CSV (attachment)."""
    try:
        df_key = f"df:{name}"
        if not redis_client.exists(df_key):
            return jsonify({'success': False, 'error': 'DataFrame not found'}), 404
        csv_string = redis_client.get(df_key)
        from flask import Response
        resp = Response(csv_string, mimetype='text/csv; charset=utf-8')
        resp.headers['Content-Disposition'] = f'attachment; filename="{name}.csv"'
        # For CORS downloads in browsers
        resp.headers['Access-Control-Expose-Headers'] = 'Content-Disposition'
        return resp
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/dataframes/<name>/download.json', methods=['GET'])
def download_dataframe_json(name):
    """Return the full DataFrame as JSON records array."""
    try:
        df_key = f"df:{name}"
        if not redis_client.exists(df_key):
            return jsonify({'success': False, 'error': 'DataFrame not found'}), 404
        csv_string = redis_client.get(df_key)
        df = pd.read_csv(io.StringIO(csv_string))
        records = df_to_records_json_safe(df)
        from flask import Response
        return Response(json.dumps(records), mimetype='application/json')
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/dataframes/<name>/profile', methods=['GET'])
def profile_dataframe(name):
    """Return basic profile/summary and chart-friendly distributions for a DataFrame."""
    try:
        df_key = f"df:{name}"
        meta_key = f"meta:{name}"
        if not redis_client.exists(df_key):
            return jsonify({'success': False, 'error': 'DataFrame not found'}), 404
        meta_json = redis_client.get(meta_key)
        metadata = json.loads(meta_json)
        csv_string = redis_client.get(df_key)
        df = pd.read_csv(io.StringIO(csv_string))

        # Column summary
        summary = []
        for col in df.columns:
            series = df[col]
            nonnull = int(series.notna().sum())
            nulls = int(series.isna().sum())
            unique = int(series.nunique(dropna=True))
            dtype = str(series.dtype)
            entry = {
                'name': col,
                'dtype': dtype,
                'nonnull': nonnull,
                'nulls': nulls,
                'unique': unique,
            }
            if pd.api.types.is_numeric_dtype(series):
                desc = series.describe(percentiles=[0.25, 0.5, 0.75])
                entry.update({
                    'min': None if pd.isna(desc.get('min')) else float(desc.get('min')),
                    'max': None if pd.isna(desc.get('max')) else float(desc.get('max')),
                    'mean': None if pd.isna(desc.get('mean')) else float(desc.get('mean')),
                    'std': None if pd.isna(desc.get('std')) else float(desc.get('std')),
                    'p25': None if pd.isna(desc.get('25%')) else float(desc.get('25%')),
                    'p50': None if pd.isna(desc.get('50%')) else float(desc.get('50%')),
                    'p75': None if pd.isna(desc.get('75%')) else float(desc.get('75%')),
                })
            summary.append(entry)

        # Numeric histograms (up to 20 bins)
        numeric_distributions = []
        for col in df.columns:
            series = df[col]
            if pd.api.types.is_numeric_dtype(series):
                # Drop NaNs
                clean = series.dropna()
                if len(clean) == 0:
                    continue
                try:
                    counts, bin_edges = np.histogram(clean, bins=min(20, max(5, int(np.sqrt(len(clean))))) )
                    # Prepare labels as ranges
                    labels = []
                    for i in range(len(bin_edges) - 1):
                        a = bin_edges[i]
                        b = bin_edges[i+1]
                        labels.append(f"{a:.2f}–{b:.2f}")
                    numeric_distributions.append({
                        'name': col,
                        'labels': labels,
                        'counts': [int(x) for x in counts.tolist()],
                    })
                except Exception:
                    pass

        # Categorical top values (top 10)
        categorical_distributions = []
        for col in df.columns:
            series = df[col]
            if pd.api.types.is_object_dtype(series) or pd.api.types.is_categorical_dtype(series):
                vc = series.astype('string').value_counts(dropna=True).head(10)
                labels = vc.index.fillna('null').tolist()
                counts = [int(x) for x in vc.values.tolist()]
                if len(labels) > 0:
                    categorical_distributions.append({
                        'name': col,
                        'labels': labels,
                        'counts': counts,
                    })

        return jsonify({
            'success': True,
            'metadata': metadata,
            'summary': summary,
            'numeric_distributions': numeric_distributions,
            'categorical_distributions': categorical_distributions,
            'row_count': int(len(df)),
            'col_count': int(len(df.columns)),
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

if __name__ == '__main__':
    PORT = int(os.getenv('PORT', '4999'))
    app.run(host='0.0.0.0', port=PORT, debug=True)
