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
import sys
import tempfile
import requests
import mimetypes
from urllib.parse import urlparse, unquote

# YAML support for pipeline import/export
try:
    import yaml  # type: ignore
except Exception:
    yaml = None

app = Flask(__name__)
CORS(app)

# Redis connection
redis_client = redis.Redis(host=os.getenv('REDIS_HOST', 'localhost'), port=int(os.getenv('REDIS_PORT', '6379')), decode_responses=True)

# Ensure upload directory exists
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Feature flags / env toggles
ENABLE_WEB_UI = str(os.getenv('ENABLE_WEB_UI', 'true')).lower() in ('1', 'true', 'yes', 'on')

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
    if not ENABLE_WEB_UI:
        return jsonify({'success': False, 'error': 'Web UI is disabled', 'hint': 'Set ENABLE_WEB_UI=true to enable'}), 404
    return render_template('index.html')

# When UI is disabled, prevent serving static assets too
@app.route('/static/<path:filename>')
def static_files(filename):
    if not ENABLE_WEB_UI:
        return jsonify({'success': False, 'error': 'Web UI is disabled'}), 404
    return send_from_directory('static', filename)

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

# --- New: DataFrame operations (compare, merge, pivot, filter, groupby) ---

def _load_df_from_cache(name: str) -> pd.DataFrame:
    df_key = f"df:{name}"
    if not redis_client.exists(df_key):
        raise ValueError(f'DataFrame "{name}" not found')
    csv_string = redis_client.get(df_key)
    return pd.read_csv(io.StringIO(csv_string))


def _save_df_to_cache(name: str, df: pd.DataFrame, description: str = '', source: str = '') -> dict:
    csv_string = df.to_csv(index=False)
    df_key = f"df:{name}"
    meta_key = f"meta:{name}"
    redis_client.set(df_key, csv_string)
    size_mb = len(csv_string.encode('utf-8')) / (1024 * 1024)
    metadata = {
        'name': name,
        'rows': int(len(df)),
        'cols': int(len(df.columns)),
        'columns': df.columns.tolist(),
        'description': description,
        'timestamp': datetime.now().isoformat(),
        'size_mb': round(size_mb, 2),
        'format': 'csv',
        'source': source or 'operation'
    }
    redis_client.set(meta_key, json.dumps(metadata))
    redis_client.sadd("dataframe_index", name)
    return metadata


def _unique_name(base: str) -> str:
    name = base
    i = 2
    while redis_client.exists(f"df:{name}"):
        name = f"{base}__v{i}"
        i += 1
    return name


@app.route('/api/ops/compare', methods=['POST'])
def op_compare():
    try:
        payload = request.get_json(force=True)
        n1 = payload.get('name1'); n2 = payload.get('name2')
        if not n1 or not n2:
            return jsonify({'success': False, 'error': 'name1 and name2 are required'}), 400
        df1 = _load_df_from_cache(n1)
        df2 = _load_df_from_cache(n2)

        # Try Spark-based comparator for robust order-independent compare and auto-caching diffs
        spark_error = None
        try:
            import socket
            from pyspark.sql import SparkSession
            master_url = os.getenv('SPARK_MASTER_URL', 'spark://localhost:7077')
            builder = (SparkSession.builder
                       .appName('UI-Compare')
                       .master(master_url)
                       .config('spark.sql.adaptive.enabled', 'true')
                       .config('spark.executor.memory', os.getenv('SPARK_EXECUTOR_MEMORY', '4g'))
                       .config('spark.driver.memory', os.getenv('SPARK_DRIVER_MEMORY', '4g'))
                       .config('spark.network.timeout', os.getenv('SPARK_NETWORK_TIMEOUT', '120s'))
                       # Align Python versions across driver and executors
                       .config('spark.pyspark.python', os.getenv('PYSPARK_PYTHON', 'python3'))
                       .config('spark.pyspark.driver.python', os.getenv('PYSPARK_DRIVER_PYTHON', 'python3')))
            driver_host = os.getenv('SPARK_DRIVER_HOST')
            if driver_host:
                builder = builder.config('spark.driver.host', driver_host)
            driver_bind = os.getenv('SPARK_DRIVER_BIND_ADDRESS')
            if driver_bind:
                builder = builder.config('spark.driver.bindAddress', driver_bind)
            spark = builder.getOrCreate()

            # Build Spark DataFrames from in-memory rows with explicit schema to avoid pandas->Spark path
            from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, LongType,
                                           DoubleType, BooleanType, TimestampType, DateType)

            def _spark_type_from_pd(dtype: str):
                dt = str(dtype)
                if dt.startswith('int64') or dt == 'Int64':
                    return LongType()
                if dt.startswith('int'):
                    return IntegerType()
                if dt.startswith('float'):
                    return DoubleType()
                if dt.startswith('bool') or dt == 'boolean':
                    return BooleanType()
                if 'datetime64' in dt or dt == 'datetime64[ns]':
                    return TimestampType()
                if 'date' == dt:
                    return DateType()
                # Fallback for object/string and others
                return StringType()

            def _schema_from_pandas(df: pd.DataFrame) -> StructType:
                fields = []
                for c in df.columns:
                    fields.append(StructField(c, _spark_type_from_pd(df[c].dtype), True))
                return StructType(fields)

            def _rows_from_pandas(df: pd.DataFrame):
                # Convert NaN/NaT to None for Spark
                for row in df.itertuples(index=False, name=None):
                    yield tuple(None if (isinstance(v, float) and pd.isna(v)) or (v is pd.NaT) else v for v in row)

            schema1 = _schema_from_pandas(df1)
            schema2 = _schema_from_pandas(df2)
            # If schemas differ at this stage, we can return early
            if [f.dataType.simpleString() for f in schema1] != [f.dataType.simpleString() for f in schema2] or list(df1.columns) != list(df2.columns):
                try:
                    spark.stop()
                except Exception:
                    pass
                return jsonify({'success': True, 'identical': False, 'result_type': 'schema_mismatch'})

            sdf1 = spark.createDataFrame(_rows_from_pandas(df1), schema=schema1)
            sdf2 = spark.createDataFrame(_rows_from_pandas(df2), schema=schema2)

            # Row count comparison
            count1, count2 = sdf1.count(), sdf2.count()
            if count1 != count2:
                try:
                    spark.stop()
                except Exception:
                    pass
                return jsonify({'success': True, 'identical': False, 'result_type': 'row_count_mismatch'})

            # Order-independent data comparison using exceptAll
            diff1 = sdf1.exceptAll(sdf2)
            diff2 = sdf2.exceptAll(sdf1)
            c1 = diff1.count()
            c2 = diff2.count()
            if c1 == 0 and c2 == 0:
                try:
                    spark.stop()
                except Exception:
                    pass
                return jsonify({'success': True, 'identical': True, 'result_type': 'identical'})

            created = []
            THRESH = int(os.getenv('COMPARE_CACHE_THRESHOLD', '100000'))
            if 0 < c1 <= THRESH:
                pdf1 = diff1.toPandas()
                name_u1 = _unique_name(f"{n1}_unique_rows")
                _save_df_to_cache(name_u1, pdf1, description=f'Rows unique to {n1} vs {n2}', source='ops:compare')
                created.append(name_u1)
            if 0 < c2 <= THRESH:
                pdf2 = diff2.toPandas()
                name_u2 = _unique_name(f"{n2}_unique_rows")
                _save_df_to_cache(name_u2, pdf2, description=f'Rows unique to {n2} vs {n1}', source='ops:compare')
                created.append(name_u2)

            try:
                spark.stop()
            except Exception:
                pass
            return jsonify({'success': True, 'identical': False, 'result_type': 'data_mismatch', 'left_unique': int(c1), 'right_unique': int(c2), 'created': created, 'note': f'Spark comparator used; master={master_url}'})
        except Exception as e:
            # Log Spark failure and fall back to pandas
            spark_error = str(e)
            try:
                app.logger.exception('Spark comparator failed: %s', e)
            except Exception:
                pass

        # Schema comparison
        schema_match = list(df1.columns) == list(df2.columns) and list(map(str, df1.dtypes)) == list(map(str, df2.dtypes))
        if not schema_match:
            return jsonify({'success': True, 'identical': False, 'result_type': 'schema_mismatch', 'note': 'pandas fallback used', 'spark_error': spark_error})

        # Row count check
        if len(df1) != len(df2):
            return jsonify({'success': True, 'identical': False, 'result_type': 'row_count_mismatch', 'note': 'pandas fallback used', 'spark_error': spark_error})

        # Data comparison (order independent): use indicator merge on all columns
        cols = list(df1.columns)
        merged_l = df1.merge(df2, how='outer', on=cols, indicator=True)
        left_only = merged_l[merged_l['_merge'] == 'left_only'][cols]
        right_only = merged_l[merged_l['_merge'] == 'right_only'][cols]
        c1 = int(len(left_only)); c2 = int(len(right_only))
        created = []
        if c1 == 0 and c2 == 0:
            return jsonify({'success': True, 'identical': True, 'result_type': 'identical', 'created': created, 'note': 'pandas fallback used', 'spark_error': spark_error})

        # Cache differing rows (cap to 100k)
        THRESH = 100_000
        if 0 < c1 <= THRESH:
            name_u1 = _unique_name(f"{n1}_unique_rows")
            _save_df_to_cache(name_u1, left_only, description=f'Rows unique to {n1} vs {n2}', source='ops:compare')
            created.append(name_u1)
        if 0 < c2 <= THRESH:
            name_u2 = _unique_name(f"{n2}_unique_rows")
            _save_df_to_cache(name_u2, right_only, description=f'Rows unique to {n2} vs {n1}', source='ops:compare')
            created.append(name_u2)

        return jsonify({'success': True, 'identical': False, 'result_type': 'data_mismatch', 'left_unique': c1, 'right_unique': c2, 'created': created, 'note': 'pandas fallback used', 'spark_error': spark_error})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/ops/merge', methods=['POST'])
def op_merge():
    try:
        p = request.get_json(force=True)
        names = p.get('names') or []
        keys = p.get('keys') or []
        how = (p.get('how') or 'inner').lower()
        if how not in ['inner', 'left', 'right', 'outer']:
            return jsonify({'success': False, 'error': 'how must be one of inner,left,right,outer'}), 400
        if len(names) < 2:
            return jsonify({'success': False, 'error': 'At least 2 dataframe names required'}), 400
        if len(keys) < 1:
            return jsonify({'success': False, 'error': 'At least 1 key is required'}), 400
        df = _load_df_from_cache(names[0])
        for nm in names[1:]:
            d2 = _load_df_from_cache(nm)
            df = df.merge(d2, on=keys, how=how)
        base = f"{'_'.join(names)}__merge_{how}_by_{'-'.join(keys)}"
        out_name = _unique_name(base)
        meta = _save_df_to_cache(out_name, df, description=f"Merge {names} on {keys} ({how})", source='ops:merge')
        return jsonify({'success': True, 'name': out_name, 'metadata': meta})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/ops/pivot', methods=['POST'])
def op_pivot():
    try:
        p = request.get_json(force=True)
        mode = (p.get('mode') or 'wider').lower()
        name = p.get('name')
        if not name:
            return jsonify({'success': False, 'error': 'name is required'}), 400
        df = _load_df_from_cache(name)
        if mode == 'wider':
            index = p.get('index') or []
            names_from = p.get('names_from')
            values_from = p.get('values_from')
            aggfunc = p.get('aggfunc') or 'first'
            if not names_from or not values_from:
                return jsonify({'success': False, 'error': 'names_from and values_from are required for wider'}), 400
            if isinstance(values_from, str):
                values_from = [values_from]
            pivoted = pd.pivot_table(df, index=index or None, columns=names_from, values=values_from, aggfunc=aggfunc)
            # Flatten MultiIndex columns if any
            if isinstance(pivoted.columns, pd.MultiIndex):
                pivoted.columns = ['__'.join([str(x) for x in tup if str(x) != '']) for tup in pivoted.columns.to_flat_index()]
            pivoted = pivoted.reset_index()
            base = f"{name}__pivot_wider_{names_from}_vals_{'-'.join(values_from)}"
            out_name = _unique_name(base)
            meta = _save_df_to_cache(out_name, pivoted, description=f"Pivot wider from {names_from} values {values_from}", source='ops:pivot')
            return jsonify({'success': True, 'name': out_name, 'metadata': meta})
        elif mode == 'longer':
            id_vars = p.get('id_vars') or []
            value_vars = p.get('value_vars') or []
            var_name = p.get('var_name') or 'variable'
            value_name = p.get('value_name') or 'value'
            if not value_vars:
                return jsonify({'success': False, 'error': 'value_vars is required for longer'}), 400
            melted = pd.melt(df, id_vars=id_vars or None, value_vars=value_vars, var_name=var_name, value_name=value_name)
            base = f"{name}__pivot_longer_{'-'.join(value_vars)}"
            out_name = _unique_name(base)
            meta = _save_df_to_cache(out_name, melted, description=f"Pivot longer value_vars={value_vars}", source='ops:pivot')
            return jsonify({'success': True, 'name': out_name, 'metadata': meta})
        else:
            return jsonify({'success': False, 'error': 'mode must be wider or longer'}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/ops/filter', methods=['POST'])
def op_filter():
    try:
        p = request.get_json(force=True)
        name = p.get('name')
        if not name:
            return jsonify({'success': False, 'error': 'name is required'}), 400
        df = _load_df_from_cache(name)
        conditions = p.get('filters') or []
        combine = (p.get('combine') or 'and').lower()
        if combine not in ['and', 'or']:
            return jsonify({'success': False, 'error': 'combine must be and/or'}), 400
        mask = None
        # Collect human-readable pieces for description
        desc_parts = []
        for cond in conditions:
            col = cond.get('col'); op = (cond.get('op') or 'eq').lower(); val = cond.get('value')
            if col not in df.columns:
                return jsonify({'success': False, 'error': f'Column {col} not found'}), 400
            s = df[col]
            m = None
            if op == 'eq':
                m = s == val
            elif op == 'ne':
                m = s != val
            elif op == 'lt':
                m = s < val
            elif op == 'lte':
                m = s <= val
            elif op == 'gt':
                m = s > val
            elif op == 'gte':
                m = s >= val
            elif op == 'in':
                vals = val
                if isinstance(val, str):
                    v = val.strip()
                    try:
                        # allow JSON array like [1,2] or ["a","b"]
                        parsed = json.loads(v)
                        if isinstance(parsed, list):
                            vals = parsed
                        else:
                            vals = [val]
                    except Exception:
                        # fallback to comma-separated string
                        vals = [x.strip() for x in v.split(',') if x.strip()]
                if not isinstance(vals, list):
                    vals = [vals]
                m = s.isin(vals)
                # for description
                val = vals
            elif op == 'nin':
                vals = val
                if isinstance(val, str):
                    v = val.strip()
                    try:
                        parsed = json.loads(v)
                        if isinstance(parsed, list):
                            vals = parsed
                        else:
                            vals = [val]
                    except Exception:
                        vals = [x.strip() for x in v.split(',') if x.strip()]
                if not isinstance(vals, list):
                    vals = [vals]
                m = ~s.isin(vals)
                val = vals
            elif op == 'contains':
                m = s.astype('string').str.contains(str(val), na=False)
            elif op == 'startswith':
                m = s.astype('string').str.startswith(str(val), na=False)
            elif op == 'endswith':
                m = s.astype('string').str.endswith(str(val), na=False)
            elif op == 'isnull':
                m = s.isna()
                val = None
            elif op == 'notnull':
                m = s.notna()
                val = None
            else:
                return jsonify({'success': False, 'error': f'Unsupported op {op}'}), 400
            # Append condition to description
            if val is None:
                desc_parts.append(f"{col} {op}")
            else:
                # Compact list display for in/nin; stringify scalars safely
                vstr = ','.join(map(str, val)) if isinstance(val, list) else str(val)
                desc_parts.append(f"{col} {op} {vstr}")
            mask = m if mask is None else (mask & m if combine == 'and' else mask | m)
        filtered = df[mask] if mask is not None else df.copy()
        out_name = _unique_name(f"{name}__filter")
        # Build detailed description including columns
        detail = (' ' + combine + ' ').join(desc_parts) if desc_parts else 'no conditions'
        meta = _save_df_to_cache(out_name, filtered, description=f"Filter: {detail}", source='ops:filter')
        return jsonify({'success': True, 'name': out_name, 'metadata': meta})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/ops/groupby', methods=['POST'])
def op_groupby():
    try:
        p = request.get_json(force=True)
        name = p.get('name')
        by = p.get('by') or []
        aggs = p.get('aggs') or {}
        if not name:
            return jsonify({'success': False, 'error': 'name is required'}), 400
        if not by:
            return jsonify({'success': False, 'error': 'by is required'}), 400
        df = _load_df_from_cache(name)
        # Validate columns
        for c in by:
            if c not in df.columns:
                return jsonify({'success': False, 'error': f'Group-by column {c} not found'}), 400
        if aggs:
            grouped = df.groupby(by).agg(aggs).reset_index()
        else:
            grouped = df.groupby(by).size().reset_index(name='count')
        # Flatten columns if needed to avoid MultiIndex in CSV
        if isinstance(grouped.columns, pd.MultiIndex):
            grouped.columns = ['__'.join([str(x) for x in tup if str(x) != '']) for tup in grouped.columns.to_flat_index()]
        base = f"{name}__groupby_{'-'.join(by)}"
        out_name = _unique_name(base)
        # Build clearer description including columns
        by_str = ','.join(by)
        aggs_str = aggs if aggs else 'count'
        meta = _save_df_to_cache(out_name, grouped, description=f"Group-by columns={by_str}; aggs={aggs_str}", source='ops:groupby')
        return jsonify({'success': True, 'name': out_name, 'metadata': meta})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/ops/select', methods=['POST'])
def op_select():
    try:
        p = request.get_json(force=True)
        name = p.get('name')
        cols = p.get('columns') or []
        if not name:
            return jsonify({'success': False, 'error': 'name is required'}), 400
        if not isinstance(cols, list) or len(cols) == 0:
            return jsonify({'success': False, 'error': 'columns (non-empty list) is required'}), 400
        df = _load_df_from_cache(name)
        # Validate columns
        missing = [c for c in cols if c not in df.columns]
        if missing:
            return jsonify({'success': False, 'error': f'Columns not found: {", ".join(missing)}'}), 400
        # Project
        projected = df[cols].copy()
        base = f"{name}__select_{'-'.join(cols)}"
        out_name = _unique_name(base)
        meta = _save_df_to_cache(out_name, projected, description=f"Select columns={','.join(cols)}", source='ops:select')
        return jsonify({'success': True, 'name': out_name, 'metadata': meta})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# --- New: Column rename operation ---
@app.route('/api/ops/rename', methods=['POST'])
def op_rename():
    try:
        p = request.get_json(force=True)
        name = p.get('name')
        rename_map = p.get('map') or p.get('rename') or p.get('columns') or {}
        if not name:
            return jsonify({'success': False, 'error': 'name is required'}), 400
        if not isinstance(rename_map, dict) or not rename_map:
            return jsonify({'success': False, 'error': 'map (object of old->new names) is required'}), 400
        df = _load_df_from_cache(name)
        # Validate keys
        missing = [old for old in rename_map.keys() if old not in df.columns]
        if missing:
            return jsonify({'success': False, 'error': f'Columns to rename not found: {", ".join(missing)}'}), 400
        # Check for conflicts after rename
        new_cols = list(df.columns)
        for old, new in rename_map.items():
            if not isinstance(new, str) or not new:
                return jsonify({'success': False, 'error': f'Invalid new name for column {old}'}), 400
            # Simulate rename
            idx = new_cols.index(old)
            new_cols[idx] = new
        if len(set(new_cols)) != len(new_cols):
            return jsonify({'success': False, 'error': 'Rename would cause duplicate column names'}), 400
        renamed = df.rename(columns=rename_map)
        base = f"{name}__rename_{'-'.join([f'{k}->{v}' for k,v in rename_map.items()])}"
        # Avoid very long names
        if len(base) > 180:
            base = f"{name}__rename"
        out_name = _unique_name(base)
        meta = _save_df_to_cache(out_name, renamed, description=f"Rename columns: {rename_map}", source='ops:rename')
        return jsonify({'success': True, 'name': out_name, 'metadata': meta})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# --- New: URL download + GET operation bridge helpers ---

def _infer_ext_from_url_or_ct(url: str, content_type: str | None) -> str:
    """Infer a file extension (with leading dot) from URL or content-type."""
    # Try URL path
    try:
        path = urlparse(url).path
        if path:
            fname = unquote(os.path.basename(path))
            _, ext = os.path.splitext(fname)
            if ext:
                return ext.lower()
    except Exception:
        pass
    # Try content-type
    if content_type:
        guessed = mimetypes.guess_extension(content_type.split(';')[0].strip())
        if guessed:
            return guessed.lower()
    # Fallback to csv
    return '.csv'


def _read_df_from_bytes(data: bytes, ext: str) -> pd.DataFrame:
    ext = (ext or '').lower()
    bio = io.BytesIO(data)
    if ext == '.csv':
        return pd.read_csv(bio)
    if ext in ['.xlsx', '.xls']:
        return pd.read_excel(bio)
    if ext == '.json':
        return pd.read_json(bio)
    # Try csv as a final fallback
    return pd.read_csv(io.BytesIO(data))


def _download_and_cache(url: str, name: str | None = None, description: str = '', force_unique: bool = True) -> dict:
    """Download a file from URL, read as DataFrame, and cache it. Returns metadata.
    If name is provided, a unique variant is used unless force_unique is False.
    """
    if not url:
        raise ValueError('url is required')
    # Safety limits
    TIMEOUT = float(os.getenv('DOWNLOAD_TIMEOUT_SECONDS', '30'))
    MAX_MB = int(os.getenv('MAX_DOWNLOAD_MB', '100'))
    r = requests.get(url, timeout=TIMEOUT, allow_redirects=True)
    if r.status_code != 200:
        raise ValueError(f'Download failed with status {r.status_code}')
    cl = r.headers.get('Content-Length')
    if cl:
        try:
            sz = int(cl)
            if sz > MAX_MB * 1024 * 1024:
                raise ValueError(f'File too large (>{MAX_MB} MB)')
        except Exception:
            pass
    content = r.content
    if len(content) > MAX_MB * 1024 * 1024:
        raise ValueError(f'File too large (>{MAX_MB} MB)')
    ext = _infer_ext_from_url_or_ct(url, r.headers.get('Content-Type'))
    # Default name from URL path
    if not name:
        try:
            path = urlparse(url).path
            base = os.path.splitext(os.path.basename(path))[0] or 'dataset'
            name = base
        except Exception:
            name = 'dataset'
    # Ensure unique if requested
    final_name = _unique_name(name) if force_unique else name
    df = _read_df_from_bytes(content, ext)
    meta = _save_df_to_cache(final_name, df, description=description or f'downloaded from {url}', source='download:get')
    # Include original filename hint
    meta['original_url'] = url
    meta['original_ext'] = ext
    return meta


def _parse_list_arg(args, key: str) -> list:
    """Parse a list-like query param: supports repeated ?key=a&key=b and comma-separated."""
    vals = args.getlist(key)
    flat: list[str] = []
    for v in vals:
        if v is None:
            continue
        if isinstance(v, str) and ',' in v:
            flat.extend([x for x in (s.strip() for s in v.split(',')) if x])
        else:
            flat.append(v)
    # Also check plural form e.g., columns vs column
    if not flat and key.endswith('s'):
        alt = key[:-1]
        vals = args.getlist(alt)
        for v in vals:
            if v is None:
                continue
            if isinstance(v, str) and ',' in v:
                flat.extend([x for x in (s.strip() for s in v.split(',')) if x])
            else:
                flat.append(v)
    return flat


# --- New: GET mirrors for operations ---

@app.route('/api/ops/compare/get', methods=['GET'])
def get_compare_via_urls():
    try:
        url1 = request.args.get('url1') or request.args.get('u1')
        url2 = request.args.get('url2') or request.args.get('u2')
        name1 = request.args.get('name1')
        name2 = request.args.get('name2')
        # If URLs provided, download and cache
        if url1:
            m1 = _download_and_cache(url1, name=name1)
            name1 = m1['name']
        if url2:
            m2 = _download_and_cache(url2, name=name2)
            name2 = m2['name']
        if not name1 or not name2:
            return jsonify({'success': False, 'error': 'Provide url1/url2 or name1/name2'}), 400
        # Delegate to POST logic
        with app.test_request_context('/api/ops/compare', method='POST', json={'name1': name1, 'name2': name2}):
            return op_compare()
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/ops/merge/get', methods=['GET'])
def get_merge_via_urls():
    try:
        urls = _parse_list_arg(request.args, 'urls') or request.args.getlist('url')
        names = _parse_list_arg(request.args, 'names') or request.args.getlist('name')
        keys = _parse_list_arg(request.args, 'keys')
        how = (request.args.get('how') or 'inner').lower()
        used_names: list[str] = []
        # Download and cache for provided URLs
        for i, url in enumerate(urls or []):
            nm = names[i] if i < len(names) else None
            meta = _download_and_cache(url, name=nm)
            used_names.append(meta['name'])
        # Append any names that refer to existing cached frames
        if not urls and names:
            used_names = names
        elif names and len(names) > len(used_names):
            # If both urls and names present, and extra names are provided after urls
            used_names.extend(names[len(used_names):])
        if len(used_names) < 2:
            return jsonify({'success': False, 'error': 'At least two inputs required via urls or names'}), 400
        if not keys:
            return jsonify({'success': False, 'error': 'keys are required (comma-separated)'}), 400
        payload = {'names': used_names, 'keys': keys, 'how': how}
        with app.test_request_context('/api/ops/merge', method='POST', json=payload):
            return op_merge()
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/ops/pivot/get', methods=['GET'])
def get_pivot_via_url():
    try:
        url = request.args.get('url')
        name = request.args.get('name')
        mode = (request.args.get('mode') or 'wider').lower()
        if url:
            meta = _download_and_cache(url, name=name)
            name = meta['name']
        if not name:
            return jsonify({'success': False, 'error': 'Provide url or name'}), 400
        payload = {'name': name, 'mode': mode}
        if mode == 'wider':
            index = _parse_list_arg(request.args, 'index')
            names_from = request.args.get('names_from')
            values_from = _parse_list_arg(request.args, 'values_from') or [request.args.get('values_from')] if request.args.get('values_from') else []
            aggfunc = request.args.get('aggfunc') or 'first'
            if not names_from or not values_from:
                return jsonify({'success': False, 'error': 'names_from and values_from required for wider'}), 400
            payload.update({'index': index, 'names_from': names_from, 'values_from': values_from, 'aggfunc': aggfunc})
        elif mode == 'longer':
            id_vars = _parse_list_arg(request.args, 'id_vars')
            value_vars = _parse_list_arg(request.args, 'value_vars')
            var_name = request.args.get('var_name') or 'variable'
            value_name = request.args.get('value_name') or 'value'
            if not value_vars:
                return jsonify({'success': False, 'error': 'value_vars required for longer'}), 400
            payload.update({'id_vars': id_vars, 'value_vars': value_vars, 'var_name': var_name, 'value_name': value_name})
        else:
            return jsonify({'success': False, 'error': 'mode must be wider or longer'}), 400
        with app.test_request_context('/api/ops/pivot', method='POST', json=payload):
            return op_pivot()
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/ops/filter/get', methods=['GET'])
def get_filter_via_url():
    try:
        url = request.args.get('url')
        name = request.args.get('name')
        combine = (request.args.get('combine') or 'and').lower()
        filters_param = request.args.get('filters')
        if url:
            meta = _download_and_cache(url, name=name)
            name = meta['name']
        if not name:
            return jsonify({'success': False, 'error': 'Provide url or name'}), 400
        filters = []
        if filters_param:
            try:
                filters = json.loads(filters_param)
            except Exception:
                return jsonify({'success': False, 'error': 'filters must be JSON array'}), 400
        payload = {'name': name, 'filters': filters, 'combine': combine}
        with app.test_request_context('/api/ops/filter', method='POST', json=payload):
            return op_filter()
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/ops/groupby/get', methods=['GET'])
def get_groupby_via_url():
    try:
        url = request.args.get('url')
        name = request.args.get('name')
        if url:
            meta = _download_and_cache(url, name=name)
            name = meta['name']
        if not name:
            return jsonify({'success': False, 'error': 'Provide url or name'}), 400
        by = _parse_list_arg(request.args, 'by')
        if not by:
            return jsonify({'success': False, 'error': 'by is required (comma-separated)'}), 400
        aggs_param = request.args.get('aggs')
        aggs = {}
        if aggs_param:
            try:
                aggs = json.loads(aggs_param)
            except Exception:
                return jsonify({'success': False, 'error': 'aggs must be JSON object'}), 400
        payload = {'name': name, 'by': by, 'aggs': aggs}
        with app.test_request_context('/api/ops/groupby', method='POST', json=payload):
            return op_groupby()
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/ops/select/get', methods=['GET'])
def get_select_via_url():
    try:
        url = request.args.get('url')
        name = request.args.get('name')
        if url:
            meta = _download_and_cache(url, name=name)
            name = meta['name']
        if not name:
            return jsonify({'success': False, 'error': 'Provide url or name'}), 400
        columns = _parse_list_arg(request.args, 'columns')
        if not columns:
            return jsonify({'success': False, 'error': 'columns are required (comma-separated)'}), 400
        payload = {'name': name, 'columns': columns}
        with app.test_request_context('/api/ops/select', method='POST', json=payload):
            return op_select()
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# GET mirror for rename
@app.route('/api/ops/rename/get', methods=['GET'])
def get_rename_via_url():
    try:
        url = request.args.get('url')
        name = request.args.get('name')
        if url:
            meta = _download_and_cache(url, name=name)
            name = meta['name']
        if not name:
            return jsonify({'success': False, 'error': 'Provide url or name'}), 400
        # Parse mapping: prefer JSON in ?map=; alternatively from/to lists (?from=a,b&to=x,y)
        mapping = {}
        mparam = request.args.get('map') or request.args.get('rename') or request.args.get('columns')
        if mparam:
            try:
                parsed = json.loads(mparam)
                if isinstance(parsed, dict):
                    mapping = parsed
            except Exception:
                return jsonify({'success': False, 'error': 'map must be a JSON object like {"old":"new"}'}), 400
        if not mapping:
            olds = _parse_list_arg(request.args, 'from') or _parse_list_arg(request.args, 'old')
            news = _parse_list_arg(request.args, 'to') or _parse_list_arg(request.args, 'new')
            if not olds or not news or len(olds) != len(news):
                return jsonify({'success': False, 'error': 'Provide map JSON or parallel from/to lists'}), 400
            mapping = {o: n for o, n in zip(olds, news)}
        payload = {'name': name, 'map': mapping}
        with app.test_request_context('/api/ops/rename', method='POST', json=payload):
            return op_rename()
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# --- New: Pipeline execution (preview and run) ---

def _apply_op(df_curr: pd.DataFrame | None, step: dict) -> tuple[pd.DataFrame, str]:
    """Apply a single pipeline step to the current DataFrame.
    Returns (new_df, description).
    Supported ops: load, merge, filter, groupby, select, rename, pivot, compare.
    """
    op = (step.get('op') or step.get('type') or '').lower()
    params = step.get('params') or {}

    # load: set current dataframe from cache by name
    if op == 'load':
        name = params.get('name') or step.get('name')
        if not name:
            raise ValueError('load: name is required')
        df = _load_df_from_cache(name)
        return df, f"load {name}"

    # merge: merge current with others (params.with), or merge list of names (params.names)
    if op == 'merge':
        how = (params.get('how') or 'inner').lower()
        keys = params.get('keys') or []
        if not keys:
            raise ValueError('merge: keys are required')
        names: list[str] = params.get('names') or []
        with_names: list[str] = params.get('with') or params.get('others') or []
        if names:
            if len(names) < 2:
                raise ValueError('merge: names must include at least two dataframes')
            df = _load_df_from_cache(names[0])
            for nm in names[1:]:
                d2 = _load_df_from_cache(nm)
                df = df.merge(d2, on=keys, how=how)
            return df, f"merge names={names} how={how} keys={keys}"
        else:
            if df_curr is None:
                raise ValueError('merge: no current dataframe; provide params.names or a prior load')
            df = df_curr.copy()
            for nm in with_names:
                d2 = _load_df_from_cache(nm)
                df = df.merge(d2, on=keys, how=how)
            return df, f"merge current with={with_names} how={how} keys={keys}"

    # filter
    if op == 'filter':
        if df_curr is None:
            raise ValueError('filter: no current dataframe; add a load step first')
        p = {
            'name': '__curr__',
            'filters': params.get('filters') or [],
            'combine': (params.get('combine') or 'and').lower()
        }
        # Inline application mirroring op_filter
        df = df_curr.copy()
        conditions = p['filters']
        combine = p['combine']
        if combine not in ['and', 'or']:
            raise ValueError('filter: combine must be and/or')
        mask = None
        desc_parts: list[str] = []
        for cond in conditions:
            col = cond.get('col'); opx = (cond.get('op') or 'eq').lower(); val = cond.get('value')
            if col not in df.columns:
                raise ValueError(f'filter: column {col} not found')
            s = df[col]
            m = None
            if opx == 'eq': m = s == val
            elif opx == 'ne': m = s != val
            elif opx == 'lt': m = s < val
            elif opx == 'lte': m = s <= val
            elif opx == 'gt': m = s > val
            elif opx == 'gte': m = s >= val
            elif opx == 'in':
                vals = val
                if isinstance(val, str):
                    v = val.strip()
                    try:
                        parsed = json.loads(v)
                        vals = parsed if isinstance(parsed, list) else [val]
                    except Exception:
                        vals = [x.strip() for x in v.split(',') if x.strip()]
                if not isinstance(vals, list): vals = [vals]
                m = s.isin(vals); val = vals
            elif opx == 'nin':
                vals = val
                if isinstance(val, str):
                    v = val.strip()
                    try:
                        parsed = json.loads(v)
                        vals = parsed if isinstance(parsed, list) else [val]
                    except Exception:
                        vals = [x.strip() for x in v.split(',') if x.strip()]
                if not isinstance(vals, list):
                    vals = [vals]
                m = ~s.isin(vals)
                val = vals
            elif opx == 'contains':
                m = s.astype('string').str.contains(str(val), na=False)
            elif opx == 'startswith':
                m = s.astype('string').str.startswith(str(val), na=False)
            elif opx == 'endswith':
                m = s.astype('string').str.endswith(str(val), na=False)
            elif opx == 'isnull':
                m = s.isna()
                val = None
            elif opx == 'notnull':
                m = s.notna()
                val = None
            else:
                raise ValueError(f'filter: unsupported op {opx}')
            vstr = '' if val is None else (','.join(map(str, val)) if isinstance(val, list) else str(val))
            desc_parts.append(f"{col} {opx}{(' '+vstr) if vstr else ''}")
            mask = m if mask is None else (mask & m if combine == 'and' else mask | m)
        filtered = df[mask] if mask is not None else df
        return filtered, f"filter {(' '+combine+' ').join(desc_parts) if desc_parts else 'no conditions'}"

    # groupby
    if op == 'groupby':
        if df_curr is None:
            raise ValueError('groupby: no current dataframe; add a load step first')
        by = params.get('by') or []
        aggs = params.get('aggs') or {}
        if not by:
            raise ValueError('groupby: by is required')
        for c in by:
            if c not in df_curr.columns:
                raise ValueError(f'groupby: column {c} not found')
        if aggs:
            grouped = df_curr.groupby(by).agg(aggs).reset_index()
        else:
            grouped = df_curr.groupby(by).size().reset_index(name='count')
        if isinstance(grouped.columns, pd.MultiIndex):
            grouped.columns = ['__'.join([str(x) for x in tup if str(x) != '']) for tup in grouped.columns.to_flat_index()]
        return grouped, f"groupby by={by} aggs={aggs or 'count'}"

    # select
    if op == 'select':
        if df_curr is None:
            raise ValueError('select: no current dataframe; add a load step first')
        cols = params.get('columns') or []
        if not cols:
            raise ValueError('select: columns are required')
        missing = [c for c in cols if c not in df_curr.columns]
        if missing:
            raise ValueError(f'select: columns not found: {", ".join(missing)}')
        projected = df_curr[cols].copy()
        return projected, f"select columns={cols}"

    # rename
    if op == 'rename':
        if df_curr is None:
            raise ValueError('rename: no current dataframe; add a load step first')
        mapping = params.get('map') or params.get('rename') or params.get('columns') or {}
        if not isinstance(mapping, dict) or not mapping:
            raise ValueError('rename: map is required')
        missing = [old for old in mapping.keys() if old not in df_curr.columns]
        if missing:
            raise ValueError(f'rename: columns to rename not found: {", ".join(missing)}')
        new_cols = list(df_curr.columns)
        for old, new in mapping.items():
            if not isinstance(new, str) or not new:
                raise ValueError(f'rename: invalid new name for {old}')
            idx = new_cols.index(old)
            new_cols[idx] = new
        if len(set(new_cols)) != len(new_cols):
            raise ValueError('rename: would cause duplicate columns')
        renamed = df_curr.rename(columns=mapping)
        return renamed, f"rename {mapping}"

    # pivot
    if op == 'pivot':
        if df_curr is None and not params.get('name'):
            raise ValueError('pivot: provide params.name or add a load step first')
        mode = (params.get('mode') or 'wider').lower()
        df = df_curr if df_curr is not None else _load_df_from_cache(params.get('name'))
        if mode == 'wider':
            index = params.get('index') or []
            names_from = params.get('names_from')
            values_from = params.get('values_from')
            aggfunc = params.get('aggfunc') or 'first'
            if not names_from or not values_from:
                raise ValueError('pivot wider: names_from and values_from are required')
            if isinstance(values_from, str):
                values_from = [values_from]
            pivoted = pd.pivot_table(df, index=index or None, columns=names_from, values=values_from, aggfunc=aggfunc)
            if isinstance(pivoted.columns, pd.MultiIndex):
                pivoted.columns = ['__'.join([str(x) for x in tup if str(x) != '']) for tup in pivoted.columns.to_flat_index()]
            pivoted = pivoted.reset_index()
            return pivoted, f"pivot wider names_from={names_from} values_from={values_from}"
        elif mode == 'longer':
            id_vars = params.get('id_vars') or []
            value_vars = params.get('value_vars') or []
            var_name = params.get('var_name') or 'variable'
            value_name = params.get('value_name') or 'value'
            if not value_vars:
                raise ValueError('pivot longer: value_vars required')
            melted = pd.melt(df, id_vars=id_vars or None, value_vars=value_vars, var_name=var_name, value_name=value_name)
            return melted, f"pivot longer value_vars={value_vars}"
        else:
            raise ValueError('pivot: mode must be wider or longer')

    # compare
    if op == 'compare':
        if df_curr is None:
            raise ValueError('compare: no current dataframe; add a load step first')
        other_name = params.get('name') or params.get('other')
        action = (params.get('on') or params.get('action') or 'mismatch').lower()
        if not other_name:
            raise ValueError('compare: params.name (other) is required')
        df_other = _load_df_from_cache(other_name)
        schema_match = list(df_curr.columns) == list(df_other.columns) and list(map(str, df_curr.dtypes)) == list(map(str, df_other.dtypes))
        if not schema_match:
            identical = False
        else:
            cols = list(df_curr.columns)
            merged_l = df_curr.merge(df_other, how='outer', on=cols, indicator=True)
            left_only = merged_l[merged_l['_merge'] == 'left_only'][cols]
            right_only = merged_l[merged_l['_merge'] == 'right_only'][cols]
            identical = len(left_only) == 0 and len(right_only) == 0
        if action == 'identical':
            if identical:
                return df_curr, 'compare: identical -> pass-through'
            note = pd.DataFrame({'__note__': ['not identical']})
            return note, 'compare: not identical'
        if not schema_match:
            return pd.DataFrame({'__note__': ['schema mismatch']}), 'compare: schema mismatch'
        cols = list(df_curr.columns)
        merged = df_curr.merge(df_other, how='outer', on=cols, indicator=True)
        mism_left = merged[merged['_merge'] == 'left_only'][cols].copy(); mism_left['__side__'] = 'left_only'
        mism_right = merged[merged['_merge'] == 'right_only'][cols].copy(); mism_right['__side__'] = 'right_only'
        out = pd.concat([mism_left, mism_right], ignore_index=True)
        return out, 'compare: mismatch rows'

    raise ValueError(f"Unsupported op: {op}")


@app.route('/api/pipeline/preview', methods=['POST'])
def pipeline_preview():
    try:
        p = request.get_json(force=True)
        start = p.get('start')
        steps = p.get('steps') or []
        max_rows = int(p.get('preview_rows') or 20)
        current: pd.DataFrame | None = None
        msgs: list[dict] = []
        if isinstance(start, list) and len(start) > 0:
            current = _load_df_from_cache(start[0])
            msgs.append({'op': 'load', 'desc': f'load {start[0]}', 'columns': current.columns.tolist(), 'preview': df_to_records_json_safe(current.head(max_rows))})
        elif isinstance(start, str) and start:
            current = _load_df_from_cache(start)
            msgs.append({'op': 'load', 'desc': f'load {start}', 'columns': current.columns.tolist(), 'preview': df_to_records_json_safe(current.head(max_rows))})
        for i, step in enumerate(steps):
            current, desc = _apply_op(current, step)
            msgs.append({'op': step.get('op') or step.get('type'), 'desc': desc, 'columns': current.columns.tolist(), 'preview': df_to_records_json_safe(current.head(max_rows))})
        final = None
        if current is not None:
            final = {'columns': current.columns.tolist(), 'preview': df_to_records_json_safe(current.head(max_rows)), 'rows': int(len(current))}
        return jsonify({'success': True, 'steps': msgs, 'final': final})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


@app.route('/api/pipeline/run', methods=['POST'])
def pipeline_run():
    try:
        p = request.get_json(force=True)
        start = p.get('start')
        steps = p.get('steps') or []
        materialize = bool(p.get('materialize') or True)
        out_name = p.get('name') or None
        current: pd.DataFrame | None = None
        if isinstance(start, list) and len(start) > 0:
            current = _load_df_from_cache(start[0])
        elif isinstance(start, str) and start:
            current = _load_df_from_cache(start)
        for step in steps:
            current, _ = _apply_op(current, step)
        if current is None:
            return jsonify({'success': False, 'error': 'Nothing to run: no start and no steps'}), 400
        created = None
        if materialize:
            base = out_name or 'pipeline_result'
            name = _unique_name(base)
            meta = _save_df_to_cache(name, current, description='pipeline result', source='ops:pipeline')
            created = {'name': name, 'metadata': meta}
        return jsonify({'success': True, 'created': created, 'rows': int(len(current)), 'columns': current.columns.tolist()})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


@app.route('/api/pipelines', methods=['OPTIONS'])
def pipelines_options():
    try:
        from flask import Response
        resp = Response('', status=204)
        # Basic CORS headers; flask_cors should also add headers, but be explicit for safety
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.headers['Access-Control-Allow-Methods'] = 'GET,POST,OPTIONS'
        resp.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
        resp.headers['Access-Control-Max-Age'] = '600'
        return resp
    except Exception:
        return '', 204


if __name__ == '__main__':
    PORT = int(os.getenv('PORT', '4999'))
    app.run(host='0.0.0.0', port=PORT, debug=True)
