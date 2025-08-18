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

            sdf1 = spark.createDataFrame(df1)
            sdf2 = spark.createDataFrame(df2)

            # Schema comparison
            if sdf1.schema != sdf2.schema:
                try:
                    spark.stop()
                except Exception:
                    pass
                return jsonify({'success': True, 'identical': False, 'result_type': 'schema_mismatch'})

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
            elif op == 'contains':
                m = s.astype('string').str.contains(str(val), na=False)
            elif op == 'startswith':
                m = s.astype('string').str.startswith(str(val), na=False)
            elif op == 'endswith':
                m = s.astype('string').str.endswith(str(val), na=False)
            elif op == 'isnull':
                m = s.isna()
            elif op == 'notnull':
                m = s.notna()
            else:
                return jsonify({'success': False, 'error': f'Unsupported op {op}'}), 400
            mask = m if mask is None else (mask & m if combine == 'and' else mask | m)
        filtered = df[mask] if mask is not None else df.copy()
        out_name = _unique_name(f"{name}__filter")
        meta = _save_df_to_cache(out_name, filtered, description=f"Filter: {len(conditions)} conditions ({combine})", source='ops:filter')
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
        meta = _save_df_to_cache(out_name, grouped, description=f"Group-by {by} aggs={aggs or 'count'}", source='ops:groupby')
        return jsonify({'success': True, 'name': out_name, 'metadata': meta})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


if __name__ == '__main__':
    PORT = int(os.getenv('PORT', '4999'))
    app.run(host='0.0.0.0', port=PORT, debug=True)
