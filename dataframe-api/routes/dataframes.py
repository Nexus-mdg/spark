"""
Core DataFrame CRUD API routes
"""
import json
import os
import io
import pandas as pd
from datetime import datetime, timedelta
from flask import Blueprint, request, jsonify

from utils.redis_client import redis_client
from utils.helpers import df_to_records_json_safe

dataframes_bp = Blueprint('dataframes', __name__)


def calculate_expiration(df_type, auto_delete_hours=None):
    """Calculate expiration datetime and TTL for a dataframe based on its type"""
    if df_type == 'static':
        return None, None
    elif df_type == 'temporary':
        expires_at = datetime.now() + timedelta(hours=1)
        ttl_seconds = 3600  # 1 hour in seconds
    elif df_type == 'ephemeral':
        hours = auto_delete_hours if auto_delete_hours and auto_delete_hours > 0 else 10
        expires_at = datetime.now() + timedelta(hours=hours)
        ttl_seconds = hours * 3600
    else:
        return None, None
    
    return expires_at.isoformat(), ttl_seconds


def set_dataframe_with_ttl(df_key, meta_key, csv_string, metadata, ttl_seconds=None):
    """Store dataframe with optional TTL"""
    # Store the dataframe data
    if ttl_seconds:
        redis_client.setex(df_key, ttl_seconds, csv_string)
        redis_client.setex(meta_key, ttl_seconds, json.dumps(metadata))
    else:
        redis_client.set(df_key, csv_string)
        redis_client.set(meta_key, json.dumps(metadata))
    
    # Add to index (note: index itself doesn't expire)
    redis_client.sadd("dataframe_index", metadata['name'])


@dataframes_bp.route('/api/dataframes', methods=['GET'])
def list_dataframes():
    """Get list of all cached DataFrames"""
    try:
        names = redis_client.smembers("dataframe_index")
        dataframes = []
        expired_names = []

        for name in names:
            meta_key = f"meta:{name}"
            df_key = f"df:{name}"
            
            # Check if dataframe still exists (not expired)
            if redis_client.exists(meta_key) and redis_client.exists(df_key):
                meta_json = redis_client.get(meta_key)
                metadata = json.loads(meta_json)
                dataframes.append(metadata)
            else:
                # Mark for cleanup from index
                expired_names.append(name)

        # Clean up expired dataframes from index
        if expired_names:
            for name in expired_names:
                redis_client.srem("dataframe_index", name)

        return jsonify({
            'success': True,
            'dataframes': dataframes,
            'count': len(dataframes)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@dataframes_bp.route('/api/dataframes/<name>', methods=['GET'])
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
            # Remove from index if it was there but dataframe is gone (expired)
            redis_client.srem("dataframe_index", name)
            return jsonify({'success': False, 'error': 'DataFrame not found or has expired'}), 404

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


@dataframes_bp.route('/api/dataframes/<name>', methods=['DELETE'])
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


@dataframes_bp.route('/api/dataframes/upload', methods=['POST'])
def upload_dataframe():
    """Upload and cache a new DataFrame"""
    try:
        # Check if file is present
        if 'file' not in request.files:
            return jsonify({'success': False, 'error': 'No file provided'}), 400

        file = request.files['file']
        name = request.form.get('name', '')
        description = request.form.get('description', '')
        df_type = request.form.get('type', 'ephemeral')  # Default to ephemeral
        auto_delete_hours = request.form.get('auto_delete_hours', '10')

        if file.filename == '':
            return jsonify({'success': False, 'error': 'No file selected'}), 400

        if not name:
            # Use filename without extension as default name
            name = os.path.splitext(file.filename)[0]

        # Validate type
        if df_type not in ['static', 'ephemeral', 'temporary']:
            return jsonify({'success': False, 'error': 'Invalid type. Must be static, ephemeral, or temporary'}), 400

        # Validate auto_delete_hours for ephemeral type
        try:
            auto_delete_hours = int(auto_delete_hours) if auto_delete_hours else 10
            if df_type == 'ephemeral' and auto_delete_hours <= 0:
                return jsonify({'success': False, 'error': 'auto_delete_hours must be a positive integer for ephemeral type'}), 400
        except ValueError:
            return jsonify({'success': False, 'error': 'auto_delete_hours must be a valid integer'}), 400

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

        # Calculate expiration
        expires_at, ttl_seconds = calculate_expiration(df_type, auto_delete_hours if df_type == 'ephemeral' else None)

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
            'original_filename': file.filename,
            'type': df_type,
            'expires_at': expires_at,
            'auto_delete_hours': auto_delete_hours if df_type == 'ephemeral' else None
        }

        # Store with TTL if applicable
        df_key = f"df:{name}"
        meta_key = f"meta:{name}"
        set_dataframe_with_ttl(df_key, meta_key, csv_string, metadata, ttl_seconds)

        return jsonify({
            'success': True,
            'message': f'DataFrame {name} uploaded successfully',
            'metadata': metadata
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@dataframes_bp.route('/api/dataframes/<name>/rename', methods=['POST'])
def rename_dataframe(name):
    """Rename dataframe and/or update its description"""
    try:
        body = request.get_json(force=True) or {}
        new_name = (body.get('new_name') or body.get('name') or '').strip()
        new_desc = body.get('description') if 'description' in body else None

        df_key_old = f"df:{name}"
        meta_key_old = f"meta:{name}"
        if not redis_client.exists(df_key_old):
            return jsonify({'success': False, 'error': 'DataFrame not found'}), 404

        # Load prior metadata if available
        meta_json = redis_client.get(meta_key_old) or '{}'
        try:
            meta = json.loads(meta_json)
        except Exception:
            meta = {}

        changed = False

        # Perform rename if requested and different
        if new_name and new_name != name:
            if redis_client.exists(f"df:{new_name}"):
                return jsonify({'success': False, 'error': f'DataFrame with name "{new_name}" already exists'}), 409
            csv_string = redis_client.get(df_key_old)

            # Write new keys
            redis_client.set(f"df:{new_name}", csv_string)
            meta['name'] = new_name
            if new_desc is not None:
                meta['description'] = new_desc
            # Ensure basic fields if missing
            try:
                if not meta.get('columns') or not meta.get('rows') or not meta.get('cols') or not meta.get('size_mb'):
                    df_tmp = pd.read_csv(io.StringIO(csv_string))
                    size_mb = len(csv_string.encode('utf-8')) / (1024 * 1024)
                    meta.update({
                        'rows': int(len(df_tmp)),
                        'cols': int(len(df_tmp.columns)),
                        'columns': df_tmp.columns.tolist(),
                        'size_mb': round(size_mb, 2),
                        'format': meta.get('format') or 'csv',
                    })
            except Exception:
                pass
            redis_client.set(f"meta:{new_name}", json.dumps(meta))

            # Update index and delete old keys
            pipe = redis_client.pipeline()
            pipe.srem('dataframe_index', name)
            pipe.sadd('dataframe_index', new_name)
            pipe.delete(df_key_old)
            pipe.delete(meta_key_old)
            pipe.execute()

            name = new_name  # for response
            changed = True
            meta_key_old = f"meta:{name}"

        # Description-only update
        if (new_desc is not None) and not changed:
            meta['description'] = new_desc
            redis_client.set(meta_key_old, json.dumps(meta))
            changed = True

        if not changed:
            return jsonify({'success': False, 'error': 'Nothing to change; provide new_name and/or description'}), 400

        return jsonify({'success': True, 'metadata': meta, 'new_name': name})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@dataframes_bp.route('/api/dataframes/<name>/type', methods=['PATCH'])
def convert_dataframe_type(name):
    """Convert DataFrame to a different type (static, ephemeral, temporary)"""
    try:
        df_key = f"df:{name}"
        meta_key = f"meta:{name}"
        
        if not redis_client.exists(df_key):
            return jsonify({'success': False, 'error': 'DataFrame not found'}), 404
        
        # Get request data
        data = request.get_json() or {}
        new_type = data.get('type', '').lower()
        auto_delete_hours = data.get('auto_delete_hours', 10)
        
        # Validate type
        if new_type not in ['static', 'ephemeral', 'temporary']:
            return jsonify({'success': False, 'error': 'Invalid type. Must be static, ephemeral, or temporary'}), 400
        
        # Validate auto_delete_hours for ephemeral type
        try:
            auto_delete_hours = int(auto_delete_hours) if auto_delete_hours else 10
            if new_type == 'ephemeral' and auto_delete_hours <= 0:
                return jsonify({'success': False, 'error': 'auto_delete_hours must be a positive integer for ephemeral type'}), 400
        except ValueError:
            return jsonify({'success': False, 'error': 'auto_delete_hours must be a valid integer'}), 400
        
        # Load current metadata
        meta_json = redis_client.get(meta_key) or '{}'
        try:
            metadata = json.loads(meta_json)
        except Exception:
            return jsonify({'success': False, 'error': 'Failed to parse DataFrame metadata'}), 500
        
        current_type = metadata.get('type', 'static')
        
        # Skip if no change needed
        if current_type == new_type and (new_type != 'ephemeral' or metadata.get('auto_delete_hours') == auto_delete_hours):
            return jsonify({'success': True, 'message': 'No type conversion needed', 'metadata': metadata})
        
        # Calculate new expiration and TTL
        expires_at, ttl_seconds = calculate_expiration(new_type, auto_delete_hours)
        
        # Update metadata
        metadata['type'] = new_type
        metadata['expires_at'] = expires_at
        if new_type == 'ephemeral':
            metadata['auto_delete_hours'] = auto_delete_hours
        elif 'auto_delete_hours' in metadata:
            # Remove auto_delete_hours for non-ephemeral types
            del metadata['auto_delete_hours']
        
        # Apply TTL changes to Redis keys
        try:
            if new_type == 'static':
                # Remove TTL (make keys persistent)
                redis_client.persist(df_key)
                redis_client.persist(meta_key)
            else:
                # Set TTL for ephemeral/temporary
                redis_client.expire(df_key, ttl_seconds)
                redis_client.expire(meta_key, ttl_seconds)
            
            # Update metadata in Redis
            redis_client.set(meta_key, json.dumps(metadata))
            
            # If converting to static, ensure metadata persists
            if new_type == 'static':
                redis_client.persist(meta_key)
            
        except Exception as e:
            return jsonify({'success': False, 'error': f'Failed to update Redis TTL: {str(e)}'}), 500
        
        return jsonify({
            'success': True,
            'message': f'DataFrame type converted from {current_type} to {new_type}',
            'metadata': metadata,
            'expires_at': expires_at,
            'ttl_seconds': ttl_seconds
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@dataframes_bp.route('/api/stats', methods=['GET'])
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


@dataframes_bp.route('/api/cache/clear', methods=['DELETE'])
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


@dataframes_bp.route('/api/dataframes/<name>/download.csv', methods=['GET'])
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


@dataframes_bp.route('/api/dataframes/<name>/download.json', methods=['GET'])
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


@dataframes_bp.route('/api/dataframes/<name>/profile', methods=['GET'])
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
                    import numpy as np
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