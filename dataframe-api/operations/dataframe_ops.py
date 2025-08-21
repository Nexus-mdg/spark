"""
DataFrame operations utility functions
"""
import json
import io
import os
import pandas as pd
from datetime import datetime
from utils.redis_client import redis_client


def _load_df_from_cache(name: str) -> pd.DataFrame:
    """Load a DataFrame from Redis cache"""
    df_key = f"df:{name}"
    if not redis_client.exists(df_key):
        raise ValueError(f'DataFrame "{name}" not found')
    csv_string = redis_client.get(df_key)
    return pd.read_csv(io.StringIO(csv_string))


def _get_source_dataframe_type_info(name: str) -> tuple:
    """
    Get type and duration info from source dataframe for inheritance
    
    Returns:
        Tuple of (type, duration_seconds) for creating derived dataframes
    """
    try:
        meta_key = f"meta:{name}"
        if not redis_client.exists(meta_key):
            return 'ephemeral', 3600  # Default fallback
        
        meta_json = redis_client.get(meta_key)
        if not meta_json:
            return 'ephemeral', 3600
            
        metadata = json.loads(meta_json)
        source_type = metadata.get('type', 'ephemeral')
        source_duration = metadata.get('duration_seconds', 3600)
        
        # For derived dataframes, inherit type but adjust duration for sub types
        if source_type == 'static':
            return 'static', None
        elif source_type == 'sub':
            return 'sub', 600  # Sub type always has 10 minute duration
        else:  # ephemeral
            return 'ephemeral', source_duration or 3600
            
    except Exception:
        return 'ephemeral', 3600  # Safe fallback


def _save_df_to_cache(name: str, df: pd.DataFrame, description: str = '', source: str = '', 
                      df_type: str = 'ephemeral', duration_seconds: int = None, 
                      inherit_from: str = None) -> dict:
    """Save a DataFrame to Redis cache and return metadata"""
    
    # If inherit_from is specified, get type info from source dataframe
    if inherit_from:
        df_type, duration_seconds = _get_source_dataframe_type_info(inherit_from)
    
    csv_string = df.to_csv(index=False)
    df_key = f"df:{name}"
    meta_key = f"meta:{name}"
    redis_client.set(df_key, csv_string)
    size_mb = len(csv_string.encode('utf-8')) / (1024 * 1024)
    
    # Handle expiration based on type
    if df_type == 'static':
        duration_seconds = None
        expires_at = None
    elif df_type == 'sub':
        duration_seconds = 600  # 10 minutes for sub type
        expires_at = (datetime.now().timestamp() + duration_seconds)
    else:  # ephemeral (default)
        if duration_seconds is None:
            duration_seconds = 3600  # Default 1 hour
        expires_at = (datetime.now().timestamp() + duration_seconds)
    
    metadata = {
        'name': name,
        'rows': int(len(df)),
        'cols': int(len(df.columns)),
        'columns': df.columns.tolist(),
        'description': description,
        'timestamp': datetime.now().isoformat(),
        'size_mb': round(size_mb, 2),
        'format': 'csv',
        'source': source or 'operation',
        'type': df_type,
        'duration_seconds': duration_seconds,
        'expires_at': expires_at
    }
    redis_client.set(meta_key, json.dumps(metadata))
    redis_client.sadd("dataframe_index", name)
    return metadata


def _rename_dataframe_in_cache(old_name: str, new_name: str) -> bool:
    """Rename a dataframe in Redis cache, updating both data and metadata"""
    df_key_old = f"df:{old_name}"
    meta_key_old = f"meta:{old_name}"
    df_key_new = f"df:{new_name}"
    meta_key_new = f"meta:{new_name}"
    
    # Check if old dataframe exists
    if not redis_client.exists(df_key_old):
        return False
    
    # Check if new name would conflict
    if redis_client.exists(df_key_new):
        return False
    
    # Get the data and metadata
    df_data = redis_client.get(df_key_old)
    meta_data = redis_client.get(meta_key_old)
    
    if not df_data:
        return False
    
    # Update metadata name if it exists
    if meta_data:
        try:
            metadata = json.loads(meta_data)
            metadata['name'] = new_name
            meta_data = json.dumps(metadata)
        except Exception:
            pass  # Keep original metadata if parsing fails
    
    # Use pipeline for atomic operations
    pipe = redis_client.pipeline()
    
    # Set new keys
    pipe.set(df_key_new, df_data)
    if meta_data:
        pipe.set(meta_key_new, meta_data)
    
    # Update index
    pipe.srem('dataframe_index', old_name)
    pipe.sadd('dataframe_index', new_name)
    
    # Delete old keys
    pipe.delete(df_key_old)
    pipe.delete(meta_key_old)
    
    # Execute all operations
    pipe.execute()
    
    return True


def _unique_name(base: str) -> str:
    """Generate a unique name by renaming existing dataframes and keeping the base name for the latest"""
    df_key = f"df:{base}"
    
    # If base doesn't exist, just return it
    if not redis_client.exists(df_key):
        return base
    
    # Base exists, so we need to rename it to make room for the new one
    # Find the next available version number
    i = 1
    while True:
        version_name = f"{base}_v_{i}"
        if not redis_client.exists(f"df:{version_name}"):
            # Found available version, rename the existing base to this version
            if _rename_dataframe_in_cache(base, version_name):
                return base  # Return the base name for the new dataframe
            else:
                # If rename failed for some reason, fall back to old behavior
                break
        i += 1
    
    # Fallback to old behavior if rename failed
    name = base
    i = 2
    while redis_client.exists(f"df:{name}"):
        name = f"{base}__v{i}"
        i += 1
    return name