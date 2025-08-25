"""
DataFrame operations utility functions
"""

import io
import json
import os
from datetime import datetime

import pandas as pd

from utils.redis_client import redis_client


def _load_df_from_cache(name: str) -> pd.DataFrame:
    """Load a DataFrame from Redis cache"""
    df_key = f"df:{name}"
    if not redis_client.exists(df_key):
        raise ValueError(f'DataFrame "{name}" not found')
    csv_string = redis_client.get(df_key)
    return pd.read_csv(io.StringIO(csv_string))


def _save_df_to_cache(
    name: str,
    df: pd.DataFrame,
    description: str = "",
    source: str = "",
    df_type: str = None,
    auto_delete_hours: int = None,
) -> dict:
    """Save a DataFrame to Redis cache and return metadata"""
    csv_string = df.to_csv(index=False)
    df_key = f"df:{name}"
    meta_key = f"meta:{name}"

    # Determine DataFrame type - default to ephemeral for all new dataframes
    if df_type is None:
        df_type = "ephemeral"

    # Import the expiration calculation function from dataframes route
    from routes.dataframes import calculate_expiration, set_dataframe_with_ttl

    # Set default auto_delete_hours for ephemeral pipeline results from environment
    if df_type == "ephemeral" and auto_delete_hours is None:
        import os

        auto_delete_hours = int(
            os.getenv("PIPELINE_RESULT_EXPIRATION_HOURS", "8")
        )  # 8 hours default

    # Calculate expiration
    expires_at, ttl_seconds = calculate_expiration(df_type, auto_delete_hours)

    size_mb = len(csv_string.encode("utf-8")) / (1024 * 1024)
    metadata = {
        "name": name,
        "rows": int(len(df)),
        "cols": int(len(df.columns)),
        "columns": df.columns.tolist(),
        "description": description,
        "timestamp": datetime.now().isoformat(),
        "size_mb": round(size_mb, 2),
        "format": "csv",
        "source": source or "operation",
        "type": df_type,
        "expires_at": expires_at,
        "auto_delete_hours": auto_delete_hours if df_type == "ephemeral" else None,
    }

    # Store with TTL if applicable
    set_dataframe_with_ttl(df_key, meta_key, csv_string, metadata, ttl_seconds)
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
            metadata["name"] = new_name
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
    pipe.srem("dataframe_index", old_name)
    pipe.sadd("dataframe_index", new_name)

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
