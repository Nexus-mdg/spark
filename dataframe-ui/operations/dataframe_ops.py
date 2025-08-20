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


def _save_df_to_cache(name: str, df: pd.DataFrame, description: str = '', source: str = '') -> dict:
    """Save a DataFrame to Redis cache and return metadata"""
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
    """Generate a unique name by appending version suffix if needed"""
    name = base
    i = 2
    while redis_client.exists(f"df:{name}"):
        name = f"{base}__v{i}"
        i += 1
    return name