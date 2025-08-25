"""
Helper functions for DataFrame processing and notifications
"""

import json
import os

import numpy as np
import pandas as pd
import requests


def df_to_records_json_safe(df: pd.DataFrame):
    """Convert a DataFrame to JSON-serializable records with nulls instead of NaN/NaT/Inf.
    Uses pandas to_json to coerce numpy types into Python-native types, then loads back.
    """
    # Replace NaN/NaT with None and +/-inf with None
    safe_df = df.replace([np.inf, -np.inf], None).where(pd.notnull(df), None)
    return json.loads(safe_df.to_json(orient="records"))


def notify_ntfy(
    title: str, message: str, tags=None, click: str | None = None, priority: int | None = None
) -> None:
    """Publish a notification to ntfy. Config via env:
    - NTFY_ENABLE: default true
    - NTFY_URL: base URL (default https://localhost:8443)
    - NTFY_TOPIC: topic (default spark)
    """
    try:
        if str(os.getenv("NTFY_ENABLE", "true")).lower() not in ("1", "true", "yes", "on"):
            return
        base = (os.getenv("NTFY_URL", "https://localhost:8443") or "https://localhost:8443").rstrip(
            "/"
        )
        topic = (os.getenv("NTFY_TOPIC", "spark") or "spark").strip("/ ")
        url = f"{base}/{topic}"
        headers = {}
        if title:
            headers["Title"] = title
        if tags:
            headers["Tags"] = ",".join(tags) if isinstance(tags, (list, tuple)) else str(tags)
        if click:
            headers["Click"] = click
        if priority is not None:
            headers["Priority"] = str(priority)
        # Prefer plain text body
        requests.post(url, data=message.encode("utf-8"), headers=headers, timeout=2)
    except Exception as e:
        # Silently fail for notifications
        pass
