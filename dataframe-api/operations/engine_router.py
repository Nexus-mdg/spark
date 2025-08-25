"""
Engine router for DataFrame operations
Routes operations to appropriate engine (Spark or Pandas) based on engine parameter
"""

import os
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

try:
    from . import pandas_engine, spark_engine
except ImportError:
    # Fallback for direct execution
    import pandas_engine
    import spark_engine


def _handle_spark_error(operation_name: str, error: Exception) -> None:
    """Handle Spark engine errors with informative messages"""
    error_msg = str(error)

    # Check for common connection issues
    if any(
        phrase in error_msg
        for phrase in ["Failed to connect", "Connection refused", "ConnectException"]
    ):
        master_url = os.getenv("SPARK_MASTER_URL", "spark://localhost:7077")
        raise ValueError(
            f"Spark engine unavailable - check if Spark cluster is running at {master_url}. Connection error: {error_msg}"
        )

    # Check for stopped SparkContext issues
    if (
        "stopped SparkContext" in error_msg
        or "Cannot call methods on a stopped SparkContext" in error_msg
    ):
        raise ValueError(
            f"Spark session error - SparkContext was stopped unexpectedly during {operation_name} operation. This may indicate resource constraints or connectivity issues."
        )

    # Check for distutils/environment issues (common in containerized Spark)
    if "distutils" in error_msg or ("No module named" in error_msg and "distutils" in error_msg):
        raise ValueError(
            f"Spark environment issue in {operation_name} operation: Missing system dependencies (distutils) in Spark environment. This is common in Python 3.10+ containers. Consider using pandas engine or updating Spark container configuration."
        )

    # Check for schema or data conversion issues
    if any(phrase in error_msg for phrase in ["schema", "convert", "toPandas", "createDataFrame"]):
        raise ValueError(
            f"Spark data conversion error in {operation_name} operation: {error_msg}. This may be due to unsupported data types or schema mismatches."
        )

    # Check for operation-specific errors
    if operation_name == "filter" and any(phrase in error_msg for phrase in ["column", "Column"]):
        raise ValueError(
            f"Spark filter error: {error_msg}. Check that all filter columns exist in the dataframe."
        )

    if operation_name == "select" and any(phrase in error_msg for phrase in ["column", "Column"]):
        raise ValueError(
            f"Spark select error: {error_msg}. Check that all selected columns exist in the dataframe."
        )

    # Generic Spark error
    raise ValueError(f"Spark engine failed for {operation_name} operation: {error_msg}")


def validate_engine(engine: str) -> str:
    """Validate and normalize engine parameter"""
    if not engine:
        return "pandas"  # Default to pandas

    engine = engine.lower().strip()
    if engine not in ["pandas", "spark"]:
        raise ValueError(f"Invalid engine '{engine}'. Must be 'pandas' or 'spark'")

    return engine


def check_spark_availability() -> bool:
    """Check if Spark is available and accessible"""
    try:
        import pyspark
        from pyspark.sql import SparkSession

        # Try to create a session briefly to test connectivity
        spark = SparkSession.builder.appName("HealthCheck").getOrCreate()
        spark.stop()
        return True
    except Exception:
        return False


def route_compare(df1: pd.DataFrame, df2: pd.DataFrame, n1: str, n2: str, engine: str) -> dict:
    """Route compare operation to appropriate engine"""
    engine = validate_engine(engine)

    if engine == "spark":
        try:
            return spark_engine.spark_compare(df1, df2, n1, n2)
        except Exception as e:
            # Don't fallback automatically - return error for explicit engine selection
            return {"success": False, "error": f"Spark engine failed: {str(e)}", "engine": "spark"}
    else:
        return pandas_engine.pandas_compare(df1, df2, n1, n2)


def route_merge(
    dfs: List[pd.DataFrame],
    names: List[str],
    keys: List[str],
    how: str,
    engine: str,
    left_on: Optional[List[str]] = None,
    right_on: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Route merge operation to appropriate engine"""
    engine = validate_engine(engine)

    if engine == "spark":
        try:
            return spark_engine.spark_merge(dfs, names, keys, how, left_on, right_on)
        except Exception as e:
            _handle_spark_error("merge", e)
    else:
        return pandas_engine.pandas_merge(dfs, names, keys, how, left_on, right_on)


def route_filter(
    df: pd.DataFrame, conditions: List[Dict], combine: str, engine: str
) -> pd.DataFrame:
    """Route filter operation to appropriate engine"""
    engine = validate_engine(engine)

    print(f"[DEBUG] route_filter: engine={engine}, conditions={len(conditions)}, combine={combine}")

    if engine == "spark":
        try:
            print(f"[DEBUG] Using Spark engine for filter operation")
            result = spark_engine.spark_filter(df, conditions, combine)
            print(f"[DEBUG] Spark filter successful: result shape={result.shape}")
            return result
        except Exception as e:
            print(f"[DEBUG] Spark filter failed: {str(e)}")
            _handle_spark_error("filter", e)
    else:
        print(f"[DEBUG] Using Pandas engine for filter operation")
        result = pandas_engine.pandas_filter(df, conditions, combine)
        print(f"[DEBUG] Pandas filter successful: result shape={result.shape}")
        return result


def route_groupby(
    df: pd.DataFrame, by: List[str], aggs: Optional[Dict], engine: str
) -> pd.DataFrame:
    """Route groupby operation to appropriate engine"""
    engine = validate_engine(engine)

    if engine == "spark":
        try:
            return spark_engine.spark_groupby(df, by, aggs)
        except Exception as e:
            _handle_spark_error("groupby", e)
    else:
        return pandas_engine.pandas_groupby(df, by, aggs)


def route_select(df: pd.DataFrame, columns: List[str], exclude: bool, engine: str) -> pd.DataFrame:
    """Route select operation to appropriate engine"""
    engine = validate_engine(engine)

    print(f"[DEBUG] route_select: engine={engine}, columns={columns}, exclude={exclude}")

    if engine == "spark":
        try:
            print(f"[DEBUG] Using Spark engine for select operation")
            result = spark_engine.spark_select(df, columns, exclude)
            print(f"[DEBUG] Spark select successful: result shape={result.shape}")
            return result
        except Exception as e:
            print(f"[DEBUG] Spark select failed: {str(e)}")
            _handle_spark_error("select", e)
    else:
        print(f"[DEBUG] Using Pandas engine for select operation")
        result = pandas_engine.pandas_select(df, columns, exclude)
        print(f"[DEBUG] Pandas select successful: result shape={result.shape}")
        return result


def route_rename(df: pd.DataFrame, rename_map: Dict[str, str], engine: str) -> pd.DataFrame:
    """Route rename operation to appropriate engine"""
    engine = validate_engine(engine)

    if engine == "spark":
        try:
            return spark_engine.spark_rename(df, rename_map)
        except Exception as e:
            _handle_spark_error("rename", e)
    else:
        return pandas_engine.pandas_rename(df, rename_map)


def route_pivot(df: pd.DataFrame, mode: str, engine: str, **kwargs) -> pd.DataFrame:
    """Route pivot operation to appropriate engine"""
    engine = validate_engine(engine)

    if engine == "spark":
        try:
            return spark_engine.spark_pivot(df, mode, **kwargs)
        except Exception as e:
            _handle_spark_error("pivot", e)
    else:
        return pandas_engine.pandas_pivot(df, mode, **kwargs)


def route_datetime(
    df: pd.DataFrame, action: str, source: str, engine: str, **kwargs
) -> pd.DataFrame:
    """Route datetime operation to appropriate engine"""
    engine = validate_engine(engine)

    if engine == "spark":
        try:
            return spark_engine.spark_datetime(df, action, source, **kwargs)
        except Exception as e:
            _handle_spark_error("datetime", e)
    else:
        return pandas_engine.pandas_datetime(df, action, source, **kwargs)


def route_mutate(
    df: pd.DataFrame, target: str, expr: str, mode: str, overwrite: bool, engine: str
) -> pd.DataFrame:
    """Route mutate operation to appropriate engine"""
    engine = validate_engine(engine)

    if engine == "spark":
        try:
            return spark_engine.spark_mutate(df, target, expr, mode, overwrite)
        except Exception as e:
            _handle_spark_error("mutate", e)
    else:
        return pandas_engine.pandas_mutate(df, target, expr, mode, overwrite)


def get_engine_info(engine: str) -> Dict[str, Any]:
    """Get information about an engine"""
    engine = validate_engine(engine)

    info = {"engine": engine, "available": True, "capabilities": []}

    if engine == "spark":
        info["available"] = check_spark_availability()
        info["capabilities"] = [
            "Large dataset processing",
            "Distributed computing",
            "SQL-based operations",
            "Memory optimization",
            "Lazy evaluation",
        ]
        if not info["available"]:
            info["error"] = "Spark not available or not configured"
    else:
        info["capabilities"] = [
            "Fast in-memory processing",
            "Rich data manipulation",
            "Flexible operations",
            "Python ecosystem integration",
        ]

    return info


def get_engine_recommendation(row_count: int, operation: str) -> str:
    """Get engine recommendation based on data size and operation"""
    # Simple heuristics for engine recommendation
    if row_count > 100_000:
        if operation in ["compare", "merge", "groupby"]:
            return "spark"  # Spark is better for large data operations

    if row_count < 10_000:
        return "pandas"  # Pandas is fine for small data

    # Medium size data - check operation type
    if operation in ["mutate", "datetime", "pivot"]:
        return "pandas"  # These are often better in pandas

    return "pandas"  # Default to pandas for medium data


def create_engine_response(
    success: bool, result_data: Any, engine: str, operation: str, error: str = None
) -> Dict[str, Any]:
    """Create standardized response with engine metadata"""
    response = {"success": success, "engine": engine, "operation": operation}

    if success:
        if isinstance(result_data, dict):
            response.update(result_data)
        else:
            response["result"] = result_data
    else:
        response["error"] = error or "Operation failed"

    # Add engine info
    response["engine_info"] = get_engine_info(engine)

    return response
