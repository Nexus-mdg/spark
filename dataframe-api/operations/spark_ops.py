"""
Spark-based DataFrame operations
This module provides Spark equivalents for pandas operations
"""
import os
import io
import json
import pandas as pd
from datetime import datetime
from utils.redis_client import redis_client
from operations.dataframe_ops import _save_df_to_cache, _unique_name


def _get_spark_session():
    """Get or create Spark session"""
    try:
        from pyspark.sql import SparkSession
        
        master_url = os.getenv('SPARK_MASTER_URL', 'spark://localhost:7077')
        builder = (SparkSession.builder
                   .appName('DataFrame-Operations')
                   .master(master_url)
                   .config('spark.sql.adaptive.enabled', 'true')
                   .config('spark.executor.memory', os.getenv('SPARK_EXECUTOR_MEMORY', '4g'))
                   .config('spark.driver.memory', os.getenv('SPARK_DRIVER_MEMORY', '4g'))
                   .config('spark.network.timeout', os.getenv('SPARK_NETWORK_TIMEOUT', '120s'))
                   .config('spark.pyspark.python', os.getenv('PYSPARK_PYTHON', 'python3'))
                   .config('spark.pyspark.driver.python', os.getenv('PYSPARK_DRIVER_PYTHON', 'python3')))
        
        driver_host = os.getenv('SPARK_DRIVER_HOST')
        if driver_host:
            builder = builder.config('spark.driver.host', driver_host)
        
        driver_bind = os.getenv('SPARK_DRIVER_BIND_ADDRESS')
        if driver_bind:
            builder = builder.config('spark.driver.bindAddress', driver_bind)
        
        return builder.getOrCreate()
    except Exception as e:
        raise Exception(f"Failed to initialize Spark session: {str(e)}")


def _load_df_to_spark(name: str):
    """Load DataFrame from Redis and convert to Spark DataFrame"""
    df_key = f"df:{name}"
    if not redis_client.exists(df_key):
        raise ValueError(f'DataFrame "{name}" not found')
    
    csv_string = redis_client.get(df_key)
    pandas_df = pd.read_csv(io.StringIO(csv_string))
    
    spark = _get_spark_session()
    return spark.createDataFrame(pandas_df)


def _spark_df_to_pandas(spark_df):
    """Convert Spark DataFrame back to pandas DataFrame"""
    return spark_df.toPandas()


def spark_select_op(name: str, columns: list, exclude: bool = False) -> dict:
    """Spark-based select operation"""
    try:
        spark_df = _load_df_to_spark(name)
        
        if exclude:
            # Select all columns except the specified ones
            keep_cols = [c for c in spark_df.columns if c not in columns]
            result_df = spark_df.select(*keep_cols)
            base = f"{name}__spark_drop_{'-'.join(columns)}"
            desc = f"Spark: Drop columns={','.join(columns)}"
        else:
            # Select only the specified columns
            missing = [c for c in columns if c not in spark_df.columns]
            if missing:
                raise ValueError(f'Columns not found: {", ".join(missing)}')
            result_df = spark_df.select(*columns)
            base = f"{name}__spark_select_{'-'.join(columns)}"
            desc = f"Spark: Select columns={','.join(columns)}"
        
        # Convert back to pandas for storage
        pandas_result = _spark_df_to_pandas(result_df)
        out_name = _unique_name(base)
        meta = _save_df_to_cache(out_name, pandas_result, description=desc, source='spark:select')
        
        return {'success': True, 'name': out_name, 'metadata': meta}
    except Exception as e:
        return {'success': False, 'error': str(e)}


def spark_filter_op(name: str, filters: list, combine: str = 'and') -> dict:
    """Spark-based filter operation"""
    try:
        spark_df = _load_df_to_spark(name)
        
        if not filters:
            raise ValueError("At least one filter is required")
        
        from pyspark.sql import functions as F
        
        conditions = []
        for f in filters:
            col_name = f.get('col')
            op = f.get('op', 'eq')
            value = f.get('value')
            
            if not col_name or col_name not in spark_df.columns:
                raise ValueError(f"Invalid column: {col_name}")
            
            col = F.col(col_name)
            
            if op == 'eq':
                conditions.append(col == value)
            elif op == 'ne':
                conditions.append(col != value)
            elif op == 'gt':
                conditions.append(col > value)
            elif op == 'gte':
                conditions.append(col >= value)
            elif op == 'lt':
                conditions.append(col < value)
            elif op == 'lte':
                conditions.append(col <= value)
            elif op == 'contains':
                conditions.append(col.contains(str(value)))
            elif op == 'startswith':
                conditions.append(col.startswith(str(value)))
            elif op == 'endswith':
                conditions.append(col.endswith(str(value)))
            elif op == 'isnull':
                conditions.append(col.isNull())
            elif op == 'notnull':
                conditions.append(col.isNotNull())
            else:
                raise ValueError(f"Unsupported filter operation: {op}")
        
        # Combine conditions
        if combine.lower() == 'and':
            final_condition = conditions[0]
            for cond in conditions[1:]:
                final_condition = final_condition & cond
        else:  # 'or'
            final_condition = conditions[0]
            for cond in conditions[1:]:
                final_condition = final_condition | cond
        
        result_df = spark_df.filter(final_condition)
        
        # Convert back to pandas for storage
        pandas_result = _spark_df_to_pandas(result_df)
        base = f"{name}__spark_filter_{combine}_{len(filters)}"
        desc = f"Spark: Filter {len(filters)} conditions ({combine})"
        out_name = _unique_name(base)
        meta = _save_df_to_cache(out_name, pandas_result, description=desc, source='spark:filter')
        
        return {'success': True, 'name': out_name, 'metadata': meta}
    except Exception as e:
        return {'success': False, 'error': str(e)}


def spark_groupby_op(name: str, by: list, aggs: dict) -> dict:
    """Spark-based groupby operation"""
    try:
        spark_df = _load_df_to_spark(name)
        
        if not by:
            raise ValueError("Groupby columns cannot be empty")
        
        missing = [c for c in by if c not in spark_df.columns]
        if missing:
            raise ValueError(f'Groupby columns not found: {", ".join(missing)}')
        
        from pyspark.sql import functions as F
        
        # Build aggregation expressions
        agg_exprs = []
        for col_name, agg_func in aggs.items():
            if col_name not in spark_df.columns:
                raise ValueError(f'Aggregation column not found: {col_name}')
            
            if agg_func == 'count':
                agg_exprs.append(F.count(col_name).alias(f"{col_name}_{agg_func}"))
            elif agg_func == 'sum':
                agg_exprs.append(F.sum(col_name).alias(f"{col_name}_{agg_func}"))
            elif agg_func == 'mean':
                agg_exprs.append(F.mean(col_name).alias(f"{col_name}_{agg_func}"))
            elif agg_func == 'min':
                agg_exprs.append(F.min(col_name).alias(f"{col_name}_{agg_func}"))
            elif agg_func == 'max':
                agg_exprs.append(F.max(col_name).alias(f"{col_name}_{agg_func}"))
            elif agg_func == 'std':
                agg_exprs.append(F.stddev(col_name).alias(f"{col_name}_{agg_func}"))
            else:
                raise ValueError(f"Unsupported aggregation function: {agg_func}")
        
        result_df = spark_df.groupBy(*by).agg(*agg_exprs)
        
        # Convert back to pandas for storage
        pandas_result = _spark_df_to_pandas(result_df)
        base = f"{name}__spark_groupby_{'-'.join(by)}"
        desc = f"Spark: GroupBy {','.join(by)}"
        out_name = _unique_name(base)
        meta = _save_df_to_cache(out_name, pandas_result, description=desc, source='spark:groupby')
        
        return {'success': True, 'name': out_name, 'metadata': meta}
    except Exception as e:
        return {'success': False, 'error': str(e)}


def spark_merge_op(names: list, keys: list, how: str = 'inner') -> dict:
    """Spark-based merge operation"""
    try:
        if len(names) != 2:
            raise ValueError("Merge requires exactly 2 dataframes")
        
        spark_df1 = _load_df_to_spark(names[0])
        spark_df2 = _load_df_to_spark(names[1])
        
        if not keys:
            raise ValueError("Join keys cannot be empty")
        
        # Check if keys exist in both dataframes
        missing_left = [k for k in keys if k not in spark_df1.columns]
        missing_right = [k for k in keys if k not in spark_df2.columns]
        
        if missing_left:
            raise ValueError(f'Keys not found in left df: {", ".join(missing_left)}')
        if missing_right:
            raise ValueError(f'Keys not found in right df: {", ".join(missing_right)}')
        
        # Perform join
        result_df = spark_df1.join(spark_df2, keys, how)
        
        # Convert back to pandas for storage
        pandas_result = _spark_df_to_pandas(result_df)
        base = f"{names[0]}__spark_merge_{names[1]}_{how}"
        desc = f"Spark: {how.upper()} merge on {','.join(keys)}"
        out_name = _unique_name(base)
        meta = _save_df_to_cache(out_name, pandas_result, description=desc, source='spark:merge')
        
        return {'success': True, 'name': out_name, 'metadata': meta}
    except Exception as e:
        return {'success': False, 'error': str(e)}


def spark_rename_op(name: str, rename_map: dict) -> dict:
    """Spark-based rename operation"""
    try:
        spark_df = _load_df_to_spark(name)
        
        if not rename_map:
            raise ValueError("Rename mapping cannot be empty")
        
        # Check if columns to rename exist
        missing = [c for c in rename_map.keys() if c not in spark_df.columns]
        if missing:
            raise ValueError(f'Columns to rename not found: {", ".join(missing)}')
        
        # Apply renames
        result_df = spark_df
        for old_name, new_name in rename_map.items():
            result_df = result_df.withColumnRenamed(old_name, new_name)
        
        # Convert back to pandas for storage
        pandas_result = _spark_df_to_pandas(result_df)
        base = f"{name}__spark_rename_{len(rename_map)}"
        desc = f"Spark: Rename {len(rename_map)} columns"
        out_name = _unique_name(base)
        meta = _save_df_to_cache(out_name, pandas_result, description=desc, source='spark:rename')
        
        return {'success': True, 'name': out_name, 'metadata': meta}
    except Exception as e:
        return {'success': False, 'error': str(e)}


def spark_pivot_op(name: str, pivot_config: dict) -> dict:
    """Spark-based pivot operation"""
    try:
        spark_df = _load_df_to_spark(name)
        
        pivot_col = pivot_config.get('pivot_col')
        value_col = pivot_config.get('value_col')
        index_cols = pivot_config.get('index_cols', [])
        
        if not pivot_col or not value_col:
            raise ValueError("pivot_col and value_col are required")
        
        if pivot_col not in spark_df.columns:
            raise ValueError(f'Pivot column not found: {pivot_col}')
        if value_col not in spark_df.columns:
            raise ValueError(f'Value column not found: {value_col}')
        
        # If no index cols specified, use all remaining columns
        if not index_cols:
            index_cols = [c for c in spark_df.columns if c not in [pivot_col, value_col]]
        
        # Perform pivot
        if index_cols:
            result_df = spark_df.groupBy(*index_cols).pivot(pivot_col).sum(value_col)
        else:
            result_df = spark_df.groupBy().pivot(pivot_col).sum(value_col)
        
        # Convert back to pandas for storage
        pandas_result = _spark_df_to_pandas(result_df)
        base = f"{name}__spark_pivot_{pivot_col}"
        desc = f"Spark: Pivot on {pivot_col}"
        out_name = _unique_name(base)
        meta = _save_df_to_cache(out_name, pandas_result, description=desc, source='spark:pivot')
        
        return {'success': True, 'name': out_name, 'metadata': meta}
    except Exception as e:
        return {'success': False, 'error': str(e)}


def spark_datetime_op(name: str, column: str, operation: dict) -> dict:
    """Spark-based datetime operation"""
    try:
        spark_df = _load_df_to_spark(name)
        
        if column not in spark_df.columns:
            raise ValueError(f'Column not found: {column}')
        
        from pyspark.sql import functions as F
        from pyspark.sql.types import TimestampType
        
        op_type = operation.get('type', 'parse')
        
        if op_type == 'parse':
            # Parse string to datetime
            format_str = operation.get('format', 'yyyy-MM-dd')
            result_df = spark_df.withColumn(
                column, 
                F.to_timestamp(F.col(column), format_str)
            )
            desc = f"Spark: Parse datetime {column}"
            
        elif op_type == 'extract':
            # Extract components from datetime
            component = operation.get('component', 'year')
            new_col = operation.get('target', f"{column}_{component}")
            
            if component == 'year':
                result_df = spark_df.withColumn(new_col, F.year(column))
            elif component == 'month':
                result_df = spark_df.withColumn(new_col, F.month(column))
            elif component == 'day':
                result_df = spark_df.withColumn(new_col, F.dayofmonth(column))
            elif component == 'hour':
                result_df = spark_df.withColumn(new_col, F.hour(column))
            elif component == 'dayofweek':
                result_df = spark_df.withColumn(new_col, F.dayofweek(column))
            else:
                raise ValueError(f"Unsupported datetime component: {component}")
            
            desc = f"Spark: Extract {component} from {column}"
        else:
            raise ValueError(f"Unsupported datetime operation: {op_type}")
        
        # Convert back to pandas for storage
        pandas_result = _spark_df_to_pandas(result_df)
        base = f"{name}__spark_datetime_{op_type}"
        out_name = _unique_name(base)
        meta = _save_df_to_cache(out_name, pandas_result, description=desc, source='spark:datetime')
        
        return {'success': True, 'name': out_name, 'metadata': meta}
    except Exception as e:
        return {'success': False, 'error': str(e)}


def spark_mutate_op(name: str, target: str, expr: str, mode: str = 'vector', overwrite: bool = False) -> dict:
    """Spark-based mutate operation"""
    try:
        spark_df = _load_df_to_spark(name)
        
        if not target:
            raise ValueError("Target column name is required")
        if not expr or not expr.strip():
            raise ValueError("Expression is required")
        
        if target in spark_df.columns and not overwrite:
            raise ValueError(f'Target column {target} already exists')
        
        from pyspark.sql import functions as F
        
        # For simplicity, convert to pandas for complex expressions
        # In a production system, you'd want to parse and convert expressions to Spark SQL
        pandas_df = _spark_df_to_pandas(spark_df)
        
        # Apply same mutation logic as pandas version
        import numpy as np
        safe_builtins = {
            'abs': abs, 'min': min, 'max': max, 'round': round,
            'int': int, 'float': float, 'str': str, 'bool': bool, 'len': len,
        }
        
        def _safe_eval(expression: str, local_ctx: dict):
            code = compile(expression, '<mutate-expr>', 'eval')
            return eval(code, {'__builtins__': {}}, {**safe_builtins, **local_ctx})
        
        base_locals = {'pd': pd, 'np': np, 'df': pandas_df}
        base_locals['col'] = lambda c: pandas_df[c]
        
        if mode == 'vector':
            result = _safe_eval(expr, base_locals)
        else:  # row mode
            result = pandas_df.apply(lambda r: _safe_eval(expr, {**base_locals, 'r': r}), axis=1)
        
        pandas_df[target] = result
        
        # Convert back to Spark then to pandas for consistency
        new_spark_df = _get_spark_session().createDataFrame(pandas_df)
        pandas_result = _spark_df_to_pandas(new_spark_df)
        
        base = f"{name}__spark_mutate_{target}"
        desc = f"Spark: Mutate {target}"
        out_name = _unique_name(base)
        meta = _save_df_to_cache(out_name, pandas_result, description=desc, source='spark:mutate')
        
        return {'success': True, 'name': out_name, 'metadata': meta}
    except Exception as e:
        return {'success': False, 'error': str(e)}