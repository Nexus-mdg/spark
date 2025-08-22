"""
Spark engine for DataFrame operations
Implements Spark-based versions of all operations for consistent engine-based architecture
"""
import json
import os
import pandas as pd
import numpy as np
from typing import Tuple, Optional, Dict, List, Any


def _get_spark_session():
    """Get or create Spark session with current configuration"""
    from pyspark.sql import SparkSession
    
    master_url = os.getenv('SPARK_MASTER_URL', 'spark://localhost:7077')
    builder = (SparkSession.builder
               .appName('UI-Operations')
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


def _pandas_to_spark(df: pd.DataFrame, spark):
    """Convert pandas DataFrame to Spark DataFrame with proper schema"""
    try:
        if df.empty:
            raise ValueError('Cannot convert empty DataFrame to Spark')
        
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
            return StringType()
        
        def _schema_from_pandas(df: pd.DataFrame) -> StructType:
            fields = []
            for c in df.columns:
                fields.append(StructField(c, _spark_type_from_pd(df[c].dtype), True))
            return StructType(fields)
        
        def _rows_from_pandas(df: pd.DataFrame):
            for row in df.itertuples(index=False, name=None):
                yield tuple(None if (isinstance(v, float) and pd.isna(v)) or (v is pd.NaT) else v for v in row)
        
        schema = _schema_from_pandas(df)
        return spark.createDataFrame(_rows_from_pandas(df), schema=schema)
    
    except Exception as e:
        raise ValueError(f'Failed to convert pandas DataFrame to Spark: {str(e)}')


def _spark_to_pandas(sdf):
    """Convert Spark DataFrame to pandas DataFrame"""
    try:
        return sdf.toPandas()
    except Exception as e:
        raise ValueError(f'Failed to convert Spark DataFrame to pandas: {str(e)}')


def spark_compare(df1: pd.DataFrame, df2: pd.DataFrame, n1: str, n2: str) -> dict:
    """
    Spark implementation for DataFrame comparison
    Returns: dict with comparison result
    """
    spark = None
    try:
        spark = _get_spark_session()
        
        # Convert to Spark DataFrames
        sdf1 = _pandas_to_spark(df1, spark)
        sdf2 = _pandas_to_spark(df2, spark)
        
        # Schema comparison
        schema1_types = [f.dataType.simpleString() for f in sdf1.schema]
        schema2_types = [f.dataType.simpleString() for f in sdf2.schema]
        
        if schema1_types != schema2_types or list(df1.columns) != list(df2.columns):
            return {
                'success': True,
                'identical': False,
                'result_type': 'schema_mismatch',
                'engine': 'spark'
            }
        
        # Row count comparison
        count1, count2 = sdf1.count(), sdf2.count()
        if count1 != count2:
            return {
                'success': True,
                'identical': False,
                'result_type': 'row_count_mismatch',
                'engine': 'spark'
            }
        
        # Order-independent data comparison using exceptAll
        diff1 = sdf1.exceptAll(sdf2)
        diff2 = sdf2.exceptAll(sdf1)
        c1 = diff1.count()
        c2 = diff2.count()
        
        if c1 == 0 and c2 == 0:
            return {
                'success': True,
                'identical': True,
                'result_type': 'identical',
                'created': [],
                'engine': 'spark'
            }
        
        created = []
        THRESH = int(os.getenv('COMPARE_CACHE_THRESHOLD', '100000'))
        if 0 < c1 <= THRESH:
            created.append(f"{n1}_unique_rows")
        if 0 < c2 <= THRESH:
            created.append(f"{n2}_unique_rows")
        
        return {
            'success': True,
            'identical': False,
            'result_type': 'data_mismatch',
            'left_unique': int(c1),
            'right_unique': int(c2),
            'created': created,
            'engine': 'spark',
            'note': f'Spark comparator used'
        }
    
    finally:
        if spark:
            try:
                spark.stop()
            except Exception:
                pass


def spark_merge(dfs: List[pd.DataFrame], names: List[str], keys: List[str], 
               how: str, left_on: Optional[List[str]] = None, 
               right_on: Optional[List[str]] = None) -> pd.DataFrame:
    """Spark implementation for DataFrame merge operation"""
    if len(dfs) < 2:
        raise ValueError('At least 2 dataframes required for merge')
    
    spark = None
    try:
        spark = _get_spark_session()
        
        # Convert to Spark DataFrames
        sdfs = [_pandas_to_spark(df, spark) for df in dfs]
        
        # Start with first DataFrame
        result_sdf = sdfs[0]
        
        # Merge with each subsequent DataFrame
        for i, sdf2 in enumerate(sdfs[1:], 1):
            if left_on and right_on:
                # Column mapping join
                join_condition = [result_sdf[l] == sdf2[r] for l, r in zip(left_on, right_on)]
                if len(join_condition) == 1:
                    join_condition = join_condition[0]
                else:
                    from functools import reduce
                    from pyspark.sql.functions import col
                    join_condition = reduce(lambda a, b: a & b, join_condition)
                
                result_sdf = result_sdf.join(sdf2, join_condition, how)
            else:
                # Traditional key-based join
                result_sdf = result_sdf.join(sdf2, keys, how)
        
        # Convert back to pandas
        return _spark_to_pandas(result_sdf)
    
    finally:
        if spark:
            try:
                spark.stop()
            except Exception:
                pass


def spark_filter(df: pd.DataFrame, conditions: List[Dict], combine: str = 'and') -> pd.DataFrame:
    """Spark implementation for DataFrame filter operation"""
    if not conditions:
        return df.copy()
    
    spark = None
    try:
        spark = _get_spark_session()
        sdf = _pandas_to_spark(df, spark)
        
        from pyspark.sql.functions import col
        from functools import reduce
        
        filter_conditions = []
        for cond in conditions:
            column = cond.get('col')
            op = (cond.get('op') or 'eq').lower()
            val = cond.get('value')
            
            if column not in sdf.columns:
                raise ValueError(f'Column {column} not found')
            
            col_ref = col(column)
            
            if op == 'eq':
                filter_conditions.append(col_ref == val)
            elif op == 'ne':
                filter_conditions.append(col_ref != val)
            elif op == 'lt':
                filter_conditions.append(col_ref < val)
            elif op == 'lte':
                filter_conditions.append(col_ref <= val)
            elif op == 'gt':
                filter_conditions.append(col_ref > val)
            elif op == 'gte':
                filter_conditions.append(col_ref >= val)
            elif op == 'in':
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
                filter_conditions.append(col_ref.isin(vals))
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
                filter_conditions.append(~col_ref.isin(vals))
            elif op == 'contains':
                filter_conditions.append(col_ref.contains(str(val)))
            elif op == 'startswith':
                filter_conditions.append(col_ref.startswith(str(val)))
            elif op == 'endswith':
                filter_conditions.append(col_ref.endswith(str(val)))
            elif op == 'isnull':
                filter_conditions.append(col_ref.isNull())
            elif op == 'notnull':
                filter_conditions.append(col_ref.isNotNull())
            else:
                raise ValueError(f'Unsupported op {op}')
        
        if filter_conditions:
            if combine == 'and':
                final_condition = reduce(lambda a, b: a & b, filter_conditions)
            else:  # or
                final_condition = reduce(lambda a, b: a | b, filter_conditions)
            
            filtered_sdf = sdf.filter(final_condition)
        else:
            filtered_sdf = sdf
        
        return _spark_to_pandas(filtered_sdf)
    
    finally:
        if spark:
            try:
                spark.stop()
            except Exception:
                pass


def spark_groupby(df: pd.DataFrame, by: List[str], aggs: Optional[Dict] = None) -> pd.DataFrame:
    """Spark implementation for DataFrame groupby operation"""
    for c in by:
        if c not in df.columns:
            raise ValueError(f'Group-by column {c} not found')
    
    spark = None
    try:
        spark = _get_spark_session()
        sdf = _pandas_to_spark(df, spark)
        
        grouped = sdf.groupBy(by)
        
        if aggs:
            from pyspark.sql.functions import avg, sum, count, min, max, first, last
            
            # Map pandas aggregation functions to Spark functions
            spark_agg_map = {
                'mean': avg, 'avg': avg, 'average': avg,
                'sum': sum,
                'count': count,
                'min': min,
                'max': max,
                'first': first,
                'last': last
            }
            
            agg_exprs = []
            for col_name, agg_func in aggs.items():
                if isinstance(agg_func, str):
                    agg_func = [agg_func]
                if isinstance(agg_func, list):
                    for func in agg_func:
                        if func in spark_agg_map:
                            agg_exprs.append(spark_agg_map[func](col_name).alias(f"{col_name}_{func}"))
                        else:
                            # Fallback to pandas for unsupported aggregations
                            return df.groupby(by).agg(aggs).reset_index()
            
            if agg_exprs:
                result_sdf = grouped.agg(*agg_exprs)
            else:
                result_sdf = grouped.count()
        else:
            result_sdf = grouped.count()
        
        return _spark_to_pandas(result_sdf)
    
    finally:
        if spark:
            try:
                spark.stop()
            except Exception:
                pass


def spark_select(df: pd.DataFrame, columns: List[str], exclude: bool = False) -> pd.DataFrame:
    """Spark implementation for DataFrame select operation"""
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f'Columns not found: {", ".join(missing)}')
    
    spark = None
    try:
        spark = _get_spark_session()
        sdf = _pandas_to_spark(df, spark)
        
        if exclude:
            keep_cols = [c for c in sdf.columns if c not in columns]
            if not keep_cols:
                raise ValueError('Cannot exclude all columns - no columns would remain')
            selected_sdf = sdf.select(keep_cols)
        else:
            if not columns:
                raise ValueError('Cannot select empty column list')
            selected_sdf = sdf.select(columns)
        
        return _spark_to_pandas(selected_sdf)
    
    except Exception as e:
        # Ensure we capture and re-raise the actual error
        if "columns" in str(e).lower() or "select" in str(e).lower():
            raise ValueError(f'Spark select operation failed: {str(e)}')
        else:
            raise e
    
    finally:
        if spark:
            try:
                spark.stop()
            except Exception:
                pass


def spark_rename(df: pd.DataFrame, rename_map: Dict[str, str]) -> pd.DataFrame:
    """Spark implementation for DataFrame rename operation"""
    missing = [old for old in rename_map.keys() if old not in df.columns]
    if missing:
        raise ValueError(f'Columns to rename not found: {", ".join(missing)}')
    
    # Check for duplicate names after rename
    new_cols = list(df.columns)
    for old, new in rename_map.items():
        if not isinstance(new, str) or not new:
            raise ValueError(f'Invalid new name for column {old}')
        idx = new_cols.index(old)
        new_cols[idx] = new
    
    if len(set(new_cols)) != len(new_cols):
        raise ValueError('Rename would cause duplicate column names')
    
    spark = None
    try:
        spark = _get_spark_session()
        sdf = _pandas_to_spark(df, spark)
        
        # Apply column renames
        for old_name, new_name in rename_map.items():
            sdf = sdf.withColumnRenamed(old_name, new_name)
        
        return _spark_to_pandas(sdf)
    
    finally:
        if spark:
            try:
                spark.stop()
            except Exception:
                pass


def spark_pivot(df: pd.DataFrame, mode: str, **kwargs) -> pd.DataFrame:
    """Spark implementation for DataFrame pivot operation"""
    spark = None
    try:
        spark = _get_spark_session()
        sdf = _pandas_to_spark(df, spark)
        
        if mode == 'wider':
            index = kwargs.get('index', [])
            names_from = kwargs.get('names_from')
            values_from = kwargs.get('values_from')
            aggfunc = kwargs.get('aggfunc', 'first')
            
            if not names_from or not values_from:
                raise ValueError('names_from and values_from are required for wider')
            
            if isinstance(values_from, str):
                values_from = [values_from]
            
            from pyspark.sql.functions import first, sum, avg, count, min, max
            
            # Map aggregation functions
            spark_agg_map = {
                'first': first,
                'sum': sum,
                'avg': avg, 'mean': avg,
                'count': count,
                'min': min,
                'max': max
            }
            
            agg_func = spark_agg_map.get(aggfunc, first)
            
            # Group by index columns and pivot
            grouped = sdf.groupBy(index) if index else sdf.groupBy()
            pivoted_sdf = grouped.pivot(names_from).agg({col: aggfunc for col in values_from})
            
            return _spark_to_pandas(pivoted_sdf)
        
        elif mode == 'longer':
            # For longer pivot, use pandas as Spark doesn't have a direct melt equivalent
            # that's as flexible
            id_vars = kwargs.get('id_vars', [])
            value_vars = kwargs.get('value_vars', [])
            var_name = kwargs.get('var_name', 'variable')
            value_name = kwargs.get('value_name', 'value')
            
            if not value_vars:
                raise ValueError('value_vars is required for longer')
            
            # Fall back to pandas for melt operation
            return pd.melt(df, id_vars=id_vars or None, value_vars=value_vars, 
                          var_name=var_name, value_name=value_name)
        
        else:
            raise ValueError('mode must be wider or longer')
    
    finally:
        if spark:
            try:
                spark.stop()
            except Exception:
                pass


def spark_datetime(df: pd.DataFrame, action: str, source: str, **kwargs) -> pd.DataFrame:
    """Spark implementation for DataFrame datetime operation"""
    if source not in df.columns:
        raise ValueError(f'Column {source} not found')
    
    spark = None
    try:
        spark = _get_spark_session()
        sdf = _pandas_to_spark(df, spark)
        
        from pyspark.sql.functions import to_timestamp, year, month, dayofmonth, date_format, col
        
        if action == 'parse':
            fmt = kwargs.get('format') or kwargs.get('fmt')
            target = kwargs.get('target') or kwargs.get('to')
            overwrite = bool(kwargs.get('overwrite'))
            
            if target and target != source:
                if (target in sdf.columns) and not overwrite:
                    raise ValueError(f'target column {target} already exists')
                
                if fmt:
                    sdf = sdf.withColumn(target, to_timestamp(col(source), fmt))
                else:
                    sdf = sdf.withColumn(target, to_timestamp(col(source)))
            else:
                if fmt:
                    sdf = sdf.withColumn(source, to_timestamp(col(source), fmt))
                else:
                    sdf = sdf.withColumn(source, to_timestamp(col(source)))
            
            return _spark_to_pandas(sdf)
        
        elif action == 'derive':
            opts = kwargs.get('outputs', {})
            
            # Default values
            want_year = bool(opts.get('year', True))
            want_month = bool(opts.get('month', True))
            want_day = bool(opts.get('day', True))
            want_year_month = bool(opts.get('year_month', True))
            
            names = kwargs.get('names', {})
            cname_year = names.get('year', 'year')
            cname_month = names.get('month', 'month')
            cname_day = names.get('day', 'day')
            cname_year_month = names.get('year_month', 'year_month')
            
            month_style = kwargs.get('month_style', 'short').lower()
            overwrite = bool(kwargs.get('overwrite', False))
            
            # Check for conflicts
            for cname, flag in [(cname_year, want_year), (cname_month, want_month), 
                               (cname_day, want_day), (cname_year_month, want_year_month)]:
                if not flag:
                    continue
                if (cname in sdf.columns) and not overwrite:
                    raise ValueError(f'Output column {cname} already exists')
            
            # Convert source to timestamp if not already
            timestamp_col = to_timestamp(col(source))
            
            # Generate columns
            if want_year:
                sdf = sdf.withColumn(cname_year, year(timestamp_col))
            
            if want_month:
                if month_style == 'long':
                    sdf = sdf.withColumn(cname_month, date_format(timestamp_col, 'MMMM'))
                elif month_style == 'num':
                    sdf = sdf.withColumn(cname_month, month(timestamp_col))
                elif month_style in ('short_lower', 'abbr_lower', 'short-lower'):
                    from pyspark.sql.functions import lower
                    sdf = sdf.withColumn(cname_month, lower(date_format(timestamp_col, 'MMM')))
                else:
                    sdf = sdf.withColumn(cname_month, date_format(timestamp_col, 'MMM'))
            
            if want_day:
                sdf = sdf.withColumn(cname_day, dayofmonth(timestamp_col))
            
            if want_year_month:
                if month_style in ('short_lower', 'abbr_lower', 'short-lower'):
                    from pyspark.sql.functions import lower
                    sdf = sdf.withColumn(cname_year_month, lower(date_format(timestamp_col, 'yyyy-MMM')))
                elif month_style == 'long':
                    sdf = sdf.withColumn(cname_year_month, date_format(timestamp_col, 'yyyy-MMMM'))
                elif month_style == 'num':
                    sdf = sdf.withColumn(cname_year_month, date_format(timestamp_col, 'yyyy-MM'))
                else:
                    sdf = sdf.withColumn(cname_year_month, date_format(timestamp_col, 'yyyy-MMM'))
            
            return _spark_to_pandas(sdf)
        
        else:
            raise ValueError('action must be parse or derive')
    
    finally:
        if spark:
            try:
                spark.stop()
            except Exception:
                pass


def spark_mutate(df: pd.DataFrame, target: str, expr: str, mode: str = 'vector', 
                overwrite: bool = False) -> pd.DataFrame:
    """Spark implementation for DataFrame mutate operation"""
    if (target in df.columns) and not overwrite:
        raise ValueError(f'target column {target} already exists')
    
    # For complex expressions, fall back to pandas as Spark SQL expressions
    # are more limited and complex to translate safely
    spark = None
    try:
        spark = _get_spark_session()
        sdf = _pandas_to_spark(df, spark)
        
        # For simple expressions, try to use Spark SQL
        # For complex ones, fall back to pandas
        if mode == 'vector':
            # Try to handle simple mathematical expressions
            from pyspark.sql.functions import expr, col
            
            # Basic expression pattern recognition
            # This is a simplified implementation - full SQL expression parsing would be more complex
            try:
                # Replace pandas-style column references with Spark SQL syntax
                spark_expr = expr.replace('col(', '').replace(')', '').replace("'", '`').replace('"', '`')
                
                # Try to use Spark SQL expression
                sdf = sdf.withColumn(target, expr(spark_expr))
                return _spark_to_pandas(sdf)
            except Exception:
                # Fall back to pandas for complex expressions
                pass
        
        # Fall back to pandas implementation for complex expressions
        try:
            from .pandas_engine import pandas_mutate
        except ImportError:
            import pandas_engine
            return pandas_engine.pandas_mutate(df, target, expr, mode, overwrite)
        return pandas_mutate(df, target, expr, mode, overwrite)
    
    finally:
        if spark:
            try:
                spark.stop()
            except Exception:
                pass