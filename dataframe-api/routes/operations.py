"""
Advanced DataFrame operations API routes
"""
import json
import os
import io
import pandas as pd
import numpy as np
from flask import Blueprint, request, jsonify

from utils.redis_client import redis_client
from utils.helpers import df_to_records_json_safe, notify_ntfy
from operations.dataframe_ops import _load_df_from_cache, _save_df_to_cache, _unique_name

operations_bp = Blueprint('operations', __name__)


@operations_bp.route('/api/ops/compare', methods=['POST'])
def op_compare():
    """Compare two DataFrames"""
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
                # notify
                notify_ntfy(
                    title=f"DF Compare: {n1} vs {n2}",
                    message="schema mismatch",
                    tags=['compare', 'schema', 'warn']
                )
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
                notify_ntfy(
                    title=f"DF Compare: {n1} vs {n2}",
                    message=f"row count mismatch: {count1} != {count2}",
                    tags=['compare', 'row_count', 'warn']
                )
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
                notify_ntfy(
                    title=f"DF Compare: {n1} vs {n2}",
                    message="identical",
                    tags=['compare', 'identical', 'ok']
                )
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
            notify_ntfy(
                title=f"DF Compare: {n1} vs {n2}",
                message=f"data mismatch: left_unique={int(c1)}, right_unique={int(c2)}; created={created}",
                tags=['compare', 'mismatch']
            )
            return jsonify({'success': True, 'identical': False, 'result_type': 'data_mismatch', 'left_unique': int(c1), 'right_unique': int(c2), 'created': created, 'note': f'Spark comparator used; master={master_url}'})
        except Exception as e:
            # Log Spark failure and fall back to pandas
            spark_error = str(e)

        # Schema comparison
        schema_match = list(df1.columns) == list(df2.columns) and list(map(str, df1.dtypes)) == list(map(str, df2.dtypes))
        if not schema_match:
            notify_ntfy(
                title=f"DF Compare: {n1} vs {n2}",
                message="schema mismatch (pandas)",
                tags=['compare', 'schema', 'warn']
            )
            return jsonify({'success': True, 'identical': False, 'result_type': 'schema_mismatch', 'note': 'pandas fallback used', 'spark_error': spark_error})

        # Row count check
        if len(df1) != len(df2):
            notify_ntfy(
                title=f"DF Compare: {n1} vs {n2}",
                message=f"row count mismatch: {len(df1)} != {len(df2)} (pandas)",
                tags=['compare', 'row_count', 'warn']
            )
            return jsonify({'success': True, 'identical': False, 'result_type': 'row_count_mismatch', 'note': 'pandas fallback used', 'spark_error': spark_error})

        # Data comparison (order independent): use indicator merge on all columns
        cols = list(df1.columns)
        merged_l = df1.merge(df2, how='outer', on=cols, indicator=True)
        left_only = merged_l[merged_l['_merge'] == 'left_only'][cols]
        right_only = merged_l[merged_l['_merge'] == 'right_only'][cols]
        c1 = int(len(left_only)); c2 = int(len(right_only))
        created = []
        if c1 == 0 and c2 == 0:
            notify_ntfy(
                title=f"DF Compare: {n1} vs {n2}",
                message="identical (pandas)",
                tags=['compare', 'identical', 'ok']
            )
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

        notify_ntfy(
            title=f"DF Compare: {n1} vs {n2}",
            message=f"data mismatch (pandas): left_unique={c1}, right_unique={c2}; created={created}",
            tags=['compare', 'mismatch']
        )
        return jsonify({'success': True, 'identical': False, 'result_type': 'data_mismatch', 'left_unique': c1, 'right_unique': c2, 'created': created, 'note': 'pandas fallback used', 'spark_error': spark_error})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@operations_bp.route('/api/ops/merge', methods=['POST'])
def op_merge():
    """Merge multiple DataFrames"""
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


@operations_bp.route('/api/ops/pivot', methods=['POST'])
def op_pivot():
    """Pivot DataFrame wider or longer"""
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


@operations_bp.route('/api/ops/filter', methods=['POST'])
def op_filter():
    """Filter DataFrame rows based on conditions"""
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
        detail = (' ' + combine + ' ').join(desc_parts) if desc_parts else 'no conditions'
        meta = _save_df_to_cache(out_name, filtered, description=f"Filter: {detail}", source='ops:filter')
        return jsonify({'success': True, 'name': out_name, 'metadata': meta})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@operations_bp.route('/api/ops/groupby', methods=['POST'])
def op_groupby():
    """Group DataFrame by columns and aggregate"""
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
        for c in by:
            if c not in df.columns:
                return jsonify({'success': False, 'error': f'Group-by column {c} not found'}), 400
        if aggs:
            grouped = df.groupby(by).agg(aggs).reset_index()
        else:
            grouped = df.groupby(by).size().reset_index(name='count')
        if isinstance(grouped.columns, pd.MultiIndex):
            grouped.columns = ['__'.join([str(x) for x in tup if str(x) != '']) for tup in grouped.columns.to_flat_index()]
        base = f"{name}__groupby_{'-'.join(by)}"
        out_name = _unique_name(base)
        by_str = ','.join(by)
        aggs_str = aggs if aggs else 'count'
        meta = _save_df_to_cache(out_name, grouped, description=f"Group-by columns={by_str}; aggs={aggs_str}", source='ops:groupby')
        return jsonify({'success': True, 'name': out_name, 'metadata': meta})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@operations_bp.route('/api/ops/select', methods=['POST'])
def op_select():
    """Select or exclude columns from DataFrame"""
    try:
        p = request.get_json(force=True)
        name = p.get('name')
        cols = p.get('columns') or []
        exclude = bool(p.get('exclude') or False)
        if not name:
            return jsonify({'success': False, 'error': 'name is required'}), 400
        if not isinstance(cols, list) or len(cols) == 0:
            return jsonify({'success': False, 'error': 'columns (non-empty list) is required'}), 400
        df = _load_df_from_cache(name)
        missing = [c for c in cols if c not in df.columns]
        if missing:
            return jsonify({'success': False, 'error': f'Columns not found: {", ".join(missing)}'}), 400
        if exclude:
            keep_cols = [c for c in df.columns if c not in cols]
            projected = df[keep_cols].copy()
            base = f"{name}__drop_{'-'.join(cols)}"
            desc = f"Drop columns={','.join(cols)}"
        else:
            projected = df[cols].copy()
            base = f"{name}__select_{'-'.join(cols)}"
            desc = f"Select columns={','.join(cols)}"
        out_name = _unique_name(base)
        meta = _save_df_to_cache(out_name, projected, description=desc, source='ops:select')
        return jsonify({'success': True, 'name': out_name, 'metadata': meta})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@operations_bp.route('/api/ops/rename', methods=['POST'])
def op_rename():
    """Rename columns in DataFrame"""
    try:
        p = request.get_json(force=True)
        name = p.get('name')
        rename_map = p.get('map') or p.get('rename') or p.get('columns') or {}
        if not name:
            return jsonify({'success': False, 'error': 'name is required'}), 400
        if not isinstance(rename_map, dict) or not rename_map:
            return jsonify({'success': False, 'error': 'map (object of old->new names) is required'}), 400
        df = _load_df_from_cache(name)
        missing = [old for old in rename_map.keys() if old not in df.columns]
        if missing:
            return jsonify({'success': False, 'error': f'Columns to rename not found: {", ".join(missing)}'}), 400
        new_cols = list(df.columns)
        for old, new in rename_map.items():
            if not isinstance(new, str) or not new:
                return jsonify({'success': False, 'error': f'Invalid new name for column {old}'}), 400
            idx = new_cols.index(old)
            new_cols[idx] = new
        if len(set(new_cols)) != len(new_cols):
            return jsonify({'success': False, 'error': 'Rename would cause duplicate column names'}), 400
        renamed = df.rename(columns=rename_map)
        base = f"{name}__rename"
        out_name = _unique_name(base)
        meta = _save_df_to_cache(out_name, renamed, description=f"Rename columns: {rename_map}", source='ops:rename')
        return jsonify({'success': True, 'name': out_name, 'metadata': meta})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@operations_bp.route('/api/ops/datetime', methods=['POST'])
def op_datetime():
    """Parse or derive datetime columns"""
    try:
        p = request.get_json(force=True)
        name = p.get('name')
        if not name:
            return jsonify({'success': False, 'error': 'name is required'}), 400
        action = (p.get('action') or 'parse').lower()
        source = p.get('source') or p.get('column') or p.get('col')
        if not source:
            return jsonify({'success': False, 'error': 'source column is required'}), 400
        df = _load_df_from_cache(name)
        if source not in df.columns:
            return jsonify({'success': False, 'error': f'Column {source} not found'}), 400
        description = ''
        base_out_name = f"{name}__datetime"
        if action == 'parse':
            fmt = p.get('format') or p.get('fmt')
            errors = (p.get('errors') or 'coerce').lower()
            target = p.get('target') or p.get('to')
            overwrite = bool(p.get('overwrite'))
            dseries = pd.to_datetime(df[source], format=fmt, errors=errors)
            out_df = df.copy()
            if target and target != source:
                if (target in out_df.columns) and not overwrite:
                    return jsonify({'success': False, 'error': f'target column {target} already exists'}), 400
                out_df[target] = dseries
                description = f"Parse date: {source} -> {target} (format={fmt or 'auto'})"
                base_out_name = f"{name}__parse_{source}_to_{target}"
            else:
                out_df[source] = dseries
                description = f"Parse date: overwrite {source} (format={fmt or 'auto'})"
                base_out_name = f"{name}__parse_{source}"
        elif action == 'derive':
            col = df[source]
            if not pd.api.types.is_datetime64_any_dtype(col):
                col = pd.to_datetime(col, errors='coerce')
            out_df = df.copy()
            opts = p.get('outputs') or {}
            want_year = bool(opts.get('year') if 'year' in opts else p.get('year') or True) if opts or ('year' in p) else True
            want_month = bool(opts.get('month') if 'month' in opts else p.get('month') or True) if opts or ('month' in p) else True
            want_day = bool(opts.get('day') if 'day' in opts else p.get('day') or True) if opts or ('day' in p) else True
            want_year_month = bool(opts.get('year_month') if 'year_month' in opts else p.get('year_month') or True) if opts or ('year_month' in p) else True
            names = p.get('names') or {}
            cname_year = (names.get('year') if isinstance(names, dict) else None) or 'year'
            cname_month = (names.get('month') if isinstance(names, dict) else None) or 'month'
            cname_day = (names.get('day') if isinstance(names, dict) else None) or 'day'
            cname_year_month = (names.get('year_month') if isinstance(names, dict) else None) or 'year_month'
            month_style = (p.get('month_style') or opts.get('month_style') or 'short').lower()
            overwrite = bool(p.get('overwrite') or False)
            for cname, flag in [(cname_year, want_year), (cname_month, want_month), (cname_day, want_day), (cname_year_month, want_year_month)]:
                if not flag:
                    continue
                if (cname in out_df.columns) and not overwrite:
                    return jsonify({'success': False, 'error': f'Output column {cname} already exists'}), 400
            if want_year:
                out_df[cname_year] = col.dt.year
            if want_month:
                if month_style == 'long':
                    out_df[cname_month] = col.dt.month_name()
                elif month_style == 'num':
                    out_df[cname_month] = col.dt.month
                elif month_style in ('short_lower', 'abbr_lower', 'short-lower'):
                    out_df[cname_month] = col.dt.strftime('%b').str.lower()
                else:
                    out_df[cname_month] = col.dt.strftime('%b')
            if want_day:
                out_df[cname_day] = col.dt.day
            if want_year_month:
                if month_style in ('short_lower', 'abbr_lower', 'short-lower'):
                    out_df[cname_year_month] = col.dt.strftime('%Y-%b').str.lower()
                elif month_style == 'long':
                    out_df[cname_year_month] = col.dt.strftime('%Y-') + col.dt.month_name()
                elif month_style == 'num':
                    out_df[cname_year_month] = col.dt.strftime('%Y-') + col.dt.month.astype('Int64').map(lambda m: f"{m:02d}" if pd.notna(m) else None)
                else:
                    out_df[cname_year_month] = col.dt.strftime('%Y-%b')
            description = f"Derive from {source}: year={want_year}, month={want_month}({month_style}), day={want_day}, year_month={want_year_month}"
            base_out_name = f"{name}__derive_{source}"
        else:
            return jsonify({'success': False, 'error': 'action must be parse or derive'}), 400
        out_name = _unique_name(base_out_name)
        meta = _save_df_to_cache(out_name, out_df, description=description, source='ops:datetime')
        return jsonify({'success': True, 'name': out_name, 'metadata': meta})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@operations_bp.route('/api/ops/mutate', methods=['POST'])
def op_mutate():
    """Create new columns with expressions"""
    try:
        p = request.get_json(force=True)
        name = p.get('name')
        target = (p.get('target') or p.get('to') or '').strip()
        expr = p.get('expr') or p.get('expression')
        mode = (p.get('mode') or 'vector').lower()  # 'vector' or 'row'
        overwrite = bool(p.get('overwrite') or False)
        if not name:
            return jsonify({'success': False, 'error': 'name is required'}), 400
        if not target:
            return jsonify({'success': False, 'error': 'target is required'}), 400
        if not isinstance(expr, str) or not expr.strip():
            return jsonify({'success': False, 'error': 'expr (string) is required'}), 400
        df = _load_df_from_cache(name)
        if (target in df.columns) and not overwrite:
            return jsonify({'success': False, 'error': f'target column {target} already exists'}), 400

        # Safe-ish eval: expose only whitelisted symbols
        safe_builtins = {
            'abs': abs, 'min': min, 'max': max, 'round': round,
            'int': int, 'float': float, 'str': str, 'bool': bool, 'len': len,
        }
        def _safe_eval(expression: str, local_ctx: dict):
            code = compile(expression, '<mutate-expr>', 'eval')
            return eval(code, {'__builtins__': {}}, {**safe_builtins, **local_ctx})

        base_locals = {'pd': pd, 'np': np, 'df': df}
        base_locals['col'] = lambda c: df[c]

        if mode not in ('vector', 'row'):
            return jsonify({'success': False, 'error': 'mode must be vector or row'}), 400

        if mode == 'vector':
            try:
                result = _safe_eval(expr, base_locals)
            except Exception as e:
                return jsonify({'success': False, 'error': f'eval error: {e}'}), 400
        else:
            try:
                result = df.apply(lambda r: _safe_eval(expr, {**base_locals, 'r': r}), axis=1)
            except Exception as e:
                return jsonify({'success': False, 'error': f'row-eval error: {e}'}), 400

        out_df = df.copy()
        # Pandas will align Series by index; scalars will broadcast
        out_df[target] = result

        out_name = _unique_name(f"{name}__mutate_{target}")
        meta = _save_df_to_cache(out_name, out_df, description=f"Mutate {target} via expr ({mode})", source='ops:mutate')
        return jsonify({'success': True, 'name': out_name, 'metadata': meta})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# Spark-based operations endpoints
from operations.spark_ops import (
    spark_select_op, spark_filter_op, spark_groupby_op, spark_merge_op,
    spark_rename_op, spark_pivot_op, spark_datetime_op, spark_mutate_op
)


@operations_bp.route('/api/ops/spark/select', methods=['POST'])
def op_spark_select():
    """Spark-based select or exclude columns from DataFrame"""
    try:
        p = request.get_json(force=True)
        name = p.get('name')
        cols = p.get('columns') or []
        exclude = bool(p.get('exclude') or False)
        
        if not name:
            return jsonify({'success': False, 'error': 'name is required'}), 400
        if not isinstance(cols, list) or len(cols) == 0:
            return jsonify({'success': False, 'error': 'columns (non-empty list) is required'}), 400
        
        result = spark_select_op(name, cols, exclude)
        if result['success']:
            return jsonify(result)
        else:
            return jsonify(result), 500
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@operations_bp.route('/api/ops/spark/filter', methods=['POST'])
def op_spark_filter():
    """Spark-based filter rows from DataFrame"""
    try:
        p = request.get_json(force=True)
        name = p.get('name')
        filters = p.get('filters') or []
        combine = p.get('combine', 'and')
        
        if not name:
            return jsonify({'success': False, 'error': 'name is required'}), 400
        if not filters:
            return jsonify({'success': False, 'error': 'filters are required'}), 400
        
        result = spark_filter_op(name, filters, combine)
        if result['success']:
            return jsonify(result)
        else:
            return jsonify(result), 500
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@operations_bp.route('/api/ops/spark/groupby', methods=['POST'])
def op_spark_groupby():
    """Spark-based groupby and aggregation"""
    try:
        p = request.get_json(force=True)
        name = p.get('name')
        by = p.get('by') or []
        aggs = p.get('aggs') or {}
        
        if not name:
            return jsonify({'success': False, 'error': 'name is required'}), 400
        if not by:
            return jsonify({'success': False, 'error': 'by (groupby columns) is required'}), 400
        # Note: aggs can be empty - will default to count like pandas version
        
        result = spark_groupby_op(name, by, aggs)
        if result['success']:
            return jsonify(result)
        else:
            return jsonify(result), 500
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@operations_bp.route('/api/ops/spark/merge', methods=['POST'])
def op_spark_merge():
    """Spark-based merge/join DataFrames"""
    try:
        p = request.get_json(force=True)
        names = p.get('names') or []
        keys = p.get('keys') or []
        how = p.get('how', 'inner')
        
        if len(names) != 2:
            return jsonify({'success': False, 'error': 'names must contain exactly 2 dataframes'}), 400
        if not keys:
            return jsonify({'success': False, 'error': 'keys (join columns) are required'}), 400
        
        result = spark_merge_op(names, keys, how)
        if result['success']:
            return jsonify(result)
        else:
            return jsonify(result), 500
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@operations_bp.route('/api/ops/spark/rename', methods=['POST'])
def op_spark_rename():
    """Spark-based rename columns in DataFrame"""
    try:
        p = request.get_json(force=True)
        name = p.get('name')
        rename_map = p.get('map') or p.get('rename') or p.get('columns') or {}
        
        if not name:
            return jsonify({'success': False, 'error': 'name is required'}), 400
        if not rename_map:
            return jsonify({'success': False, 'error': 'rename mapping is required'}), 400
        
        result = spark_rename_op(name, rename_map)
        if result['success']:
            return jsonify(result)
        else:
            return jsonify(result), 500
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@operations_bp.route('/api/ops/spark/pivot', methods=['POST'])
def op_spark_pivot():
    """Spark-based pivot operation"""
    try:
        p = request.get_json(force=True)
        name = p.get('name')
        
        if not name:
            return jsonify({'success': False, 'error': 'name is required'}), 400
        
        # Remove name from payload and pass the rest as pivot config
        pivot_config = {k: v for k, v in p.items() if k != 'name'}
        
        result = spark_pivot_op(name, pivot_config)
        if result['success']:
            return jsonify(result)
        else:
            return jsonify(result), 500
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@operations_bp.route('/api/ops/spark/datetime', methods=['POST'])
def op_spark_datetime():
    """Spark-based datetime operations"""
    try:
        p = request.get_json(force=True)
        name = p.get('name')
        column = p.get('column')
        operation = p.get('operation') or {}
        
        if not name:
            return jsonify({'success': False, 'error': 'name is required'}), 400
        if not column:
            return jsonify({'success': False, 'error': 'column is required'}), 400
        
        result = spark_datetime_op(name, column, operation)
        if result['success']:
            return jsonify(result)
        else:
            return jsonify(result), 500
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@operations_bp.route('/api/ops/spark/mutate', methods=['POST'])
def op_spark_mutate():
    """Spark-based create new columns with expressions"""
    try:
        p = request.get_json(force=True)
        name = p.get('name')
        target = (p.get('target') or p.get('to') or '').strip()
        expr = p.get('expr') or p.get('expression')
        mode = (p.get('mode') or 'vector').lower()
        overwrite = bool(p.get('overwrite') or False)
        
        if not name:
            return jsonify({'success': False, 'error': 'name is required'}), 400
        if not target:
            return jsonify({'success': False, 'error': 'target is required'}), 400
        if not isinstance(expr, str) or not expr.strip():
            return jsonify({'success': False, 'error': 'expr (string) is required'}), 400
        
        result = spark_mutate_op(name, target, expr, mode, overwrite)
        if result['success']:
            return jsonify(result)
        else:
            return jsonify(result), 500
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500