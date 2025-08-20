"""
Pipeline execution engine
"""
import json
import io
import os
import pandas as pd
import numpy as np
from operations.dataframe_ops import _load_df_from_cache, _save_df_to_cache, _unique_name
from utils.helpers import df_to_records_json_safe


def _apply_op(df_curr: pd.DataFrame | None, step: dict) -> tuple[pd.DataFrame, str]:
    """Apply a single pipeline operation step"""
    op = (step.get('op') or step.get('type') or '').lower()
    params = step.get('params') or {}

    # load
    if op == 'load':
        name = params.get('name') or step.get('name')
        if not name:
            raise ValueError('load: name is required')
        df = _load_df_from_cache(name)
        return df, f'load {name}'

    # merge
    if op == 'merge':
        how = (params.get('how') or 'inner').lower()
        keys = params.get('keys') or []
        if not keys:
            raise ValueError('merge: keys are required')
        names: list[str] = params.get('names') or []
        others: list[str] = params.get('with') or params.get('others') or []
        
        # Debug logging
        print(f"[DEBUG] merge operation: how={how}, keys={keys}, names={names}, others={others}")
        
        # Validate that keys is a list of strings
        if isinstance(keys, str):
            keys = [k.strip() for k in keys.split(',') if k.strip()]
        if not isinstance(keys, list) or not keys:
            raise ValueError('merge: keys must be a non-empty list or comma-separated string')
            
        if names:
            if len(names) < 2:
                raise ValueError('merge: names must include at least two')
            
            # Pre-validate that all required dataframes exist
            from utils.redis_client import redis_client
            missing_dfs = []
            for name in names:
                exists = redis_client.exists(f"df:{name}")
                print(f"[DEBUG] checking dataframe '{name}': exists={exists}")
                if not exists:
                    missing_dfs.append(name)
            if missing_dfs:
                raise ValueError(f'merge: dataframes not found: {", ".join(missing_dfs)}')
            
            try:
                df = _load_df_from_cache(names[0])
                print(f"[DEBUG] loaded dataframe '{names[0]}': shape={df.shape}, columns={list(df.columns)}")
            except ValueError as e:
                raise ValueError(f'merge: failed to load dataframe "{names[0]}": {str(e)}')
            for nm in names[1:]:
                try:
                    d2 = _load_df_from_cache(nm)
                    print(f"[DEBUG] loaded dataframe '{nm}': shape={d2.shape}, columns={list(d2.columns)}")
                except ValueError as e:
                    raise ValueError(f'merge: failed to load dataframe "{nm}": {str(e)}')
                # Validate that merge keys exist in both dataframes
                missing_keys_df1 = [k for k in keys if k not in df.columns]
                missing_keys_df2 = [k for k in keys if k not in d2.columns]
                if missing_keys_df1:
                    raise ValueError(f'merge: keys {missing_keys_df1} not found in dataframe "{names[0] if nm == names[1] else "previous result"}"')
                if missing_keys_df2:
                    raise ValueError(f'merge: keys {missing_keys_df2} not found in dataframe "{nm}"')
                print(f"[DEBUG] merging dataframes on keys {keys} with how='{how}'")
                df = df.merge(d2, on=keys, how=how)
                print(f"[DEBUG] merge result: shape={df.shape}")
            return df, f'merge names={names} how={how} keys={keys}'
        if df_curr is None:
            raise ValueError('merge: no current dataframe; add a load step or use params.names')
        df = df_curr.copy()
        
        # Validate that merge keys exist in current dataframe
        missing_keys_curr = [k for k in keys if k not in df.columns]
        if missing_keys_curr:
            raise ValueError(f'merge: keys {missing_keys_curr} not found in current dataframe')
            
        for nm in others:
            try:
                d2 = _load_df_from_cache(nm)
            except ValueError as e:
                raise ValueError(f'merge: failed to load dataframe "{nm}": {str(e)}')
            # Validate that merge keys exist in the other dataframe
            missing_keys_other = [k for k in keys if k not in d2.columns]
            if missing_keys_other:
                raise ValueError(f'merge: keys {missing_keys_other} not found in dataframe "{nm}"')
            df = df.merge(d2, on=keys, how=how)
        return df, f'merge with={others} how={how} keys={keys}'

    # filter
    if op == 'filter':
        if df_curr is None:
            raise ValueError('filter: no current dataframe; add a load step first')
        df = df_curr.copy()
        conditions = params.get('filters') or []
        combine = (params.get('combine') or 'and').lower()
        if combine not in ['and', 'or']:
            raise ValueError('filter: combine must be and/or')
        mask = None
        desc_parts: list[str] = []
        for cond in conditions:
            col = cond.get('col'); opx = (cond.get('op') or 'eq').lower(); val = cond.get('value')
            if col not in df.columns:
                raise ValueError(f'filter: column {col} not found')
            s = df[col]
            if opx == 'eq': m = s == val
            elif opx == 'ne': m = s != val
            elif opx == 'lt': m = s < val
            elif opx == 'lte': m = s <= val
            elif opx == 'gt': m = s > val
            elif opx == 'gte': m = s >= val
            elif opx == 'in':
                vals = val
                if isinstance(val, str):
                    v = val.strip()
                    try:
                        parsed = json.loads(v)
                        vals = parsed if isinstance(parsed, list) else [val]
                    except Exception:
                        vals = [x.strip() for x in v.split(',') if x.strip()]
                if not isinstance(vals, list):
                    vals = [vals]
                m = s.isin(vals); val = vals
            elif opx == 'nin':
                vals = val
                if isinstance(val, str):
                    v = val.strip()
                    try:
                        parsed = json.loads(v)
                        vals = parsed if isinstance(parsed, list) else [val]
                    except Exception:
                        vals = [x.strip() for x in v.split(',') if x.strip()]
                if not isinstance(vals, list):
                    vals = [vals]
                m = ~s.isin(vals); val = vals
            elif opx == 'contains': m = s.astype('string').str.contains(str(val), na=False)
            elif opx == 'startswith': m = s.astype('string').str.startswith(str(val), na=False)
            elif opx == 'endswith': m = s.astype('string').str.endswith(str(val), na=False)
            elif opx == 'isnull': m = s.isna(); val = None
            elif opx == 'notnull': m = s.notna(); val = None
            else: raise ValueError(f'filter: unsupported op {opx}')
            vstr = '' if val is None else (','.join(map(str, val)) if isinstance(val, list) else str(val))
            desc_parts.append(f"{col} {opx}{(' '+vstr) if vstr else ''}")
            mask = m if mask is None else (mask & m if combine == 'and' else mask | m)
        out = df[mask] if mask is not None else df
        return out, f"filter {(' '+combine+' ').join(desc_parts) if desc_parts else 'no conditions'}"

    # groupby
    if op == 'groupby':
        if df_curr is None:
            raise ValueError('groupby: no current dataframe; add a load step first')
        by = params.get('by') or []
        aggs = params.get('aggs') or {}
        if not by:
            raise ValueError('groupby: by is required')
        for c in by:
            if c not in df_curr.columns:
                raise ValueError(f'groupby: column {c} not found')
        if aggs:
            grouped = df_curr.groupby(by).agg(aggs).reset_index()
        else:
            grouped = df_curr.groupby(by).size().reset_index(name='count')
        if isinstance(grouped.columns, pd.MultiIndex):
            grouped.columns = ['__'.join([str(x) for x in tup if str(x) != '']) for tup in grouped.columns.to_flat_index()]
        return grouped, f"groupby by={by} aggs={aggs or 'count'}"

    # select
    if op == 'select':
        if df_curr is None:
            raise ValueError('select: no current dataframe; add a load step first')
        cols = params.get('columns') or []
        exclude = bool(params.get('exclude') or False)
        if not cols:
            raise ValueError('select: columns are required')
        missing = [c for c in cols if c not in df_curr.columns]
        if missing:
            raise ValueError(f'select: columns not found: {", ".join(missing)}')
        if exclude:
            keep_cols = [c for c in df_curr.columns if c not in cols]
            return df_curr[keep_cols].copy(), f"drop columns={cols}"
        return df_curr[cols].copy(), f"select columns={cols}"

    # rename
    if op == 'rename':
        if df_curr is None:
            raise ValueError('rename: no current dataframe; add a load step first')
        mapping = params.get('map') or params.get('rename') or params.get('columns') or {}
        if not isinstance(mapping, dict) or not mapping:
            raise ValueError('rename: map is required')
        missing = [old for old in mapping.keys() if old not in df_curr.columns]
        if missing:
            raise ValueError(f'rename: columns to rename not found: {", ".join(missing)}')
        new_cols = list(df_curr.columns)
        for old, new in mapping.items():
            if not isinstance(new, str) or not new:
                raise ValueError(f'rename: invalid new name for {old}')
            idx = new_cols.index(old)
            new_cols[idx] = new
        if len(set(new_cols)) != len(new_cols):
            raise ValueError('rename: would cause duplicate columns')
        return df_curr.rename(columns=mapping), f"rename {mapping}"

    # mutate
    if op == 'mutate':
        # df source
        df = df_curr if df_curr is not None else _load_df_from_cache(params.get('name'))
        if df is None:
            raise ValueError('mutate: add a load step or set params.name')
        target = (params.get('target') or params.get('to') or '').strip()
        expr = params.get('expr') or params.get('expression')
        mode = params.get('mode') or 'vector'
        overwrite = bool(params.get('overwrite') or False)
        if not target:
            raise ValueError('mutate: target is required')
        if not isinstance(expr, str) or not expr.strip():
            raise ValueError('mutate: expr is required')
        if (target in df.columns) and not overwrite:
            raise ValueError(f'mutate: target column {target} exists')
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
            raise ValueError('mutate: mode must be vector or row')
        if mode == 'vector':
            result = _safe_eval(expr, base_locals)
        else:
            result = df.apply(lambda r: _safe_eval(expr, {**base_locals, 'r': r}), axis=1)
        out = df.copy()
        out[target] = result
        return out, f"mutate {target} ({mode})"

    # pivot
    if op == 'pivot':
        mode = (params.get('mode') or 'wider').lower()
        df = df_curr if df_curr is not None else _load_df_from_cache(params.get('name'))
        if df is None:
            raise ValueError('pivot: add a load step or provide params.name')
        if mode == 'wider':
            index = params.get('index') or []
            names_from = params.get('names_from')
            values_from = params.get('values_from')
            aggfunc = params.get('aggfunc') or 'first'
            if not names_from or not values_from:
                raise ValueError('pivot wider: names_from and values_from are required')
            if isinstance(values_from, str):
                values_from = [values_from]
            pvt = pd.pivot_table(df, index=index or None, columns=names_from, values=values_from, aggfunc=aggfunc)
            if isinstance(pvt.columns, pd.MultiIndex):
                pvt.columns = ['__'.join([str(x) for x in tup if str(x) != '']) for tup in pvt.columns.to_flat_index()]
            pvt = pvt.reset_index()
            return pvt, f"pivot wider names_from={names_from} values_from={values_from}"
        elif mode == 'longer':
            id_vars = params.get('id_vars') or []
            value_vars = params.get('value_vars') or []
            var_name = params.get('var_name') or 'variable'
            value_name = params.get('value_name') or 'value'
            if not value_vars:
                raise ValueError('pivot longer: value_vars required')
            melted = pd.melt(df, id_vars=id_vars or None, value_vars=value_vars, var_name=var_name, value_name=value_name)
            return melted, f"pivot longer value_vars={value_vars}"
        else:
            raise ValueError('pivot: mode must be wider or longer')

    # compare (mismatch rows)
    if op == 'compare':
        if df_curr is None:
            raise ValueError('compare: no current dataframe; add a load step first')
        other_name = params.get('name') or params.get('other')
        action = (params.get('on') or params.get('action') or 'mismatch').lower()
        if not other_name:
            raise ValueError('compare: params.name (other) is required')
        df_other = _load_df_from_cache(other_name)
        schema_match = list(df_curr.columns) == list(df_other.columns) and list(map(str, df_curr.dtypes)) == list(map(str, df_other.dtypes))
        if not schema_match:
            identical = False
        else:
            cols = list(df_curr.columns)
            merged = df_curr.merge(df_other, how='outer', on=cols, indicator=True)
            left_only = merged[merged['_merge'] == 'left_only'][cols]
            right_only = merged[merged['_merge'] == 'right_only'][cols]
            identical = len(left_only) == 0 and len(right_only) == 0
        if action == 'identical':
            return (df_curr if identical else pd.DataFrame({'__note__': ['not identical']}), 'compare: identical check')
        if not schema_match:
            return pd.DataFrame({'__note__': ['schema mismatch']}), 'compare: schema mismatch'
        cols = list(df_curr.columns)
        merged = df_curr.merge(df_other, how='outer', on=cols, indicator=True)
        mism_left = merged[merged['_merge'] == 'left_only'][cols].copy(); mism_left['__side__'] = 'left_only'
        mism_right = merged[merged['_merge'] == 'right_only'][cols].copy(); mism_right['__side__'] = 'right_only'
        out = pd.concat([mism_left, mism_right], ignore_index=True)
        return out, 'compare: mismatch rows'

    # datetime
    if op == 'datetime':
        action = (params.get('action') or 'parse').lower()
        source = params.get('source') or params.get('column') or params.get('col')
        if df_curr is None:
            nm = params.get('name')
            if not nm:
                raise ValueError('datetime: no current dataframe; add a load step or set params.name')
            df = _load_df_from_cache(nm)
        else:
            df = df_curr
        if source is None or source not in df.columns:
            raise ValueError('datetime: source column is required and must exist')
        if action == 'parse':
            fmt = params.get('format') or params.get('fmt')
            errors = (params.get('errors') or 'coerce').lower()
            target = params.get('target') or params.get('to')
            overwrite = bool(params.get('overwrite') or False)
            dseries = pd.to_datetime(df[source], format=fmt, errors=errors)
            out = df.copy()
            if target and target != source:
                if (target in out.columns) and not overwrite:
                    raise ValueError(f'datetime parse: target {target} exists')
                out[target] = dseries
                return out, f"datetime parse {source}->{target} ({fmt or 'auto'})"
            out[source] = dseries
            return out, f"datetime parse overwrite {source} ({fmt or 'auto'})"
        elif action == 'derive':
            col = df[source]
            if not pd.api.types.is_datetime64_any_dtype(col):
                col = pd.to_datetime(col, errors='coerce')
            out = df.copy()
            opts = params.get('outputs') or {}
            want_year = bool(opts.get('year') if 'year' in opts else params.get('year') or True) if opts or ('year' in params) else True
            want_month = bool(opts.get('month') if 'month' in opts else params.get('month') or True) if opts or ('month' in params) else True
            want_day = bool(opts.get('day') if 'day' in opts else params.get('day') or True) if opts or ('day' in params) else True
            want_year_month = bool(opts.get('year_month') if 'year_month' in opts else params.get('year_month') or True) if opts or ('year_month' in params) else True
            names = params.get('names') or {}
            cname_year = (names.get('year') if isinstance(names, dict) else None) or 'year'
            cname_month = (names.get('month') if isinstance(names, dict) else None) or 'month'
            cname_day = (names.get('day') if isinstance(names, dict) else None) or 'day'
            cname_year_month = (names.get('year_month') if isinstance(names, dict) else None) or 'year_month'
            month_style = (params.get('month_style') or 'short').lower()
            overwrite = bool(params.get('overwrite') or False)
            for cname, flag in [(cname_year, want_year), (cname_month, want_month), (cname_day, want_day), (cname_year_month, want_year_month)]:
                if not flag:
                    continue
                if (cname in out.columns) and not overwrite:
                    raise ValueError(f'datetime derive: output column {cname} exists')
            if want_year:
                out[cname_year] = col.dt.year
            if want_month:
                if month_style == 'long':
                    out[cname_month] = col.dt.month_name()
                elif month_style == 'num':
                    out[cname_month] = col.dt.month
                elif month_style in ('short_lower', 'abbr_lower', 'short-lower'):
                    out[cname_month] = col.dt.strftime('%b').str.lower()
                else:
                    out[cname_month] = col.dt.strftime('%b')
            if want_day:
                out[cname_day] = col.dt.day
            if want_year_month:
                if month_style in ('short_lower', 'abbr_lower', 'short-lower'):
                    out[cname_year_month] = col.dt.strftime('%Y-%b').str.lower()
                elif month_style == 'long':
                    out[cname_year_month] = col.dt.strftime('%Y-') + col.dt.month_name()
                elif month_style == 'num':
                    out[cname_year_month] = col.dt.strftime('%Y-') + col.dt.month.astype('Int64').map(lambda m: f"{m:02d}" if pd.notna(m) else None)
                else:
                    out[cname_year_month] = col.dt.strftime('%Y-%b')
            return out, f"datetime derive from {source}"
        else:
            raise ValueError('datetime: action must be parse or derive')

    # chain_pipeline - execute another pipeline at this point
    if op == 'chain_pipeline':
        pipeline_name = params.get('pipeline') or params.get('name')
        if not pipeline_name:
            raise ValueError('chain_pipeline: pipeline name is required')
        
        if df_curr is None:
            raise ValueError('chain_pipeline: no current dataframe; add a load step first')
        
        print(f"[DEBUG] chain_pipeline: executing pipeline '{pipeline_name}' with input shape={df_curr.shape}")
        
        # Load the chained pipeline
        from utils.redis_client import redis_client
        import json
        
        key = f'pipeline:{pipeline_name}'
        if not redis_client.exists(key):
            raise ValueError(f'chain_pipeline: pipeline {pipeline_name} not found')
        
        obj = json.loads(redis_client.get(key))
        chained_steps = obj.get('steps') or []
        
        if not chained_steps:
            raise ValueError(f'chain_pipeline: pipeline {pipeline_name} has no steps')
        
        print(f"[DEBUG] chain_pipeline: found {len(chained_steps)} steps to execute")
        
        # Execute the chained pipeline with current dataframe as input
        current = df_curr.copy()  # Make a copy to avoid modifying original
        for i, chained_step in enumerate(chained_steps):
            print(f"[DEBUG] chain_pipeline: executing step {i+1}: {chained_step.get('op', 'unknown')}")
            current, step_desc = _apply_op(current, chained_step)
            print(f"[DEBUG] chain_pipeline: step {i+1} result: {step_desc}, shape={current.shape}")
        
        # Save the chained pipeline result as a named dataframe
        # This allows subsequent steps to access it via merge operations
        result_name = f'chained_{pipeline_name}'
        print(f"[DEBUG] chain_pipeline: saving result as '{result_name}' with shape={current.shape}")
        try:
            _save_df_to_cache(result_name, current, f'Result from chained pipeline: {pipeline_name}')
            print(f"[DEBUG] chain_pipeline: successfully saved '{result_name}' to cache")
        except Exception as e:
            raise ValueError(f'chain_pipeline: failed to save result "{result_name}": {str(e)}')
        
        # Return the original dataframe to preserve the main pipeline flow
        # The chained result is now available as a named dataframe for merge operations
        return df_curr, f'chain_pipeline: {pipeline_name} (saved as {result_name}, applied {len(chained_steps)} steps)'

    raise ValueError(f'Unsupported op: {op}')