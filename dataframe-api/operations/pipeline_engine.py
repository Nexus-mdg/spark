"""
Pipeline execution engine
"""

import pandas as pd

from operations.dataframe_ops import _load_df_from_cache, _save_df_to_cache
from operations.engine_router import (
    route_compare,
    route_datetime,
    route_filter,
    route_groupby,
    route_merge,
    route_mutate,
    route_pivot,
    route_rename,
    route_select,
    validate_engine,
)


def _apply_op(
    df_curr: pd.DataFrame | None, step: dict, preview_mode: bool = False, engine: str = "pandas"
) -> tuple[pd.DataFrame, str]:
    """Apply a single pipeline operation step

    Args:
        df_curr: Current dataframe
        step: Operation step to apply
        preview_mode: If True, don't save intermediate results to cache
        engine: Engine to use for operations ('pandas' or 'spark')
    """
    op = (step.get("op") or step.get("type") or "").lower()
    params = step.get("params") or {}

    # Validate and normalize engine parameter
    engine = validate_engine(engine)

    # load
    if op == "load":
        name = params.get("name") or step.get("name")
        if not name:
            raise ValueError("load: name is required")
        df = _load_df_from_cache(name)
        return df, f"load {name}"

    # merge
    if op == "merge":
        how = (params.get("how") or "inner").lower()
        keys = params.get("keys") or []
        left_on = params.get("left_on") or []
        right_on = params.get("right_on") or []
        names: list[str] = params.get("names") or []
        others: list[str] = params.get("with") or params.get("others") or []

        # Support both old keys format and new left_on/right_on format
        if left_on or right_on:
            # New column mapping format
            if not left_on or not right_on:
                raise ValueError(
                    "merge: both left_on and right_on are required when using column mapping"
                )
            if isinstance(left_on, str):
                left_on = [k.strip() for k in left_on.split(",") if k.strip()]
            if isinstance(right_on, str):
                right_on = [k.strip() for k in right_on.split(",") if k.strip()]
            if len(left_on) != len(right_on):
                raise ValueError("merge: left_on and right_on must have the same number of columns")
            if not left_on or not right_on:
                raise ValueError("merge: left_on and right_on must be non-empty")
        else:
            # Fallback to old keys format for backward compatibility
            if not keys:
                raise ValueError("merge: either keys or left_on/right_on are required")
            if isinstance(keys, str):
                keys = [k.strip() for k in keys.split(",") if k.strip()]
            if not isinstance(keys, list) or not keys:
                raise ValueError("merge: keys must be a non-empty list or comma-separated string")

        # Debug logging
        print(
            f"[DEBUG] merge operation: how={how}, keys={keys}, left_on={left_on}, right_on={right_on}, names={names}, others={others}, engine={engine}"
        )

        # Handle the case where we have a current dataframe AND named dataframes to merge with
        if names and df_curr is not None:
            print("[DEBUG] merge: using current dataframe + named dataframes")
            # In chained pipelines, we want to merge the current dataframe with the named ones
            # Pre-validate that all required named dataframes exist
            from utils.redis_client import redis_client

            missing_dfs = []
            for name in names:
                exists = redis_client.exists(f"df:{name}")
                print(f"[DEBUG] checking dataframe '{name}': exists={exists}")
                if not exists:
                    missing_dfs.append(name)
            if missing_dfs:
                raise ValueError(f'merge: dataframes not found: {", ".join(missing_dfs)}')

            # Load all dataframes including current one
            dfs = [df_curr.copy()]
            df_names = ["current"] + names
            for nm in names:
                try:
                    d2 = _load_df_from_cache(nm)
                    dfs.append(d2)
                    print(
                        f"[DEBUG] loaded dataframe '{nm}': shape={d2.shape}, columns={list(d2.columns)}"
                    )
                except ValueError as e:
                    raise ValueError(f'merge: failed to load dataframe "{nm}": {str(e)}')

            # Use engine router for merge operation
            result = route_merge(dfs, df_names, keys, how, engine, left_on, right_on)

            if left_on and right_on:
                return (
                    result,
                    f"merge current + names={names} how={how} left_on={left_on} right_on={right_on} [{engine} engine]",
                )
            else:
                return (
                    result,
                    f"merge current + names={names} how={how} keys={keys} [{engine} engine]",
                )

        if names:
            if len(names) < 2:
                raise ValueError("merge: names must include at least two")

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

            # Load all dataframes
            dfs = []
            for nm in names:
                try:
                    df = _load_df_from_cache(nm)
                    dfs.append(df)
                    print(
                        f"[DEBUG] loaded dataframe '{nm}': shape={df.shape}, columns={list(df.columns)}"
                    )
                except ValueError as e:
                    raise ValueError(f'merge: failed to load dataframe "{nm}": {str(e)}')

            # Use engine router for merge operation
            result = route_merge(dfs, names, keys, how, engine, left_on, right_on)

            if left_on and right_on:
                return (
                    result,
                    f"merge names={names} how={how} left_on={left_on} right_on={right_on} [{engine} engine]",
                )
            else:
                return result, f"merge names={names} how={how} keys={keys} [{engine} engine]"

        if df_curr is None:
            raise ValueError("merge: no current dataframe; add a load step or use params.names")

        # Merge current dataframe with others
        dfs = [df_curr.copy()]
        df_names = ["current"] + others
        for nm in others:
            try:
                d2 = _load_df_from_cache(nm)
                dfs.append(d2)
            except ValueError as e:
                raise ValueError(f'merge: failed to load dataframe "{nm}": {str(e)}')

        # Use engine router for merge operation
        result = route_merge(dfs, df_names, keys, how, engine, left_on, right_on)

        if left_on and right_on:
            return (
                result,
                f"merge with={others} how={how} left_on={left_on} right_on={right_on} [{engine} engine]",
            )
        else:
            return result, f"merge with={others} how={how} keys={keys} [{engine} engine]"

    # filter
    if op == "filter":
        if df_curr is None:
            raise ValueError("filter: no current dataframe; add a load step first")

        conditions = params.get("filters") or []
        combine = (params.get("combine") or "and").lower()
        if combine not in ["and", "or"]:
            raise ValueError("filter: combine must be and/or")

        # Use engine router for filter operation
        filtered = route_filter(df_curr, conditions, combine, engine)

        # Collect human-readable pieces for description
        desc_parts = []
        for cond in conditions:
            col = cond.get("col")
            op_name = (cond.get("op") or "eq").lower()
            val = cond.get("value")

            # for description
            if val is None:
                desc_parts.append(f"{col} {op_name}")
            else:
                # Compact list display for in/nin; stringify scalars safely
                vstr = ",".join(map(str, val)) if isinstance(val, list) else str(val)
                desc_parts.append(f"{col} {op_name} {vstr}")

        detail = (" " + combine + " ").join(desc_parts) if desc_parts else "no conditions"
        return filtered, f"filter {detail} [{engine} engine]"

    # groupby
    if op == "groupby":
        if df_curr is None:
            raise ValueError("groupby: no current dataframe; add a load step first")
        by = params.get("by") or []
        aggs = params.get("aggs") or {}
        if not by:
            raise ValueError("groupby: by is required")
        for c in by:
            if c not in df_curr.columns:
                raise ValueError(f"groupby: column {c} not found")

        # Use engine router for groupby operation
        grouped = route_groupby(df_curr, by, aggs, engine)

        return grouped, f"groupby by={by} aggs={aggs or 'count'} [{engine} engine]"

    # select
    if op == "select":
        if df_curr is None:
            raise ValueError("select: no current dataframe; add a load step first")
        cols = params.get("columns") or []
        exclude = bool(params.get("exclude") or False)
        if not cols:
            raise ValueError("select: columns are required")
        missing = [c for c in cols if c not in df_curr.columns]
        if missing:
            raise ValueError(f'select: columns not found: {", ".join(missing)}')

        # Use engine router for select operation
        result = route_select(df_curr, cols, exclude, engine)

        if exclude:
            return result, f"drop columns={cols} [{engine} engine]"
        return result, f"select columns={cols} [{engine} engine]"

    # rename
    if op == "rename":
        if df_curr is None:
            raise ValueError("rename: no current dataframe; add a load step first")
        mapping = params.get("map") or params.get("rename") or params.get("columns") or {}
        if not isinstance(mapping, dict) or not mapping:
            raise ValueError("rename: map is required")
        missing = [old for old in mapping.keys() if old not in df_curr.columns]
        if missing:
            raise ValueError(f'rename: columns to rename not found: {", ".join(missing)}')

        # Check for duplicate names after rename
        new_cols = list(df_curr.columns)
        for old, new in mapping.items():
            if not isinstance(new, str) or not new:
                raise ValueError(f"rename: invalid new name for {old}")
            idx = new_cols.index(old)
            new_cols[idx] = new
        if len(set(new_cols)) != len(new_cols):
            raise ValueError("rename: would cause duplicate columns")

        # Use engine router for rename operation
        result = route_rename(df_curr, mapping, engine)

        return result, f"rename {mapping} [{engine} engine]"

    # mutate
    if op == "mutate":
        # df source
        df = df_curr if df_curr is not None else _load_df_from_cache(params.get("name"))
        if df is None:
            raise ValueError("mutate: add a load step or set params.name")
        target = (params.get("target") or params.get("to") or "").strip()
        expr = params.get("expr") or params.get("expression")
        mode = params.get("mode") or "vector"
        overwrite = bool(params.get("overwrite") or False)
        if not target:
            raise ValueError("mutate: target is required")
        if not isinstance(expr, str) or not expr.strip():
            raise ValueError("mutate: expr is required")
        if (target in df.columns) and not overwrite:
            raise ValueError(f"mutate: target column {target} exists")

        # Use engine router for mutate operation
        result = route_mutate(df, target, expr, mode, overwrite, engine)

        return result, f"mutate {target} ({mode}) [{engine} engine]"

    # pivot
    if op == "pivot":
        mode = (params.get("mode") or "wider").lower()
        df = df_curr if df_curr is not None else _load_df_from_cache(params.get("name"))
        if df is None:
            raise ValueError("pivot: add a load step or provide params.name")

        if mode == "wider":
            index = params.get("index") or []
            names_from = params.get("names_from")
            values_from = params.get("values_from")
            aggfunc = params.get("aggfunc") or "first"
            if not names_from or not values_from:
                raise ValueError("pivot wider: names_from and values_from are required")
            if isinstance(values_from, str):
                values_from = [values_from]

            # Use engine router for pivot operation
            result = route_pivot(
                df,
                mode,
                engine,
                index=index,
                names_from=names_from,
                values_from=values_from,
                aggfunc=aggfunc,
            )

            return (
                result,
                f"pivot wider names_from={names_from} values_from={values_from} [{engine} engine]",
            )

        elif mode == "longer":
            id_vars = params.get("id_vars") or []
            value_vars = params.get("value_vars") or []
            var_name = params.get("var_name") or "variable"
            value_name = params.get("value_name") or "value"
            if not value_vars:
                raise ValueError("pivot longer: value_vars required")

            # Use engine router for pivot operation
            result = route_pivot(
                df,
                mode,
                engine,
                id_vars=id_vars,
                value_vars=value_vars,
                var_name=var_name,
                value_name=value_name,
            )

            return result, f"pivot longer value_vars={value_vars} [{engine} engine]"
        else:
            raise ValueError("pivot: mode must be wider or longer")

    # compare (mismatch rows)
    if op == "compare":
        if df_curr is None:
            raise ValueError("compare: no current dataframe; add a load step first")
        other_name = params.get("name") or params.get("other")
        action = (params.get("on") or params.get("action") or "mismatch").lower()
        if not other_name:
            raise ValueError("compare: params.name (other) is required")
        df_other = _load_df_from_cache(other_name)

        # Use engine router for compare operation
        result_data = route_compare(df_curr, df_other, "current", other_name, engine)

        if action == "identical":
            if result_data.get("identical"):
                return df_curr, f"compare: identical check [{engine} engine]"
            else:
                return (
                    pd.DataFrame({"__note__": ["not identical"]}),
                    f"compare: identical check [{engine} engine]",
                )

        # For mismatch action, create a pandas dataframe with mismatch info
        # Since the compare result contains metadata, we need to create actual mismatch rows
        if not result_data.get("success") or result_data.get("identical"):
            return (
                pd.DataFrame({"__note__": ["no differences found"]}),
                f"compare: mismatch rows [{engine} engine]",
            )

        # For data mismatch, create a summary dataframe
        if result_data.get("result_type") == "schema_mismatch":
            return (
                pd.DataFrame({"__note__": ["schema mismatch"]}),
                f"compare: schema mismatch [{engine} engine]",
            )
        elif result_data.get("result_type") == "row_count_mismatch":
            return (
                pd.DataFrame({"__note__": ["row count mismatch"]}),
                f"compare: row count mismatch [{engine} engine]",
            )
        elif result_data.get("result_type") == "data_mismatch":
            left_unique = result_data.get("left_unique", 0)
            right_unique = result_data.get("right_unique", 0)
            summary_df = pd.DataFrame(
                {"side": ["current", other_name], "unique_rows": [left_unique, right_unique]}
            )
            return summary_df, f"compare: data mismatch summary [{engine} engine]"

        # Fallback for pandas-style detailed mismatch (engine-agnostic implementation)
        schema_match = list(df_curr.columns) == list(df_other.columns) and list(
            map(str, df_curr.dtypes)
        ) == list(map(str, df_other.dtypes))
        if not schema_match:
            return (
                pd.DataFrame({"__note__": ["schema mismatch"]}),
                f"compare: schema mismatch [{engine} engine]",
            )
        cols = list(df_curr.columns)
        merged = df_curr.merge(df_other, how="outer", on=cols, indicator=True)
        mism_left = merged[merged["_merge"] == "left_only"][cols].copy()
        mism_left["__side__"] = "left_only"
        mism_right = merged[merged["_merge"] == "right_only"][cols].copy()
        mism_right["__side__"] = "right_only"
        out = pd.concat([mism_left, mism_right], ignore_index=True)
        return out, f"compare: mismatch rows [{engine} engine]"

    # datetime
    if op == "datetime":
        action = (params.get("action") or "parse").lower()
        source = params.get("source") or params.get("column") or params.get("col")
        if df_curr is None:
            nm = params.get("name")
            if not nm:
                raise ValueError(
                    "datetime: no current dataframe; add a load step or set params.name"
                )
            df = _load_df_from_cache(nm)
        else:
            df = df_curr
        if source is None or source not in df.columns:
            raise ValueError("datetime: source column is required and must exist")

        if action == "parse":
            fmt = params.get("format") or params.get("fmt")
            errors = (params.get("errors") or "coerce").lower()
            target = params.get("target") or params.get("to")
            overwrite = bool(params.get("overwrite") or False)

            # Use engine router for datetime operation
            result = route_datetime(
                df,
                action,
                source,
                engine,
                format=fmt,
                errors=errors,
                target=target,
                overwrite=overwrite,
            )

            if target and target != source:
                return (
                    result,
                    f"datetime parse {source}->{target} ({fmt or 'auto'}) [{engine} engine]",
                )
            return result, f"datetime parse overwrite {source} ({fmt or 'auto'}) [{engine} engine]"

        elif action == "derive":
            opts = params.get("outputs") or {}
            want_year = (
                bool(opts.get("year") if "year" in opts else params.get("year") or True)
                if opts or ("year" in params)
                else True
            )
            want_month = (
                bool(opts.get("month") if "month" in opts else params.get("month") or True)
                if opts or ("month" in params)
                else True
            )
            want_day = (
                bool(opts.get("day") if "day" in opts else params.get("day") or True)
                if opts or ("day" in params)
                else True
            )
            want_year_month = (
                bool(
                    opts.get("year_month")
                    if "year_month" in opts
                    else params.get("year_month") or True
                )
                if opts or ("year_month" in params)
                else True
            )

            names = params.get("names") or {}
            month_style = (params.get("month_style") or "short").lower()
            overwrite = bool(params.get("overwrite") or False)

            # Use engine router for datetime operation
            result = route_datetime(
                df,
                action,
                source,
                engine,
                outputs={
                    "year": want_year,
                    "month": want_month,
                    "day": want_day,
                    "year_month": want_year_month,
                },
                names=names,
                month_style=month_style,
                overwrite=overwrite,
            )

            return result, f"datetime derive from {source} [{engine} engine]"
        else:
            raise ValueError("datetime: action must be parse or derive")

    # chain_pipeline - execute another pipeline at this point
    if op == "chain_pipeline":
        pipeline_name = params.get("pipeline") or params.get("name")
        if not pipeline_name:
            raise ValueError("chain_pipeline: pipeline name is required")

        if df_curr is None:
            raise ValueError("chain_pipeline: no current dataframe; add a load step first")

        print(
            f"[DEBUG] chain_pipeline: executing pipeline '{pipeline_name}' with input shape={df_curr.shape}, engine={engine}"
        )

        # Load the chained pipeline
        import json

        from utils.redis_client import redis_client

        key = f"pipeline:{pipeline_name}"
        if not redis_client.exists(key):
            raise ValueError(f"chain_pipeline: pipeline {pipeline_name} not found")

        obj = json.loads(redis_client.get(key))
        chained_steps = obj.get("steps") or []

        if not chained_steps:
            raise ValueError(f"chain_pipeline: pipeline {pipeline_name} has no steps")

        print(
            f"[DEBUG] chain_pipeline: found {len(chained_steps)} steps to execute with engine={engine}"
        )

        # Execute the chained pipeline with current dataframe as input
        current = df_curr.copy()  # Make a copy to avoid modifying original
        for i, chained_step in enumerate(chained_steps):
            print(
                f"[DEBUG] chain_pipeline: executing step {i+1}: {chained_step.get('op', 'unknown')} with engine={engine}"
            )
            current, step_desc = _apply_op(
                current, chained_step, preview_mode, engine
            )  # Pass engine to chained operations
            print(f"[DEBUG] chain_pipeline: step {i+1} result: {step_desc}, shape={current.shape}")

        # Save the chained pipeline result as a named dataframe (only if not in preview mode)
        result_name = f"chained_{pipeline_name}"
        if preview_mode:
            print(f"[DEBUG] chain_pipeline: preview mode - skipping save of '{result_name}'")
            return (
                df_curr,
                f"chain_pipeline: {pipeline_name} (preview: applied {len(chained_steps)} steps, result shape: {current.shape}) [{engine} engine]",
            )
        else:
            print(
                f"[DEBUG] chain_pipeline: saving result as '{result_name}' with shape={current.shape}"
            )
            try:
                _save_df_to_cache(
                    result_name,
                    current,
                    f"Result from chained pipeline: {pipeline_name} [{engine} engine]",
                    source="ops:pipeline",
                )
                print(f"[DEBUG] chain_pipeline: successfully saved '{result_name}' to cache")
            except Exception as e:
                raise ValueError(f'chain_pipeline: failed to save result "{result_name}": {str(e)}')

            # Return the original dataframe to preserve the main pipeline flow
            # The chained result is now available as a named dataframe for merge operations
            return (
                df_curr,
                f"chain_pipeline: {pipeline_name} (saved as {result_name}, applied {len(chained_steps)} steps) [{engine} engine]",
            )

    raise ValueError(f"Unsupported op: {op}")
