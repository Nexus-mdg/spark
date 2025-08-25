"""
Advanced DataFrame operations API routes
"""

import io
import json
import os

import numpy as np
import pandas as pd
from flask import Blueprint, jsonify, request

from operations.dataframe_ops import _load_df_from_cache, _save_df_to_cache, _unique_name
from operations.engine_router import (create_engine_response, route_compare, route_datetime,
                                      route_filter, route_groupby, route_merge, route_mutate,
                                      route_pivot, route_rename, route_select)
from utils.helpers import df_to_records_json_safe, notify_ntfy
from utils.redis_client import redis_client

operations_bp = Blueprint("operations", __name__)


@operations_bp.route("/api/ops/compare", methods=["POST"])
def op_compare():
    """Compare two DataFrames using engine-based routing"""
    try:
        payload = request.get_json(force=True)
        n1 = payload.get("name1")
        n2 = payload.get("name2")
        engine = payload.get("engine", "pandas")  # Default to pandas for backward compatibility

        if not n1 or not n2:
            return jsonify({"success": False, "error": "name1 and name2 are required"}), 400

        df1 = _load_df_from_cache(n1)
        df2 = _load_df_from_cache(n2)

        # Route to appropriate engine
        result = route_compare(df1, df2, n1, n2, engine)

        # Handle saving of difference DataFrames if created
        if result.get("success") and not result.get("identical") and result.get("created"):
            actual_created = []
            THRESH = int(os.getenv("COMPARE_CACHE_THRESHOLD", "100000"))

            # Save left unique rows if applicable
            if f"{n1}_unique_rows" in result.get("created", []):
                if engine == "spark":
                    # For Spark, we'd need to get the actual differences
                    # For now, mark as created but implementation would save the actual diff
                    actual_created.append(f"{n1}_unique_rows")
                else:
                    # Pandas engine - recreate the differences for saving
                    cols = list(df1.columns)
                    merged_l = df1.merge(df2, how="outer", on=cols, indicator=True)
                    left_only = merged_l[merged_l["_merge"] == "left_only"][cols]
                    if len(left_only) <= THRESH:
                        name_u1 = _unique_name(f"{n1}_unique_rows")
                        _save_df_to_cache(
                            name_u1,
                            left_only,
                            description=f"Rows unique to {n1} vs {n2}",
                            source="ops:compare",
                        )
                        actual_created.append(name_u1)

            # Save right unique rows if applicable
            if f"{n2}_unique_rows" in result.get("created", []):
                if engine == "spark":
                    actual_created.append(f"{n2}_unique_rows")
                else:
                    cols = list(df1.columns)
                    merged_l = df1.merge(df2, how="outer", on=cols, indicator=True)
                    right_only = merged_l[merged_l["_merge"] == "right_only"][cols]
                    if len(right_only) <= THRESH:
                        name_u2 = _unique_name(f"{n2}_unique_rows")
                        _save_df_to_cache(
                            name_u2,
                            right_only,
                            description=f"Rows unique to {n2} vs {n1}",
                            source="ops:compare",
                        )
                        actual_created.append(name_u2)

            result["created"] = actual_created

        # Send notification
        if result.get("success"):
            if result.get("identical"):
                notify_ntfy(
                    title=f"DF Compare: {n1} vs {n2}",
                    message=f"identical ({engine} engine)",
                    tags=["compare", "identical", "ok"],
                )
            elif result.get("result_type") == "schema_mismatch":
                notify_ntfy(
                    title=f"DF Compare: {n1} vs {n2}",
                    message=f"schema mismatch ({engine} engine)",
                    tags=["compare", "schema", "warn"],
                )
            elif result.get("result_type") == "row_count_mismatch":
                notify_ntfy(
                    title=f"DF Compare: {n1} vs {n2}",
                    message=f"row count mismatch ({engine} engine)",
                    tags=["compare", "row_count", "warn"],
                )
            elif result.get("result_type") == "data_mismatch":
                left_unique = result.get("left_unique", 0)
                right_unique = result.get("right_unique", 0)
                created = result.get("created", [])
                notify_ntfy(
                    title=f"DF Compare: {n1} vs {n2}",
                    message=f"data mismatch ({engine} engine): left_unique={left_unique}, right_unique={right_unique}; created={created}",
                    tags=["compare", "mismatch"],
                )

        return jsonify(result)
    except Exception as e:
        return (
            jsonify(
                {
                    "success": False,
                    "error": str(e),
                    "engine": engine if "engine" in locals() else "unknown",
                }
            ),
            500,
        )


@operations_bp.route("/api/ops/merge", methods=["POST"])
def op_merge():
    """Merge multiple DataFrames using engine-based routing"""
    try:
        p = request.get_json(force=True)
        names = p.get("names") or []
        keys = p.get("keys") or []
        left_on = p.get("left_on") or []
        right_on = p.get("right_on") or []
        how = (p.get("how") or "inner").lower()
        engine = p.get("engine", "pandas")  # Default to pandas for backward compatibility

        if how not in ["inner", "left", "right", "outer"]:
            return (
                jsonify({"success": False, "error": "how must be one of inner,left,right,outer"}),
                400,
            )
        if len(names) < 2:
            return jsonify({"success": False, "error": "At least 2 dataframe names required"}), 400

        # Support both old keys format and new left_on/right_on format
        if left_on or right_on:
            if not left_on or not right_on:
                return (
                    jsonify(
                        {
                            "success": False,
                            "error": "Both left_on and right_on are required when using column mapping",
                        }
                    ),
                    400,
                )
            if len(left_on) != len(right_on):
                return (
                    jsonify(
                        {
                            "success": False,
                            "error": "left_on and right_on must have the same number of columns",
                        }
                    ),
                    400,
                )
            if len(left_on) < 1:
                return (
                    jsonify({"success": False, "error": "At least 1 column mapping is required"}),
                    400,
                )
        else:
            if len(keys) < 1:
                return jsonify({"success": False, "error": "At least 1 key is required"}), 400

        # Load all DataFrames
        dfs = []
        for nm in names:
            dfs.append(_load_df_from_cache(nm))

        # Route to appropriate engine
        df = route_merge(dfs, names, keys, how, engine, left_on, right_on)

        # Generate output name and description
        if left_on and right_on:
            base = f"{'_'.join(names)}__merge_{how}_by_{'-'.join(left_on)}_to_{'-'.join(right_on)}"
            description = f"Merge {names} on {left_on} → {right_on} ({how}) [{engine} engine]"
        else:
            base = f"{'_'.join(names)}__merge_{how}_by_{'-'.join(keys)}"
            description = f"Merge {names} on {keys} ({how}) [{engine} engine]"

        out_name = _unique_name(base)
        meta = _save_df_to_cache(out_name, df, description=description, source="ops:merge")
        meta["engine"] = engine

        return jsonify({"success": True, "name": out_name, "metadata": meta, "engine": engine})
    except Exception as e:
        return (
            jsonify(
                {
                    "success": False,
                    "error": str(e),
                    "engine": engine if "engine" in locals() else "unknown",
                }
            ),
            500,
        )


@operations_bp.route("/api/ops/pivot", methods=["POST"])
def op_pivot():
    """Pivot DataFrame wider or longer using engine-based routing"""
    try:
        p = request.get_json(force=True)
        mode = (p.get("mode") or "wider").lower()
        name = p.get("name")
        engine = p.get("engine", "pandas")  # Default to pandas for backward compatibility

        if not name:
            return jsonify({"success": False, "error": "name is required"}), 400

        df = _load_df_from_cache(name)

        # Prepare parameters for engine routing
        kwargs = {}
        if mode == "wider":
            kwargs["index"] = p.get("index") or []
            kwargs["names_from"] = p.get("names_from")
            kwargs["values_from"] = p.get("values_from")
            kwargs["aggfunc"] = p.get("aggfunc") or "first"

            if not kwargs["names_from"] or not kwargs["values_from"]:
                return (
                    jsonify(
                        {
                            "success": False,
                            "error": "names_from and values_from are required for wider",
                        }
                    ),
                    400,
                )

            # Route to appropriate engine
            pivoted = route_pivot(df, mode, engine, **kwargs)

            values_from = kwargs["values_from"]
            if isinstance(values_from, str):
                values_from = [values_from]

            base = f"{name}__pivot_wider_{kwargs['names_from']}_vals_{'-'.join(values_from)}"
            description = (
                f"Pivot wider from {kwargs['names_from']} values {values_from} [{engine} engine]"
            )

        elif mode == "longer":
            kwargs["id_vars"] = p.get("id_vars") or []
            kwargs["value_vars"] = p.get("value_vars") or []
            kwargs["var_name"] = p.get("var_name") or "variable"
            kwargs["value_name"] = p.get("value_name") or "value"

            if not kwargs["value_vars"]:
                return (
                    jsonify({"success": False, "error": "value_vars is required for longer"}),
                    400,
                )

            # Route to appropriate engine
            pivoted = route_pivot(df, mode, engine, **kwargs)

            base = f"{name}__pivot_longer_{'-'.join(kwargs['value_vars'])}"
            description = f"Pivot longer value_vars={kwargs['value_vars']} [{engine} engine]"

        else:
            return jsonify({"success": False, "error": "mode must be wider or longer"}), 400

        out_name = _unique_name(base)
        meta = _save_df_to_cache(out_name, pivoted, description=description, source="ops:pivot")
        meta["engine"] = engine

        return jsonify({"success": True, "name": out_name, "metadata": meta, "engine": engine})
    except Exception as e:
        return (
            jsonify(
                {
                    "success": False,
                    "error": str(e),
                    "engine": engine if "engine" in locals() else "unknown",
                }
            ),
            500,
        )


@operations_bp.route("/api/ops/filter", methods=["POST"])
def op_filter():
    """Filter DataFrame rows based on conditions using engine-based routing"""
    try:
        p = request.get_json(force=True)
        name = p.get("name")
        engine = p.get("engine", "pandas")  # Default to pandas for backward compatibility

        if not name:
            return jsonify({"success": False, "error": "name is required"}), 400

        df = _load_df_from_cache(name)
        conditions = p.get("filters") or []
        combine = (p.get("combine") or "and").lower()

        if combine not in ["and", "or"]:
            return jsonify({"success": False, "error": "combine must be and/or"}), 400

        # Route to appropriate engine
        filtered = route_filter(df, conditions, combine, engine)

        # Collect human-readable pieces for description
        desc_parts = []
        for cond in conditions:
            col = cond.get("col")
            op = (cond.get("op") or "eq").lower()
            val = cond.get("value")

            # for description
            if val is None:
                desc_parts.append(f"{col} {op}")
            else:
                # Compact list display for in/nin; stringify scalars safely
                vstr = ",".join(map(str, val)) if isinstance(val, list) else str(val)
                desc_parts.append(f"{col} {op} {vstr}")

        out_name = _unique_name(f"{name}__filter")
        detail = (" " + combine + " ").join(desc_parts) if desc_parts else "no conditions"
        description = f"Filter: {detail} [{engine} engine]"
        meta = _save_df_to_cache(out_name, filtered, description=description, source="ops:filter")
        meta["engine"] = engine

        return jsonify({"success": True, "name": out_name, "metadata": meta, "engine": engine})
    except Exception as e:
        return (
            jsonify(
                {
                    "success": False,
                    "error": str(e),
                    "engine": engine if "engine" in locals() else "unknown",
                }
            ),
            500,
        )


@operations_bp.route("/api/ops/groupby", methods=["POST"])
def op_groupby():
    """Group DataFrame by columns and aggregate using engine-based routing"""
    try:
        p = request.get_json(force=True)
        name = p.get("name")
        by = p.get("by") or []
        aggs = p.get("aggs") or {}
        engine = p.get("engine", "pandas")  # Default to pandas for backward compatibility

        if not name:
            return jsonify({"success": False, "error": "name is required"}), 400
        if not by:
            return jsonify({"success": False, "error": "by is required"}), 400

        df = _load_df_from_cache(name)

        # Route to appropriate engine
        grouped = route_groupby(df, by, aggs, engine)

        base = f"{name}__groupby_{'-'.join(by)}"
        out_name = _unique_name(base)
        by_str = ",".join(by)
        aggs_str = aggs if aggs else "count"
        description = f"Group-by columns={by_str}; aggs={aggs_str} [{engine} engine]"
        meta = _save_df_to_cache(out_name, grouped, description=description, source="ops:groupby")
        meta["engine"] = engine

        return jsonify({"success": True, "name": out_name, "metadata": meta, "engine": engine})
    except Exception as e:
        return (
            jsonify(
                {
                    "success": False,
                    "error": str(e),
                    "engine": engine if "engine" in locals() else "unknown",
                }
            ),
            500,
        )


@operations_bp.route("/api/ops/select", methods=["POST"])
def op_select():
    """Select or exclude columns from DataFrame using engine-based routing"""
    try:
        p = request.get_json(force=True)
        name = p.get("name")
        cols = p.get("columns") or []
        exclude = bool(p.get("exclude") or False)
        engine = p.get("engine", "pandas")  # Default to pandas for backward compatibility

        if not name:
            return jsonify({"success": False, "error": "name is required"}), 400
        if not isinstance(cols, list) or len(cols) == 0:
            return jsonify({"success": False, "error": "columns (non-empty list) is required"}), 400

        df = _load_df_from_cache(name)

        # Route to appropriate engine
        projected = route_select(df, cols, exclude, engine)

        if exclude:
            base = f"{name}__drop_{'-'.join(cols)}"
            desc = f"Drop columns={','.join(cols)} [{engine} engine]"
        else:
            base = f"{name}__select_{'-'.join(cols)}"
            desc = f"Select columns={','.join(cols)} [{engine} engine]"

        out_name = _unique_name(base)
        meta = _save_df_to_cache(out_name, projected, description=desc, source="ops:select")
        meta["engine"] = engine

        return jsonify({"success": True, "name": out_name, "metadata": meta, "engine": engine})
    except Exception as e:
        return (
            jsonify(
                {
                    "success": False,
                    "error": str(e),
                    "engine": engine if "engine" in locals() else "unknown",
                }
            ),
            500,
        )


@operations_bp.route("/api/ops/rename", methods=["POST"])
def op_rename():
    """Rename columns in DataFrame using engine-based routing"""
    try:
        p = request.get_json(force=True)
        name = p.get("name")
        rename_map = p.get("map") or p.get("rename") or p.get("columns") or {}
        engine = p.get("engine", "pandas")  # Default to pandas for backward compatibility

        if not name:
            return jsonify({"success": False, "error": "name is required"}), 400
        if not isinstance(rename_map, dict) or not rename_map:
            return (
                jsonify({"success": False, "error": "map (object of old->new names) is required"}),
                400,
            )

        df = _load_df_from_cache(name)

        # Route to appropriate engine
        renamed = route_rename(df, rename_map, engine)

        base = f"{name}__rename"
        out_name = _unique_name(base)
        description = f"Rename columns: {rename_map} [{engine} engine]"
        meta = _save_df_to_cache(out_name, renamed, description=description, source="ops:rename")
        meta["engine"] = engine

        return jsonify({"success": True, "name": out_name, "metadata": meta, "engine": engine})
    except Exception as e:
        return (
            jsonify(
                {
                    "success": False,
                    "error": str(e),
                    "engine": engine if "engine" in locals() else "unknown",
                }
            ),
            500,
        )


@operations_bp.route("/api/ops/datetime", methods=["POST"])
def op_datetime():
    """Parse or derive datetime columns using engine-based routing"""
    try:
        p = request.get_json(force=True)
        name = p.get("name")
        engine = p.get("engine", "pandas")  # Default to pandas for backward compatibility

        if not name:
            return jsonify({"success": False, "error": "name is required"}), 400

        action = (p.get("action") or "parse").lower()
        source = p.get("source") or p.get("column") or p.get("col")

        if not source:
            return jsonify({"success": False, "error": "source column is required"}), 400

        df = _load_df_from_cache(name)

        # Prepare parameters for engine routing
        kwargs = {}
        if action == "parse":
            kwargs["format"] = p.get("format") or p.get("fmt")
            kwargs["errors"] = (p.get("errors") or "coerce").lower()
            kwargs["target"] = p.get("target") or p.get("to")
            kwargs["overwrite"] = bool(p.get("overwrite"))

            base_out_name = f"{name}__datetime"
            if kwargs["target"] and kwargs["target"] != source:
                description = f"Parse date: {source} -> {kwargs['target']} (format={kwargs['format'] or 'auto'}) [{engine} engine]"
                base_out_name = f"{name}__parse_{source}_to_{kwargs['target']}"
            else:
                description = f"Parse date: overwrite {source} (format={kwargs['format'] or 'auto'}) [{engine} engine]"
                base_out_name = f"{name}__parse_{source}"

        elif action == "derive":
            opts = p.get("outputs") or {}
            kwargs["outputs"] = opts
            kwargs["names"] = p.get("names") or {}
            kwargs["month_style"] = (
                p.get("month_style") or opts.get("month_style") or "short"
            ).lower()
            kwargs["overwrite"] = bool(p.get("overwrite") or False)

            description = f"Derive from {source} [{engine} engine]"
            base_out_name = f"{name}__derive_{source}"

        else:
            return jsonify({"success": False, "error": "action must be parse or derive"}), 400

        # Route to appropriate engine
        out_df = route_datetime(df, action, source, engine, **kwargs)

        out_name = _unique_name(base_out_name)
        meta = _save_df_to_cache(out_name, out_df, description=description, source="ops:datetime")
        meta["engine"] = engine

        return jsonify({"success": True, "name": out_name, "metadata": meta, "engine": engine})
    except Exception as e:
        return (
            jsonify(
                {
                    "success": False,
                    "error": str(e),
                    "engine": engine if "engine" in locals() else "unknown",
                }
            ),
            500,
        )


@operations_bp.route("/api/ops/mutate", methods=["POST"])
def op_mutate():
    """Create new columns with expressions using engine-based routing"""
    try:
        p = request.get_json(force=True)
        name = p.get("name")
        target = (p.get("target") or p.get("to") or "").strip()
        expr = p.get("expr") or p.get("expression")
        mode = (p.get("mode") or "vector").lower()
        overwrite = bool(p.get("overwrite") or False)
        engine = p.get("engine", "pandas")  # Default to pandas for backward compatibility

        if not name:
            return jsonify({"success": False, "error": "name is required"}), 400
        if not target:
            return jsonify({"success": False, "error": "target is required"}), 400
        if not isinstance(expr, str) or not expr.strip():
            return jsonify({"success": False, "error": "expr (string) is required"}), 400

        df = _load_df_from_cache(name)

        # Route to appropriate engine
        out_df = route_mutate(df, target, expr, mode, overwrite, engine)

        out_name = _unique_name(f"{name}__mutate_{target}")
        description = f"Mutate {target} via expr ({mode}) [{engine} engine]"
        meta = _save_df_to_cache(out_name, out_df, description=description, source="ops:mutate")
        meta["engine"] = engine

        return jsonify({"success": True, "name": out_name, "metadata": meta, "engine": engine})
    except Exception as e:
        return (
            jsonify(
                {
                    "success": False,
                    "error": str(e),
                    "engine": engine if "engine" in locals() else "unknown",
                }
            ),
            500,
        )
