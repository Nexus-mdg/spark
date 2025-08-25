"""
Pandas engine for DataFrame operations
Extracted from existing pandas-based operations for consistent engine-based architecture
"""

import json
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


def pandas_compare(df1: pd.DataFrame, df2: pd.DataFrame, n1: str, n2: str) -> dict:
    """
    Pure pandas implementation for DataFrame comparison
    Returns: dict with comparison result
    """
    # Schema comparison
    schema_match = list(df1.columns) == list(df2.columns) and list(map(str, df1.dtypes)) == list(
        map(str, df2.dtypes)
    )
    if not schema_match:
        return {
            "success": True,
            "identical": False,
            "result_type": "schema_mismatch",
            "engine": "pandas",
        }

    # Row count check
    if len(df1) != len(df2):
        return {
            "success": True,
            "identical": False,
            "result_type": "row_count_mismatch",
            "engine": "pandas",
        }

    # Data comparison (order independent): use indicator merge on all columns
    cols = list(df1.columns)
    merged_l = df1.merge(df2, how="outer", on=cols, indicator=True)
    left_only = merged_l[merged_l["_merge"] == "left_only"][cols]
    right_only = merged_l[merged_l["_merge"] == "right_only"][cols]
    c1 = int(len(left_only))
    c2 = int(len(right_only))

    created = []
    if c1 == 0 and c2 == 0:
        return {
            "success": True,
            "identical": True,
            "result_type": "identical",
            "created": created,
            "engine": "pandas",
        }

    # Cache differing rows (cap to 100k)
    THRESH = 100_000
    if 0 < c1 <= THRESH:
        # Note: In actual implementation, would save to cache here
        # For now, just track that we would create these datasets
        created.append(f"{n1}_unique_rows")
    if 0 < c2 <= THRESH:
        created.append(f"{n2}_unique_rows")

    return {
        "success": True,
        "identical": False,
        "result_type": "data_mismatch",
        "left_unique": c1,
        "right_unique": c2,
        "created": created,
        "engine": "pandas",
    }


def pandas_merge(
    dfs: List[pd.DataFrame],
    names: List[str],
    keys: List[str],
    how: str,
    left_on: Optional[List[str]] = None,
    right_on: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Pandas implementation for DataFrame merge operation"""
    if len(dfs) < 2:
        raise ValueError("At least 2 dataframes required for merge")

    df = dfs[0].copy()
    for i, d2 in enumerate(dfs[1:], 1):
        if left_on and right_on:
            df = df.merge(d2, left_on=left_on, right_on=right_on, how=how)
        else:
            df = df.merge(d2, on=keys, how=how)

    return df


def pandas_filter(df: pd.DataFrame, conditions: List[Dict], combine: str = "and") -> pd.DataFrame:
    """Pandas implementation for DataFrame filter operation"""
    if not conditions:
        return df.copy()

    mask = None
    for cond in conditions:
        col = cond.get("col")
        op = (cond.get("op") or "eq").lower()
        val = cond.get("value")

        if col not in df.columns:
            raise ValueError(f"Column {col} not found")

        s = df[col]

        if op == "eq":
            m = s == val
        elif op == "ne":
            m = s != val
        elif op == "lt":
            m = s < val
        elif op == "lte":
            m = s <= val
        elif op == "gt":
            m = s > val
        elif op == "gte":
            m = s >= val
        elif op == "in":
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
                    vals = [x.strip() for x in v.split(",") if x.strip()]
            if not isinstance(vals, list):
                vals = [vals]
            m = s.isin(vals)
        elif op == "nin":
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
                    vals = [x.strip() for x in v.split(",") if x.strip()]
            if not isinstance(vals, list):
                vals = [vals]
            m = ~s.isin(vals)
        elif op == "contains":
            m = s.astype("string").str.contains(str(val), na=False)
        elif op == "startswith":
            m = s.astype("string").str.startswith(str(val), na=False)
        elif op == "endswith":
            m = s.astype("string").str.endswith(str(val), na=False)
        elif op == "isnull":
            m = s.isna()
        elif op == "notnull":
            m = s.notna()
        else:
            raise ValueError(f"Unsupported op {op}")

        mask = m if mask is None else (mask & m if combine == "and" else mask | m)

    return df[mask] if mask is not None else df.copy()


def pandas_groupby(df: pd.DataFrame, by: List[str], aggs: Optional[Dict] = None) -> pd.DataFrame:
    """Pandas implementation for DataFrame groupby operation"""
    for c in by:
        if c not in df.columns:
            raise ValueError(f"Group-by column {c} not found")

    if aggs:
        grouped = df.groupby(by).agg(aggs).reset_index()
    else:
        grouped = df.groupby(by).size().reset_index(name="count")

    # Flatten MultiIndex columns if any
    if isinstance(grouped.columns, pd.MultiIndex):
        grouped.columns = [
            "__".join([str(x) for x in tup if str(x) != ""])
            for tup in grouped.columns.to_flat_index()
        ]

    return grouped


def pandas_select(df: pd.DataFrame, columns: List[str], exclude: bool = False) -> pd.DataFrame:
    """Pandas implementation for DataFrame select operation"""
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f'Columns not found: {", ".join(missing)}')

    if exclude:
        keep_cols = [c for c in df.columns if c not in columns]
        return df[keep_cols].copy()
    else:
        return df[columns].copy()


def pandas_rename(df: pd.DataFrame, rename_map: Dict[str, str]) -> pd.DataFrame:
    """Pandas implementation for DataFrame rename operation"""
    missing = [old for old in rename_map.keys() if old not in df.columns]
    if missing:
        raise ValueError(f'Columns to rename not found: {", ".join(missing)}')

    # Check for duplicate names after rename
    new_cols = list(df.columns)
    for old, new in rename_map.items():
        if not isinstance(new, str) or not new:
            raise ValueError(f"Invalid new name for column {old}")
        idx = new_cols.index(old)
        new_cols[idx] = new

    if len(set(new_cols)) != len(new_cols):
        raise ValueError("Rename would cause duplicate column names")

    return df.rename(columns=rename_map)


def pandas_pivot(df: pd.DataFrame, mode: str, **kwargs) -> pd.DataFrame:
    """Pandas implementation for DataFrame pivot operation"""
    if mode == "wider":
        index = kwargs.get("index", [])
        names_from = kwargs.get("names_from")
        values_from = kwargs.get("values_from")
        aggfunc = kwargs.get("aggfunc", "first")

        if not names_from or not values_from:
            raise ValueError("names_from and values_from are required for wider")

        if isinstance(values_from, str):
            values_from = [values_from]

        pivoted = pd.pivot_table(
            df, index=index or None, columns=names_from, values=values_from, aggfunc=aggfunc
        )

        # Flatten MultiIndex columns if any
        if isinstance(pivoted.columns, pd.MultiIndex):
            pivoted.columns = [
                "__".join([str(x) for x in tup if str(x) != ""])
                for tup in pivoted.columns.to_flat_index()
            ]

        return pivoted.reset_index()

    elif mode == "longer":
        id_vars = kwargs.get("id_vars", [])
        value_vars = kwargs.get("value_vars", [])
        var_name = kwargs.get("var_name", "variable")
        value_name = kwargs.get("value_name", "value")

        if not value_vars:
            raise ValueError("value_vars is required for longer")

        return pd.melt(
            df,
            id_vars=id_vars or None,
            value_vars=value_vars,
            var_name=var_name,
            value_name=value_name,
        )

    else:
        raise ValueError("mode must be wider or longer")


def pandas_datetime(df: pd.DataFrame, action: str, source: str, **kwargs) -> pd.DataFrame:
    """Pandas implementation for DataFrame datetime operation"""
    if source not in df.columns:
        raise ValueError(f"Column {source} not found")

    if action == "parse":
        fmt = kwargs.get("format") or kwargs.get("fmt")
        errors = kwargs.get("errors", "coerce").lower()
        target = kwargs.get("target") or kwargs.get("to")
        overwrite = bool(kwargs.get("overwrite"))

        dseries = pd.to_datetime(df[source], format=fmt, errors=errors)
        out_df = df.copy()

        if target and target != source:
            if (target in out_df.columns) and not overwrite:
                raise ValueError(f"target column {target} already exists")
            out_df[target] = dseries
        else:
            out_df[source] = dseries

        return out_df

    elif action == "derive":
        col = df[source]
        if not pd.api.types.is_datetime64_any_dtype(col):
            col = pd.to_datetime(col, errors="coerce")

        out_df = df.copy()
        opts = kwargs.get("outputs", {})

        # Default values
        want_year = bool(opts.get("year", True))
        want_month = bool(opts.get("month", True))
        want_day = bool(opts.get("day", True))
        want_year_month = bool(opts.get("year_month", True))

        names = kwargs.get("names", {})
        cname_year = names.get("year", "year")
        cname_month = names.get("month", "month")
        cname_day = names.get("day", "day")
        cname_year_month = names.get("year_month", "year_month")

        month_style = kwargs.get("month_style", "short").lower()
        overwrite = bool(kwargs.get("overwrite", False))

        # Check for conflicts
        for cname, flag in [
            (cname_year, want_year),
            (cname_month, want_month),
            (cname_day, want_day),
            (cname_year_month, want_year_month),
        ]:
            if not flag:
                continue
            if (cname in out_df.columns) and not overwrite:
                raise ValueError(f"Output column {cname} already exists")

        # Generate columns
        if want_year:
            out_df[cname_year] = col.dt.year
        if want_month:
            if month_style == "long":
                out_df[cname_month] = col.dt.month_name()
            elif month_style == "num":
                out_df[cname_month] = col.dt.month
            elif month_style in ("short_lower", "abbr_lower", "short-lower"):
                out_df[cname_month] = col.dt.strftime("%b").str.lower()
            else:
                out_df[cname_month] = col.dt.strftime("%b")
        if want_day:
            out_df[cname_day] = col.dt.day
        if want_year_month:
            if month_style in ("short_lower", "abbr_lower", "short-lower"):
                out_df[cname_year_month] = col.dt.strftime("%Y-%b").str.lower()
            elif month_style == "long":
                out_df[cname_year_month] = col.dt.strftime("%Y-") + col.dt.month_name()
            elif month_style == "num":
                out_df[cname_year_month] = col.dt.strftime("%Y-") + col.dt.month.astype(
                    "Int64"
                ).map(lambda m: f"{m:02d}" if pd.notna(m) else None)
            else:
                out_df[cname_year_month] = col.dt.strftime("%Y-%b")

        return out_df

    else:
        raise ValueError("action must be parse or derive")


def pandas_mutate(
    df: pd.DataFrame, target: str, expr: str, mode: str = "vector", overwrite: bool = False
) -> pd.DataFrame:
    """Pandas implementation for DataFrame mutate operation"""
    if (target in df.columns) and not overwrite:
        raise ValueError(f"target column {target} already exists")

    # Safe-ish eval: expose only whitelisted symbols
    safe_builtins = {
        "abs": abs,
        "min": min,
        "max": max,
        "round": round,
        "int": int,
        "float": float,
        "str": str,
        "bool": bool,
        "len": len,
    }

    def _safe_eval(expression: str, local_ctx: dict):
        code = compile(expression, "<mutate-expr>", "eval")
        return eval(code, {"__builtins__": {}}, {**safe_builtins, **local_ctx})

    base_locals = {"pd": pd, "np": np, "df": df}
    base_locals["col"] = lambda c: df[c]

    if mode not in ("vector", "row"):
        raise ValueError("mode must be vector or row")

    if mode == "vector":
        result = _safe_eval(expr, base_locals)
    else:
        result = df.apply(lambda r: _safe_eval(expr, {**base_locals, "r": r}), axis=1)

    out_df = df.copy()
    out_df[target] = result

    return out_df
