"""
Code export functionality for chained pipelines
Generates R and Python code from pipeline steps
"""
import json
from typing import List, Dict, Any, Tuple


def escape_string(s: str) -> str:
    """Escape string for safe code generation"""
    if s is None:
        return ""
    return str(s).replace("'", "\\'").replace('"', '\\"').replace('\n', '\\n')


def generate_r_code(pipeline_data: Dict[str, Any]) -> str:
    """Generate R code for a chained pipeline"""
    
    steps = pipeline_data.get('steps', [])
    start = pipeline_data.get('start')
    
    lines = [
        "# Generated R code for chained pipeline",
        "# Load required libraries",
        "library(dplyr)",
        "library(tidyverse)",
        "library(lubridate)",
        "library(redux)",
        "library(jsonlite)",
        "",
        "# Function to load cached dataframes",
        "load_cached_dataframe <- function(name, meta_json) {",
        "  # Connect to Redis",
        "  redis_conn <- redux::hiredis()",
        "  ",
        "  # Fetch data from Redis cache",
        "  cached_data <- redis_conn$GET(paste0('dataframe:', name))",
        "  ",
        "  if (is.null(cached_data)) {",
        "    # Fallback to meta_json if Redis data not available",
        "    df <- jsonlite::fromJSON(meta_json)",
        "  } else {",
        "    # Parse data from Redis",
        "    df <- jsonlite::fromJSON(rawToChar(cached_data))",
        "  }",
        "  ",
        "  cat('📂 Loaded from cache:', name, '\\n')",
        "  cat('📊 Size:', nrow(df), 'rows x', ncol(df), 'columns\\n')",
        "  cat('⚡ Instant loading from RAM!\\n\\n')",
        "  return(df)",
        "}",
        "",
        "# Function to cache dataframes",
        "cache_dataframe <- function(df, name) {",
        "  # Connect to Redis",
        "  redis_con <- redux::hiredis()",
        "  ",
        "  # Calculate memory usage",
        "  size_mb <- object.size(df) / 1024^2",
        "  ",
        "  # Convert to JSON and cache",
        "  df_json <- jsonlite::toJSON(df, auto_unbox = FALSE, digits = 6)",
        "  redis_con$SET(paste0('dataframe:', name), df_json)",
        "  ",
        "  # Store metadata",
        "  metadata <- list(",
        "    name = name,",
        "    rows = nrow(df),",
        "    columns = ncol(df),",
        "    size_mb = size_mb,",
        "    cached_at = Sys.time()",
        "  )",
        "  ",
        "  metadata_json <- jsonlite::toJSON(metadata, auto_unbox = TRUE)",
        "  redis_con$SET(paste0('meta:', name), metadata_json)",
        "",
        "  # Add to index",
        "  redis_con$SADD('dataframe_index', name)",
        "",
        "  cat('💾 Cached DataFrame:', name, '\\n')",
        "  cat('📊 Size:', nrow(df), 'rows x', ncol(df), 'columns\\n')",
        "  cat('💿 Memory:', round(size_mb, 2), 'MB\\n')",
        "  cat('✅ Available in Redis cache (CSV format)\\n\\n')",
        "}",
        "",
        "# Load cached dataframes using the cache function",
        "# Note: Replace 'meta_json_string' with actual JSON metadata",
        ""
    ]
    
    # Generate main data loading
    if start:
        if isinstance(start, list) and len(start) > 0:
            df_name = start[0]
        elif isinstance(start, str):
            df_name = start
        else:
            df_name = "main_data"
        
        lines.append(f'# Load main dataframe')
        lines.append(f'current_df <- load_cached_dataframe("{escape_string(df_name)}", meta_json_{escape_string(df_name)})')
        lines.append('')
    else:
        lines.append('# No start dataframe specified')
        lines.append('current_df <- NULL')
        lines.append('')
    
    # Process each step
    step_counter = 1
    for step in steps:
        op = step.get('op', '').lower()
        params = step.get('params', {})
        
        lines.append(f'# Step {step_counter}: {op}')
        
        if op == 'load':
            name = params.get('name', '')
            lines.append(f'current_df <- load_cached_dataframe("{escape_string(name)}", meta_json_{escape_string(name)})')
            
        elif op == 'filter':
            filters = params.get('filters', [])
            combine = params.get('combine', 'and').lower()
            
            if filters:
                filter_parts = []
                for f in filters:
                    col = f.get('col', '')
                    op_name = f.get('op', 'eq')
                    value = f.get('value', '')
                    
                    if op_name == 'eq':
                        filter_parts.append(f'{col} == "{escape_string(str(value))}"')
                    elif op_name == 'ne':
                        filter_parts.append(f'{col} != "{escape_string(str(value))}"')
                    elif op_name == 'gt':
                        filter_parts.append(f'{col} > {value}')
                    elif op_name == 'gte':
                        filter_parts.append(f'{col} >= {value}')
                    elif op_name == 'lt':
                        filter_parts.append(f'{col} < {value}')
                    elif op_name == 'lte':
                        filter_parts.append(f'{col} <= {value}')
                    elif op_name == 'contains':
                        filter_parts.append(f'str_detect({col}, "{escape_string(str(value))}")')
                    elif op_name == 'startswith':
                        filter_parts.append(f'str_starts({col}, "{escape_string(str(value))}")')
                    elif op_name == 'endswith':
                        filter_parts.append(f'str_ends({col}, "{escape_string(str(value))}")')
                    elif op_name == 'isnull':
                        filter_parts.append(f'is.na({col})')
                    elif op_name == 'notnull':
                        filter_parts.append(f'!is.na({col})')
                    elif op_name == 'in':
                        if isinstance(value, list):
                            values_str = ', '.join([f'"{escape_string(str(v))}"' for v in value])
                            filter_parts.append(f'{col} %in% c({values_str})')
                        else:
                            filter_parts.append(f'{col} %in% c("{escape_string(str(value))}")')
                    elif op_name == 'nin':
                        if isinstance(value, list):
                            values_str = ', '.join([f'"{escape_string(str(v))}"' for v in value])
                            filter_parts.append(f'!({col} %in% c({values_str}))')
                        else:
                            filter_parts.append(f'!({col} %in% c("{escape_string(str(value))}"))')
                
                if filter_parts:
                    if combine == 'or':
                        filter_expr = ' | '.join([f'({f})' for f in filter_parts])
                    else:  # and
                        filter_expr = ' & '.join([f'({f})' for f in filter_parts])
                    
                    lines.append(f'current_df <- current_df %>% filter({filter_expr})')
            
        elif op == 'select':
            columns = params.get('columns', [])
            exclude = params.get('exclude', False)
            
            if columns:
                if exclude:
                    cols_str = ', '.join([f'-{col}' for col in columns])
                    lines.append(f'current_df <- current_df %>% select({cols_str})')
                else:
                    cols_str = ', '.join(columns)
                    lines.append(f'current_df <- current_df %>% select({cols_str})')
                    
        elif op == 'groupby':
            by = params.get('by', [])
            aggs = params.get('aggs', {})
            
            if by:
                by_str = ', '.join(by)
                lines.append(f'current_df <- current_df %>% group_by({by_str})')
                
                if aggs:
                    agg_parts = []
                    for col, agg_ops in aggs.items():
                        if isinstance(agg_ops, list):
                            for agg_op in agg_ops:
                                if agg_op == 'count':
                                    agg_parts.append(f'{col}_{agg_op} = n()')
                                elif agg_op in ['sum', 'mean', 'min', 'max']:
                                    agg_parts.append(f'{col}_{agg_op} = {agg_op}({col}, na.rm = TRUE)')
                                elif agg_op == 'std':
                                    agg_parts.append(f'{col}_std = sd({col}, na.rm = TRUE)')
                        else:
                            agg_op = agg_ops
                            if agg_op == 'count':
                                agg_parts.append(f'{col}_{agg_op} = n()')
                            elif agg_op in ['sum', 'mean', 'min', 'max']:
                                agg_parts.append(f'{col}_{agg_op} = {agg_op}({col}, na.rm = TRUE)')
                            elif agg_op == 'std':
                                agg_parts.append(f'{col}_std = sd({col}, na.rm = TRUE)')
                    
                    if agg_parts:
                        agg_str = ', '.join(agg_parts)
                        lines.append(f'current_df <- current_df %>% summarise({agg_str})')
                        
        elif op == 'sort':
            by = params.get('by', [])
            ascending = params.get('ascending', True)
            
            if by:
                if ascending:
                    by_str = ', '.join(by)
                    lines.append(f'current_df <- current_df %>% arrange({by_str})')
                else:
                    by_str = ', '.join([f'desc({col})' for col in by])
                    lines.append(f'current_df <- current_df %>% arrange({by_str})')
                    
        elif op == 'mutate':
            column = params.get('column', '')
            expression = params.get('expression', '')
            
            if column and expression:
                # Basic expression handling - could be enhanced
                lines.append(f'current_df <- current_df %>% mutate({column} = {escape_string(expression)})')
                
        elif op == 'rename':
            columns = params.get('columns', {})
            
            if columns:
                rename_parts = []
                for new_name, old_name in columns.items():
                    rename_parts.append(f'{new_name} = {old_name}')
                rename_str = ', '.join(rename_parts)
                lines.append(f'current_df <- current_df %>% rename({rename_str})')
                
        elif op == 'merge':
            others = params.get('with', []) or params.get('others', [])
            how = params.get('how', 'inner')
            keys = params.get('keys', []) or params.get('on', [])
            left_on = params.get('left_on', [])
            right_on = params.get('right_on', [])
            
            if others:
                for other_df in others:
                    lines.append(f'other_df <- load_cached_dataframe("{escape_string(other_df)}", meta_json_{escape_string(other_df)})')
                    
                    # R dplyr join mapping
                    join_func = {
                        'inner': 'inner_join',
                        'left': 'left_join', 
                        'right': 'right_join',
                        'outer': 'full_join'
                    }.get(how, 'inner_join')
                    
                    if left_on and right_on:
                        by_clause = []
                        for l, r in zip(left_on, right_on):
                            by_clause.append(f'"{l}" = "{r}"')
                        by_str = f'by = c({", ".join(by_clause)})'
                        lines.append(f'current_df <- current_df %>% {join_func}(other_df, {by_str})')
                    elif keys:
                        keys_str = ', '.join([f'"{k}"' for k in keys])
                        by_str = f'by = c({keys_str})'
                        lines.append(f'current_df <- current_df %>% {join_func}(other_df, {by_str})')
                    else:
                        lines.append(f'current_df <- current_df %>% {join_func}(other_df)')
                        
        elif op == 'chain_pipeline':
            pipeline_name = params.get('pipeline', '')
            lines.append(f'# Chain pipeline: {pipeline_name}')
            lines.append(f'# Note: Manual implementation required for chained pipeline "{pipeline_name}"')
            
        else:
            lines.append(f'# Unknown operation: {op}')
            lines.append(f'# Parameters: {json.dumps(params)}')
        
        lines.append('')
        step_counter += 1
    
    # Add final output
    lines.extend([
        '# Display final result',
        'print(current_df)',
        '',
        '# Cache the final result',
        'cache_dataframe(current_df, "pipeline_result")',
        '',
        '# Save result to CSV (optional)',
        '# write_csv(current_df, "result.csv")'
    ])
    
    return '\n'.join(lines)


def generate_python_code(pipeline_data: Dict[str, Any]) -> str:
    """Generate Python code for a chained pipeline"""
    
    steps = pipeline_data.get('steps', [])
    start = pipeline_data.get('start')
    
    lines = [
        "# Generated Python code for chained pipeline",
        "# Load required libraries",
        "import pandas as pd",
        "import numpy as np",
        "from sklearn.preprocessing import StandardScaler, LabelEncoder",
        "import json",
        "import redis",
        "",
        "# Function to load cached dataframes",
        "def load_cached_dataframe(name, meta_json):",
        "    # Connect to Redis",
        "    redis_client = redis.Redis(host='localhost', port=6379, db=0)",
        "    ",
        "    # Fetch data from Redis cache",
        "    cached_data = redis_client.get(f'dataframe:{name}')",
        "    ",
        "    if cached_data is None:",
        "        # Fallback to meta_json if Redis data not available",
        "        df = pd.DataFrame(json.loads(meta_json))",
        "    else:",
        "        # Parse data from Redis",
        "        df = pd.DataFrame(json.loads(cached_data.decode('utf-8')))",
        "    ",
        "    print(f'📂 Loaded from cache: {name}')",
        "    print(f'📊 Size: {df.shape[0]} rows x {df.shape[1]} columns')",
        "    print('⚡ Instant loading from RAM!\\n')",
        "    return df",
        "",
        "# Function to cache dataframes",
        "def cache_dataframe(df, name):",
        "    # Connect to Redis",
        "    redis_client = redis.Redis(host='localhost', port=6379, db=0)",
        "    ",
        "    # Calculate memory usage",
        "    size_mb = df.memory_usage(deep=True).sum() / 1024**2",
        "    ",
        "    # Convert to JSON and cache",
        "    df_json = df.to_json(orient='records', date_format='iso')",
        "    redis_client.set(f'dataframe:{name}', df_json)",
        "    ",
        "    # Store metadata",
        "    metadata = {",
        "        'name': name,",
        "        'rows': len(df),",
        "        'columns': len(df.columns),",
        "        'size_mb': size_mb,",
        "        'cached_at': pd.Timestamp.now().isoformat()",
        "    }",
        "    ",
        "    metadata_json = json.dumps(metadata)",
        "    redis_client.set(f'meta:{name}', metadata_json)",
        "",
        "    # Add to index",
        "    redis_client.sadd('dataframe_index', name)",
        "",
        "    print(f'💾 Cached DataFrame: {name}')",
        "    print(f'📊 Size: {len(df)} rows x {len(df.columns)} columns')",
        "    print(f'💿 Memory: {round(size_mb, 2)} MB')",
        "    print('✅ Available in Redis cache (CSV format)\\n')",
        "",
        "# Load cached dataframes using the cache function",
        "# Note: Replace 'meta_json_string' with actual JSON metadata",
        ""
    ]
    
    # Generate main data loading
    if start:
        if isinstance(start, list) and len(start) > 0:
            df_name = start[0]
        elif isinstance(start, str):
            df_name = start
        else:
            df_name = "main_data"
        
        lines.append(f'# Load main dataframe')
        lines.append(f'current_df = load_cached_dataframe("{escape_string(df_name)}", meta_json_{escape_string(df_name)})')
        lines.append('')
    else:
        lines.append('# No start dataframe specified')
        lines.append('current_df = None')
        lines.append('')
    
    # Process each step
    step_counter = 1
    for step in steps:
        op = step.get('op', '').lower()
        params = step.get('params', {})
        
        lines.append(f'# Step {step_counter}: {op}')
        
        if op == 'load':
            name = params.get('name', '')
            lines.append(f'current_df = load_cached_dataframe("{escape_string(name)}", meta_json_{escape_string(name)})')
            
        elif op == 'filter':
            filters = params.get('filters', [])
            combine = params.get('combine', 'and').lower()
            
            if filters:
                filter_parts = []
                for f in filters:
                    col = f.get('col', '')
                    op_name = f.get('op', 'eq')
                    value = f.get('value', '')
                    
                    if op_name == 'eq':
                        filter_parts.append(f'(current_df["{col}"] == "{escape_string(str(value))}")')
                    elif op_name == 'ne':
                        filter_parts.append(f'(current_df["{col}"] != "{escape_string(str(value))}")')
                    elif op_name == 'gt':
                        filter_parts.append(f'(current_df["{col}"] > {value})')
                    elif op_name == 'gte':
                        filter_parts.append(f'(current_df["{col}"] >= {value})')
                    elif op_name == 'lt':
                        filter_parts.append(f'(current_df["{col}"] < {value})')
                    elif op_name == 'lte':
                        filter_parts.append(f'(current_df["{col}"] <= {value})')
                    elif op_name == 'contains':
                        filter_parts.append(f'(current_df["{col}"].str.contains("{escape_string(str(value))}", na=False))')
                    elif op_name == 'startswith':
                        filter_parts.append(f'(current_df["{col}"].str.startswith("{escape_string(str(value))}", na=False))')
                    elif op_name == 'endswith':
                        filter_parts.append(f'(current_df["{col}"].str.endswith("{escape_string(str(value))}", na=False))')
                    elif op_name == 'isnull':
                        filter_parts.append(f'(current_df["{col}"].isnull())')
                    elif op_name == 'notnull':
                        filter_parts.append(f'(current_df["{col}"].notnull())')
                    elif op_name == 'in':
                        if isinstance(value, list):
                            values_str = repr(value)
                        else:
                            values_str = f'[{repr(value)}]'
                        filter_parts.append(f'(current_df["{col}"].isin({values_str}))')
                    elif op_name == 'nin':
                        if isinstance(value, list):
                            values_str = repr(value)
                        else:
                            values_str = f'[{repr(value)}]'
                        filter_parts.append(f'(~current_df["{col}"].isin({values_str}))')
                
                if filter_parts:
                    if combine == 'or':
                        filter_expr = ' | '.join(filter_parts)
                    else:  # and
                        filter_expr = ' & '.join(filter_parts)
                    
                    lines.append(f'current_df = current_df[{filter_expr}]')
            
        elif op == 'select':
            columns = params.get('columns', [])
            exclude = params.get('exclude', False)
            
            if columns:
                if exclude:
                    lines.append(f'current_df = current_df.drop(columns={repr(columns)})')
                else:
                    lines.append(f'current_df = current_df[{repr(columns)}]')
                    
        elif op == 'groupby':
            by = params.get('by', [])
            aggs = params.get('aggs', {})
            
            if by:
                lines.append(f'grouped = current_df.groupby({repr(by)})')
                
                if aggs:
                    agg_dict = {}
                    for col, agg_ops in aggs.items():
                        if isinstance(agg_ops, list):
                            agg_dict[col] = agg_ops
                        else:
                            agg_dict[col] = [agg_ops]
                    
                    # Convert agg operations to pandas equivalents
                    pandas_agg_dict = {}
                    for col, ops in agg_dict.items():
                        pandas_ops = []
                        for op in ops:
                            if op == 'std':
                                pandas_ops.append('std')
                            elif op in ['sum', 'mean', 'min', 'max', 'count']:
                                pandas_ops.append(op)
                        pandas_agg_dict[col] = pandas_ops
                    
                    lines.append(f'current_df = grouped.agg({repr(pandas_agg_dict)}).reset_index()')
                else:
                    lines.append(f'current_df = grouped.size().reset_index(name="count")')
                        
        elif op == 'sort':
            by = params.get('by', [])
            ascending = params.get('ascending', True)
            
            if by:
                lines.append(f'current_df = current_df.sort_values(by={repr(by)}, ascending={ascending})')
                    
        elif op == 'mutate':
            column = params.get('column', '')
            expression = params.get('expression', '')
            
            if column and expression:
                # Basic expression handling - could be enhanced
                lines.append(f'current_df["{column}"] = {escape_string(expression)}')
                
        elif op == 'rename':
            columns = params.get('columns', {})
            
            if columns:
                lines.append(f'current_df = current_df.rename(columns={repr(columns)})')
                
        elif op == 'merge':
            others = params.get('with', []) or params.get('others', [])
            how = params.get('how', 'inner')
            keys = params.get('keys', []) or params.get('on', [])
            left_on = params.get('left_on', [])
            right_on = params.get('right_on', [])
            
            if others:
                for other_df in others:
                    lines.append(f'other_df = load_cached_dataframe("{escape_string(other_df)}", meta_json_{escape_string(other_df)})')
                    
                    if left_on and right_on:
                        lines.append(f'current_df = current_df.merge(other_df, left_on={repr(left_on)}, right_on={repr(right_on)}, how="{how}")')
                    elif keys:
                        lines.append(f'current_df = current_df.merge(other_df, on={repr(keys)}, how="{how}")')
                    else:
                        lines.append(f'current_df = current_df.merge(other_df, how="{how}")')
                        
        elif op == 'chain_pipeline':
            pipeline_name = params.get('pipeline', '')
            lines.append(f'# Chain pipeline: {pipeline_name}')
            lines.append(f'# Note: Manual implementation required for chained pipeline "{pipeline_name}"')
            
        else:
            lines.append(f'# Unknown operation: {op}')
            lines.append(f'# Parameters: {repr(params)}')
        
        lines.append('')
        step_counter += 1
    
    # Add final output
    lines.extend([
        '# Display final result',
        'print(current_df.head())',
        'print(f"Shape: {current_df.shape}")',
        '',
        '# Cache the final result',
        'cache_dataframe(current_df, "pipeline_result")',
        '',
        '# Save result to CSV (optional)',
        '# current_df.to_csv("result.csv", index=False)'
    ])
    
    return '\n'.join(lines)