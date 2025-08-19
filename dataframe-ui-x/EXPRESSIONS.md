# Mutate Expressions Guide

This guide shows how to write expressions for the Mutate operation (Operations page) and the mutate step (Chained Operations).

What an expression runs with
- Modes
  - vector (default): expression evaluated once on the whole DataFrame; return a Series (len=df) or a scalar (broadcasts to all rows).
  - row: expression evaluated per-row with r available.
- Available names
  - pd (pandas), np (numpy)
  - df: the current pandas DataFrame
  - col(name): shortcut for df[name] (a Series)
  - r: only in row mode; the current row (pandas Series). Access with r['col'].
- Safe builtins: abs, min, max, round, int, float, str, bool, len

Tips
- Prefer vector mode. It's faster and matches pandas style.
- Convert to string before string ops: col('x').astype('string').
- When parsing dates, use errors='coerce' to get NaT on bad rows.
- When nulls must be neutral, use fillna(...). For aggregations, pandas by default skips NaN (skipna=True).

Examples

Dates
- Parse to datetime:
  - pd.to_datetime(col('date'), errors='coerce')
- Year / month / day:
  - pd.to_datetime(col('date'), errors='coerce').dt.year
  - pd.to_datetime(col('date'), errors='coerce').dt.month
  - pd.to_datetime(col('date'), errors='coerce').dt.day
- Month name and abbreviation:
  - Full: pd.to_datetime(col('date'), errors='coerce').dt.month_name()
  - Abbrev (Jan): pd.to_datetime(col('date'), errors='coerce').dt.strftime('%b')
  - Abbrev lower (jan): pd.to_datetime(col('date'), errors='coerce').dt.strftime('%b').str.lower()
- Year-month strings:
  - pd.to_datetime(col('date'), errors='coerce').dt.strftime('%Y-%m')
- Date difference in days:
  - (pd.to_datetime(col('end'), errors='coerce') - pd.to_datetime(col('start'), errors='coerce')).dt.days
- Compare with a month (DATE_INVESTIGATION > 2025-05):
  - pd.to_datetime(col('DATE_INVESTIGATION'), errors='coerce') > pd.Timestamp('2025-05-01')
  - Month-level strictly after May 2025: pd.to_datetime(col('DATE_INVESTIGATION'), errors='coerce') >= pd.Timestamp('2025-06-01')
  - Using periods (months): pd.to_datetime(col('DATE_INVESTIGATION'), errors='coerce').dt.to_period('M') > pd.Period('2025-05')
- R translation: mutate(month_name = month.abb[month(Date_proxy)])
  - Python: pd.to_datetime(col('Date_proxy'), errors='coerce').dt.strftime('%b')

Strings
- Substring (first 3 chars):
  - col('name').astype('string').str[:3]
- Concat with underscore:
  - col('first').astype('string') + '_' + col('last').astype('string')
- Lower / upper:
  - col('name').astype('string').str.lower()
  - col('name').astype('string').str.upper()
- Contains (case-insensitive):
  - col('text').astype('string').str.contains('foo', case=False, na=False)
- Starts/ends with:
  - col('code').astype('string').str.startswith('A', na=False)
  - col('code').astype('string').str.endswith('Z', na=False)
- Length and trim:
  - col('text').astype('string').str.len()
  - col('text').astype('string').str.strip()
- Replace substring:
  - col('text').astype('string').str.replace('old', 'new', regex=False)
- Map values with a dict:
  - col('status').map({'A':'Active','I':'Inactive'}).fillna('Unknown')

Numerics
- Row-wise sum/avg over multiple columns:
  - df[['a','b','c']].sum(axis=1, skipna=True)
  - df[['a','b','c']].mean(axis=1, skipna=True)
- Per-row safe sum of two columns with nulls to zero:
  - col('x').fillna(0) + col('y').fillna(0)
- Column aggregates (scalar; broadcast to all rows):
  - df['amount'].sum(skipna=True)
  - df['score'].mean(skipna=True)
  - df['score'].median(skipna=True)
- Z-score for a column (population std):
  - (col('x') - df['x'].mean()) / df['x'].std(ddof=0)
- Clip to range:
  - col('x').clip(lower=0, upper=100)

Booleans, counting, and null remove (R's na.rm)
- Flag rows Suspect == 'No data':
  - col('Suspect').astype('string').eq('No data')
- Count rows Suspect == 'No data' (scalar, like sum(..., na.rm=TRUE)):
  - (col('Suspect').astype('string').fillna('').eq('No data')).sum()
  - or: (col('Suspect') == 'No data').fillna(False).sum()
- Per-group counts aligned to each row (vector):
  - df.groupby('Group')['Suspect'].transform(lambda s: s.astype('string').fillna('').eq('No data').sum())

If/else and multi-branch
- If/else (R if_else):
  - np.where(col('score') >= 50, 'pass', 'fail')
- Multi-branch (R case_when):
  - np.select([
      col('age') < 18,
      col('age').between(18, 64, inclusive='both'),
      col('age') >= 65
    ], ['minor', 'adult', 'senior'], default='unknown')

Row mode
- Use only when you truly need cross-row side effects or complex per-row logic.
- Example: r['price'] * r['qty']
- Example (row-level string build): f"{r['code']}-{int(r['line']) if pd.notna(r['line']) else 'NA'}"

Group-wise stats aligned to rows (vector-friendly)
- Mean per group: df.groupby('grp')['x'].transform('mean')
- Sum per group: df.groupby('grp')['x'].transform('sum')
- Rank within group: df.groupby('grp')['x'].rank(method='dense')

Common recipes
- Extract with regex: col('email').astype('string').str.extract(r'@(.*)$', expand=False)
- Year-month lower, like 2025-jan: pd.to_datetime(col('date'), errors='coerce').dt.strftime('%Y-%b').str.lower()
- Safe division with zeros to NaN -> 0: (col('num')/col('den')).replace([np.inf, -np.inf], np.nan).fillna(0)

Pipeline step examples
- Single mutate (Operations):
  - target: after_may_2025
  - mode: vector
  - expr: pd.to_datetime(col('DATE_INVESTIGATION'), errors='coerce') >= pd.Timestamp('2025-06-01')
- Chained mutate (JSON step):
  - { "op": "mutate", "params": { "target": "month_name", "expr": "pd.to_datetime(col('Date_proxy'), errors='coerce').dt.strftime('%b')" } }

Limits
- Only pd, np, df, col, r (row mode) and safe builtins are available.
- Imports, filesystem, and arbitrary globals are blocked.
- Return must be a Series (len==len(df)) or a scalar; pandas will align/ broadcast on assignment.

