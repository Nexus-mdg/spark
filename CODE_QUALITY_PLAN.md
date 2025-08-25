# Dataframe API Code Quality Improvement Plan

## Summary of Current Status

### ✅ Completed in Main Formatting PR
- Applied `black` formatting to all Python files
- Applied `isort` import sorting to all files  
- Fixed 200+ blank line issues (E302, E305, W293)
- Fixed trailing whitespace issues (W291)
- Fixed missing newlines at end of files (W292)
- Fixed basic import style issues

### 📋 Remaining Issues by File

#### High Priority Files (Need Individual PRs)

**1. operations/pipeline_engine.py** (12 issues remaining)
- Mostly line length violations (E501)
- 1 blank line issue (W293)
- Critical for pipeline functionality

**2. operations/engine_router.py** (12 issues remaining)
- Line length violations (E501)
- Some logical issues may need review

**3. routes/operations.py** (11 issues remaining)
- Line length violations (E501)
- Important for API functionality

**4. operations/spark_engine.py** (10 issues remaining)
- Line length violations (E501)
- Spark integration critical

**5. routes/dataframes.py** (7 issues remaining)
- Line length violations (E501)
- Core API endpoints

#### Medium Priority Files

**6. operations/dataframe_ops.py** (3 issues)
- Minor line length issues

**7. operations/pandas_engine.py** (2 issues)
- Minor line length issues

**8. utils/helpers.py** (1 issue)
- Single line length issue

#### Special Case

**app_old.py** (194 issues)
- Contains 194 undefined name errors (F821)
- Appears to be a legacy/backup file
- Decision needed: Fix or Remove?
- Not referenced anywhere in the codebase

## Recommended Approach for Individual PRs

### For Each File Above:

1. **Create separate branch** for each file
2. **Focus on specific issues:**
   - Line length violations (E501) - Break long lines logically
   - Any remaining import issues
   - Logic/syntax errors
3. **Test functionality** after changes
4. **Create focused PR** with clear description of fixes

### Tools to Use:

```bash
# Check specific file
flake8 --max-line-length=100 --extend-ignore=E203,W503 <filename>

# Auto-format (already done)
black --line-length=100 <filename>
isort --line-length=100 <filename>

# Manual line length fixes needed for:
# - Long error messages
# - Function calls with many parameters
# - Import statements
# - String concatenations
```

### Line Length Fix Strategies:

1. **Error Messages**: Break into multi-line strings
   ```python
   # Before:
   "error": "This is a very long error message that exceeds 100 characters and needs to be broken"
   
   # After:
   "error": (
       "This is a very long error message that exceeds 100 characters "
       "and needs to be broken"
   )
   ```

2. **Function Calls**: Break parameters onto separate lines
   ```python
   # Before:
   function_call(param1, param2, param3, param4, param5, param6)
   
   # After:
   function_call(
       param1, param2, param3,
       param4, param5, param6
   )
   ```

3. **Import Statements**: Use parentheses for multiple imports
   ```python
   # Before:
   from module import item1, item2, item3, item4, item5, item6
   
   # After:
   from module import (
       item1, item2, item3,
       item4, item5, item6
   )
   ```

## Special Considerations

### app_old.py Decision Required:
- **Option A**: Fix all 194 undefined name errors by adding proper imports
- **Option B**: Remove file if it's truly unused legacy code
- **Option C**: Document as legacy and exclude from linting

### Testing:
- Each PR should include verification that the API still works
- Run existing tests if available
- Manual testing of critical endpoints

## Implementation Priority:

1. **operations/pipeline_engine.py** - Core pipeline functionality
2. **operations/engine_router.py** - Engine routing logic  
3. **routes/operations.py** - API endpoints
4. **operations/spark_engine.py** - Spark integration
5. **routes/dataframes.py** - DataFrame management
6. **Others** - Lower priority cosmetic fixes
7. **app_old.py** - Requires decision on approach