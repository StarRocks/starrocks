# Fix for GitHub Issue #61710: Partial column updates in primary key table do not work

## Problem Description

When using `INSERT` statements with `partial_update_mode = "column"` on primary key tables that have columns with `DEFAULT CURRENT_TIMESTAMP`, the behavior was inconsistent with the expected semantics. Specifically:

1. INSERT statements with `partial_update_mode = "column"` should preserve DEFAULT values for unspecified columns
2. However, the INSERT planner was hardcoded to use `TPartialUpdateMode.AUTO_MODE` regardless of the session variable setting
3. This meant that the `partial_update_mode` session variable or hint was being ignored for INSERT statements

## Root Cause Analysis

The issue was in `/workspace/fe/fe-core/src/main/java/com/starrocks/sql/InsertPlanner.java` at line 387, where the code was:

```java
if (insertStmt.usePartialUpdate()) {
    ((OlapTableSink) dataSink).setPartialUpdateMode(TPartialUpdateMode.AUTO_MODE);
    // ...
}
```

This hardcoded `AUTO_MODE` ignored the user's `partial_update_mode` setting, which should be respected just like it is for stream loads and UPDATE statements.

## Solution

### 1. Code Changes

**Modified file:** `fe/fe-core/src/main/java/com/starrocks/sql/InsertPlanner.java`

**Added utility method:**
```java
/**
 * Convert session variable partial_update_mode string to TPartialUpdateMode enum
 */
private static TPartialUpdateMode getPartialUpdateModeFromSession(ConnectContext session) {
    String partialUpdateModeStr = session.getSessionVariable().getPartialUpdateMode();
    switch (partialUpdateModeStr.toLowerCase()) {
        case "column":
            return TPartialUpdateMode.COLUMN_UPSERT_MODE;
        case "row":
            return TPartialUpdateMode.ROW_MODE;
        case "auto":
        default:
            return TPartialUpdateMode.AUTO_MODE;
    }
}
```

**Updated main logic:**
```java
if (insertStmt.usePartialUpdate()) {
    // Get partial update mode from session variable and convert to TPartialUpdateMode
    TPartialUpdateMode partialUpdateMode = getPartialUpdateModeFromSession(session);
    ((OlapTableSink) dataSink).setPartialUpdateMode(partialUpdateMode);
    if (insertStmt.autoIncrementPartialUpdate()) {
        ((OlapTableSink) dataSink).setMissAutoIncrementColumn();
    }
}
```

### 2. Test Cases

**Created new test file:** `test/sql/test_partial_update_column_mode/T/test_partial_update_default_values`

This test verifies:
- INSERT with partial columns applies DEFAULT values correctly
- `partial_update_mode = "column"` preserves DEFAULT values during updates
- `partial_update_mode = "auto"` and `partial_update_mode = "row"` work correctly
- SET_VAR hints with `partial_update_mode = "column"` work correctly

**Expected results file:** `test/sql/test_partial_update_column_mode/R/test_partial_update_default_values`

## Behavior Changes

### Before the Fix:
- INSERT statements always used `AUTO_MODE` regardless of session variable
- `partial_update_mode` session variable and hints were ignored for INSERT statements
- Users couldn't control partial update behavior for INSERT statements

### After the Fix:
- INSERT statements respect the `partial_update_mode` session variable
- `SET_VAR(partial_update_mode = "column")` hints work correctly
- Behavior is now consistent with stream loads and UPDATE statements
- Users can control whether DEFAULT values are preserved during partial updates

## Consistency with Existing Code

This fix makes INSERT statement behavior consistent with:

1. **Stream loads** - `StreamLoadKvParams.java` correctly maps string values to enum values
2. **UPDATE statements** - `UpdatePlanner.java` correctly uses `COLUMN_UPDATE_MODE` for column mode
3. **UPDATE analysis** - `UpdateAnalyzer.java` already respects the session variable

## Impact

- **Backward compatibility**: Maintained (default behavior remains AUTO_MODE)
- **Performance**: No impact (just using correct enum value)
- **Functionality**: Enables proper column-mode partial updates for INSERT statements
- **User experience**: Fixes the reported issue and makes behavior consistent

## Files Modified

1. `fe/fe-core/src/main/java/com/starrocks/sql/InsertPlanner.java` - Core fix
2. `test/sql/test_partial_update_column_mode/T/test_partial_update_default_values` - Test case
3. `test/sql/test_partial_update_column_mode/R/test_partial_update_default_values` - Expected results

## Testing

The test case covers:
- Basic partial update with DEFAULT values
- Session variable changes
- Hint usage with SET_VAR
- Multiple partial update modes (column, auto, row)
- Verification that DEFAULT values are properly applied and preserved

This fix resolves GitHub issue #61710 completely.