---
displayed_sidebar: "English"
---

# database

## Description

Returns the name of the current database. If no database is selected, an empty value is returned.

## Syntax

```Haskell
database()
```

## Parameters

This function does not require parameters.

## Return value

Returns the name of the current database as a string.

## Examples

```sql
-- Select a destination database.
use db_test

-- Query the name of the current database.
select database();
+------------+
| DATABASE() |
+------------+
| db_test    |
+------------+
```

## See also

[USE](../../sql-statements/data-definition/USE.md): Switches to a destination database.
