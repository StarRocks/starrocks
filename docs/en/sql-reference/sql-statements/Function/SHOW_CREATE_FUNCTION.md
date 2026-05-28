---
displayed_sidebar: docs
---

# SHOW CREATE FUNCTION

SHOW CREATE FUNCTION returns the `CREATE FUNCTION` DDL for a user-defined function.

## Syntax

```SQL
SHOW CREATE [GLOBAL] FUNCTION <function_name> ( <arg_type> [, ...] )
```

## Example

```sql
SHOW CREATE FUNCTION default_db.python_add(BIGINT);
```

```sql
CREATE FUNCTION default_db.python_add(BIGINT)
RETURNS BIGINT
PROPERTIES (
    "type" = "Python",
    "file" = "inline",
    "symbol" = "add",
    "input" = "arrow"
)
AS $$

import pyarrow.compute as pc

def add(x):
    return pc.add(x, 1)

$$
1 row in set (0.01 sec)
```

