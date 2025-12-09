---
title: CURRENT_WAREHOUSE
---

Returns the effective warehouse name for the current session. The result reflects the warehouse set via `SET WAREHOUSE` or the default warehouse if none was explicitly selected.

## Syntax

```SQL
current_warehouse()
```

## Return value

`VARCHAR`. The warehouse name currently in use for the session.

## Examples

```SQL
mysql> SET WAREHOUSE = 'compute_pool_x';
mysql> SELECT current_warehouse();
+---------------------+
| CURRENT_WAREHOUSE() |
+---------------------+
| compute_pool_x      |
+---------------------+
1 row in set (0.00 sec)
```

