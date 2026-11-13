---
title: CURRENT_WAREHOUSE
<<<<<<< HEAD
=======
displayed_sidebar: docs
description: "Returns the effective warehouse name for the current session, reflecting the warehouse set via SET WAREHOUSE or the default warehouse."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
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

