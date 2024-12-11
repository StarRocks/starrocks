---
displayed_sidebar: docs
---

# min

<<<<<<< HEAD
## Description
=======

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

Returns the minimum value of the expr expression.

## Syntax

```Haskell
MIN(expr)
```

## Examples

```plain text
MySQL > select min(scan_rows)
from log_statis
group by datetime;
+------------------+
| min(`scan_rows`) |
+------------------+
|                0 |
+------------------+
```

## keyword

MIN
