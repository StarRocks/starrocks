---
displayed_sidebar: docs
---

# min

<<<<<<< HEAD
## Description
=======

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

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
