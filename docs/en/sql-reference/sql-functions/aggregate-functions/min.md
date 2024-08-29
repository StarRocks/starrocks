---
displayed_sidebar: docs
---

# min

## Description

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
