---
displayed_sidebar: docs
---

# max

<<<<<<< HEAD
## Description
=======

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

Returns the maximum value of the expr expression.

## Syntax

```Haskell
MAX(expr)
```

## Examples

```plain text
MySQL > select max(scan_rows)
from log_statis
group by datetime;
+------------------+
| max(`scan_rows`) |
+------------------+
|          4671587 |
+------------------+
```

## keyword

MAX
