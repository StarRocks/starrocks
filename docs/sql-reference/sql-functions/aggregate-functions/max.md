# MAX

## Description

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
