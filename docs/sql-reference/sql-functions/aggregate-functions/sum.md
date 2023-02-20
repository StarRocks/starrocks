# SUM

## Description

Resurns the sum of all values of the selected fields.

## Syntax

```Haskell
SUM(expr)
```

## Examples

```plain text
MySQL > select sum(scan_rows)
from log_statis
group by datetime;
+------------------+
| sum(`scan_rows`) |
+------------------+
|       8217360135 |
+------------------+
```

## keyword

SUM
