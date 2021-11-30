# SUM

## description

### Syntax

```Haskell
SUM(expr)
```

It resurns the sum of all values of the selected fields.

## example

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
