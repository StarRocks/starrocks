# MIN

## description

### Syntax

```Haskell
MIN(expr)
```

It returns the minimum value of the expr expression.

## example

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
