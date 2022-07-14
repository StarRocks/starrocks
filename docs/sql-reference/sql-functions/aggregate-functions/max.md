# MAX

## description

### Syntax

```Haskell
MAX(expr)
```

It returns the maximum value of the expr expression.

## example

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
