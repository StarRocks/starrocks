
# STDDEV,STDDEV_POP

## Description

Returns the standard deviation of the expr expression.

## Syntax

```Haskell
STDDEV(expr)
```

## Examples

```plain text
MySQL > select stddev(scan_rows)
from log_statis
group by datetime;
+---------------------+
| stddev(`scan_rows`) |
+---------------------+
|  2.3736656687790934 |
+---------------------+

MySQL > select stddev_pop(scan_rows)
from log_statis
group by datetime;
+-------------------------+
| stddev_pop(`scan_rows`) |
+-------------------------+
|      2.3722760595994914 |
+-------------------------+
```

## keyword

STDDEV,STDDEV_POP,POP
