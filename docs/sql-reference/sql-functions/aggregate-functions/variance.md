# VARIANCE,VAR_POP,VARIANCE_POP

## Description

Returns the variance of an expression.

## Syntax

```Haskell
VARIANCE(expr)
```

## Return value

Returns a numerical value.

If the type of `expr` is DECIMAL, this function returns a DECIMAL value, or else returns a DOUBLE value.

## Examples

```plain text
MySQL > select variance(scan_rows)
from log_statis
group by datetime;
+-----------------------+
| variance(`scan_rows`) |
+-----------------------+
|    5.6183332881176211 |
+-----------------------+

MySQL > select var_pop(scan_rows)
from log_statis
group by datetime;
+----------------------+
| var_pop(`scan_rows`) |
+----------------------+
|   5.6230744719006163 |
+----------------------+
```

## keyword

VARIANCE,VAR_POP,VARIANCE_POP,VAR,POP
