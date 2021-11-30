# VARIANCE,VAR_POP,VARIANCE_POP

## description

### Syntax

```Haskell
VARIANCE(expr)
```

It returns the variance of the expr expression.

## example

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
