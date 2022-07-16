# VAR_SAMP,VARIANCE_SAMP

## description

### Syntax

```Haskell
VAR_SAMP(expr)
```

It returns the sample variance of the expr expression.

## example

```plain text
MySQL > select var_samp(scan_rows)
from log_statis
group by datetime;
+-----------------------+
| var_samp(`scan_rows`) |
+-----------------------+
|    5.6227132145741789 |
+-----------------------+
```

## keyword

VAR_SAMP,VARIANCE_SAMP,VAR,SAMP,VARIANCE
