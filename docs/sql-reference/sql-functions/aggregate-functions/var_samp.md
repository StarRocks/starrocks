# VAR_SAMP,VARIANCE_SAMP

## Description

Returns the sample variance of the expr expression.

## Syntax

```Haskell
VAR_SAMP(expr)
```

## Examples

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
