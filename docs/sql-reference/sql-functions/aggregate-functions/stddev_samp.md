# STDDEV_SAMP

## Description

Returns the sample standard deviation of the expr expression.

## Syntax

```Haskell
STDDEV_SAMP(expr)
```

## Examples

```plain text
MySQL > select stddev_samp(scan_rows)
from log_statis
group by datetime;
+--------------------------+
| stddev_samp(`scan_rows`) |
+--------------------------+
|        2.372044195280762 |
+--------------------------+
```

## keyword

STDDEV_SAMP,STDDEV,SAMP
