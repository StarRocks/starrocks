# STDDEV_SAMP

## description

### Syntax

```Haskell
STDDEV_SAMP(expr)
```

It returns the sample standard deviation of the expr expression

## example

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
