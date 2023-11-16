---
displayed_sidebar: "Chinese"
---

# STDDEV_SAMP

## description

### Syntax

```Haskell
STDDEV_SAMP(expr)
```

返回expr表达式的样本标准差

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
