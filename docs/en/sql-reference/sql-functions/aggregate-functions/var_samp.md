---
displayed_sidebar: docs
---

# var_samp,variance_samp

## Description

Returns the sample variance of an expression. Since v2.5.10, this function can also be used as a window function.

## Syntax

```Haskell
VAR_SAMP(expr)
```

## Parameters

`expr`: the expression. If it is a table column, it must evaluate to TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, or DECIMAL.

## Return value

Returns a DOUBLE value.

## Examples

```plaintext
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
