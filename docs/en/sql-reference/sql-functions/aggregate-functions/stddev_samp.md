---
displayed_sidebar: "English"
---

# stddev_samp

## Description

Returns the sample standard deviation of an expression. Since v2.5.10, this function can also be used as a window function.

## Syntax

```Haskell
STDDEV_SAMP(expr)
```

## Parameters

`expr`: the expression. If it is a table column, it must evaluate to TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, or DECIMAL.

## Return value

Returns a DOUBLE value. The formula is as follows, where `n` represents the row count of the table:

![image](../../../assets/stddevsamp_formula.png)

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

## See also

[stddev](./stddev.md)

## keyword

STDDEV_SAMP,STDDEV,SAMP
