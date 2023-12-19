---
displayed_sidebar: "English"
---

# percentile_hash

## Description

Constructs DOUBLE values as PERCENTILE values.

## Syntax

```Haskell
PERCENTILE_HASH(x);
```

## Parameters

`x`: The supported data type is DOUBLE.

## Return value

Returns a PERCENTILE value.

## Examples

```Plain Text
mysql> select percentile_approx_raw(percentile_hash(234.234), 0.99);
+-------------------------------------------------------+
| percentile_approx_raw(percentile_hash(234.234), 0.99) |
+-------------------------------------------------------+
|                                    234.23399353027344 |
+-------------------------------------------------------+
1 row in set (0.00 sec)
```
