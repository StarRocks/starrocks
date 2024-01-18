---
displayed_sidebar: "English"
---

# coalesce

## Description

Returns the first non-NULL expression among the input parameters. Returns NULL if non-NULL expressions cannot be found.

## Syntax

```Haskell
coalesce(expr1,...);
```

## Parameters

`expr1`: the input expressions, which must evaluate to compatible data types. The supported data types are BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, DATETIME, DATE, DECIMALV2, DECIMAL32, DECIMAL64, DECIMAL128, VARCHAR, BITMAP, PERCENTILE, HLL, and TIME.

## Return value

The return value has the same type as `expr1`.

## Examples

```Plain Text
mysql> select coalesce(3,NULL,1,1);
+-------------------------+
| coalesce(3, NULL, 1, 1) |
+-------------------------+
|                       3 |
+-------------------------+
1 row in set (0.00 sec)
```
