---
displayed_sidebar: docs
---

# radians

## Description

Converts `x` from an angle to a radian.

## Syntax

```Haskell
REDIANS(x);
```

## Parameters

`x`: supports the DOUBLE data type.

## Return value

Returns a value of the DOUBLE data type.

## Examples

```Plain
mysql> select radians(90);
+--------------------+
| radians(90)        |
+--------------------+
| 1.5707963267948966 |
+--------------------+
1 row in set (0.00 sec)
```
