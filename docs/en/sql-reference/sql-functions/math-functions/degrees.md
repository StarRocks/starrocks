---
displayed_sidebar: docs
---

# degrees

## Description

Converts the angle in radians `x` to degrees.

## Syntax

```SQL
DEGREES(x);
```

## Parameters

`x`: the angle in radians. DOUBLE is supported.

## Return value

Returns a value of the DOUBLE data type.

## Examples

```Plaintext
mysql> select degrees(3.1415926535898);
+--------------------------+
| degrees(3.1415926535898) |
+--------------------------+
|        180.0000000000004 |
+--------------------------+
1 row in set (0.07 sec)
```
