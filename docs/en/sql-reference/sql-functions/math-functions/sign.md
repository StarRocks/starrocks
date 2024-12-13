---
displayed_sidebar: docs
---

# sign

<<<<<<< HEAD
## Description
=======

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

Returns the sign of `x`. A negative number, 0, or a positive number as input corresponds to `-1`, `0`, or `1` as output, respectively.

## Syntax

```Haskell
SIGN(x);
```

## Parameters

`x`: supports the DOUBLE data type.

## Return value

Returns a value of the FLOAT data type.

## Examples

```Plain
mysql> select sign(3.14159);
+---------------+
| sign(3.14159) |
+---------------+
|             1 |
+---------------+
1 row in set (0.02 sec)
```
