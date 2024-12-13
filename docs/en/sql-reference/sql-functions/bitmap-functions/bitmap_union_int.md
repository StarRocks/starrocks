---
displayed_sidebar: docs
---

# bitmap_union_int

<<<<<<< HEAD
## Description
=======

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

Count the number of different values ​​in columns of type TINYINT, SMALLINT and INT, return the sum of COUNT (DISTINCT expr) same.

## Syntax

```Haskell
BIGINT bitmap_union_int(expr)
```

### Parameters

`expr`: column expression. The supported column type is TINYINT, SMALLINT and INT.

## Return value

Returns a value of the BIGINT type.

## Examples

```Plaintext
mysql> select bitmap_union_int(k1) from tbl1;
+------------------------+
| bitmap_union_int(`k1`) |
+------------------------+
|                      2 |
+------------------------+
```
