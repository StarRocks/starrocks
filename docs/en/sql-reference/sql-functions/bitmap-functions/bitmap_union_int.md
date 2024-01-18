---
displayed_sidebar: "English"
---

# bitmap_union_int

## Description

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
