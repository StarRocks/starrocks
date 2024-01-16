---
displayed_sidebar: "English"
---

# subdivide_bitmap

## Description

Splits a large bitmap into multiple sub-bitmaps.

This function is mainly used to export bitmaps. Bitmaps that are too large will exceed the maximum packet size allowed in the MySQL protocol.

This function is supported from v2.5.

## Syntax

```Haskell
BITMAP subdivide_bitmap(bitmap, length)
```

## Parameters

`bitmap`: The bitmap that needs to be split, required.
`length`: The maximum length of each sub-bitmap, required. Bitmaps larger than this value will be split into multiple small bitmaps.

## Return value

Returns multiple sub-bitmaps not larger than `length`.

## Examples

Suppose there is a table `t1`, in which the `c2` column is a BITMAP column.

```Plain
-- Use bitmap_to_string() to convert values in `c2` into a string.
mysql> select c1, bitmap_to_string(c2) from t1;
+------+----------------------+
| c1   | bitmap_to_string(c2) |
+------+----------------------+
|    1 | 1,2,3,4,5,6,7,8,9,10 |
+------+----------------------+

-- Split `c2` into small bitmaps whose maximum length is 3.

mysql> select c1, bitmap_to_string(subdivide_bitmap) from t1, subdivide_bitmap(c2, 3);
+------+------------------------------------+
| c1   | bitmap_to_string(subdivide_bitmap) |
+------+------------------------------------+
|    1 | 1,2,3                              |
|    1 | 4,5,6                              |
|    1 | 7,8,9                              |
|    1 | 10                                 |
+------+------------------------------------+
```
