---
displayed_sidebar: "English"
---

# subdivide_bitmap

## Description

Split a large bitmap into multiple sub-bitmaps.

This function is mainly used to export bitmap. When the bitmap is too large, it will exceed the upper limit of the packet size of the Mysql protocol.

This function is supported from v2.5.

## Syntax

```Haskell
BITMAP subdivide_bitmap(bitmap, length)
```

## Parameters

`bitmap`: The bitmap that needs to be split, required.
`length`: The size of the split. Bitmaps larger than this value will be split into multiple small bitmaps. The size of each bitmap is smaller than this value, required.

## Return value

Split into multiple sub-bitmaps not larger than length.

## Examples

```Plain
mysql> select c1, bitmap_to_string(c2) from t1;
+------+----------------------+
| c1   | bitmap_to_string(c2) |
+------+----------------------+
|    1 | 1,2,3,4,5,6,7,8,9,10 |
+------+----------------------+

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