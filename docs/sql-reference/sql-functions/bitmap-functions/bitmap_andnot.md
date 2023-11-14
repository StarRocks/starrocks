# bitmap_andnot

## Description

Returns bitmap values that exist in `lhs` but do not exist in `rhs`, and returns the new bitmap.

## Syntax

```Haskell
bitmap_andnot(BITMAP lhs, BITMAP rhs)
```

## Examples

```plain text
mysql> select bitmap_to_string(bitmap_andnot(bitmap_from_string('1, 3'), bitmap_from_string('2'))) cnt;
+------+
|cnt   |
+------+
|1,3   |
+------+

mysql> select bitmap_to_string(bitmap_andnot(bitmap_from_string('1,3,5'), bitmap_from_string('1'))) cnt;
+------+
|cnt   |
+------+
|3,5   |
+------+
```

## keyword

BITMAP_ANDNOT, BITMAP
