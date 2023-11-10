# bitmap_remove

## Description

Removes `input` from the Bitmap value `lhs` and returns a result set.

## Syntax

```Haskell
bitmap_remove(BITMAP lhs, BIGINT input)
```

## Examples

```plain text
mysql> select bitmap_to_string(**bitmap_remove**(bitmap_from_string('1, 3'), 3)) cnt;
+------+
|cnt   |
+------+
|1     |
+------+

mysql> select bitmap_to_string(**bitmap_remove**(bitmap_from_string('1,3,5'), 6)) cnt;
+------+
|cnt   |
+------+
|1,3,5 |
+------+
```

## keyword

BITMAP_REMOVE, BITMAP
