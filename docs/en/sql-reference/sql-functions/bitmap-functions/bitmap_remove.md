# bitmap_remove

## description

### Syntax

```Haskell
bitmap_remove(BITMAP lhs, BIGINT input)
```

Remove input from lhs and return as a result set.

## example

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
