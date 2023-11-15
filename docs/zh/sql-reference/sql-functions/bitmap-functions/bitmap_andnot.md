# bitmap_andnot

## description

### Syntax

```Haskell
bitmap_andnot(BITMAP lhs, BITMAP rhs)
```

计算得到在 **lhs** 中，但是不在 **rhs** 中的集合(差集).

## example

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
