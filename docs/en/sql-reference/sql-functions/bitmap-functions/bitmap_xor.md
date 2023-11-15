# bitmap_xor

## description

### Syntax

```Haskell
bitmap_xor(BITMAP lhs, BITMAP rhs)
```

Calculate the set consisting elements unique to lhs or rhs. It is logically equivalent to`bitmap_andnot(bitmap_or(lhs, rhs), bitmap_and(lhs, rhs))`(complementary set).

## example

```plain text
mysql> select bitmap_to_string(bitmap_xor(bitmap_from_string('1, 3'), bitmap_from_string('2'))) cnt;
+------+
|cnt   |
+------+
|1,2,3 |
+------+
```

## keyword

BITMAP_XOR,  BITMAP
