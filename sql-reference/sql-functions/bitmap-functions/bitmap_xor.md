# bitmap_xor

## description

### Syntax

```Haskell
bitmap_xor(BITMAP lhs, BITMAP rhs)
```

计算得到 **lhs** 独有或者 **rhs** 独有的元素所构成的集合，逻辑上等价于`bitmap_andnot(bitmap_or(lhs, rhs), bitmap_and(lhs, rhs))`(补集)

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
