# bitmap_and

## description

### Syntax

```Haskell
BITMAP BITMAP_AND(BITMAP lhs, BITMAP rhs)
```

计算两个输入bitmap的交集，返回新的bitmap.

## example

```plain text
MySQL > select bitmap_count(bitmap_and(to_bitmap(1), to_bitmap(2))) cnt;
+------+
| cnt  |
+------+
|    0 |
+------+

MySQL > select bitmap_count(bitmap_and(to_bitmap(1), to_bitmap(1))) cnt;
+------+
| cnt  |
+------+
|    1 |
+------+
```

## keyword

BITMAP_AND,BITMAP
