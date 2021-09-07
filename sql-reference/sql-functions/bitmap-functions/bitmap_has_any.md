# bitmap_has_any

## description

### Syntax

`B00LEAN BITMAP_HAS_ANY(BITMAP lhs, BITMAP rhs)`

计算两个Bitmap列是否存在相交元素，返回值是Boolean值.

## example

```Plain Text
MySQL > select bitmap_has_any(to_bitmap(1),to_bitmap(2)) cnt;
+------+
| cnt  |
+------+
|    0 |
+------+

MySQL > select bitmap_has_any(to_bitmap(1),to_bitmap(1)) cnt;
+------+
| cnt  |
+------+
|    1 |
+------+
```

## keyword

BITMAP_HAS_ANY,BITMAP
