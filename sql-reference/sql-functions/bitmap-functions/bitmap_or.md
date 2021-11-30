# bitmap_or

## description

### Syntax

```Haskell
BITMAP BITMAP_OR(BITMAP lhs, BITMAP rhs)
```

Calculate the union of two input bitmaps and return a new bitmap.

## example

```Plain Text
MySQL > select bitmap_count(bitmap_or(to_bitmap(1), to_bitmap(2))) cnt;
+------+
| cnt  |
+------+
|    2 |
+------+

MySQL > select bitmap_count(bitmap_or(to_bitmap(1), to_bitmap(1))) cnt;
+------+
| cnt  |
+------+
|    1 |
+------+
```

## keyword

BITMAP_OR,BITMAP
