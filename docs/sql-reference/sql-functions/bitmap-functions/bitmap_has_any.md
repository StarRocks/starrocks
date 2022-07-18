# bitmap_has_any

## description

### Syntax

`B00LEAN BITMAP_HAS_ANY(BITMAP lhs, BITMAP rhs)`

Calculate whether there are intersecting elements between two Bitmap columns, and the return value is Boolean value.

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
