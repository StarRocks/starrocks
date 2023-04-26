# bitmap_has_any

## Description

Calculates whether there are intersecting elements between two Bitmap columns, and the return value is Boolean value.

## Syntax

```Haskell
B00LEAN BITMAP_HAS_ANY(BITMAP lhs, BITMAP rhs)
```

## Examples

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
