---
displayed_sidebar: "English"
---

# bitmap_and

## Description

Calculates the intersection of two input bitmaps and returns the new bitmap.

## Syntax

```Haskell
BITMAP BITMAP_AND(BITMAP lhs, BITMAP rhs)
```

## Examples

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
