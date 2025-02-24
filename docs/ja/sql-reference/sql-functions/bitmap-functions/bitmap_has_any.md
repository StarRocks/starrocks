---
displayed_sidebar: docs
---

# bitmap_has_any

2 つの Bitmap 列の間に交差する要素があるかどうかを計算し、戻り値は Boolean 値です。

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