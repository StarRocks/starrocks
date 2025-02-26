---
displayed_sidebar: docs
---

# bitmap_and

2 つの入力ビットマップの交差を計算し、新しいビットマップを返します。

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