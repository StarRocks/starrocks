---
displayed_sidebar: docs
---

# bitmap_or

## Description

2 つの入力ビットマップの合併を計算し、新しいビットマップを返します。

## Syntax

```Haskell
BITMAP BITMAP_OR(BITMAP lhs, BITMAP rhs)
```

## Examples

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