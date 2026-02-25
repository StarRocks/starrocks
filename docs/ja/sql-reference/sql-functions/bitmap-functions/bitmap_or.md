---
displayed_sidebar: docs
---

# bitmap_or

2 つの入力ビットマップの和集合を計算し、新しいビットマップを返します。

## 構文

```Haskell
BITMAP BITMAP_OR(BITMAP lhs, BITMAP rhs)
```

## 例

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

## キーワード

BITMAP_OR,BITMAP