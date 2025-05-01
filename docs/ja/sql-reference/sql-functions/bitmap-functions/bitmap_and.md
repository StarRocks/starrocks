---
displayed_sidebar: docs
---

# bitmap_and

## 説明

2 つの入力ビットマップの交差を計算し、新しいビットマップを返します。

## 構文

```Haskell
BITMAP BITMAP_AND(BITMAP lhs, BITMAP rhs)
```

## 例

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

## キーワード

BITMAP_AND, BITMAP