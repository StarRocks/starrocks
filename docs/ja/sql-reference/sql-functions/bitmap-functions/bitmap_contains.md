---
displayed_sidebar: docs
---

# bitmap_contains

## 説明

入力値が bitmap 列に含まれているかどうかを計算し、Boolean 値を返します。

## 構文

```Haskell
B00LEAN BITMAP_CONTAINS(BITMAP bitmap, BIGINT input)
```

## 例

```Plain Text
MySQL > select bitmap_contains(to_bitmap(1),2) cnt;
+------+
| cnt  |
+------+
|    0 |
+------+

MySQL > select bitmap_contains(to_bitmap(1),1) cnt;
+------+
| cnt  |
+------+
|    1 |
+------+
```

## キーワード

BITMAP_CONTAINS,BITMAP