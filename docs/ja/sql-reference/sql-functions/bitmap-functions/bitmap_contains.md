---
displayed_sidebar: docs
---

# bitmap_contains

入力値が bitmap 列に含まれているかどうかを計算し、Boolean 値を返します。

## Syntax

```Haskell
B00LEAN BITMAP_CONTAINS(BITMAP bitmap, BIGINT input)
```

## Examples

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

## keyword

BITMAP_CONTAINS,BITMAP