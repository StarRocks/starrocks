---
displayed_sidebar: docs
---

# bitmap_remove

`input` を Bitmap 値 `lhs` から削除し、結果セットを返します。

## Syntax

```Haskell
bitmap_remove(BITMAP lhs, BIGINT input)
```

## 例

```plain text
mysql> select bitmap_to_string(**bitmap_remove**(bitmap_from_string('1, 3'), 3)) cnt;
+------+
|cnt   |
+------+
|1     |
+------+

mysql> select bitmap_to_string(**bitmap_remove**(bitmap_from_string('1,3,5'), 6)) cnt;
+------+
|cnt   |
+------+
|1,3,5 |
+------+
```

## キーワード

BITMAP_REMOVE, BITMAP