---
displayed_sidebar: docs
---

# bitmap_andnot

`lhs` に存在し、`rhs` に存在しない bitmap 値を返し、新しい bitmap を返します。

## 構文

```Haskell
bitmap_andnot(BITMAP lhs, BITMAP rhs)
```

## 例

```plain text
mysql> select bitmap_to_string(bitmap_andnot(bitmap_from_string('1, 3'), bitmap_from_string('2'))) cnt;
+------+
|cnt   |
+------+
|1,3   |
+------+

mysql> select bitmap_to_string(bitmap_andnot(bitmap_from_string('1,3,5'), bitmap_from_string('1'))) cnt;
+------+
|cnt   |
+------+
|3,5   |
+------+
```

## キーワード

BITMAP_ANDNOT, BITMAP