---
displayed_sidebar: docs
---

# bitmap_count

## 説明

入力されたビットマップの1ビット数を返します。

## 構文

```Haskell
INT BITMAP_COUNT(any_bitmap)
```

## 例

```Plain Text
MySQL > select bitmap_count(bitmap_from_string("1,2,4"));
+-------------------------------------------+
| bitmap_count(bitmap_from_string('1,2,4')) |
+-------------------------------------------+
|                                         3 |
+-------------------------------------------+

MySQL > select bitmap_count(NULL);
+--------------------+
| bitmap_count(NULL) |
+--------------------+
|                  0 |
+--------------------+
```

## キーワード

BITMAP, BITMAP_COUNT