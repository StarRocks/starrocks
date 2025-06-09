---
displayed_sidebar: docs
---

# bitmap_to_array

## 説明

BITMAP を BIGINT 配列に変換します。

## 構文

```Haskell
 ARRAY<BIGINT> BITMAP_TO_ARRAY (bitmap)
```

## パラメータ

`bitmap`: 変換したいビットマップ。

## 戻り値

BIGINT 配列を返します。

## 例

```Plain
select bitmap_to_array(bitmap_from_string("1, 7"));
+----------------------------------------------+
| bitmap_to_array(bitmap_from_string('1, 7'))  |
+----------------------------------------------+
| [1,7]                                        |
+----------------------------------------------+

select bitmap_to_array(NULL);
+-----------------------+
| bitmap_to_array(NULL) |
+-----------------------+
| NULL                  |
+-----------------------+
```
