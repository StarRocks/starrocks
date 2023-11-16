---
displayed_sidebar: "Chinese"
---

# bitmap_to_array

## 功能

将一个 BIMTAP 中的所有值组合成一个 bigint 数组

## 语法

```Haskell
 `ARRAY<BIGINT>` BITMAP_TO_ARRAY (bitmap)
```

## 参数说明

`bitmap`: 支持的数据类型为 BITMAP

## 返回值说明

返回值的数据类型为 ARRAY

## 示例

```Plain text
mysql> select bitmap_to_array(bitmap_from_string("1, 7"));
+----------------------------------------------+
| bitmap_to_array(bitmap_from_string('1, 7'))  |
+----------------------------------------------+
| [1,7]                                        |
+----------------------------------------------+

mysql> select bitmap_to_array(NULL);
+-----------------------+
| bitmap_to_array(NULL) |
+-----------------------+
| NULL                  |
+-----------------------+
```

## 关键词

BITMAP_TO_ARRAY, BITMAP
