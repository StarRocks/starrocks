---
displayed_sidebar: docs
---

# bitmap_to_array

## 功能

将 BITMAP 中的所有值组合成 BIGINT 类型的数组。

## 语法

```Haskell
 `ARRAY<BIGINT>` BITMAP_TO_ARRAY (bitmap)
```

## 参数说明

`bitmap`: 支持的数据类型为 BITMAP。

## 返回值说明

返回值的数据类型为 BIGINT 类型的数组。

## 示例

```Plain text
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
