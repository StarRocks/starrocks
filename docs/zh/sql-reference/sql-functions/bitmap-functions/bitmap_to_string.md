---
displayed_sidebar: "Chinese"
---

# bitmap_to_string

## 功能

将一个 bitmap 转化成一个逗号分隔的字符串。字符串中包含 bitmap 中所有 bit 位。输入是 null 的话会返回 null。

## 语法

```Haskell
BITMAP_TO_STRING(input)
```

## 参数说明

`input`: 支持的数据类型为 BITMAP。

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

```Plain Text
select bitmap_to_string(null);
+------------------------+
| bitmap_to_string(NULL) |
+------------------------+
| NULL                   |
+------------------------+

select bitmap_to_string(bitmap_empty());
+----------------------------------+
| bitmap_to_string(bitmap_empty()) |
+----------------------------------+
|                                  |
+----------------------------------+

select bitmap_to_string(to_bitmap(1));
+--------------------------------+
| bitmap_to_string(to_bitmap(1)) |
+--------------------------------+
| 1                              |
+--------------------------------+

select bitmap_to_string(bitmap_or(to_bitmap(1), to_bitmap(2)));
+---------------------------------------------------------+
| bitmap_to_string(bitmap_or(to_bitmap(1), to_bitmap(2))) |
+---------------------------------------------------------+
| 1,2                                                     |
+---------------------------------------------------------+

```
