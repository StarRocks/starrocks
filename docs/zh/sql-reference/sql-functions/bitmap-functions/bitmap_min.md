---
displayed_sidebar: "Chinese"
---

# bitmap_min

## 功能

获取Bitmap中的最小值。如果Bitmap为NULL，则返回NULL。如果Bitmap为空，默认返回NULL。

## 语法

```Haskell
bitmap_min(bitmap)
```

## 参数说明

`bitmap`：支持的数据类型为 BITMAP，可以由 [BITMAP_FROM_STRING](./bitmap_from_string.md) 等函数构造。

## 返回值说明

返回 LARGEINT 类型的值。

## 示例

```Plain Text
MySQL > select bitmap_min(bitmap_from_string("0, 1, 2, 3"));
+-------------------------------------------------+
|    bitmap_min(bitmap_from_string('0, 1, 2, 3')) |
+-------------------------------------------------+
|                                               0 |
+-------------------------------------------------+

MySQL > select bitmap_min(bitmap_from_string("-1, 0, 1, 2"));
+-------------------------------------------------+
|   bitmap_min(bitmap_from_string('-1, 0, 1, 2')) |
+-------------------------------------------------+
|                                            NULL |
+-------------------------------------------------+

MySQL > select bitmap_min(bitmap_empty());
+----------------------------------+
|       bitmap_min(bitmap_empty()) |
+----------------------------------+
|                             NULL |
+----------------------------------+

mysql> select bitmap_min(bitmap_from_string("16501189037412846863"));
+--------------------------------------------------------+
| bitmap_min(bitmap_from_string('16501189037412846863')) |
+--------------------------------------------------------+
| 16501189037412846863                                   |
+--------------------------------------------------------+
1 row in set (0.03 sec)
```
