---
displayed_sidebar: "Chinese"
---

# bitmap_max

## 功能

获取Bitmap中的最大值。如果Bitmap为NULL，则返回NULL。如果Bitmap为空，默认返回NULL。

## 语法

```Haskell
bitmap_max(bitmap)
```

## 参数说明

`bitmap`：支持的数据类型为 BITMAP，可以由 [BITMAP_FROM_STRING](./bitmap_from_string.md)等函数构造。

## 返回值说明

返回 LARGEINT 类型的值。

## 示例

```Plain Text
MySQL > select bitmap_max(bitmap_from_string("0, 1, 2, 3"));
+-------------------------------------------------+
|    bitmap_max(bitmap_from_string("0, 1, 2, 3")) |
+-------------------------------------------------+
|                         3                       |
+-------------------------------------------------+

MySQL > select bitmap_max(bitmap_from_string("-1, 0, 1, 2"));
+-------------------------------------------------+
|   bitmap_max(bitmap_from_string("-1, 0, 1, 2")) |
+-------------------------------------------------+
|                      NULL                       |
+-------------------------------------------------+

MySQL > select bitmap_max(bitmap_empty());
+----------------------------------+
|       bitmap_max(bitmap_empty()) |
+----------------------------------+
|                      NULL        |
+----------------------------------+

mysql> select bitmap_max(bitmap_from_string("1, 16501189037412846863"));
+--------------------------------------------------------------+
| bitmap_max(bitmap_from_string('1, 16501189037412846863')) |
+--------------------------------------------------------------+
| 16501189037412846863                                         |
+--------------------------------------------------------------+
1 row in set (0.02 sec)
```
