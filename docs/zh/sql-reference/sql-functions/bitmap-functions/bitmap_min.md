# BITMAP_MIN

## 功能

获取 Bitmap 中的最小值。如果 Bitmap 为 NULL，则返回 NULL。如果 Bitmap 为空，默认返回 -1。

## 语法

```Haskell
bitmap_min(bitmap)
```

## 参数说明

`bitmap`：支持的数据类型为 BITMAP，可以由 [BITMAP_FROM_STRING](./bitmap_from_string.md) 等函数构造。

## 返回值说明

返回 BIGINT 类型的值。

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
|                               -1 |
+----------------------------------+
```

## 关键词

BITMAP_MIN, BITMAP
