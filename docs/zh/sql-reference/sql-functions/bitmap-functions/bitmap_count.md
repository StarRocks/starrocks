---
displayed_sidebar: "Chinese"
---

# bitmap_count

## 功能

统计 bitmap 中不重复值的个数。

## 语法

```Haskell
INT BITMAP_COUNT(any_bitmap)
```

## 返回值说明

返回值的数据类型为 INT。

## 示例

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
