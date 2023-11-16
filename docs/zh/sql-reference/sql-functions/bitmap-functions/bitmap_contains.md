---
displayed_sidebar: "Chinese"
---

# bitmap_contains

## 功能

计算输入值是否在 Bitmap 列中。

## 语法

```Haskell
BITMAP_CONTAINS(bitmap, input)
```

## 参数说明

`bitmap`: 支持的数据类型为 BITMAP。

`input`: 输入值，支持的数据类型为 BIGINT。

## 返回值说明

返回值的数据类型为 BOOLEAN。

## 示例

```Plain Text
MySQL > select bitmap_contains(to_bitmap(1),2) cnt;
+------+
| cnt  |
+------+
|    0 |
+------+

MySQL > select bitmap_contains(to_bitmap(1),1) cnt;
+------+
| cnt  |
+------+
|    1 |
+------+
```
