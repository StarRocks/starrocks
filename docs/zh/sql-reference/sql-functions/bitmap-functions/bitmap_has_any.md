---
displayed_sidebar: "Chinese"
---

# bitmap_has_any

## 功能

计算两个 Bitmap 列是否存在相交元素，返回值是 Boolean 值。

## 语法

```Haskell
BITMAP_HAS_ANY(lhs, rhs)
```

## 参数说明

`lhs`: 支持的数据类型为 BITMAP。

`rhs`: 支持的数据类型为 BITMAP。

## 返回值说明

返回值的数据类型为 BOOLEAN。

## 示例

```Plain Text
MySQL > select bitmap_has_any(to_bitmap(1),to_bitmap(2)) cnt;
+------+
| cnt  |
+------+
|    0 |
+------+

MySQL > select bitmap_has_any(to_bitmap(1),to_bitmap(1)) cnt;
+------+
| cnt  |
+------+
|    1 |
+------+
```
