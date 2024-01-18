---
displayed_sidebar: "Chinese"
---

# bitmap_xor

## 功能

计算两个 Bitmap 中不重复元素所构成的集合，逻辑上等价于 `bitmap_andnot(bitmap_or(lhs, rhs), bitmap_and(lhs, rhs))`(补集)。

## 语法

```Haskell
bitmap_xor(lhs, rhs)
```

## 参数说明

`lhs`: 支持的数据类型为 BITMAP。

`rhs`: 支持的数据类型为 BITMAP。

## 返回值说明

返回值的数据类型为 BITMAP。

## 示例

```plain text
mysql> select bitmap_to_string(bitmap_xor(bitmap_from_string('1, 3'), bitmap_from_string('2'))) cnt;
+------+
|cnt   |
+------+
|1,2,3 |
+------+
```
