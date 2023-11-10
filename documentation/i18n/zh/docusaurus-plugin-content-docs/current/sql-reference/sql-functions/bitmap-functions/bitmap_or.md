---
displayed_sidebar: "Chinese"
---

# bitmap_or

## 功能

计算两个输入的 bitmap 的并集，并返回新的 bitmap。并集指将两个集合中的所有元素合并在一起后组成的集合。重复元素只计入一次。

## 语法

```Haskell
BITMAP_OR(lhs, rhs)
```

## 参数说明

`lhs`: 支持的数据类型为 BITMAP。

`rhs`: 支持的数据类型为 BITMAP。

## 返回值说明

返回值的数据类型为 BITMAP。

## 示例

计算两个 bitmap 的并集，并返回并集中元素个数的总和。

```Plain Text
select bitmap_count(bitmap_or(to_bitmap(1), to_bitmap(2))) cnt;
+------+
| cnt  |
+------+
|    2 |
+------+

select bitmap_count(bitmap_or(to_bitmap(1), to_bitmap(1))) cnt;
+------+
| cnt  |
+------+
|    1 |
+------+
```
