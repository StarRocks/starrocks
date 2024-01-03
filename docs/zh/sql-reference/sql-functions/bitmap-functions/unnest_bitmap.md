---
displayed_sidebar: "Chinese"
---

# unnest_bitmap

## 功能

unnest_bitmap 是一种表函数 (table function)，用于将一个 bitmap 展开成多行。

可以将 StarRocks 的 [Lateral Join](../../../using_starrocks/Lateral_join.md) 与 unnest_bitmap 功能结合使用，实现常见的列转行逻辑。

该函数可以用于替换 `unnest(bitmap_to_array(bitmap))`，性能更好，占用内存资源更少，不受数组长度的限制。

该函数从 3.1 版本开始支持。

## 语法

```Haskell
unnest_bitmap(bitmap)
```

## 参数说明

`bitmap`: 待转换的 bitmap，必填。

## 返回值说明

返回数组展开后的多行数据。返回值的数据类型为 Bigint。

## **示例**

假设有表 `t1`, 其中 `c2` 列为 bitmap 列。

```SQL
-- 使用 bitmap_to_string() 函数将 c2 列转换为一个字符串。
mysql> select c1, bitmap_to_string(c2) from t1;
+------+----------------------+
| c1   | bitmap_to_string(c2) |
+------+----------------------+
|  1   | 1,2,3,4,5,6,7,8,9,10 |
+------+----------------------+

-- 使用 unnest_bitmap() 函数将 bitmap 列展开。
mysql> select c1, unnest_bitmap from t1, unnest_bitmap(c2);
+------+---------------+
| c1   | unnest_bitmap |
+------+---------------+
|    1 |             1 |
|    1 |             2 |
|    1 |             3 |
|    1 |             4 |
|    1 |             5 |
|    1 |             6 |
|    1 |             7 |
|    1 |             8 |
|    1 |             9 |
|    1 |            10 |
+------+---------------+
```
