# APPROX_COUNT_DISTINCT

## 功能

返回类似于 `COUNT(DISTINCT col)` 结果的近似值。

> 它比 COUNT 和 DISTINCT 组合的速度更快，并使用固定大小的内存，因此对于高基数的列可以使用更少的内存。

## 语法

```Haskell
APPROX_COUNT_DISTINCT(expr)
```

## 参数说明

`epxr`: 被选取的表达式。

## 返回值说明

返回值为数值类型。

## 示例

```plain text
MySQL > select approx_count_distinct(query_id) from log_statis group by datetime;
+-----------------------------------+
| approx_count_distinct(`query_id`) |
+-----------------------------------+
| 17721                             |
+-----------------------------------+
```
