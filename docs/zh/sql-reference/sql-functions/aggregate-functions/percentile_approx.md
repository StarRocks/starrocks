---
displayed_sidebar: "Chinese"
---

# PERCENTILE_APPROX

## 功能

返回第 p 个百分位点的近似值。

> 该函数使用固定大小的内存，因此对于高基数的列可以使用更少的内存，可用于计算 tp99 等统计值。

## 语法

```Haskell
PERCENTILE_APPROX(expr, p[, compression])
```

## 参数说明

`expr`: 被选取的表达式。

`p`: 支持的数据类型为 DOUBLE，p 的值介于 0 到 1 之间。

`compression`: 可选参数，可设置范围是 [2048, 10000]，值越大精度越高，内存消耗越大，计算耗时越长。该参数未指定或设置的值在 [2048, 10000] 范围外，以 10000 的默认值运行。支持的数据类型为 DOUBLE 类型。

## 返回值说明

返回值为数值类型。

## 示例

```plain text
MySQL > select `table`, percentile_approx(cost_time,0.99)
from log_statis
group by `table`;
+----------+--------------------------------------+
| table    | percentile_approx(`cost_time`, 0.99) |
+----------+--------------------------------------+
| test     |                                54.22 |
+----------+--------------------------------------+

MySQL > select `table`, percentile_approx(cost_time,0.99, 4096)
from log_statis
group by `table`;
+----------+----------------------------------------------+
| table    | percentile_approx(`cost_time`, 0.99, 4096.0) |
+----------+----------------------------------------------+
| test     |                                        54.21 |
+----------+----------------------------------------------+
```
