---
displayed_sidebar: docs
---

# percentile_cont

## 功能

计算精确百分位数。该函数使用连续分布模型，如果未找到与百分位完全匹配的值，则返回临近两个值的线性插值。

该函数从 2.4 版本开始支持。

## 语法

```SQL
PERCENTILE_CONT (expr, percentile) 
```

## 参数说明

- `expr`: 要计算百分位数的列，列值必须为数值类型、DATE或 DATETIME类型。如果要计算物理（physics）得分的中位数，则 `expr` 设置为包含 physics 分数的列。

- `percentile`: 指定的百分位，介于 0 和 1 之间的浮点常量。如果要计算中位数，则设置为 0.5。

## 返回值说明

返回指定的百分位对应的值。如果没有找到与百分位完全匹配的值，则返回临近两个数值的线性插值。

返回值类型与 `expr` 内的数据类型相同。

## 使用说明

该函数忽略 Null 值。

## 示例

假设有表 `exam`，数据如下。

```Plain
select * from exam order by Subject;
+-----------+-------+
| Subject   | Score |
+-----------+-------+
| chemistry |    80 |
| chemistry |   100 |
| chemistry |  NULL |
| math      |    60 |
| math      |    70 |
| math      |    85 |
| physics   |    75 |
| physics   |    80 |
| physics   |    85 |
| physics   |    99 |
+-----------+-------+
```

计算每个科目得分的中位数，忽略 Null 值。

查询语句：

```SQL
SELECT Subject, PERCENTILE_CONT (Score, 0.5)  FROM exam group by Subject;
```

返回结果：

```Plain
+-----------+-----------------------------+
| Subject   | percentile_cont(Score, 0.5) |
+-----------+-----------------------------+
| chemistry |                          90 |
| math      |                          70 |
| physics   |                        82.5 |
+-----------+-----------------------------+
```
