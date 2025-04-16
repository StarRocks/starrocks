---
displayed_sidebar: docs
---

# percentile_disc_lc



计算百分位数。和 percentile_disc 行为一致. 但是实现算法不同 percentile_disc 需要获取所有的输入数据，通过归并排序获取百分位的值需要消耗的内存为所有输入数据的内存。而 percentile_disc_lc 是构建一个 key->count的一个hash表，因此当输入的基数比较低的时候即使输入的数据量很大也没有明显的内存增长。

该函数从 3.4 版本开始支持。

## 语法

```SQL
PERCENTILE_DISC_LC (expr, percentile) 
```

## 参数说明

- `expr`: 要计算百分位数的列，列值支持任意可排序的类型。

- `percentile`: 指定的百分位，介于 0 和 1 之间的浮点常量。如果要计算中位数，则设置为 0.5。

## 返回值说明

返回指定的百分位对应的值。如果没有找到与百分位完全匹配的值，则返回临近两个数值中较大的值。

返回值类型与 `expr` 内的数据类型相同。

## 使用说明

该函数忽略 Null 值。

## 示例

创建表 `exam`，插入数据。

```sql
CREATE TABLE exam (
    subject STRING,
    score INT
) 
DISTRIBUTED BY HASH(`subject`);

INSERT INTO exam VALUES
('chemistry',80),
('chemistry',100),
('chemistry',null),
('math',60),
('math',70),
('math',85),
('physics',75),
('physics',80),
('physics',85),
('physics',99);
```

```Plain
select * from exam order by subject;
+-----------+-------+
| subject   | score |
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
select subject, percentile_disc_lc (score, 0.5)
from exam group by subject;
```

返回结果：

```Plain
+-----------+--------------------------------+
| subject   | percentile_disc_lc(score, 0.5) |
+-----------+--------------------------------+
| physics   |                             85 |
| chemistry |                            100 |
| math      |                             70 |
+-----------+--------------------------------+
```
