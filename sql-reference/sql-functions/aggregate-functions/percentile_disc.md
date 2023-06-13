# percentile_disc

## 功能

计算百分位数。和 percentile_cont 不同的是，该函数如果未找到与百分位完全匹配的值，则默认返回临近两个值中较大的值。

该函数从 2.5 版本开始支持。

## 语法

```SQL
PERCENTILE_DISC (expr, percentile) 
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
    exam_result INT
) 
DISTRIBUTED BY HASH(`subject`);

insert into exam values
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
SELECT Subject, PERCENTILE_DISC (Score, 0.5)
FROM exam group by Subject;
```

返回结果：

```Plain
+-----------+-----------------------------+
| Subject   | percentile_disc(Score, 0.5) |
+-----------+-----------------------------+
| chemistry |                         100 |
| math      |                          70 |
| physics   |                          85 |
+-----------+-----------------------------+
```
