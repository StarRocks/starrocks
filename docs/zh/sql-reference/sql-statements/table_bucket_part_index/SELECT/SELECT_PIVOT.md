---
displayed_sidebar: docs
sidebar_label: "PIVOT"
---

# PIVOT

该功能从 v3.3 版本开始支持。

PIVOT 操作是 SQL 中的一项高级功能，允许您将表中的行转换为列，这对于创建数据透视表特别有用。当处理数据库报告或分析时，尤其是在需要汇总或分类数据以进行演示时，此功能非常方便。

实际上，PIVOT 是一种语法糖，可以简化 `sum(case when ... then ... end)` 之类的查询语句的编写。

## 语法

```sql
pivot:
SELECT ...
FROM ...
PIVOT (
  aggregate_function(<expr>) [[AS] alias] [, aggregate_function(<expr>) [[AS] alias] ...]
  FOR <pivot_column>
  IN (<pivot_value>)
)

pivot_column:
<column_name> 
| (<column_name> [, <column_name> ...])

pivot_value:
<literal> [, <literal> ...]
| (<literal>, <literal> ...) [, (<literal>, <literal> ...)]
```

## 参数

在 PIVOT 操作中，您需要指定几个关键组件：

- aggregate_function()：一个聚合函数，例如 SUM、AVG、COUNT 等，用于汇总数据。
- alias：聚合结果的别名，使结果更易于理解。
- FOR pivot_column：指定将执行行到列转换的列名。
- IN (pivot_value)：指定 pivot_column 的特定值，这些值将被转换为列。

## 示例

```sql
create table t1 (c0 int, c1 int, c2 int, c3 int);
SELECT * FROM t1 PIVOT (SUM(c1) AS sum_c1, AVG(c2) AS avg_c2 FOR c3 IN (1, 2, 3, 4, 5));
-- The result is equivalent to the following query:
SELECT SUM(CASE WHEN c3 = 1 THEN c1 ELSE NULL END) AS sum_c1_1,
       AVG(CASE WHEN c3 = 1 THEN c2 ELSE NULL END) AS avg_c2_1,
       SUM(CASE WHEN c3 = 2 THEN c1 ELSE NULL END) AS sum_c1_2,
       AVG(CASE WHEN c3 = 2 THEN c2 ELSE NULL END) AS avg_c2_2,
       SUM(CASE WHEN c3 = 3 THEN c1 ELSE NULL END) AS sum_c1_3,
       AVG(CASE WHEN c3 = 3 THEN c2 ELSE NULL END) AS avg_c2_3,
       SUM(CASE WHEN c3 = 4 THEN c1 ELSE NULL END) AS sum_c1_4,
       AVG(CASE WHEN c3 = 4 THEN c2 ELSE NULL END) AS avg_c2_4,
       SUM(CASE WHEN c3 = 5 THEN c1 ELSE NULL END) AS sum_c1_5,
       AVG(CASE WHEN c3 = 5 THEN c2 ELSE NULL END) AS avg_c2_5
FROM t1
GROUP BY c0;
```
