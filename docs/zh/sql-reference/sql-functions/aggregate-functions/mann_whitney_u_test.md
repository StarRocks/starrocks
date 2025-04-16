---
displayed_sidebar: "docs"
---

# mann_whitney_u_test

## 功能

对来自两个总体的样本执行Mann-Whitney检验。Mann-Whitney U检验是一种非参数检验，可以用来确定两个总体是否来自相同的分布。


## 语法

```Haskell
MANN_WHITNEY_U_TEST (sample_data, sample_treatment[, alternative[, continuity_correction]])
```

## 参数说明

- `sample_data`: 样本数据的值。必须为数值数据类型。

- `sample_treatment`: 样本数据的索引。每个元素表示对应样本所属的处理组。值应为布尔类型，`false`表示第一组，`true`表示第二组。

- `alternative` (可选): 一个常量String，指定备择假设的类型。可以是以下之一：

- - 'two-sided': 默认值。检验两个总体的均值是否不同。

- - 'less': 检验第一个总体的均值是否小于第二个总体的均值。

- - 'greater': 检验第一个总体的均值是否大于第二个总体的均值。

- `continuity_correction` (可选): 一个常量布尔值，表示是否应用连续性修正。连续性修正将U统计量向U分布的均值调整`0.5`，从而可以提高小样本大小时检验的准确性。默认值为`true`。

## 返回值说明

该函数返回一个包含以下两个元素的json数组：

Mann-Whitney U统计量和与检验相关的p值。

## 使用说明

此函数忽略NULL值。

## 示例

假设有一个名为testing_data的表，其中包含以下数据。

```sql
create table testing_data (
    id int, 
    score int, 
    treatment boolean
)
properties(
    "replication_num" = "1"
);

insert into testing_data values 
    (1, 80, false), 
    (2, 100, false), 
    (3, NULL, false), 
    (4, 60, true), 
    (5, 70, true), 
    (6, 85, true);
```


```Plain
select * from testing_data;
+------+-------+-----------+
| id   | score | treatment |
+------+-------+-----------+
|    1 |    80 |         0 |
|    2 |   100 |         0 |
|    3 |  NULL |         0 |
|    4 |    60 |         1 |
|    5 |    70 |         1 |
|    6 |    85 |         1 |
+------+-------+-----------+
```

Query:

```SQL
SELECT MANN_WHITNEY_U_TEST(score, treatment) FROM testing_data;
```

Result:

```Plain
+---------------------------------------+
| mann_whitney_u_test(score, treatment) |
+---------------------------------------+
| [5, 0.38647623077123283]              |
+---------------------------------------+
```

Query:

```SQL
SELECT MANN_WHITNEY_U_TEST(score, treatment, 'less') FROM testing_data;
```

Result:

```Plain
+-----------------------------------------------+
| mann_whitney_u_test(score, treatment, 'less') |
+-----------------------------------------------+
| [5, 0.9255426634106172]                       |
+-----------------------------------------------+
```


Query:

```SQL
SELECT MANN_WHITNEY_U_TEST(score, treatment, 'two-sided', 0) FROM testing_data;
```

Result:

```Plain
+-------------------------------------------------------+
| mann_whitney_u_test(score, treatment, 'two-sided', 0) |
+-------------------------------------------------------+
| [5, 0.2482130789899235]                               |
+-------------------------------------------------------+
```
