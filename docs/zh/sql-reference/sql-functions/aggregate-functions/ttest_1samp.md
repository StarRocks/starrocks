---
displayed_sidebar: "Chinese"
---

# ttest_1samp

## 功能

`ttest_1samp` 函数对输入数据执行单样本 t 检验。它将样本均值与指定的总体均值进行比较，并提供相关的统计信息。

## 语法

```SQL
TTEST_1SAMP (expr, alternative, mu, X, [, cuped[, alpha]])
```

## 参数说明

- `expr`: 表示要测试的列的字符串表达式。
- `alternative`: 指定备择假设的字符串，可以是 'two-sided'、'less' 或 'greater'。
- `mu`: 表示要与样本均值比较的总体均值的数值。
- `X`: 样本数据的数组。
- `cuped`: 一个可选的布尔值，指示是否使用杯状校正。默认为 false。
- `alpha`: 一个可选的数值，表示显著性水平。默认为 0.05。

## 返回值说明

该函数返回一个包含以下列的表格：
- estimate: 样本均值。
- stderr: 样本均值的标准误。
- t-statistic: 样本均值的 t 统计量。
- p-value: 与 t 统计量相关的 p 值。
- lower: 样本均值置信区间的下限。
- upper: 样本均值置信区间的上限。

## 使用说明

该函数忽略 Null 值。

## 示例

假设有一个名为 testing_data 的表，其中包含以下数据。

```Plain
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
    (3, 90, false), 
    (4, 60, true), 
    (5, 70, true), 
    (6, 85, true);
```

查询语句：

```SQL
SELECT TTEST_1SAMP("x1", 'two-sided', 80, [score]) FROM testing_data;
```

返回结果：

```Plain
estimate    stderr      t-statistic p-value     lower       upper       
0.833333    5.833333    0.142857    0.886403    -14.161727  15.828394
```

在这个例子中，`ttest_1samp` 函数用于将 score 列的均值与总体均值 80 进行比较。结果显示样本均值为 0.833333，标准误为 5.833333，t 统计量为 0.142857，p 值为 0.886403。样本均值的置信区间为 (-14.161727, 15.828394)。
