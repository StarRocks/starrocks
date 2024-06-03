---
displayed_sidebar: "Chinese"
---

# ols

## Description

`ols`函数对输入数据执行普通最小二乘法（OLS）回归分析。它估计线性回归模型的系数，并返回相关的统计信息。

## Syntax

```Haskell
ols (y, X, use_bias[, variable_names])
```

## Parameters

- y: 数值类型的因变量。
- X: 独立变量的数值类型数组。
- use_bias: 一个表示是否使用偏差的布尔值常量。
- variable_names（可选）：一个表示是否在报告中替换变量名称的字符串值常量。默认值为空字符串，不替换。

## Return value

一份包含以下信息的报告：
- 每个独立变量的系数估计值。
- 系数估计值的标准误差。
- 系数估计值的t统计量。
- 与t统计量相关的p值。

## Usage notes

函数忽略空值。

## Examples

建表：

```sql
create table testing_data (
    id int, 
    y double, 
    x1 double,
    x2 double,
    x3 double
)
properties(
    "replication_num" = "1"
);

insert into testing_data values 
    (1, 5, 1, 2, 3), 
    (2, 6, 2, 5, 7), 
    (3, 7, 3, 6, 9), 
    (4, 8, 1, 2, 3), 
    (5, 9, 3, 7, 12), 
    (6, 10, 4, 9, 15);
```

查询:

```SQL
SELECT ols(y, [x1, x2, x3], true) FROM testing_data;
```

结果:

```Plain
Call:
  lm( formula = y ~ x1 + x2 + x3 )

Coefficients:
.               Estimate    Std. Error  t value     Pr(>|t|)    
(Intercept)     6.052632    1.693232    3.574602    0.070128    
x1              0.473684    3.626427    0.130620    0.908029    
x2              -2.263158   2.558187    -0.884673   0.469661    
x3              1.473684    1.144056    1.288122    0.326620    

Residual standard error: 1.538968 on 2 degrees of freedom
Multiple R-squared: 0.729323, Adjusted R-squared: 0.323308
F-statistic: 1.796296 on 3 and 2 DF,  p-value: 0.377155
```

查询：

```SQL
SELECT ols(y, [x1, x2, x3], true, "d,a,b,c") FROM testing_data;
```

结果:

```Plain
Call:
  lm( formula = d ~ a + b + c )

Coefficients:
.               Estimate    Std. Error  t value     Pr(>|t|)    
(Intercept)     6.052632    1.693232    3.574602    0.070128    
x1              0.473684    3.626427    0.130620    0.908029    
x2              -2.263158   2.558187    -0.884673   0.469661    
x3              1.473684    1.144056    1.288122    0.326620    

Residual standard error: 1.538968 on 2 degrees of freedom
Multiple R-squared: 0.729323, Adjusted R-squared: 0.323308
F-statistic: 1.796296 on 3 and 2 DF,  p-value: 0.377155
```
