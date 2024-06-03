---
displayed_sidebar: "English"
---

# ols

## Description

The `ols` function performs an Ordinary Least Squares (OLS) regression analysis on the input data. It estimates the coefficients of a linear regression model and provides related statistical information.

## Syntax

```Haskell
ols (y, X, use_bias[, variable_names])
```

## Parameters

- `y`: The dependent variable of numeric types.

- `X`: A array of the independent variables of numeric types.

- `use_bias`: A const boolean value indicating whether to use bias.

- `variable_names` (optional): A const string value indicating whether to replace variable names in the report. The default value is ''.

## Return value

A report containing the following information:
- The coefficient estimates for each independent variable.
- The standard errors of the coefficient estimates.
- The t-statistics for the coefficient estimates.
- The p-values associated with the t-statistics.

## Usage notes

This function ignores NULLs.

## Examples

Suppose there is a table named `testing_data` with the following data.

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

Query:

```SQL
SELECT ols(y, [x1, x2, x3], true) FROM testing_data;
```

Result:

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


```SQL
SELECT ols(y, [x1, x2, x3], true, "d,a,b,c") FROM testing_data;
```

Result:

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
