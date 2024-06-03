---
displayed_sidebar: "English"
---

# ttest_1samp

## Description

The `ttest_1samp` function performs a one-sample t-test on the input data. It compares the mean of a sample to a specified population mean and provides related statistical information.

## Syntax

```Haskell
TTEST_1SAMP (expr, alternative, mu, X, [, cuped[, alpha]])
```

## Parameters

- expr: A string expression representing the column to be tested.
- alternative: A string specifying the alternative hypothesis, which can be 'two-sided', 'less', or 'greater'.
- mu: A numeric value representing the population mean to be compared with the sample mean.
- X: An array of the sample data.
- cuped: An optional string value indicating the expression of the cuped correction. Default is ''.
- alpha: An optional numeric value representing the significance level. Default is 0.05.

## Return value

The function returns a table with the following columns:
- estimate: The sample mean.
- stderr: The standard error of the sample mean.
- t-statistic: The t-statistic for the sample mean.
- p-value: The p-value associated with the t-statistic.
- lower: The lower bound of the confidence interval for the sample mean.
- upper: The upper bound of the confidence interval for the sample mean.

## Usage notes

This function ignores NULLs.

## Examples

Suppose there is a table named `testing_data` with the following data.

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
    (3, 90, false), 
    (4, 60, true), 
    (5, 70, true), 
    (6, 85, true);
```


Query:

```SQL
SELECT TTEST_1SAMP("x1", 'two-sided', 80, [score]) FROM testing_data;
```

Result:

```Plain
estimate    stderr      t-statistic p-value     lower       upper       
0.833333    5.833333    0.142857    0.886403    -14.161727  15.828394
```


