---
displayed_sidebar: "docs"
---

# mann_whitney_u_test

## Description

Performs the Mann-Whitney rank test on samples derived from two populations. The Mann-Whitney U test is a non-parametric test that can be used to determine if two populations were selected from the same distribution.

## Syntax

```Haskell
MANN_WHITNEY_U_TEST (sample_data, sample_treatment[, alternative[, continuity_correction]])
```

## Parameters

- `sample_data`: the value of sample data. It must be of numeric data types.

- `sample_treatment`: the index of sample data. where each element indicates the treatment group that the corresponding sample belongs to. The values should be of type boolean, with `false` representing the first group and `true` representing the second group.

- `alternative` (optional): A const string specifying the alternative hypothesis. It can be one of the following: 
- - 'two-sided': The default value. Tests whether the means of the two populations are different.
- - 'less': Tests whether the mean of the first population is less than the mean of the second population.
- - 'greater': Tests whether the mean of the first population is greater than the mean of the second population.

- `continuity_correction` (optional): A const boolean value indicating whether to apply a continuity correction. The continuity correction adjusts the U statistic by `0.5` towards the mean of the U distribution, which can improve the accuracy of the test for small sample sizes. The default value is `true`.

## Return value

The function returns a json array containing the following two elements:

The Mann-Whitney U statistic and the p-value associated with the test.

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
