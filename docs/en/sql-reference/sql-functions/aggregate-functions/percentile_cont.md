# percentile_cont

## Description

Computes the percentile value of `expr` with linear interpolation.

## Syntax

```Haskell
PERCENTILE_CONT (expr, percentile) 
```

## Parameters

- `expr`: the expression by which to order the values. It must be of numeric data types, DATE, or DATETIME. For example, if you want to find the median score of physics, specify the column that contains the physics scores.

- `percentile`: the percentile of the value you want to find. It is a constant floating-point number from 0 to 1. For example, if you want to find the median value, set this parameter to `0.5`.

## Return value

Returns a value that is at the specified percentile. If no input value lies exactly at the desired percentile, the result is calculated using linear interpolation of the two nearest input values.

The data type is the same as `expr`.

## Usage notes

This function ignores NULLs.

## Examples

Suppose there is a table named `exam` with the following data.

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

Calculate the median score of each subject while ignoring NULLs.

Query:

```SQL
SELECT Subject, PERCENTILE_CONT (Score, 0.5)  FROM exam group by Subject;
```

Result:

```Plain
+-----------+-----------------------------+
| Subject   | percentile_cont(Score, 0.5) |
+-----------+-----------------------------+
| chemistry |                          90 |
| math      |                          70 |
| physics   |                        82.5 |
+-----------+-----------------------------+
```
