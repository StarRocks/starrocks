# case_when

## Description

Return results from `then-clause` when the condition returns true in `when-clause`, otherwise from optional `else-clause` or NULL.

## Syntax

```SQL
CASE WHEN condition1 THEN result1
    [WHEN condition2 THEN result2]
    ...
    [WHEN conditionN THEN resultN]
    [ELSE result]
END
```
or
```SQL
CASE expression
    WHEN expression1 THEN result1
    [WHEN expression2 THEN result2]
    ...
    [WHEN expressionN THEN resultN]
    [ELSE result]
END
```
the second case equals to the first format as follows:

```SQL
CASE WHEN expression = expression1 THEN result1
    [WHEN expression = expression2 THEN result2]
    ...
    [WHEN expression = expressionN THEN resultN]
    [ELSE result]
END
```


## Parameters

`conditionN` should be compatible with bool type.

`expressionN` are compatible in data type.

`resultN` are compatible in data type.

## Return value

The return value is of the common type of all types from `then-clause`.

## Examples

```SQL
mysql> select gender, case gender when 1 then 'male' when 0 then 'female' else 'error' end gender_str from test;
+---------+-------------+
| gender  | gender_str  |
+---------+-------------+
| 1       | male        |
| 0       | female      |
| -1      | error       |
+---------+-------------+
 
mysql> select gender, case when gender = 1 then 'male' when gender = 0 then 'female' end gender_str from test;
+---------+-------------+
| gender  | gender_str  |
+---------+-------------+
| 1       | male        |
| 0       | female      |
| -1      | NULL        |
+---------+-------------+
```
