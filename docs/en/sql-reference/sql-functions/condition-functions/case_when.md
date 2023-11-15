# case

## Description

CASE is a conditional expression. It returns the result in the THEN clause if a condition in the WHEN clause evaluates to true. If none of the conditions evaluate to true, it returns the result in the optional ELSE clause. If ELSE is not present, NULL is returned.

## Syntax

The CASE expression comes in two forms:

- Simple CASE

```SQL
CASE expression
    WHEN expression1 THEN result1
    [WHEN expression2 THEN result2]
    ...
    [WHEN expressionN THEN resultN]
    [ELSE result]
END
```

For this syntax, `expression` is compared to each expression in the WHEN clause. If an equal expression is found, the result in the THEN clause is returned. If no equal expression is found, the result in the ELSE clause is returned if ELSE is present.

- Searched CASE

```SQL
CASE WHEN condition1 THEN result1
    [WHEN condition2 THEN result2]
    ...
    [WHEN conditionN THEN resultN]
    [ELSE result]
END
```

For this syntax, each condition in the WHEN clause is evaluated until one is true and the corresponding result in the THEN clause is returned. If no condition evaluates to true, the result in the ELSE clause is returned, if ELSE is present.

The first CASE equals the second one as follows:

```SQL
CASE WHEN expression = expression1 THEN result1
    [WHEN expression = expression2 THEN result2]
    ...
    [WHEN expression = expressionN THEN resultN]
    [ELSE result]
END
```

## Parameters

- `expressionN`: the expression to compare. Multiple expressions must be compatible in data types.

- `conditionN`: the condition that can evaluate to a BOOLEAN value.

- `resultN` must be compatible in data types.

## Return value

The return value is of the common type of all types in the THEN clause.

## Examples

Suppose table `test_case` has the following data:

```SQL
CREATE TABLE test_case(
    name          STRING,
    gender         INT,
    ) DISTRIBUTED BY HASH(name);

INSERT INTO test_case VALUES
    ("Andy",1),
    ("Jules",0),
    ("Angel",-1),
    ("Sam",null);

SELECT * FROM test_case;
+-------+--------+
| name  | gender |
+-------+--------+
| Angel |     -1 |
| Andy  |      1 |
| Sam   |   NULL |
| Jules |      0 |
+-------+--------+-------+
```

### Use simple CASE

- ELSE is specified and the result in ELSE is returned if no equal expression is found.

```plain
mysql> select gender, case gender 
                    when 1 then 'male'
                    when 0 then 'female'
                    else 'error'
               end gender_str
from test_case;
+--------+------------+
| gender | gender_str |
+--------+------------+
|   NULL | error      |
|      0 | female     |
|      1 | male       |
|     -1 | error      |
+--------+------------+
```

- ELSE is not specified and NULL is returned if no condition evaluates to true.

```plain
select gender, case gender 
                    when 1 then 'male'
                    when 0 then 'female'
               end gender_str
from test_case;
+--------+------------+
| gender | gender_str |
+--------+------------+
|      1 | male       |
|     -1 | NULL       |
|   NULL | NULL       |
|      0 | female     |
+--------+------------+
```

### Use searched CASE with no ELSE specified

```plain
mysql> select gender, case when gender = 1 then 'male'
                           when gender = 0 then 'female'
                      end gender_str
from test_case;
+--------+------------+
| gender | gender_str |
+--------+------------+
|   NULL | NULL       |
|     -1 | NULL       |
|      1 | male       |
|      0 | female     |
+--------+------------+
```

## Keywords

case when, case, case_when, case...when
