# STD

## Description

Returns the standard deviation of the selected field.

## Syntax

```Haskell
STD(expr)
```

## Return value

Returns a `double` value which is the standard deviation of the selected field.

## Examples

```plain
MySQL > select * from std_test;
+------+------+
| col0 | col1 |
+------+------+
|    0 |    0 |
|    1 |    2 |
|    2 |    4 |
|    3 |    6 |
|    4 |    8 |
+------+------+
```

Calculate the standard deviation of `col0` and `col1`.

```sql
MySQL > select std(col0) as std_of_col0, std(col1) as std_of_col1 from std_test;
+--------------------+--------------------+
| std_of_col0        | std_of_col1        |
+--------------------+--------------------+
| 1.4142135623730951 | 2.8284271247461903 |
+--------------------+--------------------+
```

## keyword

STD
