---
displayed_sidebar: docs
sidebar_label: "WHERE and Operators"
---

# Where and Operators

SQL operators are a series of functions used for comparison and are widely used in where clauses of select statements.

## Arithmetic operator

Arithmetic operators usually appear in expressions that contain left, right, and most often left operands

### `+` and `-`

`+` and `-` can be used as a unit or as a 2-ary operator. When used as a unit operator, such as `+1`, `-2.5` or `-col_ name`, which means the value is multiplied by `+1` or `-1`.

So the cell operator `+` returns an unchanged value, and the cell operator `-` changes the symbol bits of that value.

Users can overlay two cell operators, such as `+5` (returning a positive value), `-+2` or `+-2` (returning a negative value), but users cannot use two consecutive `-` signs, because `--` is interpreted as a comment in the following statement (when a user can use two `-` signs, a space or parenthesis is required between the two `-` signs, such as `-(-2)` or `- -2`, which actually results in `+ 2`).

When `+` or `-` is a binary operator, such as `2+2`, `3+1.5`, or `col1+col2`, it means that the left value is added or subtracted from the right value. Both left and right values must be numeric types.

### `*` and `/`

`*` and `/` represent multiplication and division respectively. The operands on both sides must be data types. When two numbers are multiplied.

Smaller operands may be promoted if needed (e.g., SMALLINT to INT or BIGINT), and the result of the expression will be promoted to the next larger type.

For example, TINYINT multiplied by INT will produce a BIGINT type of result. When two numbers are multiplied, both operands and expression results are interpreted as DOUBLE types to avoid loss of precision.

If the user wants to convert the result of the expression to another type, it needs to be converted using the CAST function.

### `%`

Modulation operator. Returns the remainder of the left operand divided by the right operand. Both left and right operands must be integers.

### `&`, `|` and `^`

The bitwise operator returns the result of bitwise AND, bitwise OR, bitwise XOR operations on two operands. Both operands require an integer type.

If the types of the two operands of a bitwise operator are inconsistent, the operands of a smaller type are promoted to the operands of a larger type, and the corresponding bitwise operations are performed.

Multiple arithmetic operators can appear in an expression, and the user can enclose the corresponding arithmetic expression in parentheses. Arithmetic operators usually do not have corresponding mathematical functions to express the same functions as arithmetic operators.

For example, we don't have the MOD() function to represent the% operator. Conversely, mathematical functions do not have corresponding arithmetic operators. For example, the power function POW() does not have a corresponding ** exponentiation operator.

Users can find out which arithmetic functions we support through the Mathematical Functions section.

## `BETWEEN` Operator

In a where clause, expressions may be compared with both upper and lower bounds. If the expression is greater than or equal to the lower bound and less than or equal to the upper bound, the result of the comparison is true.

Syntax:

```sql
expression BETWEEN lower_bound AND upper_bound
```

Data type: Usually an expression evaluates to a numeric type, which also supports other data types. If you must ensure that both the lower and upper bounds are comparable characters, you can use the cast() function.

Instructions for use: If the operand is of type string, note that a long string starting with an upper bound will not match the upper bound, which is larger than the upper bound. For example, "between'A'and'M' will not match 'MJ'.

If you need to make sure the expression works correctly, you can use functions such as upper(), lower(), substr(), trim().

Example:

```sql
select c1 from t1 where month between 1 and 6;
```

## Comparison operators

Comparison operators are used to compare two values. `=`, `!=`, `>=` apply to all data types.

The `<>` and `!=` operators are equivalent, which indicate that two values are not equal.

## `IN` Operator

The In operator compares to the VALUE collection and returns TRUE if it can match any of the elements in the collection.

Parameters and VALUE collections must be comparable. All expressions using the IN operator can be written as equivalent comparisons connected with OR, but the syntax of IN is simpler, more precise, and easier for StarRocks to optimize.

Examples:

```sql
select * from small_table where tiny_column in (1,2);
```

## `LIKE` Operator

This operator is used to compare to a string. '_' (underscore) matches a single character, '%' matches multiple characters. The parameter must match the complete string. Typically, placing'%'at the end of a string is more practical.

Examples:

```plain text
mysql> select varchar_column from small_table where varchar_column like 'm%';

+----------------+
|varchar_column  |
+----------------+
|     milan      |
+----------------+

1 row in set (0.02 sec)
```

```plain
mysql> select varchar_column from small_table where varchar_column like 'm____';

+----------------+
| varchar_column | 
+----------------+
|    milan       | 
+----------------+

1 row in set (0.01 sec)
```

## Logical Operator

Logical operators return a BOOL value, including unit and multiple operators, each of which handles parameters that are expressions that return BOOL values. Supported operators are:

AND: 2-ary operator, the AND operator returns TRUE if the parameters on the left and right are both calculated as TRUE.

OR: 2-ary operator that returns TRUE if one of the parameters on the left and right is calculated as TRUE. If both parameters are FALSE, the OR operator returns FALSE.

NOT: Unit operator, the result of inverting an expression. If the parameter is TRUE, the operator returns FALSE; If the parameter is FALSE, the operator returns TRUE.

Examples:

```plain text
mysql> select true and true;

+-------------------+
| (TRUE) AND (TRUE) | 
+-------------------+
|         1         | 
+-------------------+

1 row in set (0.00 sec)
```

```plain text
mysql> select true and false;

+--------------------+
| (TRUE) AND (FALSE) | 
+--------------------+
|         0          | 
+--------------------+

1 row in set (0.01 sec)
```

```plain text
mysql> select true or false;

+-------------------+
| (TRUE) OR (FALSE) | 
+-------------------+
|        1          | 
+-------------------+

1 row in set (0.01 sec)
```

```plain text
mysql> select not true;

+----------+
| NOT TRUE | 
+----------+
|     0    | 
+----------+

1 row in set (0.01 sec)
```

## Regular Expression Operator

Determines whether the regular expression is matched. Using POSIX standard regular expressions,'^'matches the first part of the string,'$' matches the end of the string.

"." matches any single character, "*" matches zero or more options, "+" matches one or more options, "?" means greedy representation, and so on. Regular expressions need to match complete values, not just parts of strings.

If you want to match the middle part, the front part of the regular expression can be written as'^. 'or'.'. '^'and'$' are usually omitted. The RLIKE operator and the REGEXP operator are synonyms.

The'|'operator is an optional operator. Regular expressions on either side of'|' only need to satisfy one side condition. The'|'operator and regular expressions on both sides usually need to be enclosed in ().

Examples:

```plain text
mysql> select varchar_column from small_table where varchar_column regexp '(mi|MI).*';

+----------------+
| varchar_column | 
+----------------+
|     milan      |       
+----------------+

1 row in set (0.01 sec)
```

```plain text
mysql> select varchar_column from small_table where varchar_column regexp 'm.*';

+----------------+
| varchar_column | 
+----------------+
|     milan      |  
+----------------+

1 row in set (0.01 sec)
```
