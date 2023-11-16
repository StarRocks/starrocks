# ln, dlog1, log

## Description

Computes the natural (base `e`) logarithm of a number.

## Syntax

```SQL
ln(arg)
log(arg)
dlog1(arg)
```

## Parameters

- `arg`: the value whose logarithm you want to calculate. Only the DOUBLE data type is supported.

> **NOTE**
>
> StarRocks returns NULL if `arg` is specified as a negative or 0.

## Return value

Returns a value of the DOUBLE data type.

## Example

Example 1: Calculate the natural logarithm of 3.

```SQL
mysql> select ln(3);
+--------------------+
| ln(3)              |
+--------------------+
| 1.0986122886681098 |
+--------------------+
1 row in set (0.00 sec)

mysql> select dlog1(3);
+--------------------+
| dlog1(3)              |
+--------------------+
| 1.0986122886681098 |
+--------------------+
1 row in set (0.00 sec)

mysql> select log(3);
+--------------------+
| log(3)              |
+--------------------+
| 1.0986122886681098 |
+--------------------+
1 row in set (0.00 sec)

```
