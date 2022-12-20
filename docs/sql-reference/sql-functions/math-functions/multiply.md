# multiply

## Description

Computes the product of the argument.

## Syntax

```Haskell
multiply(arg1, arg2)
```

### Parameters

`arg1`: Numeric source column or literal.
`arg2`: Numeric source column or literal.

## Return value

Returns a value of product of the argument, the return type depends on the argument.

## Usage notes

If you specify a non-numeric value, this function will fail.

## Examples

```Plain
MySQL [test]> select * from t;
+------+------+------+------+
| id   | name | job1 | job2 |
+------+------+------+------+
|    2 |    2 |    2 |    2 |
+------+------+------+------+
1 row in set (0.08 sec)

MySQL [test]> select multiply(10,2);
+-----------------+
| multiply(10, 2) |
+-----------------+
|              20 |
+-----------------+
1 row in set (0.01 sec)

MySQL [test]> select multiply(1,2.1);
+------------------+
| multiply(1, 2.1) |
+------------------+
|              2.1 |
+------------------+
1 row in set (0.01 sec)

MySQL [test]> select multiply(1.0,id) from t;
+-------------------+
| multiply(1.0, id) |
+-------------------+
|                 2 |
+-------------------+
1 row in set (0.01 sec)

MySQL [test]> 
```

## keyword

multiply