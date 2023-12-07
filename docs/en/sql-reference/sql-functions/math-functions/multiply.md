---
displayed_sidebar: "English"
---

# multiply

## Description

Computes the product of the arguments.

## Syntax

```Haskell
multiply(arg1, arg2)
```

### Parameters

`arg1`: numeric source column or literal.
`arg2`: numeric source column or literal.

## Return value

Returns the product of the two arguments. The return type depends on the arguments.

## Usage notes

If you specify a non-numeric value, this function will fail.

## Examples

```Plain
MySQL > select multiply(10,2);
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

MySQL > select * from t;
+------+------+------+------+
| id   | name | job1 | job2 |
+------+------+------+------+
|    2 |    2 |    2 |    2 |
+------+------+------+------+
1 row in set (0.08 sec)

MySQL > select multiply(1.0,id) from t;
+-------------------+
| multiply(1.0, id) |
+-------------------+
|                 2 |
+-------------------+
1 row in set (0.01 sec) 
```

## keyword

multiply
