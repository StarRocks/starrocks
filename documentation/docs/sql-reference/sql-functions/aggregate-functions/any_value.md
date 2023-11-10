# any_value

## Description

Obtains an arbitrary row from each aggregated group. You can use this function to optimize a query that has a `GROUP BY` clause.

## Syntax

```Haskell
ANY_VALUE(expr)
```

## Parameters

`expr`: the expression that gets aggregated.

## Return value

Returns an arbitrary row from each aggregated group. The return value is non-deterministic.

## Examples

```plaintext
// Original data
mysql> select * from any_value_test;
+------+------+------+
| a    | b    | c    |
+------+------+------+
|    1 |    1 |    1 |
|    1 |    2 |    1 |
|    2 |    1 |    1 |
|    2 |    2 |    2 |
|    3 |    1 |    1 |
+------+------+------+
5 rows in set (0.01 sec)

// After ANY_VALUE is used
mysql> select a,any_value(b),sum(c) from any_value_test group by a;
+------+----------------+----------+
| a    | any_value(`b`) | sum(`c`) |
+------+----------------+----------+
|    1 |              1 |        2 |
|    2 |              1 |        3 |
|    3 |              1 |        1 |
+------+----------------+----------+
3 rows in set (0.01 sec)

mysql> select c,any_value(a),sum(b) from any_value_test group by c;
+------+----------------+----------+
| c    | any_value(`a`) | sum(`b`) |
+------+----------------+----------+
|    1 |              1 |        5 |
|    2 |              2 |        2 |
+------+----------------+----------+
2 rows in set (0.01 sec)
```

## Keywords

ANY_VALUE
