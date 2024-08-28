---
displayed_sidebar: docs
---

# any_value

## Description

Obtains an arbitrary row from each aggregated group. You can use this function to optimize a query that has a `GROUP BY` clause.

## Syntax

```Haskell
ANY_VALUE(expr)
```

## Parameters

`expr`: the expression that gets aggregated. Since v3.2, `expr` can evaluate to ARRAY, MAP, and STRUCT.

## Return value

Returns an arbitrary row from each aggregated group. The return value is non-deterministic.

## Examples

Create a table `t0` and insert data into this table.

```sql
CREATE TABLE t0(
  a INT,
  b BIGINT,
  c SMALLINT,
  d ARRAY<INT>,
  e JSON,
  f MAP<INT, INT>,
  g STRUCT<a STRING, b INT>
)
DUPLICATE KEY(a)
DISTRIBUTED BY HASH(a);

INSERT INTO t0 VALUES
(1, 1, 1, [2,3,4],parse_json('{"a":1, "b":true}'), map{1:1,3:4}, row(1, 2)),
(1, 2, 1, [2,3,5],parse_json('{"a":2, "b":true}'), map{1:2,3:3},row(2, 2)),
(2, 1, 1, [2,3,6],parse_json('{"a":3, "b":true}'), map{2:1,3:2},row(3, 2)),
(2, 2, 2, [2,4,5],parse_json('{"a":4, "b":false}'),map{1:3,3:1},row(4, 2)),
(3, 1, 1, [3,3,5],parse_json('{"a":5, "b":false}'),map{2:1,3:3},row(1, 2));
```

```plain text
mysql> select * from t0 order by a;
+------+------+------+---------+----------------------+-----------+-----------------+
| a    | b    | c    | d       | e                    | f         | g               |
+------+------+------+---------+----------------------+-----------+-----------------+
|    1 |    1 |    1 | [2,3,4] | {"a": 1, "b": true}  | {1:1,3:4} | {"a":"1","b":2} |
|    1 |    2 |    1 | [2,3,5] | {"a": 2, "b": true}  | {1:2,3:3} | {"a":"2","b":2} |
|    2 |    1 |    1 | [2,3,6] | {"a": 3, "b": true}  | {2:1,3:2} | {"a":"3","b":2} |
|    2 |    2 |    2 | [2,4,5] | {"a": 4, "b": false} | {1:3,3:1} | {"a":"4","b":2} |
|    3 |    1 |    1 | [3,3,5] | {"a": 5, "b": false} | {2:1,3:3} | {"a":"1","b":2} |
+------+------+------+---------+----------------------+-----------+-----------------+
5 rows in set (0.01 sec)
```

Use `any_value` for data query. For `a = 1` and `a = 2`, an arbitrary row is returned for `b`.

```plain text
mysql> select a,any_value(b),sum(c) from t0 group by a;
+------+----------------+----------+
| a    | any_value(`b`) | sum(`c`) |
+------+----------------+----------+
|    1 |              1 |        2 |
|    2 |              1 |        3 |
|    3 |              1 |        1 |
+------+----------------+----------+
3 rows in set (0.01 sec)

mysql> select a,any_value(d),sum(b) from t0 group by a;
+------+--------------+--------+
| a    | any_value(d) | sum(b) |
+------+--------------+--------+
|    3 | [3,3,5]      |      1 |
|    1 | [2,3,4]      |      3 |
|    2 | [2,3,6]      |      3 |
+------+--------------+--------+
2 rows in set (0.01 sec)
```
