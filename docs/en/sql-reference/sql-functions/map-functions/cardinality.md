---
displayed_sidebar: "English"
---

# cardinality

## Description

Returns the number of elements in a MAP value. MAP is an unordered collection of key-value pairs, for example, `{"a":1, "b":2}`. One key-value pair constitutes one element. `{"a":1, "b":2}` contains two elements.

This function is supported from v3.0 onwards. It is the alias of [map_size()](map_size.md).

## Syntax

```Haskell
INT cardinality(any_map)
```

## Parameters

`any_map`: the MAP value from which you want to retrieve the number of elements.

## Return value

Returns a value of the INT value.

If the input is NULL, NULL is returned.

If a key or value in the MAP value is NULL, NULL is processed as a normal value.

## Examples

### Query MAP data from a StarRocks native table

From v3.1 onwards, StarRocks supports defining MAP columns when you create a table. This example uses table `test_map`, which contains the following data:

```Plain
CREATE TABLE test_map(
    col_int INT,
    col_map MAP<VARCHAR(50),INT>
  )
DUPLICATE KEY(col_int);

INSERT INTO test_map VALUES
(1,map{"a":1,"b":2}),
(2,map{"c":3}),
(3,map{"d":4,"e":5});

SELECT * FROM test_map ORDER BY col_int;
+---------+---------------+
| col_int | col_map       |
+---------+---------------+
|       1 | {"a":1,"b":2} |
|       2 | {"c":3}       |
|       3 | {"d":4,"e":5} |
+---------+---------------+
3 rows in set (0.05 sec)
```

Obtain the number of elements in each row of the `col_map` column.

```Plaintext
select cardinality(col_map) from test_map order by col_int;
+----------------------+
| cardinality(col_map) |
+----------------------+
|                    2 |
|                    1 |
|                    2 |
+----------------------+
3 rows in set (0.05 sec)
```

### Query MAP data from data lake

This example uses Hive table `hive_map`, which contains the following data:

```Plaintext
SELECT * FROM hive_map ORDER BY col_int;
+---------+---------------+
| col_int | col_map       |
+---------+---------------+
|       1 | {"a":1,"b":2} |
|       2 | {"c":3}       |
|       3 | {"d":4,"e":5} |
+---------+---------------+
```

After a [Hive catalog](../../../data_source/catalog/hive_catalog.md#create-a-hive-catalog) is created in your cluster, you can use this catalog and the cardinality() function to obtain the number of elements in each row of the `col_map` column.

```Plaintext
SELECT cardinality(col_map) FROM hive_map ORDER BY col_int;
+----------------------+
| cardinality(col_map) |
+----------------------+
|                    2 |
|                    1 |
|                    2 |
+----------------------+
3 rows in set (0.05 sec)
```
