---
displayed_sidebar: "English"
---

# map_values

## Description

Returns an array of all the values in the specified map.

This function is supported from v2.5.

## Syntax

```Haskell
map_values(any_map)
```

## Parameters

`any_map`: the MAP value from which you want to retrieve values.

## Return value

The return value is in the format of `array<valueType>`. The element type in the array matches the value type in the map.

If the input is NULL, NULL is returned. If a key or value in the MAP value is NULL, NULL is processed as a normal value and contained in the result.

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

Obtain all the values from each row of the `col_map` column.

```SQL
select map_values(col_map) from test_map order by col_int;
+---------------------+
| map_values(col_map) |
+---------------------+
| [1,2]               |
| [3]                 |
| [4,5]               |
+---------------------+
3 rows in set (0.04 sec)
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

After a [Hive catalog](../../../data_source/catalog/hive_catalog.md#create-a-hive-catalog) is created in your cluster, you can use this catalog and the map_values() function to obtain all the values from each row of the `col_map` column.

```SQL
select map_values(col_map) from hive_map order by col_int;
+---------------------+
| map_values(col_map) |
+---------------------+
| [1,2]               |
| [3]                 |
| [4,5]               |
+---------------------+
3 rows in set (0.04 sec)
```
