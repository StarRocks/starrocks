# map_size

## Description

Returns the number of elements in a MAP value. This function has an alias [cardinality()](cardinality.md). MAP is an unordered collection of key-value pairs, for example, `{"a":1, "b":2}`. One key-value pair constitutes one element, for example, `{"a":1, "b":2}` contains two elements.

From version 2.5, StarRocks supports querying complex data types MAP and STRUCT from data lakes. You can use external catalogs provided by StarRocks to query MAP and STRUCT data from Apache Hive™, Apache Hudi, and Apache Iceberg. You can only query data from ORC and Parquet files. For more information about how to use external catalogs to query external data sources, see [Overview of catalogs](../../../data_source/catalog/catalog_overview.md) and topics related to the required catalog type.

## Syntax

```Haskell
INT map_size(any_map)
```

## Parameters

`any_map`: the MAP value from which you want to retrieve the number of elements.

## Return value

Returns a value of the INT type.

If the input is NULL, NULL is returned.

If a key or value in the MAP value is NULL, NULL is processed as a normal value.

## Examples

Use table `test_map` as an example.

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

After a Hive catalog is created in your database, you can use this catalog and the map_size() function to obtain the number of elements in each row of the `col_map` column.

```Plaintext
select map_size(col_map) from test_map order by col_int;
+-------------------+
| map_size(col_map) |
+-------------------+
|                 2 |
|                 1 |
|                 2 |
+-------------------+
3 rows in set (0.05 sec)
```
