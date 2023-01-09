# map_keys

## Description

Returns an array of all the keys in the specified map.

From version 2.5, StarRocks supports querying complex data types MAP and STRUCT from data lakes. MAP is an unordered collection of key-value pairs, for example, `{"a":1, "b":2}`.

You can use external catalogs provided by StarRocks to query MAP and STRUCT data from Apache Hive™, Apache Hudi, and Apache Iceberg. You can only query data from ORC and Parquet files. For more information about how to use external catalogs to query external data sources, see [Overview of catalogs](../../../data_source/catalog/catalog_overview.md) and topics related to the required catalog type.

## Syntax

```Haskell
map_keys(any_map)
```

## Parameters

`any_map`: the MAP value from which you want to retrieve keys.

## Return value

The return value is in the format of `array<keyType>`. The element type in the array matches the key type in the map.

If the input is NULL, NULL is returned. If a key or value in the MAP value is NULL, NULL is processed as a normal value and contained in the result.

## Examples

This example uses the Hive table `hive_map`, which contains the following data:

```SQL
select * from hive_map order by col_int;
+---------+---------------+
| col_int | col_map       |
+---------+---------------+
|       1 | {"a":1,"b":2} |
|       2 | {"c":3}       |
|       3 | {"d":4,"e":5} |
+---------+---------------+
3 rows in set (0.05 sec)
```

After a Hive catalog is created in your database, you can use this catalog and the map_keys() function to obtain all the keys from each row of the `col_map` column.

```Plain
select map_keys(col_map) from hive_map order by col_int;
+-------------------+
| map_keys(col_map) |
+-------------------+
| ["a","b"]         |
| ["c"]             |
| ["d","e"]         |
+-------------------+
3 rows in set (0.05 sec)
```
