# map_values

## Description

Returns an array of all the values in the specified map.

From version 2.5, StarRocks supports querying complex data types MAP and STRUCT from data lakes. MAP is an unordered collection of key-value pairs, for example, `{"a":1, "b":2}`.

You can use external catalogs provided by StarRocks to query MAP and STRUCT data from Apache Hive™, Apache Hudi, and Apache Iceberg. You can only query data from ORC and Parquet files. For more information about how to use external catalogs to query external data sources, see [Overview of catalogs](../) and topics related to the required catalog type. 

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

This example uses the Hive table `hive_map`, which contains the following data:

```Plain
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
