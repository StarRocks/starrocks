# map_concat

## Description

Returns the union results of input maps, keeping the last value for identical keys.

From version 2.5, StarRocks supports querying complex data types MAP and STRUCT from data lakes. MAP is an unordered collection of key-value pairs, for example, `{"a":1, "b":2}`. One key-value pair constitutes one element, for example, `{"a":1, "b":2}` contains two elements.

You can use external catalogs provided by StarRocks to query MAP and STRUCT data from Apache Hive™, Apache Hudi, and Apache Iceberg. You can only query data from ORC and Parquet files. For more information about how to use external catalogs to query external data sources, see [Overview of catalogs](../../../data_source/catalog/catalog_overview.md) and topics related to the required catalog type.

## Syntax

```Haskell
any_map map_concat(any_map0, any_map1...)
```

## Return value

Returns the union results of input maps, keeping the last value for identical keys. If data types of input maps are not the same, the return type is the common super type of input maps.

## Examples


```Plain
mysql> select map_concat({1:3},{'3.323':3});
+----------------------------------+
| map_concat((1, 3), ('3.323', 3)) |
+----------------------------------+
| {"3.323":3,"1":3}                |
+----------------------------------+
1 row in set (0.19 sec)

mysql> select map_concat({1:3},{1:'4', 3:'5',null:null}, null);
+--------------------------------------------------------+
| map_concat((1, 3), (1, '4', 3, '5', NULL, NULL), NULL) |
+--------------------------------------------------------+
| {1:"4",3:"5",null:null}                                |
+--------------------------------------------------------+
1 row in set (0.01 sec)

```
