# distinct_map_keys

## Description

Remove duplicate keys in a map, as map keys must be unique in terms of semantics. It keeps the last value for identical keys, called LAST WIN. It is used when querying map data from external tables if there are duplicate keys in maps. StarRocks internal tables natively remove duplicate keys in maps.

From v2.5, StarRocks supports querying complex data types MAP and STRUCT from data lakes. MAP is an unordered collection of key-value pairs, for example, `{"a":1, "b":2}`.

You can use external catalogs provided by StarRocks to query MAP and STRUCT data from Apache Hive™, Apache Hudi, and Apache Iceberg. You can only query data from ORC and Parquet files. For more information about how to use external catalogs to query external data sources, see [Overview of catalogs](../../../data_source/catalog/catalog_overview.md) and topics related to the required catalog type.

## Syntax

```Haskell
distinct_map_keys(any_map)
```

## Parameters

`any_map`: the MAP value from which you want to remove duplicate keys.

## Return value

The return a new map without duplicate keys in each map, it keeps the last value for identical keys, called LAST WIN.

If the input is NULL, NULL is returned.

## Examples

```SQL
select distinct_map_keys(col_map) as uniuqe, col_map from external_table;
+---------------+---------------+
|      uniuqe   | col_map       |
+---------------+---------------+
|       {"a":2} | {"a":1,"a":2} |
|           NULL|          NULL |
| {"e":4,"d":5} | {"e":4,"d":5} |
+---------------+---------------+
3 rows in set (0.05 sec)
```

