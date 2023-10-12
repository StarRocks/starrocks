# map_apply

## Description

Applies a [Lambda expression](../Lambda_expression.md) to the keys and values of the original Map and generates a new Map. This function is supported from v3.0.

From v2.5, StarRocks supports querying complex data types MAP and STRUCT from data lakes. MAP is an unordered collection of key-value pairs, for example, `{"a":1, "b":2}`.

You can use external catalogs provided by StarRocks to query MAP and STRUCT data from Apache Hive™, Apache Hudi, and Apache Iceberg. You can only query data from ORC and Parquet files. For more information about how to use external catalogs to query external data sources, see [Overview of catalogs](../../../data_source/catalog/catalog_overview.md) and topics related to the required catalog type.

## Syntax

```Haskell
MAP map_apply(lambda_func, any_map)
```

## Parameters

- `lambda_func`: the Lambda expression.

- `any_map`: the map to which the Lambda expression is applied.

## Return value

Returns a map value. The data types of keys and values in the result map are determined by the result of the Lambda expression.

If any input parameter is NULL, NULL is returned.

If a key or value in the original map is NULL, NULL is processed as a normal value.

The Lambda expression must have two parameters. The first parameter represents the key. The second parameter represents the value.
