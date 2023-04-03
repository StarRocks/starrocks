# map_apply

## 功能

返回 map 中所有 key 或 value 进行 Lambda 函数运算后的 map 值。该函数从 3.0 版本开始支持。

StarRocks 从 2.5 版本开始支持查询数据湖中的复杂数据类型 MAP 和 STRUCT。您可以通过 StarRocks 提供的 external catalog 方式来查询 Apache Hive™，Apache Hudi，Apache Iceberg 中的 MAP 和 STRUCT 数据。仅支持查询 ORC 和 Parquet 类型文件。

想了解如何使用 external catalog 查询外部数据源，参见 [Catalog 概述](../../../data_source/catalog/catalog_overview.md) 和对应的 catalog 文档。

## 语法

```Haskell
MAP map_apply(lambda_func, any_map)
```

## 参数说明

- `lambda_func`: 应用于 map 的 Lambda 函数。

- `any_map`: 进行 Lambda 函数运算的 MAP 值。

## 返回值说明

返回一个 map。map 的 key 和 value 的数据类型由 Lambda 函数的运算结果决定。

如果输入参数是 NULL，结果也是 NULL。

如果 map 中的某个 key 或 value 是 NULL，该 NULL 值正常计算并返回。

## 示例

以下示例使用 map_from_arrays() 生成 map 值 `{1:"ab",3:"cd"}`。然后使用 Lambda 函数对 map 中的 key 加 1，对 value 计算字符长度，比如 "ab" 的字符长度是 2。

```SQL
mysql> select map_apply((k,v)->(k+1,length(v)), col_map) from (select map_from_arrays([1,3],["ab","cd"]) as col_map)A;
+--------------------------------------------------+
| map_apply((k, v) -> (k + 1, length(v)), col_map) |
+--------------------------------------------------+
| {2:2,4:2}                                        |
+--------------------------------------------------+
1 row in set (0.01 sec)

mysql> select map_apply((k,v)->(k+1,length(v)), col_map) from (select map_from_arrays(null,null) as col_map)A;
+--------------------------------------------------+
| map_apply((k, v) -> (k + 1, length(v)), col_map) |
+--------------------------------------------------+
| NULL                                             |
+--------------------------------------------------+
1 row in set (0.02 sec)

mysql> select map_apply((k,v)->(k+1,length(v)), col_map) from (select map_from_arrays([1,null],["ab","cd"]) as col_map)A;
+--------------------------------------------------+
| map_apply((k, v) -> (k + 1, length(v)), col_map) |
+--------------------------------------------------+
|{2:2,null:2}                                      |
+--------------------------------------------------+
```

## 参考文档

[map_apply](map_apply.md), [Lambda 表达式](../Lambda_expression.md)
