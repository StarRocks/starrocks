# transform_values

## 功能

对 Map 中的 value 进行 lambda 转换。有关 Lambda 函数的详细信息，参见 [Lambda expression](../Lambda_expression.md)。

该函数从 3.0 版本开始支持。

StarRocks 从 2.5 版本开始支持查询数据湖中的复杂数据类型 MAP 和 STRUCT。您可以通过 StarRocks 提供的 external catalog 方式来查询 Apache Hive™，Apache Hudi，Apache Iceberg 中的 MAP 和 STRUCT 数据。仅支持查询 ORC 和 Parquet 类型文件。想了解如何使用 external catalog 查询外部数据源，参见 [Catalog 概述](../../../data_source/catalog/catalog_overview.md) 和对应的 catalog 文档。

## 语法

```Haskell
map transform_values(lambda func, any_map)
```

`lambda_func` 也可以放在 `any_map` 之后，即:

```Haskell
map transform_values(any_map, lambda func)
```

## 参数说明

- `any_map`：要进行运算的 Map 值。

- `lambda_func`：对 `any_map` 的所有 value 进行转换的 Lambda 函数。

## 返回值说明

返回 map。map 的 value 类型由 Lambda 函数的运算结果决定；key 的类型与 `any_map` 中的 key 类型相同。

如果输入参数是 NULL，结果也是 NULL。

如果 MAP 中的某个 key 或 value 是 NULL，该 NULL 值正常处理并返回。

Lambda 函数里必须有两个参数，第一个代表 key，第二个代表 value。

## 示例

以下示例使用 [map_from_arrays](map_from_arrays.md) 生成一个 map 值 `{1:"ab",3:"cdd",2:null,null:"abc"}`。然后将 Lambda 函数应用到 map 中的每个 value。第一个示例将每个 value 变为 1，第二个示例将每个 value 变为 null。

```SQL
mysql> select transform_values((k,v)->1, col_map) from (select map_from_arrays([1,3,null,2,null],['ab','cdd',null,null,'abc']) as col_map)A;
+----------------------------------------+
| transform_values((k, v) -> 1, col_map) |
+----------------------------------------+
| {1:1,3:1,2:1,null:1}                   |
+----------------------------------------+
1 row in set (0.02 sec)


mysql> select transform_values((k,v)->null, col_map) from (select map_from_arrays([1,3,null,2,null],['ab','cdd',null,null,'abc']) as col_map)A;
+--------------------------------------------+
| transform_values((k, v) -> NULL, col_map)  |
+--------------------------------------------+
| {1:null,3:null,2:null,null:null} |
+--------------------------------------------+
1 row in set (0.01 sec)
```
