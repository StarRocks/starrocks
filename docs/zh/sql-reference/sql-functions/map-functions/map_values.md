---
displayed_sidebar: "Chinese"
---

# map_values

## 功能

返回 Map 中所有 value 组成的数组。MAP 里保存的是键值对 (key-value pair)，比如 `{"a":1, "b":2}`。

StarRocks 从 2.5 版本开始支持查询数据湖中的复杂数据类型 MAP 和 STRUCT。您可以通过 StarRocks 提供的 external catalog 方式来查询 Apache Hive™，Apache Hudi，Apache Iceberg 中的 MAP 和 STRUCT 数据。仅支持查询 ORC 和 Parquet 类型文件。

想了解如何使用 external catalog 查询外部数据源，参见 [Catalog 概述](../../../data_source/catalog/catalog_overview.md) 和对应的 catalog 文档。

## 语法

```Haskell
map_values(any_map)
```

## 参数说明

`any_map`:  要获取 values 的 MAP 值，必须是 MAP 类型的值。

## 返回值说明

返回 ARRAY 类型的数组，格式为 `array<valueType>`。`valueType` 的数据类型和 MAP 值里的 value 类型相同。

如果输入参数是 NULL，结果也是 NULL。

## 示例

假设 Hive 中有表 `hive_map`，数据如下：

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

通过在本地数据库创建 Hive catalog 来访问该表，获取 `col_map` 列每行的所有 values。

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
