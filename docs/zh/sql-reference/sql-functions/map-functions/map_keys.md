---
displayed_sidebar: "Chinese"
---

# map_keys

## 功能

返回 Map 中所有 key 组成的数组。MAP 里保存的是键值对 (key-value pair)，比如 `{"a":1, "b":2}`。

StarRocks 从 2.5 版本开始支持查询数据湖中的复杂数据类型 MAP 和 STRUCT。您可以通过 StarRocks 提供的 external catalog 方式来查询 Apache Hive™，Apache Hudi，Apache Iceberg 中的 MAP 和 STRUCT 数据。仅支持查询 ORC 和 Parquet 类型文件。

想了解如何使用 external catalog 查询外部数据源，参见 [Catalog 概述](../../../data_source/catalog/catalog_overview.md) 和对应的 catalog 文档。

## 语法

```Haskell
ARRAY map_keys(any_map)
```

## 参数说明

`any_map`:  要获取 keys 的 MAP 值。

## 返回值说明

返回 ARRAY 类型的数组，格式为 `array<keyType>`。`keyType` 的数据类型和 MAP 值里的 key 类型相同。

如果输入参数是 NULL，结果也是 NULL。如果 MAP 中的某个 key 或 value 是 NULL，该 NULL 值正常返回。

## 示例

假设 Hive 中有表 `hive_map`，数据如下：

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

通过在本地数据库创建 Hive catalog 来访问该表，获取 `col_map` 列每行的所有 keys。

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
