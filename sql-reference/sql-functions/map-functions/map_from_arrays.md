# map_from_arrays

## 功能

将两个 ARRAY 数组作为 key 和 value 组合成一个 MAP 对象。

StarRocks 从 2.5 版本开始支持查询数据湖中的复杂数据类型 MAP 和 STRUCT。您可以通过 StarRocks 提供的 external catalog 方式来查询 Apache Hive™，Apache Hudi，Apache Iceberg 中的 MAP 和 STRUCT 数据。仅支持查询 ORC 和 Parquet 类型文件。

想了解如何使用 external catalog 查询外部数据源，参见 [Catalog 概述](../../../data_source/catalog/catalog_overview.md) 和对应的 catalog 文档。

## 语法

```Haskell
MAP map_from_arrays(ARRAY keys, ARRAY values)
```

## 参数说明

- `keys`: 用于生成 MAP 中的 key 值。`keys` 中的元素必须唯一。
- `values`: 用于生成 MAP 中的 value 值.

## 返回值说明

返回一个 MAP 值，map 中的 key 为 `keys` 中的元素，map 中的 value 为 `values` 中的元素。

返回规则如下：

- `keys` 和 `values` 长度（元素个数）必须相同，否则返回报错。

- 如果 `keys` 或者 `values` 为 NULL, 则返回 NULL。

## 示例

```Plaintext
select map_from_arrays([1, 2], ['Star', 'Rocks']);
+--------------------------------------------+
| map_from_arrays([1, 2], ['Star', 'Rocks']) |
+--------------------------------------------+
| {1:"Star",2:"Rocks"}                       |
+--------------------------------------------+
```

```Plaintext
select map_from_arrays([1, 2], NULL);
+-------------------------------+
| map_from_arrays([1, 2], NULL) |
+-------------------------------+
| NULL                          |
+-------------------------------+
```
