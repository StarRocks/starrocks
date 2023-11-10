# STRUCT

## 描述

STRUCT 是一种复杂数据类型，可以存储不同数据类型的元素（也称字段），例如 `<a INT, b STRING>`。

Struct 中的字段名称不能重复。字段可以是基本数据类型 (Primitive Type)，例如数值、字符串、日期类型；也可以是复杂数据类型，例如 ARRAY 或 MAP。

Struct 中的字段可以是另外一个 Struct，Map，或者 Array，方便用户定义嵌套数据结构。例如 `STRUCT<a INT, b STRUCT<c INT, d INT>, c MAP<INT, INT>, d ARRAY<INT>>`。

StarRocks 从 3.1 版本开始支持存储和导入 STRUCT 数据类型。您可以在建表时定义 STRUCT 列，向表中导入 STRUCT 数据，查询 STRUCT 数据。

StarRocks 从 2.5 版本开始支持查询数据湖中的复杂数据类型 MAP 和 STRUCT。您可以通过 StarRocks 提供的 External Catalog 方式来查询 Apache Hive™，Apache Hudi，Apache Iceberg 中的 MAP 和 STRUCT 数据。仅支持查询 ORC 和 Parquet 类型文件。

想了解如何使用 External Catalog 查询外部数据源，参见 [Catalog 概述](../../../data_source/catalog/catalog_overview.md) 和对应的 Catalog 文档。

## 语法

```SQL
STRUCT<name type>
```

- `name`：字段名称，和建表语句中的列名相同。
- `type`：字段类型。可以是 StarRocks 支持的任意类型。

## 定义 STRUCT 类型列

建表时可以在 CREATE TABLE 语句中定义 STRUCT 类型的列，后续导入 STRUCT 数据到该列。

```SQL
-- 定义简单 struct。
CREATE TABLE t0(
  c0 INT,
  c1 STRUCT<a INT, b INT>
)
DUPLICATE KEY(c0);

-- 定义复杂 struct。
CREATE TABLE t1(
  c0 INT,
  c1 STRUCT<a INT, b STRUCT<c INT, d INT>, c MAP<INT, INT>, d ARRAY<INT>>
)
DUPLICATE KEY(c0);

-- 定义非 NULL struct。
CREATE TABLE t2(
  c0 INT,
  c1 STRUCT<a INT, b INT> NOT NULL
)
DUPLICATE KEY(c0);
```

STRUCT 列有如下使用限制：

- 不支持作为表的 Key 列，只能作为 Value 列。
- 不支持作为表的分区列（PARTITION BY 中定义的列）。
- 不支持作为表的分桶列 （DISTRIBUTED BY 中定义的列）。
- STRUCT 作为[聚合模型表](../../../table_design/table_types/aggregate_table.md)的 Value 列时，仅支持 replace() 作为聚合函数。

## 使用 SQL 构建 STRUCT

您可以使用 [row/struct](../../sql-functions/struct-functions/row.md) 或 [named_struct](../../sql-functions/struct-functions/named_struct.md) 函数来构建 Struct。

- `row` 和 `struct` 功能相同，支持 unnamed struct （未指定字段名称）。字段名称由 StarRocks 自动生成，比如 `col1, col2,...colN`。
- `named_struct` 支持 named struct （指定了字段名称）。 Key 和 Value 表达式必须成对出现，否则构建失败。

StarRocks 根据输入的值自动确定 Struct 的数据类型。

```SQL
select row(1, 2, 3, 4) as numbers; -- 返回 {"col1":1,"col2":2,"col3":3,"col4":4}。
select row(1, 2, null, 4) as numbers; -- 返回 {"col1":1,"col2":2,"col3":null,"col4":4}。
select row(null) as nulls; -- 返回 {"col1":null}。
select struct(1, 2, 3, 4) as numbers; -- 返回 {"col1":1,"col2":2,"col3":3,"col4":4}。
select named_struct('a', 1, 'b', 2, 'c', 3, 'd', 4) as numbers; -- 返回 {"a":1,"b":2,"c":3,"d":4}。
```

## 导入 STRUCT 类型数据

可以使用两种方式导入 STRUCT 数据到 StarRocks：[INSERT INTO](../data-manipulation/insert.md) 和 [ORC/Parquet 文件导入](../data-manipulation/BROKER_LOAD.md)。

导入过程中 StarRocks 会对重复的 Key 值进行删除。

### INSERT INTO 导入

```SQL
CREATE TABLE t0(
  c0 INT,
  c1 STRUCT<a INT, b INT>
)
DUPLICATE KEY(c0);

INSERT INTO t0 VALUES(1, row(1, 1));

SELECT * FROM t0;
+------+---------------+
| c0   | c1            |
+------+---------------+
|    1 | {"a":1,"b":1} |
+------+---------------+
```

### 从 ORC 和 Parquet 文件导入

StarRocks 的 STRUCT 类型对应 ORC 和 Parquet 格式中的嵌套列 (nested columns structure)，无需您做额外的转换或定义。具体导入操作，参考 [Broker Load 文档](../data-manipulation/BROKER_LOAD.md)。

## 查询 STRUCT 数据

您可以使用 `.` 操作符来查询指定字段对应的值，或者使用 `[]` 返回指定索引对应的值。

```Plain
mysql> select named_struct('a', 1, 'b', 2, 'c', 3, 'd', 4).a;
+------------------------------------------------+
| named_struct('a', 1, 'b', 2, 'c', 3, 'd', 4).a |
+------------------------------------------------+
| 1                                              |
+------------------------------------------------+

mysql> select row(1, 2, 3, 4).col1;
+-----------------------+
| row(1, 2, 3, 4).col1  |
+-----------------------+
| 1                     |
+-----------------------+

mysql> select row(2, 4, 6, 8)[2];
+--------------------+
| row(2, 4, 6, 8)[2] |
+--------------------+
|                  4 |
+--------------------+

mysql> select row(map{'a':1}, 2, 3, 4)[1];
+-----------------------------+
| row(map{'a':1}, 2, 3, 4)[1] |
+-----------------------------+
| {"a":1}                     |
+-----------------------------+
```
