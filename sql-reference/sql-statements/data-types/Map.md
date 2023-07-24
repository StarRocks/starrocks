# MAP

## 描述

MAP 是一种复杂数据类型，用于存储无序的键值对（key-value pair)，例如 `{a:1, b:2, c:3}`。Map 中的 Key 不能重复。一个 Map 最多支持 14 层嵌套。

StarRocks 从 3.1 版本开始支持 MAP 数据类型。您可以在建表时定义 MAP 列，向表中导入 MAP 数据，查询 MAP 数据。

StarRocks 从 2.5 版本开始支持查询数据湖中的复杂数据类型 MAP 和 STRUCT。您可以通过 StarRocks 提供的 External Catalog 方式来查询 Apache Hive™，Apache Hudi，Apache Iceberg 中的 MAP 和 STRUCT 数据。仅支持查询 ORC 和 Parquet 类型文件。

想了解如何使用 External Catalog 查询外部数据源，参见 [Catalog 概述](../../../data_source/catalog/catalog_overview.md) 和对应的 Catalog 文档。

## 语法

```SQL
MAP<key_type,value_type>
```

- `key_type`：Key 的数据类型。必须是 StarRocks 支持的基本数据类型 (Primitive Type)，例如数值、字符串、日期类型。不支持复杂类型，例如 HLL、JSON、ARRAY、MAP、BITMAP、STRUCT。
- `value_type`：Value 的数据类型。可以是 StarRocks 支持的任意类型，包括复杂类型。

Key 和 Value 取值都可以为 NULL。

## 定义 MAP 类型列

建表时可以在 CREATE TABLE 语句中定义 MAP 类型的列，后续导入 MAP 数据到该列。

```SQL
-- 定义简单 map。
CREATE TABLE t0(
  c0 INT,
  c1 MAP<INT,INT>
)
DUPLICATE KEY(c0);

-- 定义嵌套 map。
CREATE TABLE t1(
  c0 INT,
  c1 MAP<DATE, MAP<VARCHAR(10), INT>>
)
DUPLICATE KEY(c0);

-- 定义非 NULL map。
CREATE TABLE t2(
  c0 INT,
  c1 MAP<INT,DATETIME> NOT NULL
)
DUPLICATE KEY(c0);
```

MAP 列有如下使用限制：

- 不支持作为表的 Key 列，只能作为 Value 列。
- 不支持作为表的分区列（PARTITION BY 中定义的列）。
- 不支持作为表的分桶列 （DISTRIBUTED BY 中定义的列）。

## 使用 SQL 构建 MAP

可以使用以下两种语法构建 Map：

- `map{key_expr:value_expr, ...}`：Map 中的元素以逗号分隔 (`,`) ，元素中的 Key 和 Value 以冒号分隔 (`:`) ，例如 `map{a:1, b:2, c:3}`。
- `map(key_expr, value_expr ...)`：Key 和 Value 的表达式必须成对出现，否则构建失败，例如 `map(a,1,b,2,c,3)`。

StarRocks 可以根据输入的 Key 和 Value 推导出 Key 和 Value 的数据类型。

```SQL
select map{1:1, 2:2, 3:3} as numbers;
select map(1,1,2,2,3,3) as numbers; -- 返回 {1:1,2:2,3:3}。
select map{1:"apple", 2:"orange", 3:"pear"} as fruit;
select map(1, "apple", 2, "orange", 3, "pear") as fruit; -- 返回 {1:"apple",2:"orange",3:"pear"}。
select map{true:map{3.13:"abc"}, false:map{}} as nest;
select map(true, map(3.13, "abc"), false, map{}) as nest; -- 返回 {1:{3.13:"abc"},0:{}}。
```

如果 Key 或 Value 的数据类型不一致，StarRocks 自动推导出合适的数据类型 (supertype)。

```SQL
select map{1:2.2, 1.2:21} as floats_floats; -- 返回 {1.0:2.2,1.2:21.0}。
select map{12:"a", "100":1, NULL:NULL} as string_string; -- 返回 {"12":"a","100":"1",null:null}。
```

您也可以在构建 Map 时使用 `<>` 来定义 map 的数据类型。输入的 Key 值和 Value 值必须能够转换成定义的类型。

```SQL
select map<FLOAT,INT>{1:2}; -- 返回 {1:2}。
select map<INT,INT>{"12": "100"}; -- 返回 {12:100}。
```

Key 和 Value 可以为 NULL。

```SQL
select map{1:NULL};
```

构建空 Map。

```SQL
select map{} as empty_map;
select map() as empty_map; -- 返回 {}。
```

## 导入 MAP 类型数据

可以使用两种方式导入 MAP 数据到 StarRocks：[INSERT INTO](../data-manipulation/insert.md) 和 [ORC/Parquet 文件导入](../data-manipulation/BROKER%20LOAD.md)。

导入过程中 StarRocks 会对重复的 Key 值进行删除。

### INSERT INTO 导入

```SQL
  CREATE TABLE t0(
    c0 INT,
    c1 MAP<INT,INT>
  )
  DUPLICATE KEY(c0);
  
  INSERT INTO t0 VALUES(1, map{1:2,3:NULL});
```

### 从 ORC 和 Parquet 文件导入

StarRocks 的 MAP 类型对应 ORC 和 Parquet 格式中的 MAP，无需您做额外的转换或定义。具体导入操作，参考 [Broker Load 文档](../data-manipulation/BROKER%20LOAD.md)。

## 查询 MAP 数据

**示例一：**查询表 `t0` 中的 MAP 列 `c1`。

```Plain
mysql> select c1 from t0;
+--------------+
| c1           |
+--------------+
| {1:2,3:null} |
+--------------+
```

**示例二：**使用 `[ ]` 操作符，或 `element_at(any_map, any_key)` 函数来查询 key 对应的 value 值。

下列两个示例查询 Key `1` 对应的 Value 值。

```Plain
mysql> select map{1:2,3:NULL}[1];
+-----------------------+
| map(1, 2, 3, NULL)[1] |
+-----------------------+
|                     2 |
+-----------------------+

mysql> select element_at(map{1:2,3:NULL},1);
+--------------------+
| map{1:2,3:NULL}[1] |
+--------------------+
|                  2 |
+--------------------+
```

如果 Key 不存在，返回 NULL。

```Plain
mysql> select map{1:2,3:NULL}[2];
+-----------------------+
| map(1, 2, 3, NULL)[2] |
+-----------------------+
|                  NULL |
+-----------------------+
```

**示例三：**递归查询复杂 MAP 中的元素。 下列示例首先查询 Key `1` 对应的值，值为 `map{2:1}`。然后进一步查询该 Map 中 Key `2` 对应的值。

```Plain Text
mysql> select map{1:map{2:1},3:NULL}[1][2];

+----------------------------------+
| map(1, map(2, 1), 3, NULL)[1][2] |
+----------------------------------+
|                                1 |
+----------------------------------+
```

## 相关参考

- [Map 函数](../../sql-functions/map-functions/map_values.md)
- [element_at](../../sql-functions/array-functions/element_at.md)
