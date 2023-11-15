# ARRAY

本文介绍如何使用 StarRocks 中的数组类型。

数组（Array) 是数据库中的一种扩展数据类型，其相关特性在众多数据库系统中均有支持，可以广泛的应用于 A/B Test 对比、用户标签分析、人群画像等场景。StarRocks 当前支持多维数组嵌套、数组切片、比较、过滤等特性。

## 定义数组类型的列

您可以在建表时定义数组类型的列。

~~~SQL
-- 定义一维数组。
ARRAY<type>

-- 定义嵌套数组。
ARRAY<ARRAY<type>>

-- 定义 NOT NULL 数组列。
ARRAY<type> NOT NULL
~~~

数组列的定义形式为 `ARRAY<type>`，其中 `type` 表示数组内的元素类型。数组元素目前支持以下数据类型：BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、VARCHAR、CHAR、DATETIME、DATE、JSON、BINARY (3.0 及以后）、MAP (3.1 及以后）、STRUCT (3.1 及以后）、Fast Decimal (3.1 及以后)。

数组内的元素默认可以为 NULL，比如 [NULL, 1, 2]。暂时不支持指定数组元素为 NOT NULL，但是您可以定义数组列本身为 NOT NULL，比如上面的第三个示例。

> **注意**
>
> 数组类型的列在使用时有以下限制：
>
> * StarRocks 2.1 之前版本中，仅支持在明细模型表中（Duplicate Key）定义数组类型列。自 2.1 版本开始，支持在 Primary Key、Unique Key、Aggregate Key 模型表中定义数组类型列。注意在聚合模型表（Aggregate Key) 中，仅当聚合列的聚合函数为 replace 和 replace_if_not_null 时，才支持将该列定义为数组类型。
> * 数组列暂时不能作为 Key 列。
> * 数组列不能作为分桶（Distributed By）列。
> * 数组列不能作为分区（Partition By）列。
> * 数组列暂不支持 DECIMAL V3 数据类型。
> * 数组列最多支持 14 层嵌套。

示例：

~~~SQL
-- 建表并指定其中的 `c1` 列为一维数组，元素类型为 INT。
create table t0(
  c0 INT,
  c1 ARRAY<INT>
)
duplicate key(c0)
distributed by hash(c0);

-- 建表并指定 `c1` 为两层的嵌套数组，元素类型为 VARCHAR。
create table t1(
  c0 INT,
  c1 ARRAY<ARRAY<VARCHAR(10)>>
)
duplicate key(c0)
distributed by hash(c0);

-- 建表并定义 NOT NULL 数组的列。
create table t2(
  c0 INT,
  c1 ARRAY<INT> NOT NULL
)
duplicate key(c0)
distributed by hash(c0);
~~~

## 使用 SELECT 语句构造数组

您可以在 SQL 语句中通过中括号（`[]`）来构造数组常量，每个数组元素通过逗号（`,`）分隔。

示例：

~~~Plain Text
mysql> select [1, 2, 3] as numbers;

+---------+
| numbers |
+---------+
| [1,2,3] |
+---------+

mysql> select ["apple", "orange", "pear"] as fruit;

+---------------------------+
| fruit                     |
+---------------------------+
| ["apple","orange","pear"] |
+---------------------------+

mysql> select [true, false] as booleans;

+----------+
| booleans |
+----------+
| [1,0]    |
+----------+
~~~

当数组元素具有不同类型时，StarRocks 会自动推导出合适的类型（supertype）。

~~~Plain Text
mysql> select [1, 1.2] as floats;

+---------+
| floats  |
+---------+
| [1,1.2] |
+---------+

mysql> select [12, "100"];

+--------------+
| [12,'100']   |
+--------------+
| ["12","100"] |
+--------------+
~~~

您可以使用尖括号（`<>`）声明数组类型。

~~~Plain Text
mysql> select ARRAY<float>[1, 2];

+-----------------------+
| ARRAY<float>[1.0,2.0] |
+-----------------------+
| [1,2]                 |
+-----------------------+

mysql> select ARRAY<INT>["12", "100"];

+------------------------+
| ARRAY<int(11)>[12,100] |
+------------------------+
| [12,100]               |
+------------------------+
~~~

数组元素中可以包含 NULL。

~~~Plain Text
mysql> select [1, NULL];

+----------+
| [1,NULL] |
+----------+
| [1,null] |
+----------+
~~~

定义空数组时，您可以使用尖括号声明其类型。您也可以直接定义为 `[]`，此时 StarRocks 会根据上下文推断其类型，如果无法推断则会报错。

~~~Plain Text
mysql> select [];

+------+
| []   |
+------+
| []   |
+------+

mysql> select ARRAY<VARCHAR(10)>[];

+----------------------------------+
| ARRAY<unknown type: NULL_TYPE>[] |
+----------------------------------+
| []                               |
+----------------------------------+

mysql> select array_append([], 10);

+----------------------+
| array_append([], 10) |
+----------------------+
| [10]                 |
+----------------------+
~~~

## 导入数组类型的数据

StarRocks 当前支持三种方式写入数组数据。

### 通过 INSERT INTO 语句导入数组

INSERT INTO 语句导入方式适合小批量数据逐行导入或对 StarRocks 内外部表数据进行 ETL 处理并导入。

示例：

~~~SQL
create table t0(
  c0 INT,
  c1 ARRAY<INT>
)
duplicate key(c0)
distributed by hash(c0);
INSERT INTO t0 VALUES(1, [1,2,3]);
~~~

### 通过 Broker Load 批量导入 ORC 或 Parquet 文件中的数组

StarRocks 中的数组类型，与 ORC 或 Parquet 格式中的 List 结构相对应，所以无需额外指定。具体导入方法请参考 [Broker load](../data-manipulation/BROKER_LOAD.md)。

当前 StarRocks 支持直接导入 ORC 文件的 List 结构。Parquet 格式导入正在开发中。

### 通过 Stream Load 或 Routine Load 导入 CSV 格式数组

您可以使用 [Stream Load](../../../loading/StreamLoad.md#导入-csv-格式的数据) 或 [Routine Load](../../../loading/RoutineLoad.md#导入-csv-数据) 方式导入 CSV 文本文件或 Kafka 中的 CSV 格式数据，默认采用逗号分隔。

## 访问数组中的元素

您可以使用中括号（`[]`）加下标形式访问数组中某个元素。下标从 `1` 开始。

~~~Plain Text
mysql> select [1,2,3][1];

+------------+
| [1,2,3][1] |
+------------+
|          1 |
+------------+
~~~

如果您标记下标为 `0` 或负数，StarRocks **不会报错，会返回 NULL**。

~~~Plain Text
mysql> select [1,2,3][0];

+------------+
| [1,2,3][0] |
+------------+
|       NULL |
+------------+
~~~

如果您标记的下标超过数组大小，StarRocks **会返回 NULL**。

~~~Plain Text
mysql> select [1,2,3][4];

+------------+
| [1,2,3][4] |
+------------+
|       NULL |
+------------+
~~~

对于多维数组，您可以通过**递归**方式访问内部元素。

~~~Plain Text
mysql> select [[1,2],[3,4]][2];

+------------------+
| [[1,2],[3,4]][2] |
+------------------+
| [3,4]            |
+------------------+

mysql> select [[1,2],[3,4]][2][1];

+---------------------+
| [[1,2],[3,4]][2][1] |
+---------------------+
|                   3 |
+---------------------+
~~~
