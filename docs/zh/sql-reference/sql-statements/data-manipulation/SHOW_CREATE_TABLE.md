---
displayed_sidebar: "Chinese"
---

# SHOW CREATE TABLE

## 功能

查看指定表的建表语句。

自 3.0 版本起支持使用该语句查看 External Catalog 下的表，包括 Apache Hive™ 、Apache Iceberg、Apache Hudi、Delta Lake 表。

自 2.5.7 版本起，StarRocks 支持在建表和新增分区时自动设置分桶数量 (BUCKETS)，您无需手动设置分桶数量。更多信息，请参见 [设置分桶数量](../../../table_design/Data_distribution.md#设置分桶数量)。

- 如果您在建表时指定了分桶数，SHOW CREATE TABLE 会显示分桶数。

- 如果您在建表时没有指定分桶数，SHOW CREATE TABLE 不会显示分桶数，您可以通过 [SHOW PARTITIONS](SHOW_PARTITIONS.md) 查看分区的分桶数。

2.5.7 之前的版本在建表时必须设置分桶数，因此 SHOW CREATE TABLE 会显示分桶数。

> **注意**
>
> - 3.0 版本之前，只有拥有该表 SELECT_PRIV 权限的用户才可以查看。
> - 自 3.0 版本起，只有拥有该表 SELECT 权限的用户才可以查看。

## 语法

```SQL
SHOW CREATE TABLE [db_name.]table_name
```

## 参数说明

| **参数**    | **必选** | **说明**                                        |
| ---------- | -------- | ---------------------------------------------- |
| db_name    | 否       | 数据库名称。如指定，则查看指定数据库中某张表的建表语句。 |
| table_name | 是       | 表名。                                          |

## 返回结果说明

```Plain
+-----------+----------------+
| Table     | Create Table   |       
+-----------+----------------+
```

返回结果中的参数说明如下：

| **参数**     | **说明**   |
| ------------ | ---------- |
| Table        | 表名。     |
| Create Table | 建表语句。 |

## 示例

### 建表时未指定分桶数

创建一个表 `example_table`，DISTRIBUTED BY 中未指定分桶数。

```SQL
CREATE TABLE example_table
(
    k1 TINYINT,
    k2 DECIMAL(10, 2) DEFAULT "10.5",
    v1 CHAR(10) REPLACE,
    v2 INT SUM
)
ENGINE = olap
AGGREGATE KEY(k1, k2)
COMMENT "my first starrocks table"
DISTRIBUTED BY HASH(k1);
```

查看表 `example_table` 的建表语句，结果中不显示分桶数。注意建表时如果未指定 PROPERTIES，SHOW CREATE TABLE 语句会显示默认的 PROPERTIES。

```Plain
SHOW CREATE TABLE example_table\G
*************************** 1. row ***************************
       Table: example_table
Create Table: CREATE TABLE `example_table` (
  `k1` tinyint(4) NULL COMMENT "",
  `k2` decimal64(10, 2) NULL DEFAULT "10.5" COMMENT "",
  `v1` char(10) REPLACE NULL COMMENT "",
  `v2` int(11) SUM NULL COMMENT ""
) ENGINE=OLAP 
AGGREGATE KEY(`k1`, `k2`)
COMMENT "my first starrocks table"
DISTRIBUTED BY HASH(`k1`)
PROPERTIES (
"replication_num" = "3",
"in_memory" = "false",
"enable_persistent_index" = "false",
"replicated_storage" = "true",
"compression" = "LZ4"
);
```

### 建表时指定了分桶数

创建一个表 `example_table1`，DISTRIBUTED BY 中指定分桶数为 10。

```SQL
CREATE TABLE example_table1
(
    k1 TINYINT,
    k2 DECIMAL(10, 2) DEFAULT "10.5",
    v1 CHAR(10) REPLACE,
    v2 INT SUM
)
ENGINE = olap
AGGREGATE KEY(k1, k2)
COMMENT "my first starrocks table"
DISTRIBUTED BY HASH(k1) BUCKETS 10;
```

查看表 `example_table` 的建表语句，结果中显示分桶数。注意建表时如果未指定 PROPERTIES，SHOW CREATE TABLE 语句会显示默认的 PROPERTIES。

```plain
SHOW CREATE TABLE example_table1\G
*************************** 1. row ***************************
       Table: example_table1
Create Table: CREATE TABLE `example_table1` (
  `k1` tinyint(4) NULL COMMENT "",
  `k2` decimal64(10, 2) NULL DEFAULT "10.5" COMMENT "",
  `v1` char(10) REPLACE NULL COMMENT "",
  `v2` int(11) SUM NULL COMMENT ""
) ENGINE=OLAP 
AGGREGATE KEY(`k1`, `k2`)
COMMENT "my first starrocks table"
DISTRIBUTED BY HASH(`k1`) BUCKETS 10 
PROPERTIES (
"replication_num" = "3",
"in_memory" = "false",
"enable_persistent_index" = "false",
"replicated_storage" = "true",
"compression" = "LZ4"
);
```
