# SHOW CREATE TABLE

## 功能

查看指定表的建表语句。只有拥有该表 `SELECT_PRIV` 权限的用户才可以查看。注意使用 external catalog 管理的表，包括 Apache Hive™ 、Apache Iceberg 和 Apache Hudi 表暂不支持使用该语句查看。

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

创建一个表`example_table`。

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
DISTRIBUTED BY HASH(k1) BUCKETS 10;
```

查看表`example_table`的建表语句。

```Plain
SHOW CREATE TABLE example_db.example_table;

+---------------+--------------------------------------------------------+
| Table         | Create Table                                           |
+---------------+--------------------------------------------------------+
| example_table | CREATE TABLE `example_table` (
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
"storage_format" = "DEFAULT",
"enable_persistent_index" = "false"
); |
+---------------+----------------------------------------------------------+
```
