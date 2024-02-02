---
displayed_sidebar: "Chinese"
---

# CREATE TABLE LIKE

## 功能

该语句用于创建一个表结构和另一张表完全相同的空表。复制的表结构包括 Column Definition、Partitions、Table Properties 等。支持复制 MySQL 等外表。

自 3.2 版本起支持为新表指定不同的分区方式、分桶方式、或属性。

:::tip
该操作需要有在目标数据库下 CREATE TABLE 的权限，对要复制的原表有 `SELECT` 权限。权限控制请参考 [GRANT](../account-management/GRANT.md) 章节。
:::

## 语法

- 3.2 版本之前支持的语法

```sql
CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [database.]<table_name>
LIKE [database.]<source_table_name>
```

- 3.2 版本起支持为新表指定分区方式、分桶方式、属性

```sql
CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [database.]<table_name>
[partition_desc]
[distribution_desc]
[PROPERTIES ("key" = "value",...)]
LIKE [database.]<source_table_name>
```

## 参数说明

- `database`：数据库。
- `table_name`：要创建的表的名称。有关表名的命令要求，参见[系统限制](../../../reference/System_limit.md)。
- `source_table_name`：要拷贝的表的名称。
- `partition_desc`：分区方式。更多信息，参见 [CREATE TABLE](./CREATE_TABLE.md)。
- `distribution_desc`：分桶方式。更多信息，参见 [CREATE TABLE](./CREATE_TABLE.md)。
- `PROPERTIES`：表的属性。

## 示例

假设数据库 `test1` 下有表 `orders`。

```sql
create table orders (
    dt date NOT NULL,
    order_id bigint NOT NULL,
    user_id int NOT NULL,
    merchant_id int NOT NULL,
    good_id int NOT NULL,
    good_name string NOT NULL,
    price int NOT NULL,
    cnt int NOT NULL,
    revenue int NOT NULL,
    state tinyint NOT NULL
) PRIMARY KEY (dt, order_id)
PARTITION BY RANGE(`dt`) (
    PARTITION p20210820 VALUES [('2021-08-20'), ('2021-08-21')),
    PARTITION p20210821 VALUES [('2021-08-21'), ('2021-08-22')),
    PARTITION p20210929 VALUES [('2021-09-29'), ('2021-09-30')),
    PARTITION p20210930 VALUES [('2021-09-30'), ('2021-10-01'))
) DISTRIBUTED BY HASH(order_id)
PROPERTIES (
    "replication_num" = "3",
    "enable_persistent_index" = "true"
);
```

示例一：在 `test1` 库下创建一张表结构和 `orders` 完全相同的空表，表名为 `order_1`。

```sql
CREATE TABLE test1.order_1 LIKE test1.orders;
```

可以看到 `order_1` 和 `orders` 表结构完全相同。

```plaintext
show create table order_1\G
*************************** 1. row ***************************
       Table: order_1
Create Table: CREATE TABLE `order_1` (
  `dt` date NOT NULL COMMENT "",
  `order_id` bigint(20) NOT NULL COMMENT "",
  `user_id` int(11) NOT NULL COMMENT "",
  `merchant_id` int(11) NOT NULL COMMENT "",
  `good_id` int(11) NOT NULL COMMENT "",
  `good_name` varchar(65533) NOT NULL COMMENT "",
  `price` int(11) NOT NULL COMMENT "",
  `cnt` int(11) NOT NULL COMMENT "",
  `revenue` int(11) NOT NULL COMMENT "",
  `state` tinyint(4) NOT NULL COMMENT ""
) ENGINE=OLAP 
PRIMARY KEY(`dt`, `order_id`)
PARTITION BY RANGE(`dt`)
(PARTITION p20210820 VALUES [("2021-08-20"), ("2021-08-21")),
PARTITION p20210821 VALUES [("2021-08-21"), ("2021-08-22")),
PARTITION p20210929 VALUES [("2021-09-29"), ("2021-09-30")),
PARTITION p20210930 VALUES [("2021-09-30"), ("2021-10-01")))
DISTRIBUTED BY HASH(`order_id`)
PROPERTIES (
"replication_num" = "3",
"in_memory" = "false",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
```

示例二：建表时指定新表的分区方式、分桶方式和属性。

```sql
CREATE TABLE order_2
PARTITION BY date_trunc('day',dt)
DISTRIBUTED BY hash(dt)
PROPERTIES ("replication_num" = "1")
LIKE orders;
```

```plaintext
show create table order_2\G
*************************** 1. row ***************************
       Table: order_2
Create Table: CREATE TABLE `order_2` (
  `dt` date NOT NULL COMMENT "",
  `order_id` bigint(20) NOT NULL COMMENT "",
  `user_id` int(11) NOT NULL COMMENT "",
  `merchant_id` int(11) NOT NULL COMMENT "",
  `good_id` int(11) NOT NULL COMMENT "",
  `good_name` varchar(65533) NOT NULL COMMENT "",
  `price` int(11) NOT NULL COMMENT "",
  `cnt` int(11) NOT NULL COMMENT "",
  `revenue` int(11) NOT NULL COMMENT "",
  `state` tinyint(4) NOT NULL COMMENT ""
) ENGINE=OLAP 
PRIMARY KEY(`dt`, `order_id`)
PARTITION BY RANGE(date_trunc('day', dt))
()
DISTRIBUTED BY HASH(`dt`)
PROPERTIES (
"replication_num" = "1",
"in_memory" = "false",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
```

示例三：在 `test1` 库下创建一张表结构和 MySQL 外表 `table1` 相同的空表，表名为 `table2`。

```sql
CREATE TABLE test1.table2 LIKE test1.table1;
```
