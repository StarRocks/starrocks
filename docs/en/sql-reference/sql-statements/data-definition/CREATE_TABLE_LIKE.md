---
displayed_sidebar: "English"
---

# CREATE TABLE LIKE

## Description

Creates an identical empty table based on the definition of another table. The definition includes column definition, partitions, and table properties. You can copy an external table such as MySQL.

v3.2 allows you to specify a different partitioning method, bucketing method, and properties for the new table than the source table.

:::tip
This operation requires the CREATE TABLE privilege on the database in which you want to create a table, and the `SELECT` privilege on the source table based on which to create a table.
:::tip

## Syntax

- Syntax supported in versions earlier than v3.2.

```sql
CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [database.]<table_name>
LIKE [database.]<source_table_name>
```

- v3.2 supports specifying properties for the new table.

```sql
CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [database.]<table_name>
[partition_desc]
[distribution_desc]
[PROPERTIES ("key" = "value",...)]
LIKE [database.]<source_table_name>
```

## Parameters

- `database`: the database.
- `table_name`: the name of the table you want to create. For the naming conventions, see [System limits](../../../reference/System_limit.md).
- `source_table_name`: the name of the source table you want to copy.
- - `partition_desc`: the partitioning method. For more information, see [CREATE TABLE](./CREATE_TABLE.md#partition_desc).
- `distribution_desc`: the bucketing method. For more information, see [CREATE TABLE](./CREATE_TABLE.md#distribution_desc).
- `PROPERTIES`: the properties of the table. All the table properties are supported. For more inforation, see [CREATE TABLE](./CREATE_TABLE.md#properties).

## Examples

Suppose there is a table `orders` in database `test1`.

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

Example 1: In database `test1`, create an empty table `order_1` that has the same table structure as `orders`.

 ```sql
CREATE TABLE test1.order_1 LIKE test1.orders;
```

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

Example 2: Create an empty table `order_2` based on `orders` and specify properties for `order_2`.

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

Example 3: Create an empty table `table2` that has the same table structure as MySQL external table `table1`.

```sql
CREATE TABLE test1.table2 LIKE test1.table1
```
