# 使用临时分区

本文介绍如何使用临时分区功能。

您可以在一张已经定义分区规则的分区表上，创建临时分区，并为这些临时分区定义新的数据分布策略，以便给您的原子覆盖写操作或调整分区、分桶操作提供临时可用的数据载体。支持为临时分区重新设定的数据分布策略包括分区范围、分桶数、以及一些属性，例如副本数、存储介质，以满足不同需求。

在以下应用场景中，您可以使用临时分区功能：

1. 原子的覆盖写操作
如果您需要重写某一分区的数据，同时保证重写过程中可以查看数据，您可以先创建一个对应的临时分区，将新的数据导入到临时分区后，通过替换操作，原子地替换原有分区，从而达到目的。对于非分区表的原子覆盖写操作，请参考 [ALTER TABLE - SWAP](../sql-reference/sql-statements/data-definition/ALTER_TABLE.md#swap)。

2. 调整分区数据的查询并发
如果您需要为分区修改分桶数，您可以先创建一个对应分区范围的临时分区，并指定新的分桶数，然后通过 `INSERT INTO` 命令将正式分区的数据导入到临时分区中，通过替换操作，原子地替换原有分区，以达到目的。

3. 修改分区策略
如果您希望针对分区进行修改，例如合并分区，或将一个大分区分割成多个小分区，您可以先建立对应合并或分割后范围的临时分区，然后通过 `INSERT INTO` 命令将正式分区的数据导入到临时分区中，通过替换操作，原子地替换原有分区，以达到目的。

## 创建临时分区

您可以通过 ALTER TABLE 命令创建临时分区。

```sql
ALTER TABLE table_name ADD TEMPORARY PARTITION partition_name;
```

示例:

```sql
ALTER TABLE tbl1 ADD TEMPORARY PARTITION tp1 VALUES LESS THAN("2020-02-01");

ALTER TABLE tbl2 ADD TEMPORARY PARTITION tp1 VALUES [("2020-01-01"), ("2020-02-01"));

ALTER TABLE tbl1 ADD TEMPORARY PARTITION tp1 VALUES LESS THAN("2020-02-01")
("in_memory" = "true", "replication_num" = "1")
DISTRIBUTED BY HASH(k1) BUCKETS 5;
```

> 注意
>
> * 临时分区的分区列和正式分区相同，且不可修改。
> * 临时分区的分区名称不能和正式分区以及其他临时分区重复。
> * 一张表所有临时分区之间的分区范围不可重叠，但临时分区的范围和正式分区范围可以重叠。
> * 临时分区可以独立指定特定属性。包括分桶数、副本数、是否是内存表、存储介质等信息。

## 导入数据至临时分区

您可以通过 `INSERT INTO` 命令， STREAM LOAD，或者 BROKER LOAD 方式将数据导入临时分区。

### 通过 INSERT INTO 命令导入

您可以通过以下 `INSERT INTO` 命令将数据导入临时分区。

```sql
INSERT INTO table_name TEMPORARY PARTITION(tp_name1, tp_name2, ...) SELECT (sql_statement);
```

### 通过 STREAM LOAD 方式导入

详细步骤请参考 [Stream Load](../loading/StreamLoad.md)。

示例：

```Bash
curl --location-trusted -u <username>:<password> -H "label:label1" -H "temporary_partitions: tp1, tp2, ..." -T testData \
    http://host:port/api/example_db/site_access/_stream_load    
```

### 通过 BROKER LOAD 方式导入

详细步骤请参考 [Broker load](../loading/BrokerLoad.md)。

示例：

```sql
LOAD LABEL example_db.label1
(
    DATA INFILE("hdfs://hdfs_host:hdfs_port/user/starrocks/data/input/file")
    INTO TABLE `my_table`
    TEMPORARY PARTITION (tp1, tp2, ...)
    ...
)
WITH BROKER hdfs ("username"="hdfs_user", "password"="hdfs_password");
```

### 通过 ROUTINE LOAD 方式导入

详细步骤请参考 [Routine Load](../loading/RoutineLoad.md)。

示例：

```sql
CREATE ROUTINE LOAD example_db.test1 ON example_tbl
COLUMNS(k1, k2, k3, v1, v2, v3 = k1 * 100),
TEMPORARY PARTITIONS(tp1, tp2, ...),
WHERE k1 > 100
PROPERTIES
("property_type"="property_value" ,...)
FROM KAFKA
(property_desc);
```

## 在临时分区查询

您可以在指定的临时分区进行查询。

```sql
SELECT column_name FROM
table_name TEMPORARY PARTITION(temporary_partition_name1, ...);

SELECT column_name FROM
table_name TEMPORARY PARTITION(temporary_partition_name1, ...)
JOIN
table_name TEMPORARY PARTITION(temporary_partition_name1, ...)
ON ...
WHERE ...;
```

## 使用临时分区进行替换

您可以通过以下命令将一个表的正式分区替换为临时分区。

```sql
ALTER TABLE table_name REPLACE PARTITION(partition_name) WITH TEMPORARY PARTITION (temporary_partition_name1, ...)
PROPERTIES ("key" = "value");
```

> 注意
>
> * 分区替换成功后，被替换的分区将被删除且不可恢复。
> * 当表存在临时分区时，无法使用 `ALTER` 命令对表进行 Schema Change 等变更操作。
> * 当表在进行变更操作时，无法对表添加临时分区。

示例：

```sql
ALTER TABLE tbl1 REPLACE PARTITION(p1) WITH TEMPORARY PARTITION (tp1);

ALTER TABLE tbl1 REPLACE PARTITION(p1, p2) WITH TEMPORARY PARTITION (tp1, tp2, tp3);

ALTER TABLE tbl1 REPLACE PARTITION(p1, p2) WITH TEMPORARY PARTITION (tp1, tp2)
PROPERTIES (
    "strict_range" = "false",
    "use_temp_partition_name" = "true"
);
```

### 相关参数说明

* **strict_range**

    默认为 `true`。

    当该参数为 `true` 时，表示要被替换的所有正式分区的范围并集需要和替换的临时分区的范围并集完全相同。当设置为 `false` 时，只需要保证替换后，新的正式分区间的范围不重叠即可。

    示例 1：

    ```plain text
    # 待替换的分区 p1, p2, p3 的范围 (=> 并集)
    [10, 20), [20, 30), [40, 50) => [10, 30), [40, 50)

    # 替换分区 tp1, tp2 的范围 (=> 并集)
    [10, 30), [40, 45), [45, 50) => [10, 30), [40, 50)
    ```

    以上示例中范围并集相同，您可以使用 tp1 和 tp2 替换 p1, p2, p3。

    示例 2：

    ```plain text
    # 待替换的分区 p1 的范围 (=> 并集)
    [10, 50) => [10, 50)

    # 替换分区 tp1, tp2 的范围 (=> 并集)
    [10, 30), [40, 50) => [10, 30), [40, 50)
    ```

    以上示例中范围并集不相同，如果 `strict_range` 设置为 `true`，则不可以使用 tp1 和 tp2 替换 p1。如果为 `false`，且替换后的两个分区范围 [10, 30), [40, 50) 和其他正式分区不重叠，则可以替换。

* **use_temp_partition_name**

    默认为 `false`。

    当该参数为 `false`，并且待替换的分区和替换分区的个数相同时，则替换后的正式分区名称维持不变。如果为 `true`，则替换后，正式分区的名称变更为替换分区的名称。

    示例 1：

    ```sql
    ALTER TABLE tbl1 REPLACE PARTITION (p1) WITH TEMPORARY PARTITION (tp1);
    ```

    当 `use_temp_partition_name` 为 `false` 时，分区在替换后名称依然为 p1，但是相关的数据和属性都替换为 tp1 的数据和属性。

    如果 `use_temp_partition_name` 为 `true`，则在替换后，分区的名称为 tp1。p1 分区不再存在。

    示例 2：

    ```sql
    ALTER TABLE tbl1 REPLACE PARTITION (p1, p2) WITH TEMPORARY PARTITION (tp1);
    ```

    `use_temp_partition_name` 默认为 `false`，但因为待替换分区的个数和替换分区的个数不同，则该参数无效。替换后，分区名称为 tp1，p1 和 p2 不再存在。

## 删除临时分区

通过以下命令删除指定临时分区。

```sql
ALTER TABLE table_name DROP TEMPORARY PARTITION temporary_partition_name;
```

> 注意
>
> * 使用 `DROP` 命令直接删除数据库或表后，您可以在限定时间内通过 `RECOVER` 命令恢复该数据库或表，但临时分区无法被恢复。
> * 使用 `ALTER` 命令删除正式分区后，您可以在限定时间内通过 `RECOVER` 命令恢复。临时分区与正式分区不相关联，操作临时分区不会对正式分区产生影响。
> * 使用 `ALTER` 命令删除临时分区后，您无法通过 `RECOVER` 命令恢复。
> * 使用 `TRUNCATE` 命令清空表后，表的临时分区会被删除，且不可恢复。
> * 使用 `TRUNCATE` 命令清空正式分区时，临时分区不受影响。
> * 不可使用 `TRUNCATE` 命令清空临时分区。

示例：

```sql
ALTER TABLE tbl1 DROP TEMPORARY PARTITION tp1;
```
