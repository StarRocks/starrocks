# 临时分区

本文介绍如何使用临时分区功能。

您可以在一张已经定义分区规则的分区表上，创建临时分区，并为这些临时分区设定单独的数据分布策略。在原子覆盖写操作或调整分区分桶策略时候，您可以将临时分区作为临时可用的数据载体。您可以为临时分区设定的数据分布策略包括分区范围、分桶数、以及部分属性，例如副本数、存储介质。

在以下应用场景中，您可以使用临时分区功能：

- 原子覆盖写操作

  如果您需要重写某一正式分区的数据，同时保证重写过程中可以查看数据，您可以先创建一个对应的临时分区，将新的数据导入到临时分区后，通过替换操作，原子地替换原有正式分区，生成新正式分区。对于非分区表的原子覆盖写操作，请参考 [ALTER TABLE - SWAP](../sql-reference/sql-statements/data-definition/ALTER_TABLE.md#swap)。
- 调整分区数据的查询并发

  如果您需要修改某一正式分区的分桶数，您可以先创建一个对应分区范围的临时分区，并指定新的分桶数，然后通过 `INSERT INTO` 命令将原有正式分区的数据导入到临时分区中，通过替换操作，原子地替换原有正式分区，生成新正式分区。
- 修改分区策略

  如果您希望修改正式分区的分区范围，例如合并多个小分区为一个大分区，或将一个大分区分割成多个小分区，您可以先建立对应合并或分割后范围的临时分区，然后通过 `INSERT INTO` 命令将原有正式分区的数据导入到临时分区中，通过替换操作，原子地替换原有正式分区，生成新正式分区。

## 创建临时分区

您可以通过 ALTER TABLE 命令创建一个临时分区，也可以批量创建临时分区。

### 语法

**创建一个临时分区**

```SQL
ALTER TABLE <table_name> 
ADD TEMPORARY PARTITION <temporary_partition_name> VALUES [("value1"), {MAXVALUE|("value2")})]
[(partition_desc)]
[DISTRIBUTED BY HASH(<bucket_key>) BUCKETS <bucket_number>];
```

```SQL
ALTER TABLE <table_name> 
ADD TEMPORARY PARTITION <temporary_partition_name> VALUES LESS THAN {MAXVALUE|(<"value">)}
[(partition_desc)]
[DISTRIBUTED BY HASH(<bucket_key>) BUCKETS <bucket_number>];
```

**批量创建临时分区**

```SQL
ALTER TABLE <table_name>
ADD TEMPORARY PARTITIONS START ("value1") END ("value2") EVERY {(INTERVAL <num> <time_unit>)|<num>}
[(partition_desc)]
[DISTRIBUTED BY HASH(<bucket_key>) BUCKETS <bucket_number>];
```

### **参数说明**

`partition_desc`：为临时分区指定分桶数和部分属性，包括副本数、存储介质等信息。

### 示例

在 `site_access` 表中创建临时分区 `tp1`， 使用 `VALUES [(...),(...))` 语法指定其临时分区范围为 [2020-01-01, 2020-02-01)。

```SQL
ALTER TABLE site_access
ADD TEMPORARY PARTITION tp1 VALUES [("2020-01-01"), ("2020-02-01"));
```

在 `site_access` 表中创建临时分区 `tp2`，使用 `VALUES LESS THAN (...)` 语法指定其临时分区的上界为 `2020-03-01`。StarRocks 会将前一个临时分区的上界作为该临时分区的下界，生成一个左闭右开的临时分区，其范围为 [2020-02-01, 2020-03-01)。

```SQL
ALTER TABLE site_access
ADD TEMPORARY PARTITION tp2 VALUES LESS THAN ("2020-03-01");
```

在 `site_access` 表中创建临时分区 `tp3`，使用 `VALUES LESS THAN (...)` 语法指定其临时分区的上界为 `2020-04-01` 并且指定临时分区的副本数为 `1`，分桶数为 `5`。

```SQL
ALTER TABLE site_access
ADD TEMPORARY PARTITION tp3 VALUES LESS THAN ("2020-04-01")
 ("replication_num" = "1")
DISTRIBUTED BY HASH (site_id) BUCKETS 5;
```

在 `site_access` 表中批量创建临时分区，使用 `START (...) END (...) EVERY (...)` 语法指定批量创建的临时分区范围为 [2020-04-01, 2021-01-01)，分区粒度是一个月。

```SQL
ALTER TABLE site_access 
ADD TEMPORARY PARTITIONS START ("2020-04-01") END ("2021-01-01") EVERY (INTERVAL 1 MONTH);
```

### 注意事项

- 临时分区的分区列和原有正式分区相同，且不可修改。
- 临时分区的分区名称不能和正式分区以及其他临时分区重复。
- 一张表所有临时分区之间的分区范围不可重叠，但临时分区的范围和正式分区范围可以重叠。

## 查看临时分区

您可以通过如下 [SHOW TEMPORARY PARTITIONS](../sql-reference/sql-statements/data-manipulation/SHOW_PARTITIONS.md) 命令，查看表的临时分区。

```SQL
SHOW TEMPORARY PARTITIONS FROM site_access;
```

## 导入数据至临时分区

您可以通过 `INSERT INTO` 命令、STREAM LOAD、BROKER LOAD 或者 ROUTINE LOAD 方式将数据导入临时分区。

### 通过 `INSERT INTO` 命令导入

您可以通过如下 [INSERT INTO](../sql-reference/sql-statements/data-manipulation/insert.md) 命令将数据导入临时分区。

```SQL
INSERT INTO site_access TEMPORARY PARTITION (tp1) VALUES ("2020-01-01",1,"ca","lily",4);
INSERT INTO site_access TEMPORARY PARTITION (tp2) SELECT * FROM site_access_copy PARTITION p2;
INSERT INTO site_access TEMPORARY PARTITION (tp3, tp4,...) SELECT * FROM site_access_copy PARTITION (p3, p4,...);
```

### 通过 STREAM LOAD 方式导入

示例：

```Bash
curl --location-trusted -u <username>:<password> -H "label:label1" -H "temporary_partitions: tp1, tp2, ..." -T testData \
    http://host:port/api/example_db/site_access/_stream_load    
```

有关语法和参数等更多信息，请参见 [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md)。

### 通过 BROKER LOAD 方式导入

示例：

```SQL
LOAD LABEL example_db.label2
(
    DATA INFILE("hdfs://hdfs_host:hdfs_port/user/starrocks/data/input/file")
    INTO TABLE site_access
    TEMPORARY PARTITION (tp1, tp2, ...)
    ...
)
WITH BROKER
(
    StorageCredentialParams
);
```

有关语法和参数等更多信息，请参见 [BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md)。

### 通过 ROUTINE LOAD 方式导入

示例：

```SQL
CREATE ROUTINE LOAD routine_load_job ON site_access
COLUMNS (event_day,site_id,city_code,user_name,pv),
TEMPORARY PARTITION (tp1, tp2, ...)
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest"
);
```

有关语法和参数等更多信息，请参见 [CREATE ROUTINE LOAD](../sql-reference/sql-statements/data-manipulation/CREATE_ROUTINE_LOAD.md)。

## 查询临时分区的数据

您可以查询指定临时分区的数据。

```SQL
SELECT * FROM
site_access TEMPORARY PARTITION (tp1);

SELECT * FROM
site_access TEMPORARY PARTITION (tp1, tp2, ...);

SELECT event_day,site_id,pv FROM
site_access TEMPORARY PARTITION (tp1, tp2, ...);
```

您可以对两张表中指定临时分区的数据进行连接查询。

```SQL
SELECT * FROM
site_access TEMPORARY PARTITION (tp1, tp2, ...)
JOIN
site_access_copy TEMPORARY PARTITION (tp1, tp2, ...)
ON site_access.site_id=site_access1.site_id and site_access.event_day=site_access1.event_day
;
```

## 使用临时分区进行替换

您可以通过以下命令使用临时分区替换原有正式分区，形成新正式分区。分区替换成功后，原有正式分区和临时分区被删除且不可恢复。

### 语法

```SQL
ALTER TABLE <table_name> 
REPLACE PARTITION (<partition_name1>[, ...]) WITH TEMPORARY PARTITION (<temporary_partition_name1>[, ...])
[PROPERTIES ("key" = "value")];
```

### 参数说明

- `strict_range`
  默认为 `true`。

  当该参数为 `true` 时，表示原有正式分区的范围并集和临时分区的范围并集必须完全相同。当设置为 `false` 时，只需要保证替换后，新正式分区和其他正式分区的范围不重叠即可。

  - 示例 1：

    以下示例中范围并集相同，您可以使用 `tp1` 和 `tp2` 替换 `p1`、`p2` 和 `p3`。

    ```Plaintext
    # 原有正式分区 p1, p2, p3 的范围 (=> 并集)
    [10, 20), [20, 30), [40, 50) => [10, 30), [40, 50)
    
    # 临时分区 tp1, tp2 的范围 (=> 并集)
    [10, 30), [40, 45), [45, 50) => [10, 30), [40, 50)
    ```

  - 示例 2：

    以上示例中范围并集不相同，如果 `strict_range` 设置为 `true`，则不可以使用 `tp1` 和 `tp2` 替换 `p1`。如果为 `false`，且临时分区范围 [10, 30), [40, 50) 和其他正式分区不重叠，则可以替换。

    ```Plaintext
    # 原有正式分区 p1 的范围 (=> 并集)
    [10, 50) => [10, 50)
    
    # 临时分区 tp1, tp2 的范围 (=> 并集)
    [10, 30), [40, 50) => [10, 30), [40, 50)
    ```

- `use_temp_partition_name`
  默认为 `false`。

  - 如果原有正式分区和临时分区的个数相同，当该参数为默认值 `false`，则新正式分区的名称维持不变。如果为 `true`，则新正式分区的名称变更为临时分区的名称。
    在如下示例中，当该参数为 `false` 时，则新正式分区的名称依然为 `p1`，但是相关的数据和属性都替换为临时分区 `tp1` 的数据和属性。如果该参数为 `true` 时，则新正式分区的名称为 `tp1`，不再存在正式分区 `p1`。

    ```SQL
    ALTER TABLE site_access REPLACE PARTITION (p1) WITH TEMPORARY PARTITION (tp1);
    ```

  - 如果原有正式分区和临时分区的个数不同，并且该参数为默认值 `false`，但是因为原有正式分区个数和临时分区的个数不同，因此该参数为 `false` 无效。在如下示例中，新正式分区的名称为 `tp1`，不再存在正式分区 `p1` 和 `p2`。

    ```SQL
    ALTER TABLE site_access REPLACE PARTITION (p1, p2) WITH TEMPORARY PARTITION (tp1);
    ```

### 示例

使用临时分区 `tp1` 替换原有正式分区 `p1`。

```SQL
ALTER TABLE site_access REPLACE PARTITION (p1) WITH TEMPORARY PARTITION (tp1);
```

使用临时分区 `tp2` 和 `tp3` 替换原有正式分区 `p2` 和 `p3`。

```SQL
ALTER TABLE site_access REPLACE PARTITION (p2, p3) WITH TEMPORARY PARTITION (tp2, tp3);
```

使用临时分区 `tp4` 和 `tp5` 替换原有正式分区 `p4` 和 `p5`。并且指定参数 `strict_range` 为 `false`，`use_temp_partition_name` 为 `true`。

```SQL
ALTER TABLE site_access REPLACE PARTITION (p4, p5) WITH TEMPORARY PARTITION (tp4, tp5)
PROPERTIES (
    "strict_range" = "false",
    "use_temp_partition_name" = "true"
);
```

**注意事项**

- 当表存在临时分区时，无法使用 `ALTER` 命令对表进行 Schema Change 操作。
- 当对表进行 Schema Change 操作时，无法对表添加临时分区。

## 删除临时分区

通过以下命令删除临时分区 `tp1`。

```SQL
ALTER TABLE site_access DROP TEMPORARY PARTITION tp1;
```

注意事项

- 使用 `DROP` 命令直接删除数据库或表后，您可以在限定时间内通过 [RECOVER](../sql-reference/sql-statements/data-definition/RECOVER.md) 命令恢复该数据库或表，但临时分区无法被恢复。
- 使用 `ALTER` 命令删除正式分区后，您可以在限定时间内通过 [RECOVER](../sql-reference/sql-statements/data-definition/RECOVER.md) 命令恢复。正式分区与临时分区不相关联，操作正式分区不会对临时分区产生影响。
- 使用 `ALTER` 命令删除临时分区后，您无法通过 [RECOVER](../sql-reference/sql-statements/data-definition/RECOVER.md) 命令恢复。
- 使用 `TRUNCATE` 命令清空表后，表的临时分区会被删除，且不可恢复。
- 使用 `TRUNCATE` 命令清空正式分区时，临时分区不受影响。
- 不可使用 `TRUNCATE` 命令清空临时分区。
