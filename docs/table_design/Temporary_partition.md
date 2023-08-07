# Temporary partition

This topic describes how to use the temporary partition feature.

You can create temporary partitions on a partitioned table that already has defined partitioning rules and define new data distribution strategies for these temporary partitions. Temporary partitions can serve as temporary data carriers when you atomically overwrite data in a partition or when you adjust partitioning and bucketing strategies. For temporary partitions, you can reset data distribution strategies such as partition ranges, number of buckets, and properties such as the number of replicas, and the storage medium, to meet specific requirements.

You can use the temporary partition feature in the following scenarios:

- Atomic overwrite operation
  
  If you need to rewrite the data in a partition while ensuring that the data can be queried during the rewriting process, you can first create a temporary partition based on the original formal partition, and load the new data into the temporary partition. Then you can use the replace operation to atomically replace the original formal partition with the temporary partition. For atomic overwrite operations on non-partitioned tables, see [ALTER TABLE - SWAP](../sql-reference/sql-statements/data-definition/ALTER%20TABLE#swap).

- Adjust partition data query concurrency

  If you need to modify the number of buckets for a partition, you can first create a temporary partition with the same partition range as the original formal partition and specify the new number of buckets. Then, you can use the `INSERT INTO` command to load the data of the original formal partition into the temporary partition. Finally, you can use the replace operation to atomically replace the original formal partition with the temporary partition.

- Modify partitioning rules
  
  If you want to modify a partitioning strategy, such as merging partitions or splitting a large partition into multiple smaller partitions, you can first create temporary partitions with the expected merged or split ranges. Then, you can use the `INSERT INTO` command to load the data of the original formal partitions into the temporary partitions. Finally, you can use the replace operation to atomically replace the original formal partitions with the temporary partitions.

## Create temporary partitions

You can create one or more partitions at a time by using the [ALTER TABLE](../sql-reference/sql-statements/data-definition/ALTER%20TABLE.md) command.

### Syntax

#### Create a single temporary partition

```SQL
ALTER TABLE <table_name>
ADD TEMPORARY PARTITION <temporary_partition_name> VALUES [("value1"), {MAXVALUE|("value2")})]
[(partition_desc)]
[DISTRIBUTED BY HASH(<bucket_key>)];
ALTER TABLE <table_name> 
ADD TEMPORARY PARTITION <temporary_partition_name> VALUES LESS THAN {MAXVALUE|(<"value">)}
[(partition_desc)]
[DISTRIBUTED BY HASH(<bucket_key>)];
```

#### Create multiple partitions at a time

```SQL
ALTER TABLE <table_name>
ADD TEMPORARY PARTITIONS START ("value1") END ("value2") EVERY {(INTERVAL <num> <time_unit>)|<num>}
[(partition_desc)]
[DISTRIBUTED BY HASH(<bucket_key>)];
```

### Parameters

`partition_desc`: specifies the number of buckets and properties for temporary partitions, such as the number of replicas and the storage medium.

### Examples

Create a temporary partition `tp1` in the table `site_access` and specify its range as `[2020-01-01, 2020-02-01)` by using the `VALUES [(...), (...)]` syntax.

```SQL
ALTER TABLE site_access
ADD TEMPORARY PARTITION tp1 VALUES [("2020-01-01"), ("2020-02-01"));
```

Create a temporary partition `tp2` in the table `site_access` and specify its upper bound as `2020-03-01` by using the `VALUES LESS THAN (...)` syntax. StarRocks uses the upper bound of the previous temporary partition as the lower bound of this temporary partition, generating a temporary partition with the left-closed and right-open range of `[2020-02-01, 2020-03-01)`.

```SQL
ALTER TABLE site_access
ADD TEMPORARY PARTITION tp2 VALUES LESS THAN ("2020-03-01");
```

Create a temporary partition `tp3` in the table `site_access`, specify its upper bound as `2020-04-01` by using the `VALUES LESS THAN (...)` syntax, and specify the number of replicas as `1`.

```SQL
ALTER TABLE site_access
ADD TEMPORARY PARTITION tp3 VALUES LESS THAN ("2020-04-01")
 ("replication_num" = "1")
DISTRIBUTED BY HASH (site_id);
```

Create multiple partitions at a time in the table `site_access` by using the `START (...) END (...) EVERY (...)` syntax, and specify the range of these partitions as `[2020-04-01, 2021-01-01)` with a monthly partition granularity.

```SQL
ALTER TABLE site_access 
ADD TEMPORARY PARTITIONS START ("2020-04-01") END ("2021-01-01") EVERY (INTERVAL 1 MONTH);
```

### Usage notes

- The partition column for a temporary partition must be the same as the partition column for the original formal partition based on which you create the temporary partition and cannot be changed.
- The name of a temporary partition cannot be the same as the name of any formal partition or other temporary partition.
- The ranges of all temporary partitions in a table cannot overlap, but the ranges of a temporary partition and a formal partition can overlap.

## Show temporary partitions

You can view the temporary partitions by using the [SHOW TEMPORARY PARTITIONS](../sql-reference/sql-statements/data-manipulation/SHOW%20PARTITIONS) command.

```SQL
SHOW TEMPORARY PARTITIONS FROM [db_name.]table_name [WHERE] [ORDER BY] [LIMIT]
```

## Load data into temporary partitions

You can load data into one or more temporary partitions by using the `INSERT INTO` command, STREAM LOAD, or BROKER LOAD.

### Load data by using `INSERT INTO` command

Example:

```SQL
INSERT INTO site_access TEMPORARY PARTITION (tp1) VALUES ("2020-01-01",1,"ca","lily",4);
INSERT INTO site_access TEMPORARY PARTITION (tp2) SELECT * FROM site_access_copy PARTITION p2;
INSERT INTO site_access TEMPORARY PARTITION (tp3, tp4,...) SELECT * FROM site_access_copy PARTITION (p3, p4,...);
```

For detailed syntax and parameter descriptions, see [INSERT INTO](../sql-reference/sql-statements/data-manipulation/insert.md).

### Load data by using STREAM LOAD

Example:

```bash
curl --location-trusted -u root: -H "label:123" -H "Expect:100-continue" -H "temporary_partitions: tp1, tp2, ..." -T testData \
    http://host:port/api/example_db/site_access/_stream_load    
```

For detailed syntax and parameter descriptions, see [STREAM LOAD](..sql-reference/sql-statements/data-manipulation/STREAM%20LOAD.md).

### Load data by using BROKER LOAD

Example:

```SQL
LOAD LABEL example_db.label1
(
    DATA INFILE("hdfs://hdfs_host:hdfs_port/user/starrocks/data/input/file")
    INTO TABLE my_table
    TEMPORARY PARTITION (tp1, tp2, ...)
    ...
)
WITH BROKER
(
    StorageCredentialParams
);
```

Note that `StorageCredentialParams` represents a group of authentication parameters which vary depending on the authentication method you choose. For detailed syntax and parameter descriptions, see [Broker Load](../sql-reference/sql-statements/data-manipulation/BROKER%20LOAD.md).

### Load data by using ROUTINE LOAD

Example:

```SQL
CREATE ROUTINE LOAD example_db.site_access ON example_tbl
COLUMNS(col, col2,...),
TEMPORARY PARTITIONS(tp1, tp2, ...)
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest"
);
```

For detailed syntax and parameter descriptions, see [CREATE ROUTINE LOAD](../sql-reference/sql-statements/data-manipulation/CREATE%20ROUTINE%20LOAD.md).

## Query data in temporary partitions

You can use the [SELECT](../sql-reference/sql-statements/data-manipulation/SELECT.md) statement to query data in specified temporary partitions.

```SQL
SELECT * FROM
site_access TEMPORARY PARTITION (tp1);

SELECT * FROM
site_access TEMPORARY PARTITION (tp1, tp2, ...);

SELECT event_day,site_id,pv FROM
site_access TEMPORARY PARTITION (tp1, tp2, ...);
```

You can use the JOIN clause to query data in temporary partitions from two tables.

```SQL
SELECT * FROM
site_access TEMPORARY PARTITION (tp1, tp2, ...)
JOIN
site_access_copy TEMPORARY PARTITION (tp1, tp2, ...)
ON site_access.site_id=site_access1.site_id and site_access.event_day=site_access1.event_day;
```

## Replace original formal partitions with temporary partitions

You can use the [ALTER TABLE](../sql-reference/sql-statements/data-definition/ALTER%20TABLE.md) statement to replace the original formal partition with a temporary partition, thereby creating a new formal partition.

> **NOTE**
>
> The original formal partition and temporary partition that you operated in the ALTER TABLE statement are deleted and cannot be recovered.

### Syntax

```SQL
ALTER TABLE table_name REPLACE PARTITION (partition_name) WITH TEMPORARY PARTITION (temporary_partition_name1, ...)
PROPERTIES ("key" = "value");
```

### Parameters

- **strict_range**
  
  Default value: `true`.

  When this parameter is set to `true`, the unions of ranges of all original formal partitions must be exactly the same as the unions of ranges of the temporary partitions that are used for replacement. When this parameter is set to `false`, you only need to make sure that the ranges of the new formal partitions do not overlap with other formal partitions after replacement.

  - Example 1:
  
    In the following example, the unions of the original formal partitions `p1`, `p2`, and `p3` are the same as the unions of the temporary partitions `tp1` and `tp2`, and you can use `tp1` and `tp2` to replace `p1`, `p2`, and `p3`.

      ```plaintext
      # Ranges of original formal partitions p1, p2, and p3 => Unions of these ranges
      [10, 20), [20, 30), [40, 50) => [10, 30), [40, 50)
      
      # Ranges of temporary partitions tp1 and tp2 => Unions of these ranges
      [10, 30), [40, 45), [45, 50) => [10, 30), [40, 50)
      ```

  - Example 2:

    In the following example, the union of ranges of the original formal partition differs from the union of ranges of the temporary partitions. If the value of the parameter `strict_range` is set to `true`, the temporary partitions `tp1` and `tp2` cannot replace the original formal partition `p1`. If the value is set to `false`, and the ranges [10, 30) and [40, 50) of the temporary partitions do not overlap with other formal partitions, the temporary partitions can replace the original formal partition.

      ```plaintext
      # Range of original formal partition p1 => Union of the range
      [10, 50) => [10, 50)
      
      # Ranges of temporary partitions tp1 and tp2 => Unions of these ranges
      [10, 30), [40, 50) => [10, 30), [40, 50)
      ```

- **use_temp_partition_name**
  
  Default value: `false`.

  If the number of original formal partitions is the same as the number of temporary partitions used for replacement, the names of the new formal partitions remain unchanged after replacement when this parameter is set to `false`. When this parameter is set to `true`, the names of the temporary partitions are used as the names of the new formal partitions after replacement.

  In the following example, the partition name of the new formal partition remains `p1` after replacement when this parameter is set to `false`. However, its related data and properties are replaced with the data and properties of the temporary partition `tp1`. When this parameter is set to `true`, the partition name of the new formal partition is changed to `tp1` after replacement. The original formal partition `p1` no longer exists.

    ```sql
    ALTER TABLE tbl1 REPLACE PARTITION (p1) WITH TEMPORARY PARTITION (tp1);
    ```

  If the number of formal partitions to be replaced is different from the number of temporary partitions used for replacement, and this parameter remains the default value `false`, the value `false` of this parameter is invalid.

  In the following example, after replacement, the name for the new formal partition is changed to `tp1`, and the original formal partitions `p1` and `p2` no longer exist.

    ```SQL
    ALTER TABLE site_access REPLACE PARTITION (p1, p2) WITH TEMPORARY PARTITION (tp1);
    ```

### Examples

Replace the original formal partition `p1` with the temporary partition `tp1`.

```SQL
ALTER TABLE site_access REPLACE PARTITION (p1) WITH TEMPORARY PARTITION (tp1);
```

Replace the original formal partitions `p2` and `p3` with the temporary partitions `tp2` and `tp3`.

```SQL
ALTER TABLE site_access REPLACE PARTITION (p2, p3) WITH TEMPORARY PARTITION (tp2, tp3);
```

Replace the original formal partitions `p4` and `p5` with the temporary partitions `tp4` and `tp5`, and specify the parameters `strict_range` as `false` and `use_temp_partition_name` as `true`.

```SQL
ALTER TABLE site_access REPLACE PARTITION (p4, p5) WITH TEMPORARY PARTITION (tp4, tp5)
PROPERTIES (
    "strict_range" = "false",
    "use_temp_partition_name" = "true"
);
```

### Usage notes

- When a table has temporary partitions, you cannot use the `ALTER` command to perform Schema Change operations on the table.
- When performing Schema Change operations on a table, you cannot add temporary partitions to the table.

## Delete temporary partitions

Use the following command to delete the temporary partition `tp1`.

```SQL
ALTER TABLE site_access DROP TEMPORARY PARTITION tp1;
```

Take note of the following limits:

- If you use the `DROP` command to directly delete a database or table, you can recover the database or table within a limited time period by using the `RECOVER` command. However, temporary partitions cannot be recovered.
- After using the `ALTER` command to delete a formal partition, you can recover it within a limited time period by using the `RECOVER` command. Temporary partitions are not bound with formal partitions, so operations on temporary partitions do not affect formal partitions.
- After using the `ALTER` command to delete a temporary partition, you cannot recover it by using the `RECOVER` command.
- When using the `TRUNCATE` command to delete data in a table, the temporary partitions of the table are deleted and cannot be recovered.
- When using the `TRUNCATE` command to delete data in a formal partition, the temporary partitions are not affected.
- The TRUNCATE command cannot be used to delete data in a temporary partition.
