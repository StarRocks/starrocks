---
displayed_sidebar: "English"
---

# SHOW PARTITIONS

## Description

Displays partition information, including common partitions and [temporary partitions](../../../table_design/Temporary_partition.md).

Syntax:

```sql
SHOW [TEMPORARY] PARTITIONS FROM [db_name.]table_name [WHERE] [ORDER BY] [LIMIT]
```

> NOTE
>
> - This syntax only supports StarRocks tables (`"ENGINE" = "OLAP"`). For Elasticsearch and Hive tables, use SHOW PROC '/dbs/db_id/table_id/partitions'.
> - This operation requires the SELECT__PRIV privilege on the specified table.

## Description of return fields

```plaintext
+-------------+---------------+----------------+---------------------+--------------------+--------+--------------+-------+--------------------+---------+----------------+---------------+---------------------+--------------------------+----------+------------+----------+
| PartitionId | PartitionName | VisibleVersion | VisibleVersionTime  | VisibleVersionHash | State  | PartitionKey | Range | DistributionKey    | Buckets | ReplicationNum | StorageMedium | CooldownTime        | LastConsistencyCheckTime | DataSize | IsInMemory | RowCount |
+-------------+---------------+----------------+---------------------+--------------------+--------+--------------+-------+--------------------+---------+----------------+---------------+---------------------+--------------------------+----------+------------+----------+
```

| **Field**                | **Description**                                              |
| ------------------------ | ------------------------------------------------------------ |
| PartitionId              | The ID of the partition.                                |
| PartitionName            | The name of the partition.                                   |
| VisibleVersion           | The version number of the last successful load transaction. The version number increases by 1 with each successful load transaction. |
| VisibleVersionTime       | The timestamp of the last successful load transaction.       |
| VisibleVersionHash       | The hash value for the version number of the last successful load transaction. |
| State                    | The status of the partition. Fixed value: `Normal`.           |
| PartitionKey             | The partition key that consists of one or more partition columns. |
| Range                    | The range of the partition, which is a right half-open interval. |
| DistributionKey          | The bucket key of hash bucketing.                            |
| Buckets                  | The number of buckets for the partition.                     |
| ReplicationNum           | The number of replicas per tablet in the partition.        |
| StorageMedium            | The storage medium to store the data in the partition. The value `HHD` indicates hard disk drives, and the value `SSD` indicates solid-state drives. |
| CooldownTime             | The cooldown time for data in the partition. If the initial storage medium is SSD, the storage medium is switched from SSD to HDD after the time specified by this parameter. Format: "yyyy-MM-dd HH:mm:ss". |
| LastConsistencyCheckTime | The time of the last consistency check. `NULL` indicates no consistency check was performed. |
| DataSize                 | The size of data in the partition.                          |
| IsInMemory               | Whether all data in the partition is stored in memory.          |
| RowCount                 | The number of data rows of the partition.                    |

## Examples

1. Display information of all regular partitions from the specified table `site_access` under the specified database `test`.

    ```SQL
    MySQL > show partitions from test.site_access\G
    *************************** 1. row ***************************
                PartitionId: 20990
            PartitionName: p2019 
            VisibleVersion: 1
        VisibleVersionTime: 2023-08-08 15:45:13
        VisibleVersionHash: 0
                    State: NORMAL
                PartitionKey: datekey
                    Range: [types: [DATE]; keys: [2019-01-01]; ..types: [DATE]; keys: [2020-01-01]; )
            DistributionKey: site_id
                    Buckets: 6
            ReplicationNum: 3
            StorageMedium: HDD
                CooldownTime: 9999-12-31 23:59:59
    LastConsistencyCheckTime: NULL
                    DataSize:  4KB   
                IsInMemory: false
                    RowCount: 3 
    1 row in set (0.00 sec)
    ```

2. Display information of all temporary partitions from the specified table `site_access` under the specified database `test`.

    ```sql
    SHOW TEMPORARY PARTITIONS FROM test.site_access;
    ```

3. Display the information of the specified partition `p1` of the specified table `site_access` under the specified database `test`.

    ```sql
    -- Regular partition
    SHOW PARTITIONS FROM test.site_access WHERE PartitionName = "p1";
    -- Temporary partition
    SHOW TEMPORARY PARTITIONS FROM test.site_access WHERE PartitionName = "p1";
    ```

4. Display the latest partition information of the specified table `site_access` under the specified database `test`.

    ```sql
    -- Regular partition
    SHOW PARTITIONS FROM test.site_access ORDER BY PartitionId DESC LIMIT 1;
    -- Temporary partition
    SHOW TEMPORARY PARTITIONS FROM test.site_access ORDER BY PartitionId DESC LIMIT 1;
    ```
