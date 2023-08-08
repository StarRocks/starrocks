# SHOW PARTITIONS

## Description

Displays partition information, including common partitions and [temporary partitions](../../../table_design/Temporary_partition.md).

> **NOTE**
>
> For v3.0 and later, this operation requires only one TABLE-level privilege.

## Syntax

```sql
SHOW [TEMPORARY] PARTITIONS FROM [db_name.]table_name [WHERE] [ORDER BY] [LIMIT]
```

> NOTE
>
> This syntax only supports StarRocks tables (`"ENGINE" = "OLAP"`). For Elasticsearch and Hive tables, use SHOW PROC '/dbs/db_id/table_id/partitions'.
> Since v3.0, this operation requires the SELECT privilege on the specified table. For v2.5 and earlier versions, this operation requires the SELECT__PRIV privilege on the specified table.

## Description of return fields

```plaintext
+-------------+---------------+----------------+---------------------+--------------------+--------+--------------+-------+--------------------+---------+----------------+---------------+---------------------+--------------------------+----------+------------+----------+
| PartitionId | PartitionName | VisibleVersion | VisibleVersionTime  | VisibleVersionHash | State  | PartitionKey | Range | DistributionKey    | Buckets | ReplicationNum | StorageMedium | CooldownTime        | LastConsistencyCheckTime | DataSize | IsInMemory | RowCount |
+-------------+---------------+----------------+---------------------+--------------------+--------+--------------+-------+--------------------+---------+----------------+---------------+---------------------+--------------------------+----------+------------+----------+
```

| **Field**                | **Description**                                              |
| ------------------------ | ------------------------------------------------------------ |
| PartitionId              | The partition ID.                                            |
| PartitionName            | The name of the partition.                                   |
| VisibleVersion           | The version number of the last successful load transaction. The version number increases by 1 with each successful load transaction. |
| VisibleVersionTime       | The timestamp of the last successful load transaction.       |
| VisibleVersionHash       | The hash value for the version number of the last successful load transaction. |
| State                    | The state of the partition. Fixed value: `Normal`.           |
| PartitionKey             | The partition key that consists of one or more partition columns. |
| Range                    | The range of the partition, which is a right half-open interval. |
| DistributionKey          | The bucket key of hash bucketing.                            |
| Buckets                  | The number of buckets for the partition.                     |
| ReplicationNum           | The number of replicas for a tablet in the partition.        |
| StorageMedium            | Data storage device. HHD means hard disk drives. SSD means Solid-state drives. |
| CooldownTime             | cooling time for data in the partition. After the cooling time,  data in this partition on SSDs is automatically migrated to HDDs by StarRocks. |
| LastConsistencyCheckTime | The time of the last consistency check. NULL indicates no consistency check was performed. |
| DataSize                 | Data size of the partition.                                  |
| IsInMemory               | Whether all the partition data is stored in memory.          |
| RowCount                 | The number of data rows of the partition.                    |


## Examples

1. Display information of all regular partitions from the specified table under the specified db.

    ```SQL
    MySQL [test]> show partitions from test.site_access\G
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

2. Display information of all temporary partitions from the specified table under the specified db.

    ```sql
    SHOW TEMPORARY PARTITIONS FROM example_db.table_name;
    ```

3. Display the information of the specified partition of the specified table under the specified db.

    ```sql
    -- Common partition
    SHOW PARTITIONS FROM example_db.table_name WHERE PartitionName = "p1";
    -- Temporary partition
    SHOW TEMPORARY PARTITIONS FROM example_db.table_name WHERE PartitionName = "p1";
    ```

4. Display the latest partition information of the specified table under the specified db.

    ```sql
    -- Common partition
    SHOW PARTITIONS FROM example_db.table_name ORDER BY PartitionId DESC LIMIT 1;
    -- Temporary partition
    SHOW TEMPORARY PARTITIONS FROM example_db.table_name ORDER BY PartitionId DESC LIMIT 1;
    ```
