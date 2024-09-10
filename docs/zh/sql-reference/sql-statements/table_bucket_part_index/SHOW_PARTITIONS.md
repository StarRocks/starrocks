---
displayed_sidebar: docs
keywords: ['fenqu']
---

# SHOW PARTITIONS

## 功能

该语句用于展示正常分区或临时分区信息。

## 语法

```sql
SHOW [TEMPORARY] PARTITIONS FROM [db_name.]table_name [WHERE] [ORDER BY] [LIMIT]
```

> 说明
>
> * 支持 PartitionId，PartitionName，State，Buckets，ReplicationNum，LastConsistencyCheckTime 等列的过滤。
> * 该语法只支持 StarRocks 表 (即 `"ENGINE" = "OLAP"`)。
> * 自 3.0 版本起，该操作需要对应表的 SELECT 权限。 3.0 版本之前，该操作需要对应数据库和表的 SELECT_PRIV 权限。

## 返回结果说明

```SQL
+-------------+---------------+----------------+---------------------+--------------------+--------+--------------+-------+--------------------+---------+----------------+---------------+---------------------+--------------------------+----------+------------+----------+
| PartitionId | PartitionName | VisibleVersion | VisibleVersionTime  | VisibleVersionHash | State  | PartitionKey | Range | DistributionKey    | Buckets | ReplicationNum | StorageMedium | CooldownTime        | LastConsistencyCheckTime | DataSize | IsInMemory | RowCount |
+-------------+---------------+----------------+---------------------+--------------------+--------+--------------+-------+--------------------+---------+----------------+---------------+---------------------+--------------------------+----------+------------+----------+
```

| **字段**                     | **说明**                                                         |
| ------------------------ | ------------------------------------------------------------ |
| PartitionId              | 分区 ID。                                                    |
| PartitionName            | 分区名。                                                     |
| VisibleVersion           | 最后一次成功导入的版本号。每次成功导入，则版本号加 1。   |
| VisibleVersionTime       | 最后一次成功导入的时间。                                   |
| VisibleVersionHash       | 最后一次成功导入的版本号的哈希值。                         |
| State                    | 分区的状态。固定为 `Normal`。                                |
| PartitionKey             | 分区键，由一个或多个分区列组成。                                                     |
| Range                    | Range 分区的范围，为左闭右开区间。                           |
| DistributionKey          | 分区中数据进行哈希分桶时的分桶键。                           |
| Buckets                  | 分区中的分桶数量。                                           |
| ReplicationNum           | 分区中每个 Tablet 的副本数量。                                |
| StorageMedium            | 数据存储介质。返回值为 `HDD` 表示机械硬盘，返回值为 `SSD` 表示固态硬盘。           |
| CooldownTime             | 数据降冷时间。如果一开始数据的存储介质为 SSD ，在该时间点之后，数据存储介质会从 SSD 切换为 HDD。 格式："yyyy-MM-dd HH:mm:ss"。|
| LastConsistencyCheckTime | 最后一次一致性检查的时间。`NULL` 表示没有进行一致性检查。    |
| DataSize                 | 分区中数据大小。                                             |
| IsInMemory               | 该分区数据是否全部存储在内存中。                             |
| RowCount                 | 该分区数据行数。                                             |
| MaxCS                    | 该分区最大 Compaction Score。仅限存算分离集群。                   |

## 示例

1. 查询指定数据库（例如 `test`）下指定表（例如 `site_access`）的所有正式分区信息：

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

2. 查询指定数据库（例如 `test`）下指定表（例如 `site_access`）的所有临时分区信息：

    ```sql
    SHOW TEMPORARY PARTITIONS FROM test.site_access;
    ```

3. 查询指定数据库（例如 `test`）下指定表（例如 `site_access`）的指定分区（例如 `p1`）的信息。

    ```sql
    -- 正常分区
    SHOW PARTITIONS FROM test.site_access WHERE PartitionName = "p1";
    -- 临时分区
    SHOW TEMPORARY PARTITIONS FROM test.site_access WHERE PartitionName = "p1";
    ```

4. 查询指定数据库（例如 `test`）下指定表（例如 `site_access`）的最新分区的信息。

    ```sql
    -- 正常分区
    SHOW PARTITIONS FROM test.site_access ORDER BY PartitionId DESC LIMIT 1;
    -- 临时分区
    SHOW TEMPORARY PARTITIONS FROM test.site_access ORDER BY PartitionId DESC LIMIT 1;
    ```
