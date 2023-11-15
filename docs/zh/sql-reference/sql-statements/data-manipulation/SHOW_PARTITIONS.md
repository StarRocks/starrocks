# SHOW PARTITIONS

## 功能

该语句用于展示正常分区或临时分区信息。

## 语法

```sql
SHOW [TEMPORARY] PARTITIONS FROM [db_name.]table_name [WHERE] [ORDER BY] [LIMIT]
```

> 说明
>
> * 支持PartitionId,PartitionName,State,Buckets,ReplicationNum,LastConsistencyCheckTime等列的过滤。
> * 该语法只支持OLAP表，ES表和HIVE表请使用 SHOW PROC '/dbs/db_id/table_id/partitions';

## 示例

1. 展示指定 db 下指定表的所有分区信息。

    ```sql
    -- 正常分区
    SHOW PARTITIONS FROM example_db.table_name;
    -- 临时分区
    SHOW TEMPORARY PARTITIONS FROM example_db.table_name;
    ```

2. 展示指定 db 下指定表的指定分区的信息。

    ```sql
    -- 正常分区
    SHOW PARTITIONS FROM example_db.table_name WHERE PartitionName = "p1";
    -- 临时分区
    SHOW TEMPORARY PARTITIONS FROM example_db.table_name WHERE PartitionName = "p1";
    ```

3. 展示指定 db 下指定表的最新分区的信息。

    ```sql
    -- 正常分区
    SHOW PARTITIONS FROM example_db.table_name ORDER BY PartitionId DESC LIMIT 1;
    -- 临时分区
    SHOW TEMPORARY PARTITIONS FROM example_db.table_name ORDER BY PartitionId DESC LIMIT 1;
    ```
