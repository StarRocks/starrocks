# SHOW PARTITIONS

## Description

Displays partition information, including common partitions and [temporary partitions](../../../table_design/Temporary_partition.md).

## Syntax

```sql
SHOW [TEMPORARY] PARTITIONS FROM [db_name.]table_name [WHERE] [ORDER BY] [LIMIT]
```

> NOTE
>
> This syntax only supports StarRocks tables (`"ENGINE" = "OLAP"`). For Elasticsearch and Hive tables, use SHOW PROC '/dbs/db_id/table_id/partitions'.
> Since v3.0, this operation requires the SELECT privilege on the specified table. For v2.5 and earlier versions, this operation requires the SELECT__PRIV privilege on the specified table.

## Examples

1. Display all partition information of the specified table under the specified db

    ```sql
    -- Common partition
    SHOW PARTITIONS FROM example_db.table_name;
    -- Temporary partition
    SHOW TEMPORARY PARTITIONS FROM example_db.table_name;
    ```

2. Display the information of the specified partition of the specified table under the specified db

    ```sql
    -- Common partition
    SHOW PARTITIONS FROM example_db.table_name WHERE PartitionName = "p1";
    -- Temporary partition
    SHOW TEMPORARY PARTITIONS FROM example_db.table_name WHERE PartitionName = "p1";
    ```

3. Display the latest partition information of the specified table under the specified db

    ```sql
    -- Common partition
    SHOW PARTITIONS FROM example_db.table_name ORDER BY PartitionId DESC LIMIT 1;
    -- Temporary partition
    SHOW TEMPORARY PARTITIONS FROM example_db.table_name ORDER BY PartitionId DESC LIMIT 1;
    ```
