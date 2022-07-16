# SHOW PARTITIONS

## description

This statement is used to display partition information

Syntax:

```sql
SHOW PARTITIONS FROM [db_name.]table_name [WHERE] [ORDER BY] [LIMIT];
```

Note:

```plain text
Support the filtering of PartitionId,PartitionName,State,Buckets,ReplicationNum,LastConsistencyCheckTime and other columns. This syntax only supports OLAP tables, ES tables and HIVE tables. Please use SHOW PROC '/dbs/db_id/table_id/partitions'SHOW PROC '/dbs/db_id/table_id/partitions'
```

## example

1. Display all partition information of the specified table under the specified db

    ```sql
    SHOW PARTITIONS FROM example_db.table_name;
    ```

2. Display the information of the specified partition of the specified table under the specified db

    ```sql
    SHOW PARTITIONS FROM example_db.table_name WHERE PartitionName = "p1";
    ```

3. Display the latest partition information of the specified table under the specified db

    ```sql
    SHOW PARTITIONS FROM example_db.table_name ORDER BY PartitionId DESC LIMIT 1;
    ```

## keyword

SHOW,PARTITIONS
