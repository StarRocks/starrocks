# SHOW ALTER

## description

This statement is used to show the execution of various modification tasks currently in progress

Syntax：

```sql
SHOW ALTER [CLUSTER | TABLE [COLUMN | ROLLUP] [FROM db_name]];
```

Note：

```plain text
TABLE COLUMN: Show ALTER tasks to modify columns
Supports Syntax [WHERE TableName|CreateTime|FinishTime|State] [ORDER BY] [LIMIT]
TABLE ROLLUP: Shows tasks for creating or deleting ROLLUP indexes
If db_is not specified Name, using the current default DB
CLUSTER: Shows tasks related to cluster operations (only used by administrators! To be implemented...)
```

## example

1. Task execution showing all modified columns of default db

    ```sql
    SHOW ALTER TABLE COLUMN;
    ```

2. Shows task execution for a table that last modified columns

    ```sql
    SHOW ALTER TABLE COLUMN WHERE TableName = "table1" ORDER BY CreateTime DESC LIMIT 
    ```

3. Shows task execution for creating or deleting ROLLUP index for specified db

    ```sql
    SHOW ALTER TABLE ROLLUP FROM example_db;
    ````

4. Show cluster operations related tasks (Administrators only! To be implemented...)

    ```SQL
    SHOW ALTER CLUSTER;
    ```

## keyword

SHOW,ALTER
