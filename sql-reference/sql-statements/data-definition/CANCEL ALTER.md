# CANCEL ALTER

## description

This statement is used to cancel an ALTER operation.
1.Cancel  ALTER TABLE COLUMN operation.

Syntax:

```sql
CANCEL ALTER TABLE COLUMN
FROM db_name.table_name
```

2.Cancel ALTER TABLE ROLLUP operation.

Syntax:

```sql
CANCEL ALTER TABLE ROLLUP
FROM db_name.table_name
```

3.Batch cancelling rollup based on job id.

Syntax:

```sql
CANCEL ALTER TABLE ROLLUP
FROM db_name.table_name (jobid,...)
```

Note:
This command is an asynchronous operation. To check whether it's successfully executed,  use`show alter table rollup`to check the status of tasks.

4.Cancel ALTER CLUSTER operation.

Syntax:

```sql
（To be realized...）
```

## example

[CANCEL ALTER TABLE COLUMN]

1. Cancel the ALTER COLUMN operation on my_table.

    ```sql
    CANCEL ALTER TABLE COLUMN
    FROM example_db.my_table;
    ```

    [CANCEL ALTER TABLE ROLLUP]

2. Cancel the ADD ROLLUP operation on my_table.

    ```sql
    CANCEL ALTER TABLE ROLLUP
    FROM example_db.my_table;
    ```

    [CANCEL ALTER TABLE ROLLUP]

3. Cancel ADD ROLLUP operation on my_table based on job id.

    ```sql
    CANCEL ALTER TABLE ROLLUP
    FROM example_db.my_table (12801,12802);
    ```

## keyword

CANCEL,ALTER,TABLE,COLUMN,ROLLUP
