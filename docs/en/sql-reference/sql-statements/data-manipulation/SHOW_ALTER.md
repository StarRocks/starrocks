---
displayed_sidebar: "English"
---

# SHOW ALTER TABLE

## Description

Shows the execution of the following ongoing ALTER TABLE tasks:

- Modify columns
- Optimize table structure (since v3.2), including modifying bucketing method and the number of buckets.
- Create and delete ROLLUP indexes

## Syntax

- Show the execution of tasks related to modifying columns or optimizing table structure.

    ```sql
    SHOW ALTER TABLE { COLUMN | OPTIMIZE } [FROM db_name] [WHERE TableName|CreateTime|FinishTime|State] [ORDER BY] [LIMIT]
    ```

- Show the execution of tasks related to adding or deleting ROLLUP indexes.

    ```sql
    SHOW ALTER TABLE ROLLUP [FROM db_name]
    ```

## Parameters

- `{COLUMN ｜ OPTIMIZE | ROLLUP}`：

  - If `COLUMN` is specified, this statement shows tasks for modifying columns.
  - If `OPTIMIZE` is specified, this statement shows tasks for optimizing table structure.
  - If `ROLLUP` is specified, this statement shows tasks for adding or deleting ROLLUP indexes.

- `db_name`: optional. If `db_name` is not specified, the current database is used by default.

## Examples

1. Show the execution status of tasks related to modifying columns, optimizing table structure, and creating or deleting ROLLUP indexes in the current database.

    ```sql
    SHOW ALTER TABLE COLUMN;
    SHOW ALTER TABLE OPTIMIZE;
    SHOW ALTER TABLE ROLLUP;
    ```

2. Show the execution status of tasks related to modifying columns, optimizing table structure, and creating or deleting ROLLUP indexes in a specified database.

    ```sql
    SHOW ALTER TABLE COLUMN FROM example_db;
    SHOW ALTER TABLE OPTIMIZE FROM example_db;
    SHOW ALTER TABLE ROLLUP FROM example_db;
    ```

3. Show the execution of the most recent task related to modifying columns or optimizing table structure in a specified table.

    ```sql
    SHOW ALTER TABLE COLUMN WHERE TableName = "table1" ORDER BY CreateTime DESC LIMIT 1;
    SHOW ALTER TABLE OPTIMIZE WHERE TableName = "table1" ORDER BY CreateTime DESC LIMIT 1; 
    ```

## References

- [CREATE TABLE](../data-definition/CREATE_TABLE.md)
- [ALTER TABLE](../data-definition/ALTER_TABLE.md)
- [SHOW TABLES](../data-manipulation/SHOW_TABLES.md)
- [SHOW CREATE TABLE](../data-manipulation/SHOW_CREATE_TABLE.md)
