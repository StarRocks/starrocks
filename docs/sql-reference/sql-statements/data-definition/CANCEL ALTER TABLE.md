# CANCEL ALTER TABLE

## Description

Cancels the following operations performed with the ALTER TABLE statement on a given table:

- Table schema: add and delete columns, reorder columns, and modify data types of columns.
- Rollup index: create and delete rollup indexes.

This statement is a synchronous operation and requires you to have the `ALTER_PRIV` privilege on the table.

## Syntax

- Cancel schema changes.

    ```SQL
    CANCEL ALTER TABLE COLUMN FROM [db_name.]table_name
    ```

- Cancel changes to rollup indexes.

    ```SQL
    CANCEL ALTER TABLE ROLLUP FROM [db_name.]table_name
    ```

## Parameters

| **Parameter** | **Required** | **Description**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| db_name       | No           | The name of the database to which the table belongs. If this parameter is not specified, your current database is used by default. |
| table_name    | Yes          | The table name.                                              |

## Examples

Example 1：Cancel the schema changes to `example_table` in the `example_db`database.

```SQL
CANCEL ALTER TABLE COLUMN FROM example_db.example_table;
```

Example 2：Cancel rollup index changes to `example_table` in your current database.

```SQL
CANCEL ALTER TABLE ROLLUP FROM example_table;
```
