---
displayed_sidebar: docs
---

# CANCEL ALTER TABLE

## Description

<<<<<<< HEAD
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

Example 1: Cancel the schema changes to `example_table` in the `example_db`database.

```SQL
CANCEL ALTER TABLE COLUMN FROM example_db.example_table;
```

Example 2: Cancel rollup index changes to `example_table` in your current database.

```SQL
CANCEL ALTER TABLE ROLLUP FROM example_table;
```
=======
Cancels the execution of the ongoing ALTER TABLE operation, including:

- Modify columns.
- Optimize table schema (from v3.2), including modifying the bucketing method and the number of buckets.
- Create and delete the rollup index.

> **NOTICE**
>
> - This statement is a synchronous operation.
> - This statement requires you to have the `ALTER_PRIV` privilege on the table.
> - This statement only supports canceling asynchronous operations using ALTER TABLE (as mentioned above) and does not support canceling synchronous operations using ALTER TABLE, such as rename.

## Syntax

   ```SQL
   CANCEL ALTER TABLE { COLUMN | OPTIMIZE | ROLLUP } FROM [db_name.]table_name
   ```

## Parameters

- `{COLUMN ｜ OPTIMIZE | ROLLUP}`

  - If `COLUMN` is specified, this statement cancels operations of modifying columns.
  - If `OPTIMIZE` is specified, this statement cancels operations of optimizing table schema.
  - If `ROLLUP` is specified, this statement cancels operations of adding or deleting the rollup index.

- `db_name`: optional. The name of the database to which the table belongs. If this parameter is not specified, your current database is used by default.
- `table_name`: required. The table name.

## Examples

1. Cancel the operation of modifying columns for `example_table` in the database `example_db`.

   ```SQL
   CANCEL ALTER TABLE COLUMN FROM example_db.example_table;
   ```

2. Cancel the operation of optimizing table schema for `example_table` in the database `example_db`.

   ```SQL
   CANCEL ALTER TABLE OPTIMIZE FROM example_db.example_table;
   ```

3. Cancel the operation of adding or deleting the rollup index for `example_table` in the current database.

   ```SQL
   CANCEL ALTER TABLE ROLLUP FROM example_table;
   ```
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
