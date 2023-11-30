---
displayed_sidebar: "English"
---

# CANCEL ALTER TABLE

## Description

Cancels the execution of the following ongoing ALTER TABLE tasks on a given table:

- Modify columns
- Optimize table structure (since v3.2), including modifying bucketing method and the number of buckets.
- Create and delete ROLLUP indexes

> **NOTICE**
>
> This statement is a synchronous operation and requires you to have the `ALTER_PRIV` privilege on the table.

## Syntax

   ```SQL
   CANCEL ALTER TABLE { COLUMN | OPTIMIZE | ROLLUP } FROM [db_name.]table_name
   ```

## Parameters

- `{COLUMN ｜ OPTIMIZE | ROLLUP}`：

  - If `COLUMN` is specified, this statement cancels tasks for modifying columns.
  - If `OPTIMIZE` is specified, this statement cancels tasks for optimizing table structure.
  - If `ROLLUP` is specified, this statement cancels tasks for adding or deleting ROLLUP indexes.

- `db_name`: optional. The name of the database to which the table belongs. If this parameter is not specified, your current database is used by default.
- `table_name`: required. The table name.

## Examples

1. Cancel the task of modifying columns for `example_table` in the database `example_db`.

   ```SQL
   CANCEL ALTER TABLE COLUMN FROM example_db.example_table;
   ```

2. Cancel the task of optimizing table structure for `example_table` in the database `example_db`.

   ```SQL
   CANCEL ALTER TABLE OPTIMIZE FROM example_db.example_table;
   ```

3. Cancel the task of adding or deleting Rollup index for `example_table` in the current database.

   ```SQL
   CANCEL ALTER TABLE ROLLUP FROM example_table;
   ```
