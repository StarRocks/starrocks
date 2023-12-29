---
displayed_sidebar: "English"
---

# CANCEL ALTER TABLE

## Description

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

- `{COLUMN ｜ OPTIMIZE | ROLLUP}`：

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
