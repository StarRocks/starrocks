---
displayed_sidebar: docs
description: "Shows the execution status of ongoing ALTER TABLE operations including column changes, schema optimization, and rollup index changes."
---

# SHOW ALTER TABLE

SHOW ALTER TABLE shows the execution of the ongoing ALTER TABLE operations, including:

- Modify columns.
- Optimize table schema (from v3.2), including modifying the bucketing method and the number of buckets.
- Create and delete the rollup index.
- Modify metadata-only properties of cloud-native (shared-data) tables, such as `file_bundling`, `enable_persistent_index`, `persistent_index_type`, and `compaction_strategy`. These changes run asynchronously and are shown under `COLUMN`.

## Syntax

- Show the execution of operations of modifying columns or optimizing table schema.

    ```sql
    SHOW ALTER TABLE { COLUMN | OPTIMIZE } [FROM db_name] [WHERE TableName|CreateTime|FinishTime|State] [ORDER BY] [LIMIT]
    ```

- Show the execution of operations of adding or deleting the rollup index.

    ```sql
    SHOW ALTER TABLE ROLLUP [FROM db_name]
    ```

## Parameters

- `{COLUMN ｜ OPTIMIZE | ROLLUP}`:

  - If `COLUMN` is specified, this statement shows operations of modifying columns. For cloud-native (shared-data) tables, it also shows asynchronous metadata-only changes triggered by `ALTER TABLE ... SET (...)`, such as modifying `file_bundling`, `enable_persistent_index`, `persistent_index_type`, or `compaction_strategy`.
  - If `OPTIMIZE` is specified, this statement shows operations of optimizing table structure.
  - If `ROLLUP` is specified, this statement shows operations of adding or deleting the rollup index.

- `db_name`: optional. If `db_name` is not specified, the current database is used by default.

## Examples

1. Show the execution of operations of modifying columns, optimizing table schema, and creating or deleting the rollup index in the current database.

    ```sql
    SHOW ALTER TABLE COLUMN;
    SHOW ALTER TABLE OPTIMIZE;
    SHOW ALTER TABLE ROLLUP;
    ```

2. Show the execution of operations related to modifying columns, optimizing table schema, and creating or deleting the rollup index in a specified database.

    ```sql
    SHOW ALTER TABLE COLUMN FROM example_db;
    SHOW ALTER TABLE OPTIMIZE FROM example_db;
    SHOW ALTER TABLE ROLLUP FROM example_db;
    ```

3. Show the execution of the most recent operation of modifying columns or optimizing table schema in a specified table.

    ```sql
    SHOW ALTER TABLE COLUMN WHERE TableName = "table1" ORDER BY CreateTime DESC LIMIT 1;
    SHOW ALTER TABLE OPTIMIZE WHERE TableName = "table1" ORDER BY CreateTime DESC LIMIT 1; 
    ```

## References

- [CREATE TABLE](CREATE_TABLE.md)
- [ALTER TABLE](ALTER_TABLE.md)
- [SHOW TABLES](SHOW_TABLES.md)
- [SHOW CREATE TABLE](SHOW_CREATE_TABLE.md)
