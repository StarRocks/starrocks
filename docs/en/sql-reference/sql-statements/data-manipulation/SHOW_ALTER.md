---
displayed_sidebar: "English"
---

# SHOW ALTER TABLE

## Description

Shows the execution of ongoing ALTER TABLE tasks.

## Syntax

```sql
SHOW ALTER TABLE {COLUMN | ROLLUP} [FROM <db_name>]
```

## Parameters

- COLUMN | ROLLUP

  - If COLUMN is specified, this statement shows tasks for modifying columns. If you need to nest a WHERE clause, the supported syntax is `[WHERE TableName|CreateTime|FinishTime|State] [ORDER BY] [LIMIT]`.

  - If ROLLUP is specified, this statement shows tasks for creating or deleting ROLLUP indexes.

- `db_name`: optional. If `db_name` is not specified, the current database is used by default.

## Examples

Example 1: Show column modification tasks in the current database.

```sql
SHOW ALTER TABLE COLUMN;
```

Example 2: Show the latest column modification task of a table.

```sql
SHOW ALTER TABLE COLUMN WHERE TableName = "table1"
ORDER BY CreateTime DESC LIMIT 1;
 ```

Example 3: Show tasks for creating or deleting ROLLUP indexes in a specified database.

```sql
SHOW ALTER TABLE ROLLUP FROM example_db;
````

## References

- [CREATE TABLE](../data-definition/CREATE_TABLE.md)
- [ALTER TABLE](../data-definition/ALTER_TABLE.md)
- [SHOW TABLES](../data-manipulation/SHOW_TABLES.md)
- [SHOW CREATE TABLE](../data-manipulation/SHOW_CREATE_TABLE.md)
