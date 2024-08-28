---
displayed_sidebar: docs
---

# CREATE TABLE LIKE

## Description

Creates an identical empty table based on the definition of another table. The definition includes column definition, partitions, and table properties.

## Syntax

```sql
CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [database.]table_name LIKE [database.]table_name
```

> **NOTE**

1. You must have the `SELECT` privilege on the original table.
2. You can copy an external table such as MySQL.

## Example

<<<<<<< HEAD:docs/en/sql-reference/sql-statements/data-definition/CREATE_TABLE_LIKE.md
1. Under the test1 Database, create an empty table with the same table structure as table1, named table2.
=======
- `TEMPORARY`: Creates a temporary table. From v3.3.1, StarRocks supports creating temporary tables in the Default Catalog. For more information, see [Temporary Table](../../../table_design/StarRocks_table_design.md#temporary-table).
- `database`: the database.
- `table_name`: the name of the table you want to create. For the naming conventions, see [System limits](../../System_limit.md).
- `source_table_name`: the name of the source table you want to copy.
- - `partition_desc`: the partitioning method. For more information, see [CREATE TABLE](./CREATE_TABLE.md#partition_desc).
- `distribution_desc`: the bucketing method. For more information, see [CREATE TABLE](./CREATE_TABLE.md#distribution_desc).
- `PROPERTIES`: the properties of the table. All the table properties are supported. For more information, see [ALTER TABLE](ALTER_TABLE.md#modify-table-properties).
>>>>>>> e06217c368 ([Doc] Ref docs (#50111)):docs/en/sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE_LIKE.md

    ```sql
    CREATE TABLE test1.table2 LIKE test1.table1
    ```

2. Under the test2 Database, create an empty table with the same table structure as test1.table1, named table2.

    ```sql
    CREATE TABLE test2.table2 LIKE test1.table1
    ```

3. Under the test1 Database, create an empty table with the same table structure as MySQL external table, named table2.

    ```sql
    CREATE TABLE test1.table2 LIKE test1.table1
    ```
