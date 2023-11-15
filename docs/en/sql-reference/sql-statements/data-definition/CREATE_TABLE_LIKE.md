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

1. Under the test1 Database, create an empty table with the same table structure as table1, named table2.

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
