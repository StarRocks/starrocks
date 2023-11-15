# CREATE TABLE LIKE

## description

This statement is used to create an identical empty table based on the structure of another table.

Syntax:

```sql
CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [database.]table_name LIKE [database.]table_name
```

Note:

1. The replicated table structures include Column Definition, Partitions, Table Properties, etc.
2. `SELECT`permission is required on the original table.
3. Support to copy external table such as MySQL.

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

## keyword

CREATE,TABLE,LIKE
