# DESCRIBE

## Description

This statement is used to display schema information for the specified table.

Syntax

```sql
DESC[RIBE] [db_name.]table_name [ALL];
```

Note:

If ALL is specified, the schema of all indexes(rollup) of the table is displayed

## example

1. Show Base table schema

    ```sql
    DESC table_name;
    ```

2. Show the schema of all indexes in the table

    ```sql
    DESC db1.table_name ALL;
    ```

## keyword

DESCRIBE,DESC
