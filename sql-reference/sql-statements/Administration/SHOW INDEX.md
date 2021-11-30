# SHOW INDEX

## description

This statement is used to show information related to index in a table. It currently only supports bitmap index.

Syntax:

```sql
SHOW INDEX[ES] FROM [db_name.]table_name [FROM database];
或者
SHOW KEY[S] FROM [db_name.]table_name [FROM database];
```

## example

1. Show all indexes under the specified table_name:

    ```sql
    SHOW INDEX FROM example_db.table_name;
    ```

## keyword

SHOW,INDEX
