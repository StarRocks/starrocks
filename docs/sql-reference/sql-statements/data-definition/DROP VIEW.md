# DROP VIEW

## description

This statement is used to drop a logical view VIEW

Syntax:

```sql
DROP VIEW [IF EXISTS]
[db_name.]view_name;
```

## example

1. If it exists, then drop the view example_view on example_db.

    ```sql
    DROP VIEW IF EXISTS example_db.example_view;
    ```

## keyword

DROP,VIEW
