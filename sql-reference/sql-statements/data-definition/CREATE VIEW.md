# CREATE VIEW

## description

This statement is used to create a logical view.

Syntax:

```sql
CREATE VIEW [IF NOT EXISTS]
[db_name.]view_name
(column1[ COMMENT "col comment"][, column2, ...])
AS query_stmt
```

Note:

1. The view is a logical view with no physical storage. All queries on the view are equivalent to subqueries corresponding to the view.
2. query_stmt is arbitrarily supported SQL.

## Example

1. Create view example_view on example_db.

    ```sql
    CREATE VIEW example_db.example_view (k1, k2, k3, v1)
    AS
    SELECT c1 as k1, k2, k3, SUM(v1) FROM example_table
    WHERE k1 = 20160112 GROUP BY k1,k2,k3;
    ```

2. Create a view containing comment.

    ```sql
    CREATE VIEW example_db.example_view
    (
    k1 COMMENT "first key",
    k2 COMMENT "second key",
    k3 COMMENT "third key",
    v1 COMMENT "first value"
    )
    COMMENT "my first view"
    AS
    SELECT c1 as k1, k2, k3, SUM(v1) FROM example_table
    WHERE k1 = 20160112 GROUP BY k1,k2,k3;
    ```

## keyword

CREATE,VIEW
