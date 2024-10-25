---
displayed_sidebar: docs
---

# ALTER VIEW

## Description

Modifies the definition of a view.

## Syntax

```sql
ALTER VIEW
[db_name.]view_name
(column1[ COMMENT "col comment"][, column2, ...])
AS query_stmt
```

Note:

1. View is logical, where the data isn't stored in the physical medium. The view will be used as a subquery in the statement when queried. Therefore, modifying the definition of views is equivalent to modifying query_stmt.
2. query_stmt is arbitrarily supported SQL.

## Examples

Alter `example_view` on `example_db`.

```sql
ALTER VIEW example_db.example_view
(
c1 COMMENT "column 1",
c2 COMMENT "column 2",
c3 COMMENT "column 3"
)
AS SELECT k1, k2, SUM(v1) 
FROM example_table
GROUP BY k1, k2
```
