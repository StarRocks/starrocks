---
displayed_sidebar: docs
description: "ALTER VIEW Modifies the definition of a view."
---

# ALTER VIEW

ALTER VIEW Modifies the definition of a view.

## Syntax

```sql
ALTER VIEW
[db_name.]view_name
(column1[ COMMENT "col comment"][, column2, ...])
AS query_stmt
```

```sql
ALTER VIEW [db_name.]view_name RENAME new_view_name
```

Note:

1. View is logical, where the data isn't stored in the physical medium. The view will be used as a subquery in the statement when queried. Therefore, modifying the definition of views is equivalent to modifying query_stmt.
2. query_stmt is arbitrarily supported SQL.
3. RENAME changes the name of the view only. The privileges granted on the view, its comment, and its SQL SECURITY characteristic are preserved.

:::warning
A view is referenced by name. After you rename a view, any other view, materialized view, or application SQL that still refers to the old name fails with a "table not found" error at query time; no warning is raised at rename time. Materialized views built on the renamed view are set to inactive, and they cannot be activated again until you either rename the view back or recreate the materialized views.
:::

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

Rename `example_view` on `example_db` to `example_view_new_name`.

```sql
ALTER VIEW example_db.example_view RENAME example_view_new_name
```
