---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns a JSON array of all materialized views that reference a given base table."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# inspect_related_mv

`inspect_related_mv(table_name)`

This function returns related materialized views of a table in JSON array format.

## Arguments

`table_name`: The name of the table (VARCHAR).

## Return Value

Returns a VARCHAR string containing a JSON array of related materialized views.

## Examples

Example 1: Inspect `ss` table related mvs:
```
mysql> select inspect_related_mv('ss');
+---------------------------------------------------------------------+
| inspect_related_mv('ss')                                            |
+---------------------------------------------------------------------+
| [{"id":28806,"name":"mv_on_view_1"},{"id":28844,"name":"test_mv1"}] |
+---------------------------------------------------------------------+
1 row in set (0.01 sec)

```