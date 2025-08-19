---
displayed_sidebar: docs
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