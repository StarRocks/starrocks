---
displayed_sidebar: docs
---

# inspect_mv_relationships

`inspect_mv_relationships()`

This function returns the content in `ConnectorTblMetaInfoMgr`, which contains mapping information from base external table to materialized view.

## Arguments

None.

## Return Value

Returns a VARCHAR string containing the mapping information in JSON format.

## Examples

Example 1: Inspect current mv relations in ConnectorTblMetaInfoMgr:
```
mysql> select inspect_mv_relationships();
+----------------------------+
| inspect_mv_relationships() |
+----------------------------+
| {}                         |
+----------------------------+
1 row in set (0.01 sec)

```