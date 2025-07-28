---
displayed_sidebar: docs
---

# inspect_mv_refresh_info

`inspect_mv_refresh_info(mv_name)`

This function returns refresh information of a materialized view.

## Arguments

`mv_name`: The name of the materialized view (VARCHAR).

## Return Value

Returns a VARCHAR string containing the refresh information of the materialized view in JSON format including:
- `tableToUpdatePartitions`: the mv's base tables' to-refresh partition meta infos;
- `baseOlapTableVisibleVersionMap`: the mv's olap base tables' already refreshed version map;
- `baseExternalTableInfoVisibleVersionMap`: the mv's external base tables' already refreshed version map;

## Examples

Example 1: Inspect a materialized view's refresh information to check its current refresh status:

```
mysql> select inspect_mv_refresh_info('test_mv1');
+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| inspect_mv_refresh_info('test_mv1')                                                                                                                                                                                          |
+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| {"tableToUpdatePartitions":{},"baseOlapTableVisibleVersionMap":{"ss":{"ss":{"id":28672,"version":4,"lastRefreshTime":1751439875145,"lastFileModifiedTime":-1,"fileNumber":-1}}},"baseExternalTableInfoVisibleVersionMap":{}} |
+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)

```