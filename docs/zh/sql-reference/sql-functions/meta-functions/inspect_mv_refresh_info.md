---
displayed_sidebar: docs
---

# inspect_mv_refresh_info

`inspect_mv_refresh_info(mv_name)`

此函数返回物化视图的刷新信息。

## 参数

`mv_name`: 物化视图的名称 (VARCHAR)。

## 返回值

返回包含物化视图刷新信息的 JSON 格式的 VARCHAR 字符串，包括：
- `tableToUpdatePartitions`: MV 基表需要刷新的分区元信息；
- `baseOlapTableVisibleVersionMap`: MV 的 OLAP 基表已刷新版本映射；
- `baseExternalTableInfoVisibleVersionMap`: MV 的外部基表已刷新版本映射；

## 示例

示例1: 检查物化视图的刷新信息以查看其当前刷新状态:
```
mysql> select inspect_mv_refresh_info('test_mv1');
+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| inspect_mv_refresh_info('test_mv1')                                                                                                                                                                                          |
+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| {"tableToUpdatePartitions":{},"baseOlapTableVisibleVersionMap":{"ss":{"ss":{"id":28672,"version":4,"lastRefreshTime":1751439875145,"lastFileModifiedTime":-1,"fileNumber":-1}}},"baseExternalTableInfoVisibleVersionMap":{}} |
+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)
