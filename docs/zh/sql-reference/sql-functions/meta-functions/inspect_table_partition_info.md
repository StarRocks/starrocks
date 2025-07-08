---
displayed_sidebar: docs
---

# inspect_table_partition_info

`inspect_table_partition_info(table_name)`

此函数返回表的分区信息。

## 参数

`table_name`: 表的名称 (VARCHAR)。

## 返回值

返回包含表分区信息的 JSON 格式的 VARCHAR 字符串。

## 示例

示例1: 检查表的分区信息:
```
mysql> select inspect_table_partition_info('ss');
+-----------------------------------------------------------------------------------------------------------+
| inspect_table_partition_info('ss')                                                                        |
+-----------------------------------------------------------------------------------------------------------+
| {"ss":{"id":28672,"version":4,"lastRefreshTime":1751439875145,"lastFileModifiedTime":-1,"fileNumber":-1}} |
+-----------------------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)
