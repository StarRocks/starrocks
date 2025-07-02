---
displayed_sidebar: docs
---

# inspect_table_partition_info

`inspect_table_partition_info(table_name)`

This function returns partition information of a table.

## Arguments

`table_name`: The name of the table (VARCHAR).

## Return Value

Returns a VARCHAR string containing the partition information of the table in JSON format.

## Examples

Example 1: Inspect a table's partitino information:
```
mysql> select inspect_table_partition_info('ss');
+-----------------------------------------------------------------------------------------------------------+
| inspect_table_partition_info('ss')                                                                        |
+-----------------------------------------------------------------------------------------------------------+
| {"ss":{"id":28672,"version":4,"lastRefreshTime":1751439875145,"lastFileModifiedTime":-1,"fileNumber":-1}} |
+-----------------------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)

```