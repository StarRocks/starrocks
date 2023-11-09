---
displayed_sidebar: "English"
---

# SHOW SQLBLACKLIST

## Description

Shows the SQL regular expressions in the SQL blacklist.

For more about SQL Blacklist, see [Manage SQL Blacklist](../../../administration/management/resource_management/Blacklist.md).

## Syntax

```SQL
SHOW SQLBLACKLIST
```

## Return value

| **Return**    | **Description**                                              |
| ------------- | ------------------------------------------------------------ |
| Index         | The index number of the SQL regular expression that has been added to the blacklist. |
| Forbidden SQL | The SQL regular expression that has been added to the blacklist. |

## Examples

```Plain
mysql> SHOW SQLBLACKLIST;
+-------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Index | Forbidden SQL                                                                                                                                                                                                                                                                                          |
+-------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 1     | select count\(\*\) from .+                                                                                                                                                                                                                                                                             |
| 2     | select id_int \* 4, id_tinyint, id_varchar from test_all_type_nullable except select id_int, id_tinyint, id_varchar from test_basic except select \(id_int \* 9 \- 8\) \/ 2, id_tinyint, id_varchar from test_all_type_nullable2 except select id_int, id_tinyint, id_varchar from test_basic_nullable |
| 3     | select id_int from test_all_type_select1 order by id_int limit [1-7], [5-7]                                                                                                                                                                                                                            |
| 4     | select count\(distinct .+\) from .+                                                                                                                                                                                                                                                                    |
+-------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```
