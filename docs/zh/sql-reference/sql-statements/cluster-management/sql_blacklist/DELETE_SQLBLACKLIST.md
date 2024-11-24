---
displayed_sidebar: docs
---

# DELETE SQLBLACKLIST

## 功能

从 SQL 黑名单中删除一个 SQL 正则表达式。

有关 SQL 黑名单的更多信息，请参阅 [管理 SQL 黑名单](../../../../administration/management/resource_management/Blacklist.md)。

:::tip

该操作需要 SYSTEM 级 BLACKLIST 权限。请参考 [GRANT](../../account-management/GRANT.md) 为用户赋权。

:::

## 语法

```SQL
DELETE SQLBLACKLIST <sql_index_number>
```

## 参数说明

`sql_index_number`：SQL 正则表达式在黑名单中的序号。用逗号（,）和一个空格分隔多个序号。您可以使用 [SHOW SQLBLACKLIST](SHOW_SQLBLACKLIST.md) 查询相应索引号。

## 示例

```Plain
mysql> DELETE SQLBLACKLIST 3, 4;

mysql> SHOW SQLBLACKLIST;
+-------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Index | Forbidden SQL                                                                                                                                                                                                                                                                                          |
+-------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 1     | select count\(\*\) from .+                                                                                                                                                                                                                                                                             |
| 2     | select id_int \* 4, id_tinyint, id_varchar from test_all_type_nullable except select id_int, id_tinyint, id_varchar from test_basic except select \(id_int \* 9 \- 8\) \/ 2, id_tinyint, id_varchar from test_all_type_nullable2 except select id_int, id_tinyint, id_varchar from test_basic_nullable |
+-------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```
