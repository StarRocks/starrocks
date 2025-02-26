---
displayed_sidebar: docs
---

# SHOW SQLBLACKLIST

## Description

SQL ブラックリストにある SQL 正規表現を表示します。

SQL ブラックリストの詳細については、[Manage SQL Blacklist](../../../../administration/management/resource_management/Blacklist.md) を参照してください。

:::tip

この操作には SYSTEM レベルの BLACKLIST 権限が必要です。この権限を付与するには、[GRANT](../../account-management/GRANT.md) の指示に従ってください。

:::

## Syntax

```SQL
SHOW SQLBLACKLIST
```

## Return value

| **Return**    | **Description**                                              |
| ------------- | ------------------------------------------------------------ |
| Index         | ブラックリストに追加された SQL 正規表現のインデックス番号です。 |
| Forbidden SQL | ブラックリストに追加された SQL 正規表現です。 |

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