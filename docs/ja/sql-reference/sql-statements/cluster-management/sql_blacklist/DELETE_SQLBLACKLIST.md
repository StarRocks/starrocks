---
displayed_sidebar: docs
---

# DELETE SQLBLACKLIST

## 説明

SQL ブラックリストから SQL 正規表現を削除します。

SQL ブラックリストの詳細については、[Manage SQL Blacklist](../../../../administration/management/resource_management/Blacklist.md) を参照してください。

:::tip

この操作には、SYSTEM レベルの BLACKLIST 権限が必要です。[GRANT](../../account-management/GRANT.md) の指示に従って、この権限を付与することができます。

:::

## 構文

```SQL
DELETE SQLBLACKLIST <sql_index_number>
```

## パラメータ

`sql_index_number`: ブラックリスト内の SQL 正規表現のインデックス番号。複数のインデックス番号はカンマ (,) とスペースで区切ります。インデックス番号は [SHOW SQLBLACKLIST](SHOW_SQLBLACKLIST.md) を使用して取得できます。

## 例

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