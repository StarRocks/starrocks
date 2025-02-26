---
displayed_sidebar: docs
sidebar_position: 80
---

# ブラックリスト管理

場合によっては、管理者が特定のSQLパターンを無効にして、SQLがクラスタのクラッシュや予期しない高い同時実行クエリを引き起こすのを避ける必要があります。

StarRocksは、ユーザーがSQLブラックリストを追加、表示、削除することを可能にします。

## 構文

`enable_sql_blacklist` を使用してSQLブラックリストを有効にします。デフォルトはFalse（オフ）です。

~~~sql
admin set frontend config ("enable_sql_blacklist" = "true")
~~~

ADMIN_PRIV権限を持つ管理者ユーザーは、次のコマンドを実行してブラックリストを管理できます。

~~~sql
ADD SQLBLACKLIST "<sql>"
DELETE SQLBLACKLIST <sql_index_number>
SHOW SQLBLACKLISTS
~~~

* `enable_sql_blacklist` がtrueの場合、すべてのSQLクエリはsqlblacklistによってフィルタリングされる必要があります。一致する場合、ユーザーはそのSQLがブラックリストにあることを通知されます。それ以外の場合、SQLは通常通り実行されます。SQLがブラックリストにある場合、メッセージは次のようになります。

`ERROR 1064 (HY000): Access denied; sql 'select count (*) from test_all_type_select_2556' is in blacklist`

## ブラックリストの追加

~~~sql
ADD SQLBLACKLIST "<sql>"
~~~

**sql** は特定のタイプのSQLに対する正規表現です。

:::tip
現在、StarRocksはSELECT文をSQLブラックリストに追加することをサポートしています。
:::

SQL自体には、正規表現のセマンティクスと混同される可能性のある一般的な文字 `(`, `)`, `*`, `.` が含まれているため、エスケープ文字を使用してそれらを区別する必要があります。SQLで頻繁に使用される `(` と `)` については、エスケープ文字を使用する必要はありません。他の特殊文字には、エスケープ文字 `\` をプレフィックスとして使用する必要があります。例えば：

* `count(\*)` を禁止する：

~~~sql
ADD SQLBLACKLIST "select count(\\*) from .+"
~~~

* `count(distinct)` を禁止する：

~~~sql
ADD SQLBLACKLIST "select count(distinct .+) from .+"
~~~

* order by limit `x`, `y`, `1 <= x <=7`, `5 <=y <=7` を禁止する：

~~~sql
ADD SQLBLACKLIST "select id_int from test_all_type_select1 order by id_int limit [1-7], [5-7]"
~~~

* 複雑なSQLを禁止する：

~~~sql
ADD SQLBLACKLIST "select id_int \\* 4, id_tinyint, id_varchar from test_all_type_nullable except select id_int, id_tinyint, id_varchar from test_basic except select (id_int \\* 9 \\- 8) \\/ 2, id_tinyint, id_varchar from test_all_type_nullable2 except select id_int, id_tinyint, id_varchar from test_basic_nullable"
~~~

## ブラックリストの表示

~~~sql
SHOW SQLBLACKLIST
~~~

結果形式：`Index | Forbidden SQL`

例えば：

~~~sql
mysql> show sqlblacklist;
+-------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Index | Forbidden SQL                                                                                                                                                                                                                                                                                          |
+-------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 1     | select count\(\*\) from .+                                                                                                                                                                                                                                                                             |
| 2     | select id_int \* 4, id_tinyint, id_varchar from test_all_type_nullable except select id_int, id_tinyint, id_varchar from test_basic except select \(id_int \* 9 \- 8\) \/ 2, id_tinyint, id_varchar from test_all_type_nullable2 except select id_int, id_tinyint, id_varchar from test_basic_nullable |
| 3     | select id_int from test_all_type_select1 order by id_int limit [1-7], [5-7]                                                                                                                                                                                                                            |
| 4     | select count\(distinct .+\) from .+                                                                                                                                                                                                                                                                    |
+-------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

~~~

`Forbidden SQL` に表示されるSQLは、すべてのSQLセマンティック文字がエスケープされています。

## ブラックリストの削除

~~~sql
DELETE SQLBLACKLIST <sql_index_number>
~~~

`<sql_index_number>` はカンマ（,）で区切られたSQL IDのリストです。

例えば、上記のブラックリストのNo.3とNo.4のSQLを削除します：

~~~sql
delete sqlblacklist  3, 4;
~~~

その後、残りのsqlblacklistは次のようになります：

~~~sql
mysql> show sqlblacklist;
+-------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Index | Forbidden SQL                                                                                                                                                                                                                                                                                          |
+-------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 1     | select count\(\*\) from .+                                                                                                                                                                                                                                                                             |
| 2     | select id_int \* 4, id_tinyint, id_varchar from test_all_type_nullable except select id_int, id_tinyint, id_varchar from test_basic except select \(id_int \* 9 \- 8\) \/ 2, id_tinyint, id_varchar from test_all_type_nullable2 except select id_int, id_tinyint, id_varchar from test_basic_nullable |
+-------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

~~~