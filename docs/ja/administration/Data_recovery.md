---
displayed_sidebar: docs
---

# データリカバリー

StarRocks は、誤って削除されたデータベース/テーブル/パーティションのデータリカバリーをサポートしています。`drop table` または `drop database` の後、StarRocks はデータをすぐに物理的に削除せず、一定期間（デフォルトで1日）ゴミ箱に保持します。管理者は `RECOVER` コマンドを使用して、誤って削除されたデータを復元できます。

## 関連コマンド

構文:

~~~sql
-- 1) データベースを復元
RECOVER DATABASE db_name;
-- 2) テーブルを復元
RECOVER TABLE [db_name.]table_name;
-- 3) パーティションを復元
RECOVER PARTITION partition_name FROM [db_name.]table_name;
~~~

## 注意事項

1. この操作は削除されたメタ情報のみを復元できます。デフォルトの期間は1日で、`fe.conf` の `catalog_trash_expire_second` パラメータで設定できます。
2. メタ情報が削除された後に同じ名前とタイプの新しいメタ情報が作成された場合、以前に削除されたメタ情報は復元できません。

## 例

1. `example_db` という名前のデータベースを復元

    ~~~sql
    RECOVER DATABASE example_db;
    ~~~

2. `example_tbl` という名前のテーブルを復元

    ~~~sql
    RECOVER TABLE example_db.example_tbl;
    ~~~

3. テーブル `example_tbl` の `p1` という名前のパーティションを復元

    ~~~sql
    RECOVER PARTITION p1 FROM example_tbl;
    ~~~