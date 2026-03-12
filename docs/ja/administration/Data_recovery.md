---
displayed_sidebar: docs
---

# データリカバリ

誤って削除されたデータベース/テーブル/パーティションをリカバリします。`drop table`または`drop database`の後、StarRocksはデータをすぐに物理的に削除せず、一定期間（デフォルトで1日）Trashに保持します。管理者は`RECOVER`コマンドを使用して、誤って削除されたデータをリカバリできます。

## 関連コマンド

構文:

```sql
-- 1) データベースのリカバリ
RECOVER DATABASE db_name;
-- 2) テーブルのリカバリ
RECOVER TABLE [db_name.]table_name;
-- 3) パーティションのリカバリ
RECOVER PARTITION partition_name FROM [db_name.]table_name;
```

## 注意

1. この操作は、削除されたメタ情報のみを復元できます。デフォルトの期間は1日であり、`fe.conf`の`catalog_trash_expire_second`パラメータで設定できます。
2. メタ情報が削除された後に、同じ名前とタイプの新しいメタ情報が作成された場合、以前に削除されたメタ情報は復元できません。

## 例

1. `example_db`という名前のデータベースをリカバリします

   ```sql
   RECOVER DATABASE example_db;
   ```

2. `example_tbl`という名前のテーブルをリカバリします

   ```sql
   RECOVER TABLE example_db.example_tbl;
   ```

3. テーブル`example_tbl`内の`p1`という名前のパーティションをリカバリします

   ```sql
   RECOVER PARTITION p1 FROM example_tbl;
   ```
