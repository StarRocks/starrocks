---
displayed_sidebar: docs
---

# SHOW DELETE

## 説明

指定されたデータベース内の Duplicate Key、Unique Key、および 集計テーブル で正常に実行された過去の DELETE 操作をクエリします。データ削除の詳細については、 [DELETE](DELETE.md) を参照してください。

このコマンドは、主キーテーブル で実行された DELETE 操作をクエリするためには使用できないことに注意してください。

## 構文

```sql
SHOW DELETE [FROM <db_name>]
```

`db_name`: データベース名、省略可能。このパラメータが指定されていない場合、デフォルトで現在のデータベースが使用されます。

返されるフィールド:

- `TableName`: データが削除されたテーブル。
- `PartitionName`: データが削除されたパーティション。テーブルが非パーティションテーブルの場合、`*` が表示されます。
- `CreateTime`: DELETE タスクが作成された時間。
- `DeleteCondition`: 指定された DELETE 条件。
- `State`: DELETE タスクのステータス。

## 例

`database` のすべての過去の DELETE 操作を表示します。

```sql
SHOW DELETE FROM database;

+------------+---------------+---------------------+-----------------+----------+
| TableName  | PartitionName | CreateTime          | DeleteCondition | State    |
+------------+---------------+---------------------+-----------------+----------+
| mail_merge | *             | 2023-03-14 10:39:03 | name EQ "Peter" | FINISHED |
+------------+---------------+---------------------+-----------------+----------+
```