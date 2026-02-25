---
displayed_sidebar: docs
---

# SHOW DYNAMIC PARTITION TABLES

## 説明

このステートメントは、データベース内で動的パーティション化プロパティが設定されているすべてのパーティションテーブルのステータスを表示するために使用されます。

## 構文

```sql
SHOW DYNAMIC PARTITION TABLES FROM <db_name>
```

このステートメントは以下のフィールドを返します:

- TableName: テーブルの名前。
- Enable: 動的パーティション化が有効かどうか。
- TimeUnit: パーティションの時間粒度。
- Start: 動的パーティション化の開始オフセット。
- End: 動的パーティション化の終了オフセット。
- Prefix: パーティション名のプレフィックス。
- Buckets: 各パーティションのバケット数。
- ReplicationNum: テーブルのレプリカ数。
- StartOf: 指定された TimeUnit に応じた各週/月の最初の日。
- LastUpdateTime: テーブルが最後に更新された時間。
- LastSchedulerTime: テーブル内のデータが最後にスケジュールされた時間。
- State: テーブルのステータス。
- LastCreatePartitionMsg: 最新のパーティション作成操作のメッセージ。
- LastDropPartitionMsg: 最新のパーティション削除操作のメッセージ。

## 例

`db_test` で動的パーティション化プロパティが設定されているすべてのパーティションテーブルのステータスを表示します。

```sql
SHOW DYNAMIC PARTITION TABLES FROM db_test;
```