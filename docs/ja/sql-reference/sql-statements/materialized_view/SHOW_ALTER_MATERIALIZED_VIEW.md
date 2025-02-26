---
displayed_sidebar: docs
---

# SHOW ALTER MATERIALIZED VIEW

## 説明

同期マテリアライズドビューの構築状況を表示します。

:::tip

この操作には特権は必要ありません。

:::

## 構文

```SQL
SHOW ALTER MATERIALIZED VIEW [ { FROM | IN } db_name]
```

角括弧 [] 内のパラメータはオプションです。

## パラメータ

| **パラメータ** | **必須** | **説明**                                                      |
| -------------- | -------- | ------------------------------------------------------------ |
| db_name        | いいえ   | マテリアライズドビューが存在するデータベースの名前。このパラメータが指定されていない場合、デフォルトで現在のデータベースが使用されます。 |

## 戻り値

| **戻り値**     | **説明**                                      |
| -------------- | --------------------------------------------- |
| JobId          | リフレッシュジョブのID。                       |
| TableName      | テーブルの名前。                               |
| CreateTime     | リフレッシュジョブが作成された時間。           |
| FinishedTime   | リフレッシュジョブが完了した時間。             |
| BaseIndexName  | ベーステーブルの名前。                         |
| RollupIndexName| マテリアライズドビューの名前。                 |
| RollupId       | マテリアライズドビューロールアップのID。       |
| TransactionId  | 実行待ちのトランザクションID。                 |
| State          | ジョブの状態。                                 |
| Msg            | エラーメッセージ。                             |
| Progress       | リフレッシュジョブの進捗。                     |
| Timeout        | リフレッシュジョブのタイムアウト。             |

## 例

例 1: 同期マテリアライズドビューの構築状況

```Plain
MySQL > SHOW ALTER MATERIALIZED VIEW\G
*************************** 1. row ***************************
          JobId: 475991
      TableName: lineorder
     CreateTime: 2022-08-24 19:46:53
   FinishedTime: 2022-08-24 19:47:15
  BaseIndexName: lineorder
RollupIndexName: lo_mv_sync_1
       RollupId: 475992
  TransactionId: 33067
          State: FINISHED
            Msg: 
       Progress: NULL
        Timeout: 86400
*************************** 2. row ***************************
          JobId: 477337
      TableName: lineorder
     CreateTime: 2022-08-24 19:47:25
   FinishedTime: 2022-08-24 19:47:45
  BaseIndexName: lineorder
RollupIndexName: lo_mv_sync_2
       RollupId: 477338
  TransactionId: 33068
          State: FINISHED
            Msg: 
       Progress: NULL
        Timeout: 86400
2 rows in set (0.00 sec)
```