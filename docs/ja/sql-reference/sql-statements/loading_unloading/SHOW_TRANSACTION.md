---
displayed_sidebar: docs
---

# SHOW TRANSACTION

## 説明

この構文は、指定されたトランザクション ID のトランザクション詳細を表示するために使用されます。

構文:

```sql
SHOW TRANSACTION
[FROM <db_name>]
WHERE id = transaction_id
```

返される結果の例:

```plain text
TransactionId: 4005
Label: insert_8d807d5d-bcdd-46eb-be6d-3fa87aa4952d
Coordinator: FE: 10.74.167.16
TransactionStatus: VISIBLE
LoadJobSourceType: INSERT_STREAMING
PrepareTime: 2020-01-09 14:59:07
CommitTime: 2020-01-09 14:59:09
FinishTime: 2020-01-09 14:59:09
Reason:
ErrorReplicasCount: 0
ListenerId: -1
TimeoutMs: 300000
```

* TransactionId: トランザクション ID
* Label: タスクに対応するインポートラベル
* Coordinator: トランザクション調整を担当するノード
* TransactionStatus: トランザクションのステータス
* PREPARE: 準備段階
* COMMITTED: トランザクションは成功したが、データは見えない
* VISIBLE: トランザクションは成功し、データが見える
* ABORTED: トランザクションが失敗
* LoadJobSourceType: インポートタスクのタイプ
* PrepareTime: トランザクションの開始時間
* CommitTime: トランザクションが正常にコミットされた時間
* FinishTime: データが見えるようになった時間
* Reason: エラーメッセージ
* ErrorReplicasCount: エラーのあるレプリカの数
* ListenerId: 関連するインポートジョブの ID
* TimeoutMs: トランザクションのタイムアウト時間（ミリ秒）

## 例

1. ID 4005 のトランザクションを表示するには:

    ```sql
    SHOW TRANSACTION WHERE ID=4005;
    ```

2. 指定されたデータベースで、ID 4005 のトランザクションを表示するには:

    ```sql
    SHOW TRANSACTION FROM db WHERE ID=4005;
    ```