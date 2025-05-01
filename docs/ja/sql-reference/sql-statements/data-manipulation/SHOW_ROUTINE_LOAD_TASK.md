---
displayed_sidebar: docs
---

# SHOW ROUTINE LOAD TASK

## 説明

Routine Load ジョブ内のロードタスクの実行情報を表示します。

:::note

Routine Load ジョブとその中のロードタスクの関係については、[Load data using Routine Load](../../../loading/RoutineLoad.md#basic-concepts) を参照してください。

:::

## 構文

```SQL
SHOW ROUTINE LOAD TASK
[  FROM <db_name>]
[  WHERE JobName = <job_name> ]
```

:::note

通常の横方向のテーブル形式ではなく、縦方向に結果を表示するには、ステートメントに `\G` オプションを追加できます（例: `SHOW ROUTINE LOAD TASK WHERE JobName = <job_name>\G`）。

:::

## パラメータ

| **パラメータ** | **必須** | **説明**                                             |
| ------------- | ------------ | ----------------------------------------------------------- |
| db_name       | いいえ           | Routine Load ジョブが属するデータベースの名前。 |
| JobName       | いいえ           | Routine Load ジョブの名前。                               |

## 出力

| **パラメータ**        | **説明**                                              |
| -------------------- | ------------------------------------------------------------ |
| TaskId               | StarRocks によって自動生成されるロードタスクのグローバルに一意の ID。 |
| TxnId                | ロードタスクが属するトランザクションの ID。        |
| TxnStatus            | ロードタスクが属するトランザクションのステータス。`UNKNOWN` は、ロードタスクがまだディスパッチまたは実行されていないため、トランザクションが開始されていない可能性があることを示します。 |
| JobId                | ロードジョブの ID。                                          |
| CreateTime           | ロードタスクが作成された日時。            |
| LastScheduledTime    | ロードタスクが最後にスケジュールされた日時。     |
| ExecuteStartTime     | ロードタスクが実行された日時。           |
| Timeout              | FE パラメータ [`routine_load_task_timeout_second`](../../../administration/Configuration.md#routine_load_task_timeout_second) と Routine Load ジョブの [job_properties](./CREATE_ROUTINE_LOAD.md#job_properties) の `task_timeout_second` パラメータによって制御されるロードタスクのタイムアウト期間。 |
| BeId                 | ロードタスクを実行する BE の ID。                    |
| DataSourceProperties | トピックのパーティション内のメッセージを消費するロードタスクの進捗状況（オフセットで測定）。 |
| Message              | タスクエラー情報を含むロードタスクに対して返される情報。 |

## 例

Routine Load ジョブ `example_tbl_ordertest` 内のすべてのロードタスクを表示します。

```SQL
MySQL [example_db]> SHOW ROUTINE LOAD TASK WHERE JobName = "example_tbl_ordertest";  
+--------------------------------------+-------+-----------+-------+---------------------+---------------------+------------------+---------+------+------------------------------------+-----------------------------------------------------------------------------+
| TaskId                               | TxnId | TxnStatus | JobId | CreateTime          | LastScheduledTime   | ExecuteStartTime | Timeout | BeId | DataSourceProperties               | Message                                                                     |
+--------------------------------------+-------+-----------+-------+---------------------+---------------------+------------------+---------+------+------------------------------------+-----------------------------------------------------------------------------+
| abde6998-c19a-43d6-b48c-6ca7e14144a3 | -1    | UNKNOWN   | 10208 | 2023-12-22 12:46:10 | 2023-12-22 12:47:00 | NULL             | 60      | -1   | Progress:{"0":6},LatestOffset:null | there is no new data in kafka/pulsar, wait for 10 seconds to schedule again |
+--------------------------------------+-------+-----------+-------+---------------------+---------------------+------------------+---------+------+------------------------------------+-----------------------------------------------------------------------------+
1 row in set (0.00 sec)
```