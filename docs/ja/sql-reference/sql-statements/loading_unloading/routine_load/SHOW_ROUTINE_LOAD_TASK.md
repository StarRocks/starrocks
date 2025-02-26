---
displayed_sidebar: docs
---

# SHOW ROUTINE LOAD TASK

import RoutineLoadPrivNote from '../../../../_assets/commonMarkdown/RoutineLoadPrivNote.md'

## 説明

Routine Load ジョブ内のロードタスクの実行情報を表示します。

:::note

- <RoutineLoadPrivNote />

:::

## 構文

```SQL
SHOW ROUTINE LOAD TASK
[  FROM <db_name>]
WHERE JobName = <job_name>
```

:::note

通常の横向きのテーブル形式ではなく、縦向きに結果を表示するために、ステートメントに `\G` オプションを追加することができます（例: `SHOW ROUTINE LOAD TASK WHERE JobName = <job_name>\G`）。

:::

## パラメータ

| **パラメータ** | **必須** | **説明**                                             |
| ------------- | ------------ | ----------------------------------------------------------- |
| db_name       | いいえ           | Routine Load ジョブが属するデータベースの名前。 |
| JobName       | はい          | Routine Load ジョブの名前。                               |

## 出力

| **パラメータ**        | **説明**                                              |
| -------------------- | ------------------------------------------------------------ |
| TaskId               | StarRocks によって自動生成されるロードタスクのグローバルに一意な ID。 |
| TxnId                | ロードタスクが属するトランザクションの ID。        |
| TxnStatus            | ロードタスクが属するトランザクションのステータス。`UNKNOWN` は、ロードタスクがまだディスパッチまたは実行されていないため、トランザクションが開始されていないことを示します。 |
| JobId                | ロードジョブの ID。                                          |
| CreateTime           | ロードタスクが作成された日時。            |
| LastScheduledTime    | ロードタスクが最後にスケジュールされた日時。     |
| ExecuteStartTime     | ロードタスクが実行された日時。           |
| Timeout              | FE パラメータ [`routine_load_task_timeout_second`](../../../../administration/management/FE_configuration.md#routine_load_task_timeout_second) と [job_properties](./CREATE_ROUTINE_LOAD.md) の `task_timeout_second` パラメータによって制御されるロードタスクのタイムアウト期間。 |
| BeId                 | ロードタスクを実行する BE の ID。                    |
| DataSourceProperties | トピックのパーティション内のメッセージを消費するロードタスクの進捗状況（オフセットで測定）。 |
| Message              | タスクエラー情報を含むロードタスクに対して返される情報。 |

## 例

Routine Load ジョブ `example_tbl_ordertest` のすべてのロードタスクを表示します。

```SQL
MySQL [example_db]> SHOW ROUTINE LOAD TASK WHERE JobName = "example_tbl_ordertest";  
+--------------------------------------+-------+-----------+-------+---------------------+---------------------+------------------+---------+------+------------------------------------+-----------------------------------------------------------------------------+
| TaskId                               | TxnId | TxnStatus | JobId | CreateTime          | LastScheduledTime   | ExecuteStartTime | Timeout | BeId | DataSourceProperties               | Message                                                                     |
+--------------------------------------+-------+-----------+-------+---------------------+---------------------+------------------+---------+------+------------------------------------+-----------------------------------------------------------------------------+
| abde6998-c19a-43d6-b48c-6ca7e14144a3 | -1    | UNKNOWN   | 10208 | 2023-12-22 12:46:10 | 2023-12-22 12:47:00 | NULL             | 60      | -1   | Progress:{"0":6},LatestOffset:null | there is no new data in kafka/pulsar, wait for 10 seconds to schedule again |
+--------------------------------------+-------+-----------+-------+---------------------+---------------------+------------------+---------+------+------------------------------------+-----------------------------------------------------------------------------+
1 row in set (0.00 sec)
```