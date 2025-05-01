---
displayed_sidebar: docs
---

# SUBMIT TASK

## Description

ETL ステートメントを非同期タスクとして送信します。この機能は StarRocks v2.5 からサポートされています。

StarRocks v2.5 は [CREATE TABLE AS SELECT](../data-definition/CREATE_TABLE_AS_SELECT.md) の非同期タスクの送信をサポートしています。

## Syntax

```SQL
SUBMIT TASK [task_name] AS <etl_statement>
```

## Parameters

| **Parameter** | **Description**                                              |
| ------------- | ------------------------------------------------------------ |
| task_name     | タスク名。                                                   |
| etl_statement | 非同期タスクとして送信したい ETL ステートメント。StarRocks は現在、[CREATE TABLE AS SELECT](../data-definition/CREATE_TABLE_AS_SELECT.md) の非同期タスクの送信をサポートしています。 |

## Usage notes

このステートメントは、ETL ステートメントを実行するタスクを保存するテンプレートである Task を作成します。Task の情報は、[Information Schema](../../../administration/information_schema.md) のメタデータテーブル `tasks` をクエリすることで確認できます。

```SQL
SELECT * FROM INFORMATION_SCHEMA.tasks;
SELECT * FROM information_schema.tasks WHERE task_name = '<task_name>';
```

Task を実行すると、それに応じて TaskRun が生成されます。TaskRun は ETL ステートメントを実行するタスクを示します。TaskRun には次の状態があります：

- `PENDING`: タスクは実行待ちです。
- `RUNNING`: タスクは実行中です。
- `FAILED`: タスクは失敗しました。
- `SUCCESS`: タスクは正常に実行されました。

TaskRun の状態は、[Information Schema](../../../administration/information_schema.md) のメタデータテーブル `task_runs` をクエリすることで確認できます。

```SQL
SELECT * FROM INFORMATION_SCHEMA.task_runs;
SELECT * FROM information_schema.task_runs WHERE task_name = '<task_name>';
```

## Configure via FE configuration items

非同期 ETL タスクは、以下の FE 設定項目を使用して構成できます：

| **Parameter**                | **Default value** | **Description**                                              |
| ---------------------------- | ----------------- | ------------------------------------------------------------ |
| task_ttl_second              | 86400            | Task が有効な期間。単位：秒。有効期間を超えたタスクは削除されます。 |
| task_check_interval_second   | 14400             | 無効なタスクを削除する時間間隔。単位：秒。                   |
| task_runs_ttl_second         | 86400            | TaskRun が有効な期間。単位：秒。有効期間を超えた TaskRun は自動的に削除されます。また、`FAILED` および `SUCCESS` 状態の TaskRun も自動的に削除されます。 |
| task_runs_concurrency        | 4                 | 並行して実行できる TaskRun の最大数。                        |
| task_runs_queue_length       | 500               | 実行待ちの TaskRun の最大数。デフォルト値を超えると、受信タスクは保留されます。 |
| task_runs_max_history_number | 10000      | 保持する TaskRun レコードの最大数。                          |

## Examples

例 1: `CREATE TABLE tbl1 AS SELECT * FROM src_tbl` の非同期タスクを送信し、タスク名を `etl0` と指定します：

```SQL
SUBMIT TASK etl0 AS CREATE TABLE tbl1 AS SELECT * FROM src_tbl;
```