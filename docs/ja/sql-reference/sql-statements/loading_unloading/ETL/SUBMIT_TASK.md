---
displayed_sidebar: docs
---

# SUBMIT TASK

## Description

ETL ステートメントを非同期タスクとして送信します。

このステートメントを使用して、以下を行うことができます：

- バックグラウンドで長時間実行されるタスクを実行する（v2.5以降でサポート）
- 定期的にタスクをスケジュールする（v3.3以降でサポート）

サポートされているステートメントには以下が含まれます：

- [CREATE TABLE AS SELECT](../../table_bucket_part_index/CREATE_TABLE_AS_SELECT.md) （v3.0以降）
- [INSERT](../INSERT.md) （v3.0以降）
- [CACHE SELECT](../../../../data_source/data_cache_warmup.md) （v3.3以降）

タスクの一覧を表示するには `INFORMATION_SCHEMA.tasks` をクエリし、タスクの実行履歴を表示するには `INFORMATION_SCHEMA.task_runs` をクエリします。詳細については、[Usage Notes](#usage-notes) を参照してください。

非同期タスクを削除するには [DROP TASK](DROP_TASK.md) を使用します。

## Syntax

```SQL
SUBMIT TASK <task_name> 
[SCHEDULE [START(<schedule_start>)] EVERY(INTERVAL <schedule_interval>) ]
[PROPERTIES(<"key" = "value"[, ...]>)]
AS <etl_statement>
```

## Parameters

| **Parameter**      | **Required** | **Description**                                                                                     |
| -------------      | ------------ | ---------------------------------------------------------------------------------------------------- |
| task_name          | Yes     | タスクの名前。                                                                               |
| schedule_start     | No      | スケジュールされたタスクの開始時間。                                                                 |
| schedule_interval  | No      | スケジュールされたタスクが実行される間隔。最小間隔は10秒です。          |
| etl_statement      | Yes     | 非同期タスクとして送信したい ETL ステートメント。StarRocks は現在、[CREATE TABLE AS SELECT](../../table_bucket_part_index/CREATE_TABLE_AS_SELECT.md) および [INSERT](../../loading_unloading/INSERT.md) の非同期タスクの送信をサポートしています。 |

## Usage notes

このステートメントは、ETL ステートメントを実行するタスクを保存するテンプレートである Task を作成します。Task の情報を確認するには、メタデータビュー [`tasks` in Information Schema](../../../information_schema/tasks.md) をクエリします。

```SQL
SELECT * FROM INFORMATION_SCHEMA.tasks;
SELECT * FROM information_schema.tasks WHERE task_name = '<task_name>';
```

Task を実行すると、それに応じて TaskRun が生成されます。TaskRun は ETL ステートメントを実行するタスクを示します。TaskRun には以下の状態があります：

- `PENDING`: タスクは実行待ちです。
- `RUNNING`: タスクは実行中です。
- `FAILED`: タスクは失敗しました。
- `SUCCESS`: タスクは正常に実行されました。

TaskRun の状態を確認するには、メタデータビュー [`task_runs` in Information Schema](../../../information_schema/task_runs.md) をクエリします。

```SQL
SELECT * FROM INFORMATION_SCHEMA.task_runs;
SELECT * FROM information_schema.task_runs WHERE task_name = '<task_name>';
```

## Configure via FE configuration items

非同期 ETL タスクを以下の FE 設定項目を使用して設定できます：

| **Parameter**                | **Default value** | **Description**                                              |
| ---------------------------- | ----------------- | ------------------------------------------------------------ |
| task_ttl_second              | 86400             | Task が有効な期間。単位：秒。有効期間を超えたタスクは削除されます。ここで、Task は一度だけ実行される非定期的なタスクを示します。 |
| task_check_interval_second   | 3600              | 無効な Task を削除するための時間間隔。単位：秒。    |
| task_runs_ttl_second         | 86400             | TaskRun が有効な期間。単位：秒。有効期間を超えた TaskRun は自動的に削除されます。さらに、`FAILED` および `SUCCESS` 状態の TaskRun も自動的に削除されます。`TaskRun` は定期的なタスクの個々の実行を示します。  |
| task_runs_concurrency        | 4                 | 並行して実行できる TaskRun の最大数。  |
| task_runs_queue_length       | 500               | 実行待ちの TaskRun の最大数。デフォルト値を超えると、受信タスクは一時停止されます。 |
| task_runs_max_history_number | 10000             | 保持する TaskRun レコードの最大数。 |
| task_min_schedule_interval_s | 10                | Task 実行の最小間隔。単位：秒。 |

## Examples

例 1: `CREATE TABLE tbl1 AS SELECT * FROM src_tbl` の非同期タスクを送信し、タスク名を `etl0` と指定します：

```SQL
SUBMIT TASK etl0 AS CREATE TABLE tbl1 AS SELECT * FROM src_tbl;
```

例 2: `INSERT INTO tbl2 SELECT * FROM src_tbl` の非同期タスクを送信し、タスク名を `etl1` と指定します：

```SQL
SUBMIT TASK etl1 AS INSERT INTO tbl2 SELECT * FROM src_tbl;
```

例 3: `INSERT OVERWRITE tbl3 SELECT * FROM src_tbl` の非同期タスクを送信します：

```SQL
SUBMIT TASK AS INSERT OVERWRITE tbl3 SELECT * FROM src_tbl;
```

例 4: タスク名を指定せずに `INSERT OVERWRITE insert_wiki_edit SELECT * FROM source_wiki_edit` の非同期タスクを送信し、ヒントを使用してクエリタイムアウトを `100000` 秒に拡張します：

```SQL
SUBMIT /*+set_var(query_timeout=100000)*/ TASK AS
INSERT OVERWRITE insert_wiki_edit
SELECT * FROM source_wiki_edit;
```

例 5: INSERT OVERWRITE ステートメントの非同期タスクを作成します。タスクは1分間隔で定期的に実行されます。

```SQL
SUBMIT TASK
SCHEDULE EVERY(INTERVAL 1 MINUTE)
AS
INSERT OVERWRITE insert_wiki_edit
    SELECT dt, user_id, count(*) 
    FROM source_wiki_edit 
    GROUP BY dt, user_id;
```