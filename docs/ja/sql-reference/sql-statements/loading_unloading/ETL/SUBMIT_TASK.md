---
displayed_sidebar: docs
---

# SUBMIT TASK

## 説明

ETL ステートメントを非同期タスクとして送信します。

このステートメントを使用して以下を行うことができます:

- バックグラウンドで長時間実行されるタスクを実行する (v2.5 以降でサポート)
- 定期的にタスクをスケジュールする (v3.3 以降でサポート)

サポートされているステートメントには以下が含まれます:

- [CREATE TABLE AS SELECT](../../table_bucket_part_index/CREATE_TABLE_AS_SELECT.md) (v3.0 以降)
- [INSERT](../INSERT.md) (v3.0 以降)
- [CACHE SELECT](../../../../data_source/data_cache_warmup.md) (v3.3 以降)

タスクの一覧は `INFORMATION_SCHEMA.tasks` をクエリすることで確認でき、タスクの実行履歴は `INFORMATION_SCHEMA.task_runs` をクエリすることで確認できます。詳細については、[使用上の注意](#usage-notes)を参照してください。

非同期タスクを削除するには [DROP TASK](DROP_TASK.md) を使用できます。

## 構文

```SQL
SUBMIT TASK <task_name> 
[SCHEDULE [START(<schedule_start>)] EVERY(INTERVAL <schedule_interval>) ]
[PROPERTIES(<"key" = "value"[, ...]>)]
AS <etl_statement>
```

## パラメータ

| **パラメータ**      | **必須** | **説明**                                                                                     |
| -------------      | ------------ | ---------------------------------------------------------------------------------------------------- |
| task_name          | はい     | タスクの名前です。                                                                               |
| schedule_start     | いいえ      | スケジュールされたタスクの開始時間です。                                                                 |
| schedule_interval  | いいえ      | スケジュールされたタスクが実行される間隔で、最小間隔は10秒です。          |
| etl_statement      | はい     | 非同期タスクとして送信したい ETL ステートメントです。StarRocks は現在、[CREATE TABLE AS SELECT](../../table_bucket_part_index/CREATE_TABLE_AS_SELECT.md) と [INSERT](../../loading_unloading/INSERT.md) の非同期タスクの送信をサポートしています。 |

## 使用上の注意

このステートメントは、ETL ステートメントを実行するタスクを保存するテンプレートである Task を作成します。Task の情報は、メタデータビュー [`tasks` in Information Schema](../../../information_schema/tasks.md) をクエリすることで確認できます。

```SQL
SELECT * FROM INFORMATION_SCHEMA.tasks;
SELECT * FROM information_schema.tasks WHERE task_name = '<task_name>';
```

Task を実行すると、それに応じて TaskRun が生成されます。TaskRun は、ETL ステートメントを実行するタスクを示します。TaskRun には以下の状態があります:

- `PENDING`: タスクは実行待ちです。
- `RUNNING`: タスクは実行中です。
- `FAILED`: タスクは失敗しました。
- `SUCCESS`: タスクは正常に実行されました。

TaskRun の状態は、メタデータビュー [`task_runs` in Information Schema](../../../information_schema/task_runs.md) をクエリすることで確認できます。

```SQL
SELECT * FROM INFORMATION_SCHEMA.task_runs;
SELECT * FROM information_schema.task_runs WHERE task_name = '<task_name>';
```

## FE 設定項目による設定

非同期 ETL タスクは、以下の FE 設定項目を使用して設定できます:

| **パラメータ**                | **デフォルト値** | **説明**                                              |
| ---------------------------- | ----------------- | ------------------------------------------------------------ |
| task_ttl_second              | 86400             | Task が有効である期間です。単位: 秒。有効期間を超えたタスクは削除されます。ここで、Task は一度だけ実行される非定期タスクを示します。 |
| task_check_interval_second   | 3600              | 無効な Task を削除する時間間隔です。単位: 秒。    |
| task_runs_ttl_second         | 86400             | TaskRun が有効である期間です。単位: 秒。有効期間を超えた TaskRun は自動的に削除されます。さらに、`FAILED` および `SUCCESS` 状態の TaskRun も自動的に削除されます。`TaskRun` は定期タスクの個々の実行を示します。  |
| task_runs_concurrency        | 4                 | 並行して実行できる TaskRun の最大数です。  |
| task_runs_queue_length       | 500               | 実行待ちの TaskRun の最大数です。デフォルト値を超えると、受信タスクは一時停止されます。 |
| task_runs_max_history_number | 10000             | 保持する TaskRun レコードの最大数です。 |
| task_min_schedule_interval_s | 10                | Task 実行の最小間隔です。単位: 秒。 |

## 例

例 1: `CREATE TABLE tbl1 AS SELECT * FROM src_tbl` の非同期タスクを送信し、タスク名を `etl0` と指定します:

```SQL
SUBMIT TASK etl0 AS CREATE TABLE tbl1 AS SELECT * FROM src_tbl;
```

例 2: `INSERT INTO tbl2 SELECT * FROM src_tbl` の非同期タスクを送信し、タスク名を `etl1` と指定します:

```SQL
SUBMIT TASK etl1 AS INSERT INTO tbl2 SELECT * FROM src_tbl;
```

例 3: `INSERT OVERWRITE tbl3 SELECT * FROM src_tbl` の非同期タスクを送信します:

```SQL
SUBMIT TASK AS INSERT OVERWRITE tbl3 SELECT * FROM src_tbl;
```

例 4: タスク名を指定せずに `INSERT OVERWRITE insert_wiki_edit SELECT * FROM source_wiki_edit` の非同期タスクを送信し、ヒントを使用してクエリタイムアウトを `100000` 秒に延長します:

```SQL
SUBMIT /*+set_var(query_timeout=100000)*/ TASK AS
INSERT OVERWRITE insert_wiki_edit
SELECT * FROM source_wiki_edit;
```

例 5: INSERT OVERWRITE ステートメントの非同期タスクを作成します。このタスクは 1 分間隔で定期的に実行されます。

```SQL
SUBMIT TASK
SCHEDULE EVERY(INTERVAL 1 MINUTE)
AS
INSERT OVERWRITE insert_wiki_edit
    SELECT dt, user_id, count(*) 
    FROM source_wiki_edit 
    GROUP BY dt, user_id;
```