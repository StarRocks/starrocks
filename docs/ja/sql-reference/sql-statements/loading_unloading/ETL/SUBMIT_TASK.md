---
displayed_sidebar: docs
---

# SUBMIT TASK

## 説明

ETL ステートメントを非同期タスクとして送信します。この機能は StarRocks v2.5 からサポートされています。

StarRocks v3.0 は [CREATE TABLE AS SELECT](../../table_bucket_part_index/CREATE_TABLE_AS_SELECT.md) および [INSERT](../INSERT.md) の非同期タスクの送信をサポートしています。

非同期タスクを削除するには、[DROP TASK](DROP_TASK.md) を使用します。

## 構文

```SQL
SUBMIT TASK [task_name] AS <etl_statement>
```

## パラメータ

| **パラメータ** | **説明**                                              |
| ------------- | ------------------------------------------------------------ |
| task_name     | タスク名。                                               |
| etl_statement | 非同期タスクとして送信したい ETL ステートメント。StarRocks は現在、[CREATE TABLE AS SELECT](../../table_bucket_part_index/CREATE_TABLE_AS_SELECT.md) および [INSERT](../INSERT.md) の非同期タスクの送信をサポートしています。 |

## 使用上の注意

このステートメントは、ETL ステートメントを実行するタスクを保存するテンプレートである Task を作成します。Task の情報は、メタデータビュー [`Information Schema` の `tasks`](../../../information_schema/tasks.md) をクエリすることで確認できます。

```SQL
SELECT * FROM INFORMATION_SCHEMA.tasks;
SELECT * FROM information_schema.tasks WHERE task_name = '<task_name>';
```

Task を実行すると、それに応じて TaskRun が生成されます。TaskRun は ETL ステートメントを実行するタスクを示します。TaskRun には次の状態があります：

- `PENDING`: タスクは実行待ちです。
- `RUNNING`: タスクは実行中です。
- `FAILED`: タスクは失敗しました。
- `SUCCESS`: タスクは正常に実行されました。

TaskRun の状態は、メタデータビュー [`Information Schema` の `task_runs`](../../../information_schema/task_runs.md) をクエリすることで確認できます。

```SQL
SELECT * FROM INFORMATION_SCHEMA.task_runs;
SELECT * FROM information_schema.task_runs WHERE task_name = '<task_name>';
```

## FE 設定項目による設定

次の FE 設定項目を使用して非同期 ETL タスクを設定できます：

| **パラメータ**                | **デフォルト値** | **説明**                                              |
| ---------------------------- | ----------------- | ------------------------------------------------------------ |
| task_ttl_second              | 86400             | Task が有効な期間。単位：秒。有効期間を超えたタスクは削除されます。ここで、Task は一度だけ実行される非周期的なタスクを示します。 |
| task_check_interval_second   | 3600              | 無効なタスクを削除する時間間隔。単位：秒。    |
| task_runs_ttl_second         | 86400             | TaskRun が有効な期間。単位：秒。有効期間を超えた TaskRun は自動的に削除されます。さらに、`FAILED` および `SUCCESS` 状態の TaskRun も自動的に削除されます。`TaskRun` は周期的なタスクの個々の実行を示します。  |
| task_runs_concurrency        | 4                 | 並行して実行できる TaskRun の最大数。  |
| task_runs_queue_length       | 500               | 実行待ちの TaskRun の最大数。この数がデフォルト値を超えると、受信タスクは一時停止されます。 |
| task_runs_max_history_number | 10000      | 保持する TaskRun レコードの最大数。 |

## 例

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

例 4: `INSERT OVERWRITE insert_wiki_edit SELECT * FROM source_wiki_edit` の非同期タスクをタスク名を指定せずに送信し、ヒントを使用してクエリのタイムアウトを `100000` 秒に延長します：

```SQL
SUBMIT /*+set_var(query_timeout=100000)*/ TASK AS
INSERT OVERWRITE insert_wiki_edit
SELECT * FROM source_wiki_edit;
```