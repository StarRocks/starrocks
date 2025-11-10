---
displayed_sidebar: docs
---

# task_runs

`task_runs` は非同期タスクの実行に関する情報を提供します。

`task_runs` には以下のフィールドが含まれています:

| **Field**     | **Description**                                              |
| ------------- | ------------------------------------------------------------ |
| QUERY_ID      | クエリの ID。                                                |
| TASK_NAME     | タスクの名前。                                               |
| CREATE_TIME   | タスクが作成された時間。                                     |
| FINISH_TIME   | タスクが終了した時間。                                       |
| STATE         | タスクの状態。 有効な値は `PENDING`、`RUNNING`、`FAILED`、`SUCCESS` です。バージョン 3.1.12 から、特にマテリアライズドビューのリフレッシュタスク用に新しい状態 `MERGED` が追加されました。新しいリフレッシュタスクが提出され、古いタスクがまだ保留キューにある場合、これらのタスクはマージされ、その優先度レベルが維持されます。 |
| CATALOG       | タスクが属するカタログ。                                     |
| DATABASE      | タスクが属するデータベース。                                 |
| DEFINITION    | タスクの SQL 定義。                                          |
| EXPIRE_TIME   | タスクが期限切れになる時間。                                 |
| ERROR_CODE    | タスクのエラーコード。                                       |
| ERROR_MESSAGE | タスクのエラーメッセージ。                                   |
| PROGRESS      | タスクの進捗。                                               |
| EXTRA_MESSAGE | タスクの追加メッセージ。例えば、非同期マテリアライズドビュー作成タスクにおけるパーティション情報など。 |
| PROPERTIES    | タスクのプロパティ。                                         |
| JOB_ID        | タスクのジョブ ID。                                          |
| PROCESS_TIME  | タスクの処理時間。                                           |

タスク実行記録は、[SUBMIT TASK](../sql-statements/loading_unloading/ETL/SUBMIT_TASK.md) または [CREATE MATERIALIZED VIEW](../sql-statements/materialized_view/CREATE_MATERIALIZED_VIEW.md) によって生成されます。

:::note
マテリアライズドビューリフレッシュ操作は複数のタスク実行を生成することがあり、各タスク実行は `partition_refresh_number` 設定に従って分割されたリフレッシュサブタスクを表します。
:::

## `EXTRA_MESSAGE`

マテリアライズドビューリフレッシュタスク実行の場合、`EXTRA_MESSAGE` フィールドにはマテリアライズドビュータスク実行の詳細メッセージが含まれます。詳細は [マテリアライズドビュータスク実行の理解](../../using_starrocks/async_mv/materialized_view_task_run_details.md) で確認できます。