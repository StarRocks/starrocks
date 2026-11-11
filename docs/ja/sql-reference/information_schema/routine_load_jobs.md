---
displayed_sidebar: docs
description: "routine_load_jobsはRoutine Loadジョブに関する情報を提供します。"
---

# routine_load_jobs

`routine_load_jobs` はルーチンロードジョブに関する情報を提供します。

`routine_load_jobs` は、ロード先テーブルに対して少なくとも 1 つの権限を持つユーザー、または OPERATE 権限を持つユーザーに、ルーチンロードジョブを表示します。ロード先テーブルに対する SELECT 権限がない場合、`JOB_PROPERTIES`、`DATA_SOURCE_PROPERTIES`、`CUSTOM_PROPERTIES`、`ERROR_LOG_URLS`、`TRACKING_SQL`、`OTHER_MSG` は NULL になり、`REASONS_OF_STATE_CHANGED` はエラーコードのみを保持します。`root` ユーザーはすべてのルーチンロードジョブの完全な情報を参照できます。ロード先テーブルが削除されたルーチンロードジョブは、`root` ユーザーにのみ完全な情報が表示され、OPERATE 権限を持つユーザーには上記のフィールドを除いた情報が表示されます。

`routine_load_jobs` には以下のフィールドが提供されています:

| **フィールド**                 | **説明**                                         |
| ------------------------ | ------------------------------------------------ |
| ID                       | ルーチンロードジョブの ID。                      |
| NAME                     | ルーチンロードジョブの名前。                     |
| CREATE_TIME              | ルーチンロードジョブの作成時刻。                 |
| PAUSE_TIME               | ルーチンロードジョブの一時停止時刻。             |
| END_TIME                 | ルーチンロードジョブの終了時刻。                 |
| DB_NAME                  | ルーチンロードジョブが属するデータベースの名前。 |
| TABLE_NAME               | データがロードされるテーブルの名前。             |
| STATE                    | ルーチンロードジョブの状態。                     |
| DATA_SOURCE_TYPE         | データソースのタイプ。                           |
| CURRENT_TASK_NUM         | 現在のタスク数。                                 |
| JOB_PROPERTIES           | ルーチンロードジョブのプロパティ。               |
| DATA_SOURCE_PROPERTIES   | データソースのプロパティ。                       |
| CUSTOM_PROPERTIES        | ルーチンロードジョブのカスタムプロパティ。       |
| STATISTICS               | ルーチンロードジョブの統計情報。                 |
| PROGRESS                 | ルーチンロードジョブの進捗。                     |
| REASONS_OF_STATE_CHANGED | 状態変更の理由。                                 |
| ERROR_LOG_URLS           | エラーログの URL。                               |
| TRACKING_SQL             | 追跡用の SQL ステートメント。                    |
| OTHER_MSG                | その他のメッセージ。                             |
| LATEST_SOURCE_POSITION   | 最新のソース位置（JSON 形式）。                  |
| OFFSET_LAG               | オフセットラグ（JSON 形式）。                    |
| TIMESTAMP_PROGRESS       | タイムスタンプの進捗（JSON 形式）。              |
