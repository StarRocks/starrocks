---
displayed_sidebar: docs
---

# task_runs

`task_runs` は非同期タスクの実行に関する情報を提供します。

`task_runs` には以下のフィールドが提供されます。

| **Field**     | **Description**                                              |
| ------------- | ------------------------------------------------------------ |
| QUERY_ID      | クエリのID。                                                 |
| TASK_NAME     | タスクの名前。                                               |
| CREATE_TIME   | タスクが作成された時間。                                     |
| FINISH_TIME   | タスクが終了した時間。                                       |
| STATE         | タスクの状態。 有効な値は `PENDING`、`RUNNING`、`FAILED`、`SUCCESS` です。バージョン v3.1.12 から、特にマテリアライズドビューのリフレッシュタスク用に新しい状態 `MERGED` が追加されました。新しいリフレッシュタスクが提出され、古いタスクがまだ保留キューにある場合、これらのタスクはマージされ、その優先度レベルが維持されます。 |
| DATABASE      | タスクが属するデータベース。                                 |
| DEFINITION    | タスクのSQL定義。                                            |
| EXPIRE_TIME   | タスクの有効期限。                                           |
| ERROR_CODE    | タスクのエラーコード。                                       |
| ERROR_MESSAGE | タスクのエラーメッセージ。                                   |
| PROGRESS      | タスクの進捗。                                               |
| EXTRA_MESSAGE | タスクの追加メッセージ。例えば、非同期マテリアライズドビュー作成タスクにおけるパーティション情報など。 |