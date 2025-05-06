---
displayed_sidebar: docs
---

# tasks

`tasks` は非同期タスクに関する情報を提供します。

`tasks` には以下のフィールドが提供されています:

| **Field**   | **Description**                                              |
| ----------- | ------------------------------------------------------------ |
| TASK_NAME   | タスクの名前。                                               |
| CREATE_TIME | タスクが作成された時間。                                     |
| SCHEDULE    | タスクのスケジュール。このタスクが定期的にトリガーされる場合、このフィールドには `START xxx EVERY xxx` が表示されます。 |
| DATABASE    | タスクが属するデータベース。                                 |
| DEFINITION  | タスクの SQL 定義。                                          |
| EXPIRE_TIME | タスクが期限切れになる時間。                                 |