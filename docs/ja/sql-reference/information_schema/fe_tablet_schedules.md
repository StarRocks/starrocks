---
displayed_sidebar: docs
---

# fe_tablet_schedules

`fe_tablet_schedules` は FE ノード上のタブレットスケジューリングタスクに関する情報を提供します。

`fe_tablet_schedules` には以下のフィールドが提供されています:

| **フィールド**        | **説明**                                         |
| --------------- | ------------------------------------------------ |
| TABLE_ID        | タブレットが属するテーブルの ID。                |
| PARTITION_ID    | タブレットが属するパーティションの ID。          |
| TABLET_ID       | タブレットの ID。                                |
| TYPE            | スケジューリングタスクのタイプ（例: `CLONE`、`REPAIR`）。 |
| PRIORITY        | スケジューリングタスクの優先度。                 |
| STATE           | スケジューリングタスクの状態（例: `PENDING`、`RUNNING`、`FINISHED`）。 |
| TABLET_STATUS   | タブレットのステータス。                         |
| CREATE_TIME     | スケジューリングタスクの作成時刻。               |
| SCHEDULE_TIME   | スケジューリングタスクがスケジュールされた時刻。 |
| FINISH_TIME     | スケジューリングタスクの終了時刻。               |
| CLONE_SRC       | クローンタスクのソース BE ID。                   |
| CLONE_DEST      | クローンタスクの宛先 BE ID。                     |
| CLONE_BYTES     | クローンタスクでクローンされたバイト数。         |
| CLONE_DURATION  | クローンタスクの期間（秒）。                     |
| MSG             | スケジューリングタスクに関連するメッセージ。     |
