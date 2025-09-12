---
displayed_sidebar: docs
---

# fe_tablet_schedules

`fe_tablet_schedules` は FE ノード上のタブレットスケジューリングタスクに関する情報を提供します。

`fe_tablet_schedules` には以下のフィールドが提供されています:

| **フィールド**        | **説明**                                      |
| --------------- | ------------------------------------------------ |
| TABLET_ID       | タブレットの ID。                                  |
| TABLE_ID        | タブレットが属するテーブルの ID。                     |
| PARTITION_ID    | タブレットが属するパーティションの ID。                |
| TYPE            | タスクの種類。有効な値：`REPAIR` と `BALANCE`。       |
| STATE           | タスクの状態。                                      |
| SCHEDULE_REASON | タスクスケジューリングの理由。                        |
| MEDIUM          | タブレットが配置されているストレージメディア。           |
| PRIORITY        | タスクの現在の優先度。                               |
| ORIG_PRIORITY   | タスクの元の優先度。                                 |
| LAST_PRIORITY_ADJUST_TIME | タスクの優先度が最後に調整された時刻。        |
| VISIBLE_VERSION | タブレットの表示用データバージョン。                    |
| COMMITTED_VERSION | タブレットのコミット済みデータバージョン。             |
| SRC_BE_ID       | ソースレプリカが存在する BE の ID。                   |
| SRC_PATH        | ソースレプリカが存在するパス。                        |
| DEST_BE_ID      | ターゲットレプリカが存在する BE の ID。               |
| DEST_PATH       | ターゲットレプリカが存在するパス。                    |
| CREATE_TIME     | タスクが作成された時刻。                            |
| SCHEDULE_TIME   | タスクが実行スケジューリングを開始した時刻。           |
| FINISH_TIME     | タスクが完了した時刻。                              |
| CLONE_BYTES     | クローンされたファイルのサイズ（バイト単位）。           |
| CLONE_DURATION  | クローン処理に要した時間（秒単位）。                   |
| CLONE_RATE      | クローン速度（MB/秒）。                             |
| FAILED_SCHEDULE_COUNT | タスクのスケジュール失敗回数。                 |
| FAILED_RUNNING_COUNT | タスクの実行失敗回数。                        |
| MSG             | タスクのスケジューリングと実行に関する情報。           |
