---
displayed_sidebar: docs
---

# warehouse_queries

`warehouse_queries` は各ウェアハウスで実行されているクエリに関する情報を提供します。

`warehouse_queries` には以下のフィールドが提供されています:

| **フィールド**            | **説明**                                         |
| --------------------- | ------------------------------------------------ |
| WAREHOUSE_ID          | ウェアハウスの ID。                              |
| WAREHOUSE_NAME        | ウェアハウスの名前。                             |
| QUERY_ID              | クエリの ID。                                    |
| STATE                 | クエリの状態（例: `PENDING`、`RUNNING`、`FINISHED`）。 |
| EST_COSTS_SLOTS       | クエリの推定コストスロット。                     |
| ALLOCATE_SLOTS        | クエリに割り当てられたスロット。                 |
| QUEUED_WAIT_SECONDS   | クエリがキューで待機した時間（秒）。             |
| QUERY                 | SQL クエリ文字列。                               |
| QUERY_START_TIME      | クエリの開始時刻。                               |
| QUERY_END_TIME        | クエリの終了時刻。                               |
| QUERY_DURATION        | クエリの期間。                                   |
| EXTRA_MESSAGE         | 追加メッセージ。                                 |