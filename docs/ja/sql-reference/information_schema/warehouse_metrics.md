---
displayed_sidebar: docs
---

# warehouse_metrics

`warehouse_metrics` は各ウェアハウスのメトリックに関する情報を提供します。

`warehouse_metrics` には以下のフィールドが提供されています:

| **フィールド**                | **説明**                                         |
| ------------------------- | ------------------------------------------------ |
| WAREHOUSE_ID              | ウェアハウスの ID。                              |
| WAREHOUSE_NAME            | ウェアハウスの名前。                             |
| QUEUE_PENDING_LENGTH      | キュー内の保留中のクエリ数。                     |
| QUEUE_RUNNING_LENGTH      | キュー内の実行中のクエリ数。                     |
| MAX_PENDING_LENGTH        | キューで許可される保留中のクエリの最大数。       |
| MAX_PENDING_TIME_SECOND   | キュー内のクエリの最大保留時間（秒）。           |
| EARLIEST_QUERY_WAIT_TIME  | 最も早いクエリの待機時間。                       |
| MAX_REQUIRED_SLOTS        | 最大必要スロット数。                             |
| SUM_REQUIRED_SLOTS        | 必要スロットの合計。                             |
| REMAIN_SLOTS              | 残りのスロット。                                 |
| MAX_SLOTS                 | 最大スロット数。                                 |
| EXTRA_MESSAGE             | 追加メッセージ。                                 |
