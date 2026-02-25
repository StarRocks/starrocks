---
displayed_sidebar: docs
---

# be_compactions

`be_compactions` は、Compaction タスクに関する統計情報を提供します。

`be_compactions` には次のフィールドが含まれています:

| **Field**                         | **Description**                                      |
| --------------------------------- | ---------------------------------------------------- |
| BE_ID                             | BE の ID。                                           |
| CANDIDATES_NUM                    | Compaction タスクの候補の数。                        |
| BASE_COMPACTION_CONCURRENCY       | 実行中のベース Compaction タスクの数。               |
| CUMULATIVE_COMPACTION_CONCURRENCY | 実行中の累積 Compaction タスクの数。                 |
| LATEST_COMPACTION_SCORE           | 最後の Compaction タスクのスコア。                   |
| CANDIDATE_MAX_SCORE               | タスク候補の最大 Compaction スコア。                 |
| MANUAL_COMPACTION_CONCURRENCY     | 実行中の手動 Compaction タスクの数。                 |
| MANUAL_COMPACTION_CANDIDATES_NUM  | 手動 Compaction タスクの候補の数。                   |