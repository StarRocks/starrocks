---
displayed_sidebar: docs
---

# be_compactions

`be_compactions` 提供有关 Compaction 任务的统计信息。

`be_compactions` 提供以下字段：

| **字段**                          | **描述**                                      |
| --------------------------------- | --------------------------------------------- |
| BE_ID                             | BE 的 ID。                                    |
| CANDIDATES_NUM                    | 当前等待执行的 Compaction 任务数。            |
| BASE_COMPACTION_CONCURRENCY       | 当前正在执行的 Base Compaction 任务数。       |
| CUMULATIVE_COMPACTION_CONCURRENCY | 当前正在执行的 Cumulative Compaction 任务数。 |
| LATEST_COMPACTION_SCORE           | 上次 Compaction 任务的 Compaction Score。     |
| CANDIDATE_MAX_SCORE               | 等待执行的任务的最大 Compaction Score。       |
| MANUAL_COMPACTION_CONCURRENCY     | 当前正在执行的手动 Compaction 任务数。        |
| MANUAL_COMPACTION_CANDIDATES_NUM  | 当前等待执行的手动 Compaction 任务数。        |
