---
displayed_sidebar: "English"
---

# be_compactions

`be_compactions` provides statistical information on compaction tasks.

The following fields are provided in `be_compactions`:

| **Field**                         | **Description**                                         |
| --------------------------------- | ------------------------------------------------------- |
| BE_ID                             | ID of the BE.                                           |
| CANDIDATES_NUM                    | Number of candidates for compaction tasks.              |
| BASE_COMPACTION_CONCURRENCY       | Number of base compaction tasks that are running.       |
| CUMULATIVE_COMPACTION_CONCURRENCY | Number of cumulative compaction tasks that are running. |
| LATEST_COMPACTION_SCORE           | Compaction score of the last compaction task.           |
| CANDIDATE_MAX_SCORE               | The maximum compaction score of the task candidate.     |
| MANUAL_COMPACTION_CONCURRENCY     | Number of manual compaction tasks that are running.     |
| MANUAL_COMPACTION_CANDIDATES_NUM  | Number of candidates for manual compaction tasks.       |
