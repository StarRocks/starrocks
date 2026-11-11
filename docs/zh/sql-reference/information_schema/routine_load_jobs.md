---
displayed_sidebar: docs
description: "routine_load_jobs 提供例行导入作业的信息。"
---

# routine_load_jobs

`routine_load_jobs` 提供有关例行导入作业的信息。

对例行导入作业的目标表拥有至少一项权限，或者拥有 OPERATE 权限的用户，可以在 `routine_load_jobs` 中看到该作业。如果没有目标表的 SELECT 权限，`JOB_PROPERTIES`、`DATA_SOURCE_PROPERTIES`、`CUSTOM_PROPERTIES`、`ERROR_LOG_URLS`、`TRACKING_SQL` 和 `OTHER_MSG` 为 NULL，`REASONS_OF_STATE_CHANGED` 只保留错误码。`root` 用户可以看到所有例行导入作业的完整信息。目标表已被删除的例行导入作业，只有 `root` 用户能看到完整信息，拥有 OPERATE 权限的用户看到的是隐藏上述字段后的信息。

`routine_load_jobs` 提供以下字段：

| **字段**                 | **描述**                                         |
| ------------------------ | ------------------------------------------------ |
| ID                       | 例行导入作业的 ID。                              |
| NAME                     | 例行导入作业的名称。                             |
| CREATE_TIME              | 例行导入作业的创建时间。                         |
| PAUSE_TIME               | 例行导入作业的暂停时间。                         |
| END_TIME                 | 例行导入作业的结束时间。                         |
| DB_NAME                  | 例行导入作业所属数据库的名称。                   |
| TABLE_NAME               | 数据导入到的表的名称。                           |
| STATE                    | 例行导入作业的状态。                             |
| DATA_SOURCE_TYPE         | 数据源的类型。                                   |
| CURRENT_TASK_NUM         | 当前任务数量。                                   |
| JOB_PROPERTIES           | 例行导入作业的属性。                             |
| DATA_SOURCE_PROPERTIES   | 数据源的属性。                                   |
| CUSTOM_PROPERTIES        | 例行导入作业的自定义属性。                       |
| STATISTICS               | 例行导入作业的统计信息。                         |
| PROGRESS                 | 例行导入作业的进度。                             |
| REASONS_OF_STATE_CHANGED | 状态变更的原因。                                 |
| ERROR_LOG_URLS           | 错误日志的 URL。                                 |
| TRACKING_SQL             | 用于跟踪的 SQL 语句。                            |
| OTHER_MSG                | 其他消息。                                       |
| LATEST_SOURCE_POSITION   | 最新的源位置，JSON 格式。                        |
| OFFSET_LAG               | 偏移量滞后，JSON 格式。                          |
| TIMESTAMP_PROGRESS       | 时间戳进度，JSON 格式。                          |
