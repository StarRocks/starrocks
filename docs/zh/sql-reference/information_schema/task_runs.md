---
displayed_sidebar: docs
---

# task_runs

`task_runs` 提供有关异步任务执行的信息。

`task_runs` 提供以下字段：

| 字段          | 描述                                                         |
| ------------- | ------------------------------------------------------------ |
| QUERY_ID      | 查询的 ID。                                                  |
| TASK_NAME     | 任务的名称。                                                 |
| CREATE_TIME   | 任务创建的时间。                                             |
| FINISH_TIME   | 任务完成的时间。                                             |
| STATE         | 任务的状态，包括 `PENDING`（待处理）、`RUNNING`（运行中）、`FAILED`（失败）和 `SUCCESS`（成功）。从 v3.1.12 开始，新增了一个专门用于物化视图刷新任务的状态 `MERGED`。当提交新的刷新任务时，如果旧任务仍在 PENDING 队列中，这些任务将被合并，并保持其优先级属性。 |
| CATALOG       | 任务所属的 Catalog。                                         |
| DATABASE      | 任务所属的数据库。                                           |
| DEFINITION    | 任务的 SQL 定义。                                            |
| EXPIRE_TIME   | 任务过期的时间。                                             |
| ERROR_CODE    | 任务的错误代码。                                             |
| ERROR_MESSAGE | 任务的错误消息。                                             |
| PROGRESS      | 任务的进度。                                                 |
| EXTRA_MESSAGE | 任务的额外消息，例如在异步物化视图创建任务中的分区信息。     |
| PROPERTIES    | 任务的属性。                                                 |
| JOB_ID        | 任务的作业 ID。                                              |
| PROCESS_TIME  | 任务的处理时间。                                             |

任务运行记录由 [SUBMIT TASK](../sql-statements/loading_unloading/ETL/SUBMIT_TASK.md) 或 [CREATE MATRIALIZED VIEW](../sql-statements/materialized_view/CREATE_MATERIALIZED_VIEW.md) 生成。

注意：
- 一个 `MATERIALIZED VIEW REFRESH` 可能会生成多个任务运行记录，每个任务运行代表一个根据 `partition_refresh_number` 配置拆分的刷新子任务。

## EXTRA_MESSAGE
对于 `MATERIALIZED VIEW REFRESH` 任务运行，`EXTRA_MESSAGE` 字段将包含物化视图任务运行的详细消息，您可以在 [materialized_view_task_run_details](./materialized_view_task_run_details.md) 中找到更多详细信息。
