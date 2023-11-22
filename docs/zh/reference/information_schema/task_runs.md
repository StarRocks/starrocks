---
displayed_sidebar: "Chinese"
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
| STATE         | 任务的状态，包括 `PENDING`（待处理）、`RUNNING`（运行中）、`FAILED`（失败）和 `SUCCESS`（成功）。 |
| DATABASE      | 任务所属的数据库。                                           |
| DEFINITION    | 任务的 SQL 定义。                                            |
| EXPIRE_TIME   | 任务过期的时间。                                             |
| ERROR_CODE    | 任务的错误代码。                                             |
| ERROR_MESSAGE | 任务的错误消息。                                             |
| PROGRESS      | 任务的进度。                                                 |
| EXTRA_MESSAGE | 任务的额外消息，例如在异步物化视图创建任务中的分区信息。     |
