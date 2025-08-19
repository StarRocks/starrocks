---
displayed_sidebar: docs
---

# fe_tablet_schedules

`fe_tablet_schedules` 提供有关 FE 节点上 Tablet 调度任务的信息。

`fe_tablet_schedules` 提供以下字段：

| **字段**        | **描述**                                         |
| --------------- | ------------------------------------------------ |
| TABLE_ID        | Tablet 所属表的 ID。                             |
| PARTITION_ID    | Tablet 所属分区的 ID。                           |
| TABLET_ID       | Tablet 的 ID。                                   |
| TYPE            | 调度任务的类型（例如，`CLONE`、`REPAIR`）。      |
| PRIORITY        | 调度任务的优先级。                               |
| STATE           | 调度任务的状态（例如，`PENDING`、`RUNNING`、`FINISHED`）。 |
| TABLET_STATUS   | Tablet 的状态。                                  |
| CREATE_TIME     | 调度任务的创建时间。                             |
| SCHEDULE_TIME   | 调度任务被调度的时间。                           |
| FINISH_TIME     | 调度任务的完成时间。                             |
| CLONE_SRC       | 克隆任务的源 BE ID。                             |
| CLONE_DEST      | 克隆任务的目标 BE ID。                           |
| CLONE_BYTES     | 克隆任务的字节数。                               |
| CLONE_DURATION  | 克隆任务的持续时间（秒）。                       |
| MSG             | 与调度任务相关的消息。                           |
