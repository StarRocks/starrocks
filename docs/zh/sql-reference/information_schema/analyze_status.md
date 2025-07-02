---
displayed_sidebar: docs
---

# analyze_status

`analyze_status` 提供有关统计信息收集任务状态的信息。

`analyze_status` 提供以下字段：

| **字段**     | **描述**                                         |
| ------------ | ------------------------------------------------ |
| Id           | 统计信息收集任务的 ID。                          |
| Catalog      | 表所属的 Catalog。                               |
| Database     | 表所属的数据库。                                 |
| Table        | 正在收集统计信息的表的名称。                     |
| Columns      | 正在收集统计信息的列。                           |
| Type         | 统计信息收集任务的类型。可选值：`FULL`、`SAMPLE`。 |
| Schedule     | 统计信息收集任务的调度类型。可选值：`ONCE`、`AUTOMATIC`。 |
| Status       | 统计信息收集任务的状态。可选值：`PENDING`、`RUNNING`、`FINISH`、`FAILED`。 |
| StartTime    | 统计信息收集任务的开始时间。                     |
| EndTime      | 统计信息收集任务的结束时间。                     |
| Properties   | 统计信息收集任务的属性。                         |
| Reason       | 统计信息收集任务状态的原因。                     |
