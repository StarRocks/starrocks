---
displayed_sidebar: "Chinese"
---

# SHOW ANALYZE STATUS

## 功能

查看当前所有采集任务的状态。**该语句不支持查看自定义采集任务的状态，如要查看，请使用 SHOW ANALYZE JOB;。**该语句从 2.4 版本开始支持。

## 语法

```SQL
SHOW ANALYZE STATUS [LIKE | WHERE predicate]
```

您可以使用 `Like 或 Where` 来筛选需要返回的信息。

目前 `SHOW ANALYZE STATUS` 会返回如下列。

| **列名**   | **说明**                                                     |
| ---------- | ------------------------------------------------------------ |
| Id         | 采集任务的 ID。                                               |
| Database   | 数据库名。                                                   |
| Table      | 表名。                                                       |
| Columns    | 列名列表。                                                   |
| Type       | 统计信息的类型，包括 FULL，SAMPLE，HISTOGRAM。               |
| Schedule   | 调度的类型。`ONCE` 表示手动，`SCHEDULE` 表示自动。             |
| Status     | 任务状态，包括 RUNNING（正在执行）、SUCCESS（执行成功）和 FAILED（执行失败）。 |
| StartTime  | 任务开始执行的时间。                                         |
| EndTime    | 任务结束执行的时间。                                         |
| Properties | 自定义参数信息。                                             |
| Reason     | 任务失败的原因。如果执行成功则为 NULL。                      |

## 相关文档

[ANALYZE TABLE](../data-definition/ANALYZE_TABLE.md)：创建手动采集任务。

[KILL ANALYZE](../data-definition/KILL_ANALYZE.md)：取消正在运行中（Running）的统计信息收集任务。

想了解更多 CBO 统计信息采集的内容，参见 [CBO 统计信息](../../../using_starrocks/Cost_based_optimizer.md)。
