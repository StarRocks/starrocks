---
displayed_sidebar: docs
---

# SHOW ANALYZE JOB

## 功能

查看自定义自动采集任务的信息和状态。自定义自动采集任务用于采集 CBO 统计信息。该语句从 2.4 版本开始支持。

默认情况下，StarRocks 会周期性自动采集表的全量统计信息。默认检查更新时间为 5 分钟一次，如果发现有数据更新，会自动触发采集。如果您不希望使用自动全量采集，可以设置 FE 配置项 `enable_collect_full_statistic` 为 `false`，系统会停止自动全量采集，根据您创建的自定义任务进行定制化采集。

## 语法

```SQL
SHOW ANALYZE JOB [WHERE predicate]
```

您可以使用 WHERE 子句设定筛选条件，进行返回结果筛选。该语句会返回如下列。

| **列名**     | **说明**                                                     |
| ------------ | ------------------------------------------------------------ |
| Id           | 采集任务的 ID。                                               |
| Database     | 数据库名。                                                   |
| Table        | 表名。                                                       |
| Columns      | 列名列表。                                                   |
| Type         | 统计信息的类型。取值： FULL，SAMPLE。                        |
| Schedule     | 调度的类型。自动采集任务固定为 `SCHEDULE`。                   |
| Properties   | 自定义参数信息。                                             |
| Status       | 任务状态，包括 PENDING（等待）、RUNNING（正在执行）、SUCCESS（执行成功）和 FAILED（执行失败）。 |
| LastWorkTime | 最近一次采集时间。                                           |
| Reason       | 任务失败的原因。如果执行成功则为 `NULL`。                    |

## 示例

```SQL
-- 查看集群全部自定义采集任务。
SHOW ANALYZE JOB

-- 查看数据库 `test` 下的自定义采集任务。
SHOW ANALYZE JOB where `database` = 'test';
```

## 相关文档

[CREATE ANALYZE](CREATE_ANALYZE.md)：创建自定义自动采集任务。

[DROP ANALYZE](DROP_ANALYZE.md)：删除自动采集任务。

[KILL ANALYZE](KILL_ANALYZE.md)：取消正在运行中（Running）的统计信息收集任务。

想了解更多 CBO 统计信息采集的内容，参见 [CBO 统计信息](../../../using_starrocks/Cost_based_optimizer.md)。
