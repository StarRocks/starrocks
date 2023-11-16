---
displayed_sidebar: "Chinese"
---

# DROP ANALYZE

## 功能

删除自定义自动采集任务。自定义自动采集任务用于采集 CBO 统计信息。

默认情况下，StarRocks 会周期性自动采集表的全量统计信息。默认检查更新时间为 5 分钟一次，如果发现有数据更新，会自动触发采集。如果您不希望使用自动全量采集，可以设置 FE 配置项 `enable_collect_full_statistic` 为 `false`，系统会停止自动全量采集，根据您创建的自定义任务进行定制化采集。

## 语法

```SQL
DROP ANALYZE <ID>
```

可以通过 SHOW ANALYZE JOB 查看任务的 ID。

## 示例

```SQL
DROP ANALYZE 266030;
```

## 相关文档

[CREATE ANALYZE](../data-definition/CREATE_ANALYZE.md)：创建自定义自动采集任务。

[SHOW ANALYZE JOB](../data-definition/SHOW_ANALYZE_JOB.md)：查看自定义自动采集任务。

[KILL ANALYZE](../data-definition/KILL_ANALYZE.md)：取消正在运行中（Running）的统计信息收集任务。

想了解更多 CBO 统计信息采集的内容，参见[CBO 统计信息](../../../using_starrocks/Cost_based_optimizer.md)。
