# KILL ANALYZE

## 功能

取消正在运行中（Running）的统计信息收集任务，包括手动采集任务和自定义自动采集任务。该语句从 2.4 版本开始支持。

## 语法

```SQL
KILL ANALYZE <ID>
```

手动采集任务的任务 ID 可以在 SHOW ANALYZE STATUS 中查看。自定义自动采集任务的任务 ID 可以在 SHOW ANALYZE JOB 中查看。

## 相关文档

[SHOW ANALYZE STATUS](../data-definition/SHOW_ANALYZE_STATUS.md)

[SHOW ANALYZE JOB](../data-definition/SHOW_ANALYZE_JOB.md)
