---
displayed_sidebar: docs
---

# inspect_task_runs

`inspect_task_runs()`

此函数返回 TaskManager 的所有状态。

## 参数

无。

## 返回值

返回包含 TaskManager 状态的 JSON 格式的 VARCHAR 字符串。

## 示例

示例1: 检查当前TaskManager的全局待处理/运行中任务运行信息:
```
mysql> select inspect_task_runs();
+-----------------------------------------------------------------------------------------------------------------------------+
| inspect_task_runs()                                                                                                         |
+-----------------------------------------------------------------------------------------------------------------------------+
| {"pendingTaskRunQueue":{"pendingTaskRunMap":{},"pendingTaskRunQueue":[]},"runningTaskRunMap":{},"runningSyncTaskRunMap":{}} |
+-----------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)
