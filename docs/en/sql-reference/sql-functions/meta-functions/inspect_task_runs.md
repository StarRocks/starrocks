---
displayed_sidebar: docs
---

# inspect_task_runs

`inspect_task_runs()`

This function returns all status about the TaskManager.

## Arguments

None.

## Return Value

Returns a VARCHAR string containing the TaskManager status in JSON format.

## Examples

Example 1: Inspect current TaskManger global pending/running task runs information:
```
mysql> select inspect_task_runs();
+-----------------------------------------------------------------------------------------------------------------------------+
| inspect_task_runs()                                                                                                         |
+-----------------------------------------------------------------------------------------------------------------------------+
| {"pendingTaskRunQueue":{"pendingTaskRunMap":{},"pendingTaskRunQueue":[]},"runningTaskRunMap":{},"runningSyncTaskRunMap":{}} |
+-----------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)

```