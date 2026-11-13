---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns the current status of all tasks in the FE TaskManager as a JSON string."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
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