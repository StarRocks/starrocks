---
displayed_sidebar: docs
---

# inspect_task_runs

`inspect_task_runs()`

この関数は、TaskManager に関するすべてのステータスを返します。

## 引数

なし。

## 戻り値

TaskManager のステータスを含む JSON 形式の VARCHAR 文字列を返します。

## 例

例1: 現在のTaskManagerのグローバル保留中/実行中タスク実行情報を検査する:
```
mysql> select inspect_task_runs();
+-----------------------------------------------------------------------------------------------------------------------------+
| inspect_task_runs()                                                                                                         |
+-----------------------------------------------------------------------------------------------------------------------------+
| {"pendingTaskRunQueue":{"pendingTaskRunMap":{},"pendingTaskRunQueue":[]},"runningTaskRunMap":{},"runningSyncTaskRunMap":{}} |
+-----------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)

```

