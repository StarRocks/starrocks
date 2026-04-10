---
displayed_sidebar: docs
---

# ALTER TASK

## 功能

修改通过 [SUBMIT TASK](SUBMIT_TASK.md) 语句提交的异步 ETL 任务。此功能从 v4.1 起支持。

您可以使用该语句执行以下操作：

- 暂停正在运行的任务
- 恢复已暂停的任务
- 更新任务属性

## 语法

```SQL
ALTER TASK [IF EXISTS] <task_name> { RESUME | SUSPEND | SET ('key' = 'value'[, ...]) }
```

## 参数说明

| **参数**    | **必须** | **说明**                                         |
|-----------|--------|------------------------------------------------|
| IF EXISTS | 否      | 如果声明该参数，修改不存在的任务系统不会报错。如果不声明该参数，修改不存在的任务系统会报错。 |
| task_name | 是      | 待修改任务名。                                      |
| RESUME    | 否      | 恢复已暂停的任务。对于周期性任务，将按照原有调度计划进行调度；对于手动任务，将可继续手动执行。 |
| SUSPEND   | 否      | 暂停正在运行的任务。对于周期性任务，将停止任务调度器并终止正在运行的任务运行。              |
| SET       | 否      | 更新任务的属性。属性将与现有属性合并，并应用于后续的任务执行。                         |

## 使用说明

- 您可以通过查询 Information Schema 中的元数据视图 `tasks` 和 `task_runs` 来查看异步任务的信息。

  ```SQL
  SELECT * FROM INFORMATION_SCHEMA.tasks;
  SELECT * FROM information_schema.tasks WHERE task_name = '<task_name>';
  SELECT * FROM information_schema.task_runs;
  SELECT * FROM information_schema.task_runs WHERE task_name = '<task_name>';
  ```

- `SUSPEND` 操作会停止周期性任务的调度器，并终止当前正在运行的任务运行。任务状态将变为 `PAUSE`。

- `RESUME` 操作会重新启动周期性任务的调度器。任务状态将变为 `ACTIVE`。

- `SET` 操作更新任务属性，这些属性将应用于后续的任务执行。您可以通过添加 `session.` 前缀的会话变量来更改任务运行时的连接上下文配置。

## 示例

示例一：暂停名为 `etl_task` 的任务：

```SQL
ALTER TASK etl_task SUSPEND;
```

示例二：恢复名为 `etl_task` 的已暂停任务：

```SQL
ALTER TASK etl_task RESUME;
```

示例三：更新名为 `etl_task` 的任务的查询超时时间：

```SQL
ALTER TASK etl_task SET ('session.query_timeout' = '5000');
```

示例四：更新任务的多个属性：

```SQL
ALTER TASK etl_task SET (
    'session.query_timeout' = '5000',
    'session.enable_profile' = 'true'
);
```

示例五：仅在任务存在时暂停任务（如果任务不存在则避免报错）：

```SQL
ALTER TASK IF EXISTS etl_task SUSPEND;
```
