---
displayed_sidebar: docs
---

# DROP TASK

## 功能

删除通过 [SUBMIT TASK](SUBMIT_TASK.md) 语句提交的异步 ETL 任务。此功能从 StarRocks v2.5.7 起支持。

> **说明**
>
> 通过 DROP TASK 删除任务将同时取消该任务对应的 TaskRun。

## 语法

```SQL
DROP TASK [IF EXISTS] `<task_name>` [FORCE]
```

## 参数说明

| **参数**    | **必须** | **说明**                                         |
|-----------|--------|------------------------------------------------|
| IF EXISTS | 否      | 如果声明该参数，删除不存在的任务系统不会报错。如果不声明该参数，删除不存在的任务系统会报错。 |
| task_name | 是      | 待删除任务名。为避免解析失败，请使用反括号（`）包裹任务名。                 |
| FORCE     | 否      | 强制删除任务。                                        |

## 使用说明

您可以通过查询 Information Schema 中的元数据视图 `tasks` 和 `task_runs` 来查看异步任务的信息。

```SQL
SELECT * FROM INFORMATION_SCHEMA.tasks;
SELECT * FROM information_schema.tasks WHERE task_name = '<task_name>';
SELECT * FROM information_schema.task_runs;
SELECT * FROM information_schema.task_runs WHERE task_name = '<task_name>';
```

您可以通过 SHOW MATERIALIZED VIEWS 语句获取物化视图刷新任务的 `task_name`。

```SQL
SHOW MATERIALIZED VIEWS;
SHOW MATERIALIZED VIEWS WHERE name = '<mv_name>';
```

## 示例

```Plain
MySQL > SUBMIT /*+set_var(query_timeout=100000)*/ TASK `ctas` AS
    -> CREATE TABLE insert_wiki_edit_new
    -> AS SELECT * FROM source_wiki_edit;
+----------+-----------+
| TaskName | Status    |
+----------+-----------+
| ctas     | SUBMITTED |
+----------+-----------+
1 row in set (1.19 sec)

MySQL > DROP TASK `ctas`;
Query OK, 0 rows affected (0.35 sec)
```

删除不存在的任务

- 当未声明 `IF EXISTS` 参数时，删除一个不存在的 task `test_task` 会报错。

```Plain
MySQL > DROP TASK test_task;
Query 1 ERROR: Getting analyzing error. Detail message: Task test_task is not exist.
```

- 当声明 `IF EXISTS` 参数时，删除一个不存在的 `test_task` 不会报错。

```Plain
MySQL > DROP TASK IF EXISTS test_task;
Query OK, 0 rows affected (0.00 sec)
```
