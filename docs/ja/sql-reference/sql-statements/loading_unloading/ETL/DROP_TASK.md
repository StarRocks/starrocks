---
displayed_sidebar: docs
---

# DROP TASK

## Description

[SUBMIT TASK](SUBMIT_TASK.md) を使用して送信された非同期 ETL タスクを削除します。この機能は StarRocks v2.5.7 からサポートされています。

> **NOTE**
>
> DROP TASK でタスクを削除すると、対応する TaskRun も同時にキャンセルされます。

## Syntax

```SQL
DROP TASK `<task_name>`
```

## Parameters

| **Parameter** | **Description**                                                                 |
| ------------- | ------------------------------------------------------------------------------- |
| task_name     | 削除するタスクの名前。解析エラーを防ぐために、タスク名をバッククォート (`) で囲んでください。 |

## Usage notes

Information Schema のメタデータビュー `tasks` と `task_runs` をクエリすることで、非同期タスクの情報を確認できます。

```SQL
SELECT * FROM INFORMATION_SCHEMA.tasks;
SELECT * FROM information_schema.tasks WHERE task_name = '<task_name>';
SELECT * FROM information_schema.task_runs;
SELECT * FROM information_schema.task_runs WHERE task_name = '<task_name>';
```

## Examples

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