---
displayed_sidebar: docs
---

# DROP TASK

## 説明

[SUBMIT TASK](SUBMIT_TASK.md) を使用して送信された非同期 ETL タスクを削除します。この機能は StarRocks v2.5.7 からサポートされています。

> **注意**
>
> DROP TASK でタスクを削除すると、対応する TaskRun も同時にキャンセルされます。

## 構文

```SQL
DROP TASK `<task_name>` [FORCE]
```

## パラメータ

| **パラメータ** | **必須** | **説明**               |
| ------------- | -------- | ---------------------- |
| task_name     | はい     | 削除するタスクの名前。タスク名をバッククォート (`) で囲んで、解析エラーを防いでください。 |
| FORCE         | いいえ   | タスクの強制削除を行います。 |

## 使用上の注意

Information Schema のメタデータビュー `tasks` と `task_runs` をクエリすることで、非同期タスクの情報を確認できます。

```SQL
SELECT * FROM INFORMATION_SCHEMA.tasks;
SELECT * FROM information_schema.tasks WHERE task_name = '<task_name>';
SELECT * FROM information_schema.task_runs;
SELECT * FROM information_schema.task_runs WHERE task_name = '<task_name>';
```

マテリアライズドビューのリフレッシュタスクの `task_name` は、SHOW MATERIALIZED VIEWS ステートメントを使用して取得できます。

```SQL
SHOW MATERIALIZED VIEWS;
SHOW MATERIALIZED VIEWS WHERE name = '<mv_name>';
```

## 例

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