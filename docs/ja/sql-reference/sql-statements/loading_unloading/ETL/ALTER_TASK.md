---
displayed_sidebar: docs
---

# ALTER TASK

## 説明

[SUBMIT TASK](SUBMIT_TASK.md) を使用して送信された非同期 ETL タスクを変更します。この機能は v4.1 からサポートされています。

このステートメントを使用して以下を行うことができます：

- 実行中のタスクを一時停止する
- 一時停止されたタスクを再開する
- タスクのプロパティを更新する

## 構文

```SQL
ALTER TASK [IF EXISTS] <task_name> { RESUME | SUSPEND | SET ('key' = 'value'[, ...]) }
```

## パラメータ

| **パラメータ** | **必須** | **説明**               |
| ------------- | -------- | ---------------------- |
| IF EXISTS     | いいえ    | このパラメータを指定すると、StarRocks は存在しないタスクを変更してもエラーが発生しません。このパラメータを指定しないと、StarRocks は存在しないタスクを変更するときにエラーを発生させます。 |
| task_name     | はい     | 変更するタスクの名前。 |
| RESUME        | いいえ   | 一時停止されたタスクを再開します。タスクは元のスケジュールに従ってスケジュールされます（定期タスクの場合）、または手動実行が可能になります（手動タスクの場合）。 |
| SUSPEND       | いいえ   | 実行中のタスクを一時停止します。定期タスクの場合、タスクスケジューラが停止し、実行中のタスク実行が終了します。 |
| SET           | いいえ   | タスクのプロパティを更新します。プロパティは既存のプロパティとマージされ、後続のタスク実行に適用されます。 |

## 使用上の注意

- Information Schema のメタデータビュー `tasks` と `task_runs` をクエリすることで、非同期タスクの情報を確認できます。

  ```SQL
  SELECT * FROM INFORMATION_SCHEMA.tasks;
  SELECT * FROM information_schema.tasks WHERE task_name = '<task_name>';
  SELECT * FROM information_schema.task_runs;
  SELECT * FROM information_schema.task_runs WHERE task_name = '<task_name>';
  ```

- `SUSPEND` アクションは、定期タスクのタスクスケジューラを停止し、現在実行中のタスク実行を終了します。タスクの状態は `PAUSE` に変更されます。

- `RESUME` アクションは、定期タスクのタスクスケジューラを再開します。タスクの状態は `ACTIVE` に変更されます。

- `SET` アクションは、後続のタスク実行に適用されるタスクプロパティを更新します。`session.` プレフィックスを持つセッション変数を追加することで、タスク実行時の接続コンテキスト設定を変更できます。

## 例

例 1: `etl_task` という名前のタスクを一時停止します：

```SQL
ALTER TASK etl_task SUSPEND;
```

例 2: `etl_task` という名前の一時停止されたタスクを再開します：

```SQL
ALTER TASK etl_task RESUME;
```

例 3: `etl_task` という名前のタスクのクエリタイムアウトを更新します：

```SQL
ALTER TASK etl_task SET ('session.query_timeout' = '5000');
```

例 4: タスクの複数のプロパティを更新します：

```SQL
ALTER TASK etl_task SET (
    'session.query_timeout' = '5000',
    'session.enable_profile' = 'true'
);
```

例 5: タスクが存在する場合のみタスクを一時停止します（タスクが存在しない場合はエラーを回避）：

```SQL
ALTER TASK IF EXISTS etl_task SUSPEND;
```
