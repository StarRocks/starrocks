---
displayed_sidebar: docs
---

# STOP ROUTINE LOAD

import RoutineLoadPrivNote from '../../../../_assets/commonMarkdown/RoutineLoadPrivNote.md'

## 説明

Routine Load ジョブを停止します。

<RoutineLoadPrivNote />

:::warning

- 停止した Routine Load ジョブは再開できません。そのため、このステートメントを実行する際は慎重に進めてください。
- Routine Load ジョブを一時停止するだけでよい場合は、[PAUSE ROUTINE LOAD](PAUSE_ROUTINE_LOAD.md) を実行できます。

:::

## 構文

```SQL
STOP ROUTINE LOAD FOR [db_name.]<job_name>
```

## パラメータ

| **パラメータ** | **必須** | **説明**                                              |
| ------------- | -------- | ----------------------------------------------------- |
| db_name       |          | Routine Load ジョブが属するデータベースの名前。        |
| job_name      | ✅        | Routine Load ジョブの名前。                           |

## 例

データベース `example_db` 内の Routine Load ジョブ `example_tbl1_ordertest1` を停止します。

```SQL
STOP ROUTINE LOAD FOR example_db.example_tbl1_ordertest1;
```