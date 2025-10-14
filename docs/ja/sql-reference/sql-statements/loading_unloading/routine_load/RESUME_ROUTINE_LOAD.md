---
displayed_sidebar: docs
---

# RESUME ROUTINE LOAD

import RoutineLoadPrivNote from '../../../../_assets/commonMarkdown/RoutineLoadPrivNote.mdx'

## Description

Routine Load ジョブを再開します。ジョブは一時的に **NEED_SCHEDULE** 状態になります。これは、ジョブが再スケジュールされているためです。しばらくすると、ジョブは **RUNNING** 状態に戻り、データソースからメッセージを消費し続け、データをロードします。ジョブの情報は [SHOW ROUTINE LOAD](SHOW_ROUTINE_LOAD.md) ステートメントを使用して確認できます。

<RoutineLoadPrivNote />

## Syntax

```SQL
RESUME ROUTINE LOAD FOR [db_name.]<job_name>
```

## Parameters

| **Parameter** | **Required** | **Description**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| db_name       |              | Routine Load ジョブが属するデータベースの名前。               |
| job_name      | ✅            | Routine Load ジョブの名前。                                  |

## Examples

データベース `example_db` の Routine Load ジョブ `example_tbl1_ordertest1` を再開します。

```SQL
RESUME ROUTINE LOAD FOR example_db.example_tbl1_ordertest1;
```