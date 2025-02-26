---
displayed_sidebar: docs
---

# PAUSE ROUTINE LOAD

import RoutineLoadPrivNote from '../../../../_assets/commonMarkdown/RoutineLoadPrivNote.md'

## 説明

Routine Load ジョブを一時停止しますが、このジョブを終了させることはありません。[RESUME ROUTINE LOAD](RESUME_ROUTINE_LOAD.md) を実行して再開できます。ロードジョブが一時停止された後、[SHOW ROUTINE LOAD](SHOW_ROUTINE_LOAD.md) および [ALTER ROUTINE LOAD](./ALTER_ROUTINE_LOAD.md) を実行して、情報を表示および変更できます。

<RoutineLoadPrivNote />

## 構文

```SQL
PAUSE ROUTINE LOAD FOR [db_name.]<job_name>;
```

## パラメータ

| パラメータ | 必須 | 説明 |
| --------- | ---- | ------------------------------------------------------------ |
| db_name   |      | Routine Load ジョブが属するデータベースの名前。 |
| job_name  | ✅   | Routine Load ジョブの名前。1 つのテーブルに複数の Routine Load ジョブが存在する可能性があるため、識別可能な情報（例: Kafka トピック名やロードジョブを作成した時刻）を使用して意味のある Routine Load ジョブ名を設定し、複数の Routine Load ジョブを区別することをお勧めします。同じデータベース内で Routine Load ジョブの名前は一意でなければなりません。 |

## 例

データベース `example_db` の Routine Load ジョブ `example_tbl1_ordertest1` を一時停止します。

```sql
PAUSE ROUTINE LOAD FOR example_db.example_tbl1_ordertest1;
```