---
displayed_sidebar: docs
---

# analyze_status

`analyze_status` は分析ジョブのステータスに関する情報を提供します。

`analyze_status` には以下のフィールドが提供されています:

| **フィールド**    | **説明**                                         |
| ------------ | ------------------------------------------------ |
| Id           | 分析ジョブの ID。                                |
| Catalog      | テーブルが属するカタログ。                       |
| Database     | テーブルが属するデータベース。                   |
| Table        | 分析されているテーブルの名前。                   |
| Columns      | 分析されている列。                               |
| Type         | 分析ジョブのタイプ。有効な値: `FULL`、`SAMPLE`。 |
| Schedule     | 分析ジョブのスケジュールタイプ。有効な値: `ONCE`、`AUTOMATIC`。 |
| Status       | 分析ジョブのステータス。有効な値: `PENDING`、`RUNNING`、`FINISH`、`FAILED`。 |
| StartTime    | 分析ジョブの開始時刻。                           |
| EndTime      | 分析ジョブの終了時刻。                           |
| Properties   | 分析ジョブのプロパティ。                         |
| Reason       | 分析ジョブのステータスの理由。                   |
