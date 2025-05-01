---
displayed_sidebar: docs
---

# CANCEL RESTORE

指定されたデータベースで進行中の RESTORE タスクをキャンセルします。

> **注意**
>
> COMMIT フェーズ中に RESTORE タスクがキャンセルされると、復元されたデータは破損し、アクセスできなくなります。この場合、再度 RESTORE 操作を実行し、ジョブが完了するのを待つしかありません。

## 構文

```SQL
CANCEL RESTORE FROM <db_name>
```

## パラメーター

| **パラメーター** | **説明**                                      |
| --------------- | ---------------------------------------------- |
| db_name         | RESTORE タスクが属するデータベースの名前です。 |

## 例

例 1: データベース `example_db` の下で RESTORE タスクをキャンセルします。

```SQL
CANCEL RESTORE FROM example_db;
```