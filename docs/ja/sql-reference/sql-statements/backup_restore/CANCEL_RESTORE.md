---
displayed_sidebar: docs
---

# CANCEL RESTORE

指定されたデータベースで進行中の RESTORE タスクをキャンセルします。

> **注意**
>
> RESTORE タスクが COMMIT フェーズ中にキャンセルされた場合、復元されたデータは破損し、アクセスできなくなります。この場合、再度 RESTORE 操作を実行し、ジョブが完了するのを待つ必要があります。

## 構文

```SQL
CANCEL RESTORE FROM <db_name>
```

## パラメータ

| **パラメータ** | **説明**                                      |
| ------------- | ---------------------------------------------- |
| db_name       | RESTORE タスクが属するデータベースの名前。     |

## 例

例 1: データベース `example_db` の RESTORE タスクをキャンセルします。

```SQL
CANCEL RESTORE FROM example_db;
```