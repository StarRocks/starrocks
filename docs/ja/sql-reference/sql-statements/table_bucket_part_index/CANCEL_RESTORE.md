---
displayed_sidebar: docs
---

# CANCEL RESTORE

## 説明

指定されたデータベースで進行中の RESTORE タスクをキャンセルします。詳細については、 [data backup and restoration](../../../administration/management/Backup_and_restore.md) を参照してください。

> **注意**
>
> RESTORE タスクが COMMIT フェーズ中にキャンセルされると、復元されたデータは破損し、アクセスできなくなります。この場合、再度 RESTORE 操作を実行し、ジョブが完了するのを待つしかありません。

## 構文

```SQL
CANCEL RESTORE FROM <db_name>
```

## パラメータ

| **パラメータ** | **説明**                                        |
| ------------- | ------------------------------------------------ |
| db_name       | RESTORE タスクが属するデータベースの名前です。   |

## 例

例 1: データベース `example_db` の RESTORE タスクをキャンセルします。

```SQL
CANCEL RESTORE FROM example_db;
```