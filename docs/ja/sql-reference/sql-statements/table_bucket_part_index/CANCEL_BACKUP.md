---
displayed_sidebar: docs
---

# CANCEL BACKUP

## 説明

指定されたデータベースで進行中の BACKUP タスクをキャンセルします。詳細については、 [data backup and restoration](../../../administration/management/Backup_and_restore.md) を参照してください。

## 構文

```SQL
CANCEL BACKUP FROM <db_name>
```

## パラメータ

| **パラメータ** | **説明**                                       |
| ------------- | ----------------------------------------------- |
| db_name       | BACKUP タスクが属するデータベースの名前。       |

## 例

例 1: データベース `example_db` の下で BACKUP タスクをキャンセルします。

```SQL
CANCEL BACKUP FROM example_db;
```