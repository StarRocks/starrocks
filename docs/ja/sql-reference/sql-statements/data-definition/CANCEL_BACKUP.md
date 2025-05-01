---
displayed_sidebar: docs
---

# CANCEL BACKUP

## Description

指定されたデータベースで進行中の BACKUP タスクをキャンセルします。詳細については、 [data backup and restoration](../../../administration/Backup_and_restore.md) を参照してください。

## Syntax

```SQL
CANCEL BACKUP FROM <db_name>
```

## Parameters

| **Parameter** | **Description**                                       |
| ------------- | ----------------------------------------------------- |
| db_name       | BACKUP タスクが属するデータベースの名前です。 |

## Examples

Example 1: データベース `example_db` の BACKUP タスクをキャンセルします。

```SQL
CANCEL BACKUP FROM example_db;
```