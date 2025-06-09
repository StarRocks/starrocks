---
displayed_sidebar: docs
---

# CANCEL BACKUP

指定されたデータベースで進行中の BACKUP タスクをキャンセルします。

## Syntax

```SQL
CANCEL BACKUP FROM <db_name>
```

## Parameters

| **Parameter** | **Description**                                       |
| ------------- | ----------------------------------------------------- |
| db_name       | BACKUP タスクが属するデータベースの名前。             |

## Examples

例 1: データベース `example_db` の BACKUP タスクをキャンセルします。

```SQL
CANCEL BACKUP FROM example_db;
```