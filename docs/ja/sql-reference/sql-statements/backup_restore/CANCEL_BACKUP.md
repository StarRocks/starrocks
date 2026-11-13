---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "指定されたデータベースで進行中の BACKUP タスクをキャンセルします。"
products: [oss, enterprise, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# CANCEL BACKUP

指定されたデータベースで進行中の BACKUP タスクをキャンセルします。

## Syntax

```SQL
CANCEL BACKUP { FROM <db_name> | FOR EXTERNAL CATALOG }
```

## Parameters

| **Parameter** | **Description**                                       |
| ------------- | ----------------------------------------------------- |
| db_name       | BACKUP タスクが属するデータベースの名前です。 |
| FOR EXTERNAL CATALOG | external catalog メタデータの進行中の BACKUP タスクをキャンセルします。 |

## Examples

例 1: データベース `example_db` の下で BACKUP タスクをキャンセルします。

```SQL
CANCEL BACKUP FROM example_db;
```