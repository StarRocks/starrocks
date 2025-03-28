---
displayed_sidebar: docs
---

# CANCEL BACKUP

Cancels an ongoing BACKUP task in a specified database.

## Syntax

```SQL
CANCEL BACKUP { FROM <db_name> | FOR EXTERNAL CATALOG }
```

## Parameters

| **Parameter** | **Description**                                       |
| ------------- | ----------------------------------------------------- |
| db_name       | Name of the database that the BACKUP task belongs to. |
| FOR EXTERNAL CATALOG | Cancels the ongoing BACKUP task for the external catalog metadata. |

## Examples

Example 1: Cancels the BACKUP task under the database `example_db`.

```SQL
CANCEL BACKUP FROM example_db;
```
