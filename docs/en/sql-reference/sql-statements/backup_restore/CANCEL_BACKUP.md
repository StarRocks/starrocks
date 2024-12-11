---
displayed_sidebar: docs
---

# CANCEL BACKUP

## Description

<<<<<<< HEAD
Cancels an on-going BACKUP task in a specified database. For more information, see [data backup and restoration](../../../administration/management/Backup_and_restore.md).
=======
Cancels an ongoing BACKUP task in a specified database.
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

## Syntax

```SQL
CANCEL BACKUP FROM <db_name>
```

## Parameters

| **Parameter** | **Description**                                       |
| ------------- | ----------------------------------------------------- |
| db_name       | Name of the database that the BACKUP task belongs to. |

## Examples

Example 1: Cancels the BACKUP task under the database `example_db`.

```SQL
CANCEL BACKUP FROM example_db;
```
