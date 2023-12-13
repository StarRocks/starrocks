---
displayed_sidebar: "English"
---

# CANCEL RESTORE

## Description

Cancels an on-going RESTORE task in a specified database. For more information, see [data backup and restoration](../../../administration/Backup_and_restore.md).

> **CAUTION**
>
> If a RESTORE task is canceled during the COMMIT phase, the restored data will be corrupted and inaccessible. In this case, you can only perform the RESTORE  operation again and wait for the job to complete.

## Syntax

```SQL
CANCEL RESTORE FROM <db_name>
```

## Parameters

| **Parameter** | **Description**                                        |
| ------------- | ------------------------------------------------------ |
| db_name       | Name of the database that the RESTORE task belongs to. |

## Examples

Example 1: Cancels the RESTORE task under the database `example_db`.

```SQL
CANCEL RESTORE FROM example_db;
```
