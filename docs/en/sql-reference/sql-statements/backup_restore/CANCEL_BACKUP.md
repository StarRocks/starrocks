---
displayed_sidebar: docs
---

# CANCEL BACKUP

## Description

Cancels an ongoing BACKUP task in a specified database.

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
