---
displayed_sidebar: "English"
---

# CANCEL REFRESH MATERIALIZED VIEW

## Description

Cancels a refresh task for an asynchronous materialized view.

## Syntax

```SQL
CANCEL REFRESH MATERIALIZED VIEW [<database_name>.]<materialized_view_name>
```

## Parameters

| **Parameter**          | **Required** | **Description**                                              |
| ---------------------- | ------------ | ------------------------------------------------------------ |
| database_name          | No           | Name of the database where the materialized view resides. If this parameter is not specified, the current database is used. |
| materialized_view_name | Yes          | Name of the materialized view.                               |

## Examples

Example 1: Cancel the refresh task for the ASYNC refresh materialized view `lo_mv1`.

```SQL
CANCEL REFRESH MATERIALIZED VIEW lo_mv1;
```
