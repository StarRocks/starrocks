---
displayed_sidebar: "English"
---

# CANCEL LOAD

## Description

Cancels a given load job: [Broker Load](../data-manipulation/BROKER_LOAD.md), [Spark Load](../data-manipulation/SPARK_LOAD.md), or [INSERT](../data-manipulation/insert.md). A load job in the `PREPARED`, `CANCELLED` or `FINISHED` state cannot be canceled.

Canceling a load job is an asynchronous process. You can use the [SHOW LOAD](../data-manipulation/SHOW_LOAD.md) statement to check whether a load job is successfully canceled. The load job is successfully canceled if the value of `State` is `CANCELLED` and the value of `type` (displayed in `ErrorMsg`) is `USER_CANCEL`.

## Syntax

```SQL
CANCEL LOAD
[FROM db_name]
WHERE LABEL = "label_name"
```

## Parameters

| **Parameter** | **Required** | **Description**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| db_name       | No           | The name of the database to which the load job belongs. If this parameter is not specified, a load job in your current database is canceled by default. |
| label_name    | Yes          | The label of the load job.                                   |

## Examples

Example 1: Cancel the load job whose label is `example_label` in the current database.

```SQL
CANCEL LOAD
WHERE LABEL = "example_label";
```

Example 2: Cancel the load job whose label is `example_label` in the `example_db` database.

```SQL
CANCEL LOAD
FROM example_db
WHERE LABEL = "example_label";
```
