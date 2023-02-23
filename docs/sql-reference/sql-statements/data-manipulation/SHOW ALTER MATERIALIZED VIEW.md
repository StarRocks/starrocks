# SHOW ALTER MATERIALIZED VIEW

## Description

Shows the building status of single-table sync refresh materialized views.

## Syntax

```SQL
SHOW ALTER MATERIALIZED VIEW [ { FROM | IN } db_name]
```

Parameters in brackets [] is optional.

## Parameters

| **Parameter** | **Required** | **Description**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| db_name       | no           | The name of the database to which the materialized view resides. If this parameter is not specified, the current database is used by default. |

## Returns

| **Return**      | **Description**                                  |
| --------------- | ------------------------------------------------ |
| JobId           | The ID of refresh job.                           |
| TableName       | The name of the table.                           |
| CreateTime      | The time when refresh job is created.            |
| FinishedTime    | The time when refresh job is finished.           |
| BaseIndexName   | The name of the base table.                      |
| RollupIndexName | The name of the materialized view.               |
| RollupId        | The ID of the materialized view rollup.          |
| TransactionId   | The ID of transaction that waits to be executed. |
| State           | The state of the job.                            |
| Msg             | Error message.                                   |
| Progress        | Progress of the refresh job.                     |
| Timeout         | Timeout for the refresh job.                     |

## Examples

Example 1: building status of single-table sync refresh materialized views

```Plain
MySQL > SHOW ALTER MATERIALIZED VIEW\G
*************************** 1. row ***************************
          JobId: 475991
      TableName: lineorder
     CreateTime: 2022-08-24 19:46:53
   FinishedTime: 2022-08-24 19:47:15
  BaseIndexName: lineorder
RollupIndexName: lo_mv_sync_1
       RollupId: 475992
  TransactionId: 33067
          State: FINISHED
            Msg: 
       Progress: NULL
        Timeout: 86400
*************************** 2. row ***************************
          JobId: 477337
      TableName: lineorder
     CreateTime: 2022-08-24 19:47:25
   FinishedTime: 2022-08-24 19:47:45
  BaseIndexName: lineorder
RollupIndexName: lo_mv_sync_2
       RollupId: 477338
  TransactionId: 33068
          State: FINISHED
            Msg: 
       Progress: NULL
        Timeout: 86400
2 rows in set (0.00 sec)
```
