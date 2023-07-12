# REFRESH MATERIALIZED VIEW

## Description

Manually refresh a specific asynchronous materialized view or partitions within.

> **CAUTION**
>
> You can only manually refresh materialized views that adopt ASYNC or MANUAL refresh strategy. You can check the refresh strategy of an asynchronous materialized view using [SHOW MATERIALIZED VIEW](../data-manipulation/SHOW%20MATERIALIZED%20VIEW.md).

## Syntax

```SQL
REFRESH MATERIALIZED VIEW [database.]mv_name
[PARTITION START ("<partition_start_date>") END ("<partition_end_date>")]
[FORCE]
[WITH { SYNC | ASYNC } MODE]
```

## Parameters

| **Parameter**             | **Required** | **Description**                                        |
| ------------------------- | ------------ | ------------------------------------------------------ |
| mv_name                   | yes          | The name of the materialized view to refresh manually. |
| PARTITION START () END () | no           | Manually refresh partitions within a certain time interval. |
| partition_start_date      | no           | The start date of the partitions to refresh manually.  |
| partition_end_date        | no           | The end date of the partitions to refresh manually.    |
| FORCE                     | no           | If you specify this parameter, StarRocks forcibly refreshes the corresponding materialized view or partitions. If you do not specify this parameter, StarRocks automatically judges if the data is updated and refreshes the partition only when needed.  |
| WITH ... MODE             | no           | Make a synchronous or asynchronous call of the refresh task. `SYNC` indicates making a synchronous call of the refresh task, and StarRocks returns the task result only when the task succeeds or fails. `ASYNC` indicates making an asynchronous call of the refresh task, and StarRocks returns success right after the task is submitted, leaving the task to be executed asynchronously in the background. You can check the refresh task status of an asynchronous materialized view by querying the `tasks` and `task_runs` metadata tables in StarRocks' Information Schema. For more information, see [Check the execution status of asynchronous materialized view](../../../using_starrocks/Materialized_view.md#check-the-execution-status-of-asynchronous-materialized-view). Default: `ASYNC`. Supported from v3.1.0 onwards. |

> **CAUTION**
>
>  When refreshing materialized views created based on the external catalogs, StarRocks refreshes all partitions in the materialized views.

## Examples

Example 1: Manually refresh a specific materialized view via an asynchronous call.

```Plain
REFRESH MATERIALIZED VIEW lo_mv1;

REFRESH MATERIALIZED VIEW lo_mv1 WITH ASYNC MODE;
```

Example 2: Manually refresh certain partitions of a specific materialized view.

```Plain
REFRESH MATERIALIZED VIEW lo_mv1 
PARTITION START ("2020-02-01") END ("2020-03-01");
```

Example 3: Forcibly refresh certain partitions of a specific materialized view.

```Plain
REFRESH MATERIALIZED VIEW lo_mv1
PARTITION START ("2020-02-01") END ("2020-03-01") FORCE;
```

Example 4: Manually refresh a materialized view via a synchronous call.

```Plain
REFRESH MATERIALIZED VIEW lo_mv1 WITH SYNC MODE;
```
