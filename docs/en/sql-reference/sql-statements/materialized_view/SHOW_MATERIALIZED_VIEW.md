---
displayed_sidebar: docs
description: "SHOW MATERIALIZED VIEWS shows all or one specific asynchronous materialized view."
---

# SHOW MATERIALIZED VIEWS

SHOW MATERIALIZED VIEWS shows all or one specific asynchronous materialized view.

Since v3.0, the name of this statement is changed from SHOW MATERIALIZED VIEW to SHOW MATERIALIZED VIEWS.

:::tip

This operation does not require privileges.

:::

## Syntax

```SQL
SHOW MATERIALIZED VIEWS
[FROM db_name]
[
WHERE NAME { = "mv_name" | LIKE "mv_name_matcher"}
]
```

:::note

Since v3.3, `SHOW MATERIALIZED VIEWS` command will track the state of all task_runs if a refresh task consists of multiple partitions/task_runs to refresh. Only when all task_runs are success, `last_refresh_state` will return `SUCCESS`.

:::

## Parameters

| **Parameter**   | **Required** | **Description**                                              |
| --------------- | ------------ | ------------------------------------------------------------ |
| db_name         | no           | The name of the database to which the materialized view resides. If this parameter is not specified, the current database is used by default. |
| mv_name         | no           | The name of the materialized view to show.                   |
| mv_name_matcher | no           | The matcher used to filter materialized views.               |

## Returns

| **Return**                 | **Description**                                              |
| -------------------------- | ------------------------------------------------------------ |
| id                         | The ID of the materialized view.                             |
| database_name              | The name of the database in which the materialized view resides. |
| name                       | The name of the materialized view.                           |
| refresh_type               | The refresh type of the materialized view. Valid values: `SYNC` (synchronous materialized view) and `ASYNC` (asynchronous materialized view, regardless of how the refresh is triggered). |
| is_active                  | Whether the materialized view state is active. Valid Value: `true` and `false`. |
| inactive_reason            | The reason why the materialized view is inactive.            |
| partition_type             | The partition type of the materialized view, including RANGE and UNPARTITIONED.                |
| task_id                    | ID of the materialized view refresh task.                  |
| task_name                  | Name of the materialized view refresh task.                |
| last_refresh_start_time    | The start time of the last refresh of the materialized view. |
| last_refresh_finished_time | The end time of the last refresh of the materialized view.   |
| last_refresh_duration      | The time taken by the last refresh. Unit: seconds.           |
| last_refresh_state         | The status of the last refresh, including PENDING, RUNNING, FAILED, SUCCESS, and SKIPPED. When no data changes are detected on the base table partition, the refresh for the corresponding materialized view partition is skipped. |
| last_refresh_force_refresh | Whether the last refresh is a FORCE refresh.                 |
| last_refresh_start_partition | The start partition of the last refresh in the materialized view. |
| last_refresh_end_partition | The end partition of the last refresh in the materialized view. |
| last_refresh_base_refresh_partitions | The base table partitions that were refreshed in the last refresh. |
| last_refresh_mv_refresh_partitions | The materialized view partitions that were refreshed in the last refresh. |
| last_refresh_error_code    | The error code for the last failed refresh of the materialized view (if the materialized view state is not active). |
| last_refresh_error_message | The reason why the last refresh failed (if the materialized view state is not active). |
| rows                       | The number of data rows in the materialized view.            |
| text                       | The statement used to create the materialized view.          |
| extra_message              | Extra information about the latest refresh task.             |
| query_rewrite_status       | Query rewrite status of the materialized view.               |
| creator                    | Creator of the materialized view refresh task.               |
| last_refresh_process_time  | The process start time of the latest refresh task.           |
| last_refresh_job_id        | Job ID of the latest refresh task.                           |
| last_refresh_time          | Time up to which base table updates are reflected in the materialized view. |
| warehouse                  | Name of the warehouse that the asynchronous materialized view uses for its refresh tasks. Empty in shared-nothing mode, or for synchronous (rollup) materialized views. |
| refresh_mode               | Configured refresh mode of the asynchronous materialized view. Valid values: `PCT` (partition change tracking, where only changed partitions are refreshed), `INCREMENTAL` (incremental view maintenance), and `AUTO`. Empty for synchronous materialized views. |
| refresh_trigger            | How a refresh is triggered. Valid values: `NONE` (synchronous materialized view), `MANUAL` (only via REFRESH MATERIALIZED VIEW), `SCHEDULED` (periodic, via an EVERY interval), and `ON_BASE_TABLE_CHANGE` (automatically when a base table loads or changes). |
| refresh_policy             | Human-readable refresh policy. Valid values: `NONE`, `MANUAL`, `ON_BASE_TABLE_CHANGE`, or a schedule such as `START("yyyy-MM-dd HH:mm:ss") EVERY(INTERVAL n unit)` (the `START` clause is present only if a start time was defined). |
| resource_group             | Resource group used for the materialized view's refresh tasks (from the materialized view's `resource_group` property). Defaults to `default_mv_wg` when not set. |
| query_rewrite_status_reason | The reason behind `query_rewrite_status`. Valid values: `OK`, `MV_INACTIVE`, `QUERY_REWRITE_DISABLED`, `UNSUPPORTED_DEFINITION`, and `UNKNOWN`. |

## Examples

The following examples is based on this business scenario:

```Plain
-- Create Table: customer
CREATE TABLE customer ( C_CUSTKEY     INTEGER NOT NULL,
                        C_NAME        VARCHAR(25) NOT NULL,
                        C_ADDRESS     VARCHAR(40) NOT NULL,
                        C_NATIONKEY   INTEGER NOT NULL,
                        C_PHONE       CHAR(15) NOT NULL,
                        C_ACCTBAL     double   NOT NULL,
                        C_MKTSEGMENT  CHAR(10) NOT NULL,
                        C_COMMENT     VARCHAR(117) NOT NULL,
                        PAD char(1) NOT NULL)
    ENGINE=OLAP
DUPLICATE KEY(`c_custkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c_custkey`)
PROPERTIES (
"replication_num" = "3",
"storage_format" = "DEFAULT"
);

-- Create MV: customer_mv
CREATE MATERIALIZED VIEW customer_mv
DISTRIBUTED BY HASH(c_custkey)
REFRESH MANUAL
PROPERTIES (
    "replication_num" = "3"
)
AS SELECT
              c_custkey, c_phone, c_acctbal, count(1) as c_count, sum(c_acctbal) as c_sum
   FROM
              customer
   GROUP BY c_custkey, c_phone, c_acctbal;

-- Refresh the MV
REFRESH MATERIALIZED VIEW customer_mv;
```

Example 1: Show a specific materialized view.

```Plain
mysql> SHOW MATERIALIZED VIEWS WHERE NAME='customer_mv'\G
*************************** 1. row ***************************
                        id: 10142
                      name: customer_mv
             database_name: test
              refresh_type: ASYNC
                 is_active: true
   last_refresh_start_time: 2023-02-17 10:27:33
last_refresh_finished_time: 2023-02-17 10:27:33
     last_refresh_duration: 0
        last_refresh_state: SUCCESS
             inactive_code: 0
           inactive_reason:
                      text: CREATE MATERIALIZED VIEW `customer_mv`
COMMENT "MATERIALIZED_VIEW"
DISTRIBUTED BY HASH(`c_custkey`)
REFRESH MANUAL
PROPERTIES (
"replication_num" = "3",
"storage_medium" = "HDD"
)
AS SELECT `customer`.`c_custkey`, `customer`.`c_phone`, `customer`.`c_acctbal`, count(1) AS `c_count`, sum(`customer`.`c_acctbal`) AS `c_sum`
FROM `test`.`customer`
GROUP BY `customer`.`c_custkey`, `customer`.`c_phone`, `customer`.`c_acctbal`;
                      rows: 0
                 warehouse:
              refresh_mode: PCT
           refresh_trigger: MANUAL
            refresh_policy: MANUAL
            resource_group: default_mv_wg
1 row in set (0.11 sec)
```

Example 2: Show materialized views by matching the name.

```Plain
mysql> SHOW MATERIALIZED VIEWS WHERE NAME LIKE 'customer_mv'\G
*************************** 1. row ***************************
                        id: 10142
                      name: customer_mv
             database_name: test
              refresh_type: ASYNC
                 is_active: true
   last_refresh_start_time: 2023-02-17 10:27:33
last_refresh_finished_time: 2023-02-17 10:27:33
     last_refresh_duration: 0
        last_refresh_state: SUCCESS
             inactive_code: 0
           inactive_reason:
                      text: CREATE MATERIALIZED VIEW `customer_mv`
COMMENT "MATERIALIZED_VIEW"
DISTRIBUTED BY HASH(`c_custkey`)
REFRESH MANUAL
PROPERTIES (
"replication_num" = "3",
"storage_medium" = "HDD"
)
AS SELECT `customer`.`c_custkey`, `customer`.`c_phone`, `customer`.`c_acctbal`, count(1) AS `c_count`, sum(`customer`.`c_acctbal`) AS `c_sum`
FROM `test`.`customer`
GROUP BY `customer`.`c_custkey`, `customer`.`c_phone`, `customer`.`c_acctbal`;
                      rows: 0
                 warehouse:
              refresh_mode: PCT
           refresh_trigger: MANUAL
            refresh_policy: MANUAL
            resource_group: default_mv_wg
1 row in set (0.12 sec)
```
