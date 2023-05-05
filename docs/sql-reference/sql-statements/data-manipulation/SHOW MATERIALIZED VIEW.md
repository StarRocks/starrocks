# SHOW MATERIALIZED VIEW

## Description

Shows all or one specific asynchronous materialized view.

## Syntax

```SQL
SHOW MATERIALIZED VIEW
[FROM db_name]
[
WHERE
[NAME { = "mv_name" | LIKE "mv_name_matcher"}
]
```

Parameters in brackets [] is optional.

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
| refresh_type               | The refresh type of the materialized view, including ROLLUP, MANNUL, ASYNC, and INCREMENTAL. |
| is_active                  | Whether the materialized view state is active. Valid Value: `true` and `false`. |
| partition_type             | The partition type of the materialized view, including RANGE and UNPARTITIONED.                |
| task_id                    | ID of the materialized view refresh task.                  |
| task_name                  | Name of the materialized view refresh task.                |
| last_refresh_start_time    | The start time of the last refresh of the materialized view. |
| last_refresh_finished_time | The end time of the last refresh of the materialized view.   |
| last_refresh_duration      | The time taken by the last refresh. Unit: seconds.           |
| last_refresh_state         | The status of the last refresh, including PENDING, RUNNING, FAILED, and SUCCESS. |
| last_refresh_force_refresh | Whether the last refresh is a FORCE refresh.                 |
| last_refresh_start_partition | The start partition of the last refresh in the materialized view. |
| last_refresh_end_partition | The end partition of the last refresh in the materialized view. |
| last_refresh_base_refresh_partitions | The base table partitions that were refreshed in the last refresh. |
| last_refresh_mv_refresh_partitions | The materialized view partitions that were refreshed in the last refresh. |
| last_refresh_error_code    | The error code for the last failed refresh of the materialized view (if the materialized view state is not active). |
| last_refresh_error_message | The reason why the last refresh failed (if the materialized view state is not active). |
| rows                       | The number of data rows in the materialized view.            |
| text                       | The statement used to create the materialized view.          |

## Examples

Example 1: Show a specific materialized view

```Plain
MySQL > SHOW MATERIALIZED VIEW WHERE NAME = "lo_mv1"\G
*************************** 1. row ***************************
           id: 475899
         name: lo_mv1
database_name: wlc_test
         text: CREATE MATERIALIZED VIEW `lo_mv1`
COMMENT "MATERIALIZED_VIEW"
DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 10 
REFRESH ASYNC
PROPERTIES (
"replication_num" = "3",
"storage_medium" = "HDD"
)
AS SELECT `wlc_test`.`lineorder`.`lo_orderkey` AS `lo_orderkey`, `wlc_test`.`lineorder`.`lo_custkey` AS `lo_custkey`, sum(`wlc_test`.`lineorder`.`lo_quantity`) AS `total_quantity`, sum(`wlc_test`.`lineorder`.`lo_revenue`) AS `total_revenue`, count(`wlc_test`.`lineorder`.`lo_shipmode`) AS `shipmode_count` FROM `wlc_test`.`lineorder` GROUP BY `wlc_test`.`lineorder`.`lo_orderkey`, `wlc_test`.`lineorder`.`lo_custkey` ORDER BY `wlc_test`.`lineorder`.`lo_orderkey` ASC ;
         rows: 0
1 row in set (0.42 sec)
```

Example 2: Show specific materialized views by matching the name

```Plain
MySQL > SHOW MATERIALIZED VIEW WHERE NAME LIKE "lo_mv%"\G
*************************** 1. row ***************************
           id: 475985
         name: lo_mv2
database_name: wlc_test
         text: CREATE MATERIALIZED VIEW `lo_mv2`
COMMENT "MATERIALIZED_VIEW"
PARTITION BY (`lo_orderdate`)
DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 10 
REFRESH ASYNC START("2023-07-01 10:00:00") EVERY(INTERVAL 1 DAY)
PROPERTIES (
"replication_num" = "3",
"storage_medium" = "HDD"
)
AS SELECT `wlc_test`.`lineorder`.`lo_orderkey` AS `lo_orderkey`, `wlc_test`.`lineorder`.`lo_orderdate` AS `lo_orderdate`, `wlc_test`.`lineorder`.`lo_custkey` AS `lo_custkey`, sum(`wlc_test`.`lineorder`.`lo_quantity`) AS `total_quantity`, sum(`wlc_test`.`lineorder`.`lo_revenue`) AS `total_revenue`, count(`wlc_test`.`lineorder`.`lo_shipmode`) AS `shipmode_count` FROM `wlc_test`.`lineorder` GROUP BY `wlc_test`.`lineorder`.`lo_orderkey`, `wlc_test`.`lineorder`.`lo_orderdate`, `wlc_test`.`lineorder`.`lo_custkey` ORDER BY `wlc_test`.`lineorder`.`lo_orderkey` ASC ;
         rows: 0
*************************** 2. row ***************************
           id: 475899
         name: lo_mv1
database_name: wlc_test
         text: CREATE MATERIALIZED VIEW `lo_mv1`
COMMENT "MATERIALIZED_VIEW"
DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 10 
REFRESH ASYNC
PROPERTIES (
"replication_num" = "3",
"storage_medium" = "HDD"
)
AS SELECT `wlc_test`.`lineorder`.`lo_orderkey` AS `lo_orderkey`, `wlc_test`.`lineorder`.`lo_custkey` AS `lo_custkey`, sum(`wlc_test`.`lineorder`.`lo_quantity`) AS `total_quantity`, sum(`wlc_test`.`lineorder`.`lo_revenue`) AS `total_revenue`, count(`wlc_test`.`lineorder`.`lo_shipmode`) AS `shipmode_count` FROM `wlc_test`.`lineorder` GROUP BY `wlc_test`.`lineorder`.`lo_orderkey`, `wlc_test`.`lineorder`.`lo_custkey` ORDER BY `wlc_test`.`lineorder`.`lo_orderkey` ASC ;
         rows: 0
*************************** 3. row ***************************
           id: 477338
         name: lo_mv_sync_2
database_name: wlc_test
         text: CREATE MATERIALIZED VIEW lo_mv_sync_2 REFRESH SYNC AS select lo_orderkey, lo_orderdate, lo_custkey, sum(lo_quantity) as total_quantity, sum(lo_revenue) as total_revenue, count(lo_shipmode) as shipmode_count from lineorder group by lo_orderkey, lo_orderdate, lo_custkey
         rows: 0
*************************** 4. row ***************************
           id: 475992
         name: lo_mv_sync_1
database_name: wlc_test
         text: CREATE MATERIALIZED VIEW lo_mv_sync_1ASselect lo_orderkey, lo_orderdate, lo_custkey, sum(lo_quantity) as total_quantity, sum(lo_revenue) as total_revenue, count(lo_shipmode) as shipmode_countfrom lineorder group by lo_orderkey, lo_orderdate, lo_custkey
         rows: 0
4 rows in set (0.04 sec)
```
