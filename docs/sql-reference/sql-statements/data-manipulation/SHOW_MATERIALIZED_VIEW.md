# SHOW MATERIALIZED VIEW

## Description

Shows all or one specific materialized view.

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

| **Return**    | **Description**                                              |
| ------------- | ------------------------------------------------------------ |
| id            | The ID of the materialized view.                             |
| name          | The name of the materialized view.                           |
| database_name | The name of the database to which the materialized view resides. |
| text          | The statement used to create the materialized view.          |
| rows          | The rows of data in the materialized view.                   |

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
