---
displayed_sidebar: "English"
---

# SHOW CREATE MATERIALIZED VIEW

## Description

Shows the definition of a specific asynchronous materialized view.

## Syntax

```SQL
SHOW CREATE MATERIALIZED VIEW [database.]<mv_name>
```

Parameters in brackets [] is optional.

## Parameters

| **Parameter** | **Required** | **Description**                            |
| ------------- | ------------ | ------------------------------------------ |
| mv_name       | yes          | The name of the materialized view to show. |

## Returns

| **Return**               | **Description**                          |
| ------------------------ | ---------------------------------------- |
| Materialized View        | The name of the materialized view.       |
| Create Materialized View | The definition of the materialized view. |

## Examples

Example 1: Show the definition of a specific materialized view

```Plain
MySQL > SHOW CREATE MATERIALIZED VIEW lo_mv1\G
*************************** 1. row ***************************
       Materialized View: lo_mv1
Create Materialized View: CREATE MATERIALIZED VIEW `lo_mv1`
COMMENT "MATERIALIZED_VIEW"
DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 10 
REFRESH ASYNC
PROPERTIES (
"replication_num" = "3",
"storage_medium" = "HDD"
)
AS SELECT `wlc_test`.`lineorder`.`lo_orderkey` AS `lo_orderkey`, `wlc_test`.`lineorder`.`lo_custkey` AS `lo_custkey`, sum(`wlc_test`.`lineorder`.`lo_quantity`) AS `total_quantity`, sum(`wlc_test`.`lineorder`.`lo_revenue`) AS `total_revenue`, count(`wlc_test`.`lineorder`.`lo_shipmode`) AS `shipmode_count` FROM `wlc_test`.`lineorder` GROUP BY `wlc_test`.`lineorder`.`lo_orderkey`, `wlc_test`.`lineorder`.`lo_custkey` ORDER BY `wlc_test`.`lineorder`.`lo_orderkey` ASC ;
1 row in set (0.01 sec)
```
