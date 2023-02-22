# SHOW MATERIALIZED VIEW

## Description

Shows all or one specific materialized view.

## Syntax

```SQL
SHOW MATERIALIZED VIEW
[FROM db_name]
[
WHERE NAME { = "mv_name" | LIKE "mv_name_matcher"}
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
| name                       | The name of the materialized view.                           |
| database_name              | The name of the database in which the materialized view resides. |
| refresh_type               | The refresh type of the materialized view, including ROLLUP, MANNUL, ASYNC, and INCREMENTAL. |
| is_active                  | Whether the materialized view state is active. Valid Value: `true` and `false`. |
| last_refresh_start_time    | The start time of the last refresh of the materialized view. |
| last_refresh_finished_time | The end time of the last refresh of the materialized view.   |
| last_refresh_duration      | The time taken by the last refresh. Unit: seconds.           |
| last_refresh_state         | The status of the last refresh, including PENDING, RUNNING, FAILED, and SUCCESS. |
| inactive_code              | The error code for the last failed refresh of the materialized view (if the materialized view state is not active). |
| inactive_reason            | The reason why the last refresh failed (if the materialized view state is not active). |
| text                       | The statement used to create the materialized view.          |
| rows                       | The number of data rows in the materialized view.            |

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
DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 10
PROPERTIES (
"replication_num" = "1",
"in_memory" = "false",
"storage_format" = "DEFAULT"
);

-- Create MV: customer_mv
create materialized view customer_mv
DISTRIBUTED BY HASH(c_custkey) buckets 10
REFRESH MANUAL
PROPERTIES (
    "replication_num" = "1"
)
as select
              c_custkey, c_phone, c_acctbal, count(1) as c_count, sum(c_acctbal) as c_sum
   from
              customer
   group by c_custkey, c_phone, c_acctbal;

-- Create MV: customer_mv
CREATE MATERIALIZED VIEW test_customer_mv
DISTRIBUTED BY HASH(c_custkey) buckets 10
REFRESH MANUAL
PROPERTIES (
    "replication_num" = "1"
)
AS SELECT
              c_custkey, c_phone, c_acctbal, count(1) AS c_count, sum(c_acctbal) AS c_sum
   FROM
              customer
   GROUP BY c_custkey, c_phone, c_acctbal; 

REFRESH MATERIALIZED VIEW customer_mv;
```

Example 1: Show a specific materialized view.

```Plain
mysql> SHOW MATERIALIZED VIEW WHERE NAME='customer_mv'\G;
*************************** 1. row ***************************
                        id: 10142
                      name: customer_mv
             database_name: test
              refresh_type: MANUAL
                 is_active: true
   last_refresh_start_time: 2023-02-17 10:27:33
last_refresh_finished_time: 2023-02-17 10:27:33
     last_refresh_duration: 0
        last_refresh_state: SUCCESS
             inactive_code: 0
           inactive_reason:
                      text: CREATE MATERIALIZED VIEW `customer_mv`
COMMENT "MATERIALIZED_VIEW"
DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 10
REFRESH MANUAL
PROPERTIES (
"replication_num" = "1",
"storage_medium" = "HDD"
)
AS SELECT `customer`.`c_custkey`, `customer`.`c_phone`, `customer`.`c_acctbal`, count(1) AS `c_count`, sum(`customer`.`c_acctbal`) AS `c_sum`
FROM `test`.`customer`
GROUP BY `customer`.`c_custkey`, `customer`.`c_phone`, `customer`.`c_acctbal`;
                      rows: 0
1 row in set (0.11 sec)
```

Example 2: Show materialized views by matching the name.

```Plain
mysql> SHOW MATERIALIZED VIEW WHERE NAME LIKE 'customer_mv'\G;
*************************** 1. row ***************************
                        id: 10142
                      name: customer_mv
             database_name: test
              refresh_type: MANUAL
                 is_active: true
   last_refresh_start_time: 2023-02-17 10:27:33
last_refresh_finished_time: 2023-02-17 10:27:33
     last_refresh_duration: 0
        last_refresh_state: SUCCESS
             inactive_code: 0
           inactive_reason:
                      text: CREATE MATERIALIZED VIEW `customer_mv`
COMMENT "MATERIALIZED_VIEW"
DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 10
REFRESH MANUAL
PROPERTIES (
"replication_num" = "1",
"storage_medium" = "HDD"
)
AS SELECT `customer`.`c_custkey`, `customer`.`c_phone`, `customer`.`c_acctbal`, count(1) AS `c_count`, sum(`customer`.`c_acctbal`) AS `c_sum`
FROM `test`.`customer`
GROUP BY `customer`.`c_custkey`, `customer`.`c_phone`, `customer`.`c_acctbal`;
                      rows: 0
1 row in set (0.12 sec)
```
