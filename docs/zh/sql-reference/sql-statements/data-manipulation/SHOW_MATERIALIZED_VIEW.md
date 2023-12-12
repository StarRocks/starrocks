---
displayed_sidebar: "Chinese"
---

# SHOW MATERIALIZED VIEWS

## 功能

展示所有或指定异步物化视图信息。该语句从 3.0 版本开始更名为 SHOW MATERIALIZED VIEWS，之前版本为 SHOW MATERIALIZED VIEW。

> **注意**
>
> 该命令当前仅针对异步物化视图生效。针对同步物化视图您可以通过 [SHOW ALTER MATERIALIZED VIEW](../data-manipulation/SHOW_ALTER_MATERIALIZED_VIEW.md) 命令查看当前数据库中同步物化视图的构建状态。
> 该操作不需要权限。

## 语法

```SQL
SHOW MATERIALIZED VIEWS
[FROM db_name]
[
WHERE NAME { = "mv_name" | LIKE "mv_name_matcher"}
]
```

## 参数

| **参数**        | **必选** | **说明**                                                     |
| --------------- | -------- | ------------------------------------------------------------ |
| db_name         | 否       | 物化视图所属的数据库名称。如果不指定该参数，则默认使用当前数据库。 |
| mv_name         | 否       | 用于精确匹配的物化视图名称。                                 |
| mv_name_matcher | 否       | 用于模糊匹配的物化视图名称 matcher。                         |

## 返回

返回最近一次 REFRESH 任务的状态。

| **返回**                   | **说明**                                                    |
| -------------------------- | --------------------------------------------------------- |
| id                         | 物化视图 ID。                                               |
| database_name              | 物化视图所属的数据库名称。                                     |
| name                       | 物化视图名称。                                               |
| refresh_type               | 物化视图的更新方式，包括 ROLLUP、MANUAL、ASYNC、INCREMENTAL。   |
| is_active                  | 物化视图状态是否为 active。有效值：`true` 和 `false`。          |
| partition_type             | 物化视图的分区类型，包括 RANGE 和 UNPARTITIONED。|
| task_id                    | 物化视图的刷新任务 ID。                                       |
| task_name                  | 物化视图的刷新任务名称。                                       |
| last_refresh_start_time    | 物化视图上一次刷新开始时间。                                    |
| last_refresh_finished_time | 物化视图上一次刷新结束时间。                                    |
| last_refresh_duration      | 物化视图上一次刷新耗时（单位秒）。                               |
| last_refresh_state         | 物化视图上一次刷新的状态，包括 PENDING、RUNNING、FAILED、SUCCESS。 |
| last_refresh_force_refresh | 物化视图上一次刷新是否为强制（FORCE）刷新。                      ｜
| last_refresh_start_partition | 上一次刷新开始的物化视图分区。                                ｜
| last_refresh_end_partition | 上一次刷新结束的物化视图分区。                                  ｜
| last_refresh_base_refresh_partitions | 上一次刷新基表更新的分区。                            ｜
| last_refresh_mv_refresh_partitions | 上一次刷新物化视图刷新的分区。                          ｜
| last_refresh_error_code    | 物化视图上一次刷新失败的 ErrorCode（如果物化视图状态不为 active）。 |
| last_refresh_error_message | 物化视图上一次刷新失败的 ErrorMessage（如果物化视图状态不为 active）。 |
| rows                       | 物化视图中数据行数。                                           |
| text                       | 创建物化视图的查询语句。                                        |

## 示例

以下示例基于当前业务情景：

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
"storage_format" = "DEFAULT"
);

-- Create MV: customer_mv
CREATE MATERIALIZED VIEW customer_mv
DISTRIBUTED BY HASH(c_custkey) buckets 10
REFRESH MANUAL
PROPERTIES (
    "replication_num" = "1"
)
AS SELECT
              c_custkey, c_phone, c_acctbal, count(1) as c_count, sum(c_acctbal) as c_sum
   FROM
              customer
   GROUP BY c_custkey, c_phone, c_acctbal;

-- Refresh the MV
REFRESH MATERIALIZED VIEW customer_mv;
```

示例一：通过精确匹配查看特定物化视图

```Plain
mysql> show materialized views  where name='customer_mv'\G;
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

示例二：通过模糊匹配查看物化视图

```Plain
mysql> show materialized views  where name like 'customer_mv'\G;
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
