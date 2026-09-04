---
displayed_sidebar: docs
description: "展示所有或指定异步物化视图的信息。"
---

# SHOW MATERIALIZED VIEWS

## 功能

展示所有或指定异步物化视图信息。该语句从 3.0 版本开始更名为 SHOW MATERIALIZED VIEWS，之前版本为 SHOW MATERIALIZED VIEW。

> **注意**
>
> 该命令当前仅针对异步物化视图生效。针对同步物化视图您可以通过 [SHOW ALTER MATERIALIZED VIEW](SHOW_ALTER_MATERIALIZED_VIEW.md) 命令查看当前数据库中同步物化视图的构建状态。
> 该操作不需要权限。

## 语法

```SQL
SHOW MATERIALIZED VIEWS
[FROM db_name]
[
WHERE NAME { = "mv_name" | LIKE "mv_name_matcher"}
]
```

:::note

自 v3.3 起，如果单个物化视图刷新任务包含多个分区或 task_runs，`SHOW MATERIALIZED VIEWS` 语句将追踪所有 task_runs 的状态。仅当所有 task_runs 成功后，`last_refresh_state` 字段才会返回 `SUCCESS`。

:::

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
| refresh_type               | 物化视图的更新方式，有效值：`SYNC`（同步物化视图）和 `ASYNC`（异步物化视图，无论以何种方式触发刷新）。   |
| is_active                  | 物化视图状态是否为 active。有效值：`true` 和 `false`。          |
| inactive_reason            | 物化视图失效的原因。                                          |
| partition_type             | 物化视图的分区类型，包括 RANGE 和 UNPARTITIONED。|
| task_id                    | 物化视图的刷新任务 ID。                                       |
| task_name                  | 物化视图的刷新任务名称。                                       |
| last_refresh_start_time    | 物化视图上一次刷新开始时间。                                    |
| last_refresh_finished_time | 物化视图上一次刷新结束时间。                                    |
| last_refresh_duration      | 物化视图上一次刷新耗时（单位秒）。                               |
| last_refresh_state         | 物化视图上一次刷新的状态，包括 PENDING、RUNNING、FAILED、SUCCESS、SKIPPED。如果未检测到基表分区上的数据发生变化，则会跳过对应物化视图分区的刷新操作。 |
| last_refresh_force_refresh | 物化视图上一次刷新是否为强制（FORCE）刷新。                      |
| last_refresh_start_partition | 上一次刷新开始的物化视图分区。                                |
| last_refresh_end_partition | 上一次刷新结束的物化视图分区。                                  |
| last_refresh_base_refresh_partitions | 上一次刷新基表更新的分区。                            |
| last_refresh_mv_refresh_partitions | 上一次刷新物化视图刷新的分区。                          |
| last_refresh_error_code    | 物化视图上一次刷新失败的 ErrorCode（如果物化视图状态不为 active）。 |
| last_refresh_error_message | 物化视图上一次刷新失败的 ErrorMessage（如果物化视图状态不为 active）。 |
| rows                       | 物化视图中数据行数。                                           |
| text                       | 创建物化视图的查询语句。                                        |
| extra_message              | 最近一次刷新任务的额外信息。                                    |
| query_rewrite_status       | 物化视图的查询改写状态。                                        |
| creator                    | 最近一次刷新任务的创建者。                                      |
| last_refresh_process_time  | 最近一次刷新任务的处理开始时间。                                |
| last_refresh_job_id        | 最近一次刷新任务的作业 ID。                                     |
| last_refresh_time          | 物化视图已反映基表更新的最新时间。                              |
| warehouse                  | 异步物化视图执行刷新任务所使用的 warehouse 名称。在存算一体模式下，或对于同步（rollup）物化视图，该值为空。 |
| refresh_mode               | 异步物化视图配置的刷新模式。有效值：`PCT`（分区变更跟踪，仅刷新发生变更的分区）、`INCREMENTAL`（增量视图维护）和 `AUTO`（尽可能增量，遇到无法构建增量计划的变更时回退为 `PCT`）。对于同步物化视图为空。 |
| refresh_trigger            | 刷新的触发方式。有效值：`NONE`（同步物化视图）、`MANUAL`（仅通过 REFRESH MATERIALIZED VIEW 触发）、`SCHEDULED`（周期性触发，通过 EVERY 间隔）和 `ON_BASE_TABLE_CHANGE`（基表导入或变更时自动触发）。 |
| refresh_policy             | 可读的刷新策略。有效值：`NONE`、`MANUAL`、`ON_BASE_TABLE_CHANGE`，或形如 `START("yyyy-MM-dd HH:mm:ss") EVERY(INTERVAL n unit)` 的调度（仅当定义了起始时间时才包含 `START` 子句）。 |
| resource_group             | 物化视图刷新任务所使用的资源组（来自物化视图的 `resource_group` 属性）。未设置时默认为 `default_mv_wg`。 |
| query_rewrite_status_reason | `query_rewrite_status` 的原因。有效值：`OK`、`MV_INACTIVE`、`QUERY_REWRITE_DISABLED`、`UNSUPPORTED_DEFINITION` 和 `UNKNOWN`。 |
| last_freshness_confirmed_at | 最近一次成功刷新的开始时间，在整次刷新（其全部 task run）完成后才记录；确认基表无变化、无需刷新的刷新同样会确认新鲜度。物化视图反映该时刻的基表数据。区别于 `last_refresh_time`（基表数据版本时间），这是墙钟时间。首次成功刷新前、以及同步物化视图，为空。按分区范围的 REFRESH（部分刷新）不推进该值。 |
| base_table_refresh_version_times | 各基表的数据版本时间，以 JSON 对象给出：键为基表的 `catalog.database.table` 名称，值为观测到的最新数据版本时间。这是 `last_refresh_time`（所有基表的单一最大值）背后的按表明细：外部/数据湖基表上报分区源修改时间，OLAP（内部）基表上报可见版本提交时间。无任何基表有可记录时间时为 `{}`。 |
| effective_refresh_mode | 该物化视图实际建成的刷新模式。有效值与 `refresh_mode` 相同:`PCT`、`INCREMENTAL`、`AUTO`。它通常与 `refresh_mode` 一致,只有一种情况例外 —— `refresh_mode` 为 `AUTO`,但定义无法增量维护,`CREATE` 于是建成了 `PCT` 物化视图,此列即为 `PCT`。该判定在建表时一次完成且永不改变:只能通过重建物化视图重新尝试增量。同步物化视图为空。 |
| effective_refresh_mode_reason | `effective_refresh_mode` 与 `refresh_mode` 不同的原因 —— 即该定义无法增量维护的具体说明。建表时记录,之后不再更新。两个模式列一致时为空。 |
| last_executed_refresh_mode | 最近一次刷新实际使用的刷新模式。有效值:`PCT`、`INCREMENTAL`。当 `refresh_mode` 与 `effective_refresh_mode` 都为 `AUTO` 时,此列为 `PCT` 表示只有那一次刷新发生了回退,后续刷新仍可回到增量;而若 `effective_refresh_mode` 为 `PCT`,则表示该物化视图从不尝试增量。因基表无变化而被跳过的刷新不会改变此列的值。首次刷新之前,以及同步物化视图,为空。 |
| last_refresh_mode_reason | 最近一次刷新为什么以 `last_executed_refresh_mode` 的方式执行,而没有走增量刷新。未发生模式降级时为空。取值:`NON_APPEND_ONLY_CHANGE`(基表发生了非 append-only 变更,例如删除分区、清空分区、覆盖写入、外表删除或行级删除)、`BASELINE_UNREACHABLE`(记录的基线已不是表当前 head 的祖先 —— 快照过期，或表被回滚、被替换)、`BASELINE_MISSING`(根本没有可读取增量的基线:首次刷新,或元数据修复之后)、`CHANGE_CAPTURE_DISABLED`(窗口内某个版本是在该基表关闭变更捕获期间产生的)、`FORCE_REFRESH`(强制刷新)、`UNKNOWN`(以上都无法归类的回退;原因在 FE 日志里,不在 `ERROR_MESSAGE` —— 回退本身成功时该字段为空)。 |
| last_refresh_mode_reason_table | 导致该模式决策的基表,格式为 `catalog.database.table`。没有单一基表导致时为空:`FORCE_REFRESH` 源自请求本身而非某张表,由 BE 在读取变更时报出的原因定位到的是 tablet 而非表。 |

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
"replication_num" = "3",
"storage_format" = "DEFAULT"
);

-- Create MV: customer_mv
CREATE MATERIALIZED VIEW customer_mv
DISTRIBUTED BY HASH(c_custkey) buckets 10
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

示例一：通过精确匹配查看特定物化视图

```Plain
mysql> show materialized views  where name='customer_mv'\G
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
DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 10
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

示例二：通过模糊匹配查看物化视图

```Plain
mysql> show materialized views  where name like 'customer_mv'\G
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
DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 10
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
