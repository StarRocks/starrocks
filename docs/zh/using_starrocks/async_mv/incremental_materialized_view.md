---
displayed_sidebar: docs
description: "介绍如何在原生表上创建、使用、刷新和监控增量物化视图，以增量大小的刷新成本获得新鲜的查询结果。"
sidebar_position: 2
keywords: ['zengliang', 'wuhua', 'IMV', 'zengliangshuaxin']
---

# 增量物化视图

本文介绍如何在存算分离集群中创建、使用、刷新、监控和排查增量物化视图（IMV）的问题。

增量物化视图是异步物化视图的一种**严格增量刷新模式**：首次刷新会建立一份完整的基线数据，此后的每次刷新只消费自上次成功刷新以来发生的变更，并将结果合并到物化视图中。如果某个变更或查询形态无法被安全地增量维护，系统会显式返回错误，而不会静默回退为全量重新计算。

:::note

- 增量物化视图仅支持存算分离集群中的云原生表和 Iceberg 只追加（append-only）外部表，不支持存算一体集群。
- 增量物化视图不参与自动查询改写，您必须直接查询该物化视图。
- Incremental View Maintenance（IVM）是源代码和错误信息中使用的内部名称，而 IMV 是系统表字段和产品文档中使用的名称，两者指代同一能力。
:::

## 背景介绍

### 为什么需要增量物化视图

异步物化视图默认使用 PCT（Partition Change Tracking，分区变更跟踪）刷新方式。当基表某个分区中的数据发生变化时，整个分区都会被重新计算。

这会带来一个结构性问题：**刷新成本与受影响分区中的数据总量成正比，而不是与变更的数据量成正比**。例如，如果某个天级分区包含 3000 万行数据，即使只新增 1 万行，也需要重新计算全部 3000 万行。随着历史数据不断累积，刷新耗时会持续增加。最终，您可能不得不降低刷新频率以控制成本，但这是以牺牲数据新鲜度为代价的。

增量物化视图可以拉平这条曲线：**刷新成本只取决于当前批次变更的数据量，与历史数据总量解耦**。

增量物化视图在以下场景中尤为有用：

- 现有异步物化视图的刷新成本随数据量持续增长。
- 对数据新鲜度要求严格的仪表盘。
- 基表以追加写入为主的大型明细表上的运营、广告、交易或物联网聚合报表。

### 与其他物化视图的区别

|                                | 单表聚合                        | 多表关联      | 查询改写 | 刷新成本                                | 刷新策略                                             | 基表                                                  |
| ------------------------------ | ------------------------------- | --------------------- | ------------- | ------------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 增量物化视图  | 支持（有限制）   | 仅支持 INNER/CROSS JOIN | 不支持 | ∝ 当前批次的变更量                  | <ul><li>基表变更触发</li><li>定时刷新</li><li>手动刷新</li></ul> | 存算分离原生表（DUPLICATE / PRIMARY / AGGREGATE KEY），Iceberg 只追加表 |
| 异步物化视图 | 支持                       | 支持             | 支持     | ∝ 受影响分区的数据总量         | <ul><li>定时刷新</li><li>手动刷新</li></ul>   | 支持多表定义；可以基于 Default Catalog、External Catalog、已有物化视图和已有视图构建 |
| 同步物化视图  | 仅部分聚合函数 | 不支持         | 支持     | 随数据导入同步刷新 | 导入过程中同步刷新                         | 仅支持基于 Default Catalog 的单表定义   |

### 相关概念

- **变更数据捕获（Change Data Capture，CDC）**：一种从存算分离表的 tablet 元数据中派生行级变更的机制。DUPLICATE KEY 和 AGGREGATE KEY 表可以原生派生变更，无需额外配置或写入开销。PRIMARY KEY 表必须显式设置表属性 `enable_change_data_capture = 'true'`，以便在每次导入操作期间记录用于定位变更所需的轻量级元数据。

- **增量窗口（Delta window）**：一次增量刷新所消费的变更范围，表示为 `(上一次刷新位点, 当前位点]`。对于原生表，位点是 **Bookmark ID**；对于 Iceberg 表，位点是 snapshot ID。增量窗口是连续的，不存在缺口或重叠。

- **Bookmark**：物化视图为某张原生表持有的一个版本引用，用于确保增量刷新始终从一致的历史版本读取数据。代价在于：**在旧的基表文件仍被引用期间，无法被回收**。参见[成本与开销](#成本与开销)。

- **净变更（Net change）**：当同一行在一个窗口内被多次修改时，只保留“窗口开始时该值的删除 + 窗口结束时该值的插入”这一对变更。在同一窗口内被创建又被删除的行则完全不会出现。增量刷新**始终消费净变更**。

- **撤回（Retraction）**：将基表上的 `DELETE` / `UPDATE` 操作反映到物化视图中。由于物化视图始终是 PRIMARY KEY 表，因此可以使用 UPSERT / DELETE 语义精确更新对应的行。

### 主要限制

为了保证严格的增量语义，增量物化视图在多个方面存在限制。在采用该特性之前，请在方案设计阶段确认这些限制。完整列表请参见[能力边界](#能力边界)。

- **不参与查询改写**

  无论查询改写相关的配置如何设置，改写功能始终处于禁用状态，也无法通过开关启用。如果您希望在不修改现有业务 SQL 的情况下获得加速效果，需要评估迁移成本：所有消费方都必须显式查询该物化视图。如果这一成本过高，请继续使用异步物化视图（PCT）。

- **仅在存算分离集群中支持**

  存算一体集群不支持该特性，且没有变通方案。在存算一体集群上实现实时报表加速，需要重新规划集群架构，或者继续使用异步物化视图（PCT）。

- **分区级或表级的破坏性操作会永久中断增量链**

  `INSERT OVERWRITE`、`DROP PARTITION` 和 `TRUNCATE` 会导致刷新失败并使物化视图被置为未激活状态。此时必须删除并重新创建该物化视图。如果基表存在按计划执行的、基于保留策略的 `DROP PARTITION` 作业，或使用 `INSERT OVERWRITE` 进行数据回填，请先设计与之兼容的工作流，例如将受影响的分区范围隔离开来，或者接受周期性地重建物化视图。**在已启用 CDC 的 PRIMARY KEY 基表上执行行级 `DELETE` / `UPDATE` 是正常支持的**，不受此限制影响。

- **SQL 支持范围有限**

  OUTER / SEMI / ANTI JOIN、窗口函数、CTE、多层嵌套子查询、`LIMIT`、包含聚合的 `HAVING`、`GROUPING SETS` / `ROLLUP` / `CUBE`、不带 `GROUP BY` 的全局聚合，以及其他不受支持的算子都会被拒绝。仅有 13 个聚合函数在允许列表中。如果报表使用了 `count(distinct)`、`stddev`、`variance`、`percentile_approx`、`min_by` 等函数，需要改写查询。精确去重计数可使用 `bitmap_union(to_bitmap(col))`，近似去重计数可使用 `hll_union(hll_hash(col))`。百分位数、标准差等开销较高的统计量，通常应保留在针对原始表的下钻查询中。不受支持的查询应继续使用异步物化视图（PCT）。

- **基表类型受限**

  不支持 UNIQUE KEY 表。AGGREGATE KEY 表仅支持严格上卷。物化视图不能作为基表使用（即不支持 MV on MV）。如需在 UNIQUE KEY 表上实现加速，请考虑迁移到 PRIMARY KEY 模型。现有的多层物化视图链路必须被拉平，使每个物化视图都直接引用基表。

- **无法原地在 PCT 与 INCREMENTAL 之间切换**

  无法通过 `ALTER` 跨模式修改 `refresh_mode`。您不能“先低成本试用，之后再切回去”。在变更生产环境的工作负载之前，请先在测试环境中或使用影子物化视图验证方案。

## 创建增量物化视图

### 语法

要创建增量物化视图，需要在 `PROPERTIES` 中将 `refresh_mode` 指定为 `INCREMENTAL`：

```SQL
CREATE MATERIALIZED VIEW <mv_name>
[PARTITION BY <expr>]
[DISTRIBUTED BY HASH(<col>) [BUCKETS <n>]]
REFRESH [IMMEDIATE | DEFERRED]
    { ASYNC
    | { ASYNC | SCHEDULE } [START('<start_time>')] EVERY(INTERVAL <n> <unit>)
    | MANUAL }
PROPERTIES ("refresh_mode" = "INCREMENTAL")
AS <query>;
```

:::note

- `refresh_mode` 的有效取值为 `PCT`（默认值）和 `INCREMENTAL`。其他任何取值都会返回 `Invalid refresh_mode: <v>. Only INCREMENTAL, PCT are supported.`
- `START('<start_time>')` **只能与 `EVERY(...)` 一起使用**，不能单独出现。
- `IMMEDIATE` / `DEFERRED` 作用于整个刷新配置，包括 `ASYNC`，并不限于 `MANUAL`。
- 如果使用 PRIMARY KEY 表作为基表，必须通过将表属性 `enable_change_data_capture` 设置为 `true` 来启用 CDC。
:::

### 示例 1：单表过滤与投影（DUPLICATE KEY 基表）

```SQL
CREATE TABLE base_dup (
    k INT,
    v INT
) DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 1;

INSERT INTO base_dup VALUES (1, 10), (2, 20), (3, 30);

CREATE MATERIALIZED VIEW mv_dup
DISTRIBUTED BY HASH(k) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES ("refresh_mode" = "INCREMENTAL")
AS SELECT k, v FROM base_dup WHERE v > 15;

REFRESH MATERIALIZED VIEW mv_dup WITH SYNC MODE;
SELECT k, v FROM mv_dup ORDER BY k;
```

```Plain
+------+------+
| k    | v    |
+------+------+
|    2 |   20 |
|    3 |   30 |
+------+------+
```

在插入更多数据后，只有满足谓词条件的新增行会被增量写入：

```SQL
INSERT INTO base_dup VALUES (4, 40), (5, 50), (6, 5);
REFRESH MATERIALIZED VIEW mv_dup WITH SYNC MODE;
SELECT k, v FROM mv_dup ORDER BY k;
```

```Plain
+------+------+
| k    | v    |
+------+------+
|    2 |   20 |
|    3 |   30 |
|    4 |   40 |
|    5 |   50 |
+------+------+
```

### 示例 2：单表聚合（DUPLICATE KEY 基表）

以下示例覆盖了大部分允许使用的聚合函数：

```SQL
CREATE TABLE base_orders (
    order_id       BIGINT,
    region         VARCHAR(8),
    amount         DECIMAL(10,2),
    score          BIGINT,
    nullable_score INT,
    flag           BOOLEAN,
    uid            INT
) DUPLICATE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 1;

INSERT INTO base_orders VALUES
    (1001, 'CN', 99.90, 10,    5, FALSE,  1),
    (1002, 'US', 20.50, 20, NULL, FALSE, 10),
    (1003, 'CN', 15.50, 30, NULL, TRUE,   1),
    (1004, 'US', 75.00, 40,    8, FALSE, 10);

CREATE MATERIALIZED VIEW mv_orders
DISTRIBUTED BY HASH(region) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES ("refresh_mode" = "INCREMENTAL")
AS SELECT region,
          SUM(amount)                AS s,
          COUNT(*)                   AS c_all,
          COUNT(nullable_score)      AS c_non_null,
          AVG(amount)                AS avg_dec,
          MIN(amount)                AS mn,
          MAX(amount)                AS mx,
          APPROX_COUNT_DISTINCT(uid) AS acd,
          BOOL_OR(flag)              AS any_flag
   FROM base_orders
   GROUP BY region;

REFRESH MATERIALIZED VIEW mv_orders WITH SYNC MODE;

-- 新增一个 JP 分组，并向已有的 CN 分组追加数据
INSERT INTO base_orders VALUES
    (1005, 'JP', 100.00, 50,   12, FALSE, 99),
    (1006, 'CN',  25.00, 60, NULL, FALSE,  2);
REFRESH MATERIALIZED VIEW mv_orders WITH SYNC MODE;
SELECT * FROM mv_orders ORDER BY region;
```

只有 `CN` 和 `JP` 分组会被重新计算，`US` 分组不受影响。

### 示例 3：INNER JOIN（两张 DUPLICATE KEY 基表）

```SQL
CREATE TABLE orders (
    order_id BIGINT, uid BIGINT, amount DECIMAL(10,2)
) DUPLICATE KEY(order_id) DISTRIBUTED BY HASH(order_id) BUCKETS 1;

CREATE TABLE users (
    user_id BIGINT, country VARCHAR(8)
) DUPLICATE KEY(user_id) DISTRIBUTED BY HASH(user_id) BUCKETS 1;

INSERT INTO users  VALUES (1, 'CN'), (2, 'US');
INSERT INTO orders VALUES (1001, 1, 99.90), (1002, 2, 20.50);

CREATE MATERIALIZED VIEW mv_join
DISTRIBUTED BY HASH(order_id) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES ("refresh_mode" = "INCREMENTAL")
AS SELECT o.order_id, u.country, o.amount
   FROM orders o JOIN users u ON o.uid = u.user_id;

REFRESH MATERIALIZED VIEW mv_join WITH SYNC MODE;

-- 同时增量更新维度表和事实表
INSERT INTO users  VALUES (3, 'JP');
INSERT INTO orders VALUES (1003, 1, 15.50), (1004, 3, 200.00);
REFRESH MATERIALIZED VIEW mv_join WITH SYNC MODE;
SELECT * FROM mv_join ORDER BY order_id;
```

```Plain
+----------+---------+--------+
| order_id | country | amount |
+----------+---------+--------+
|     1001 | CN      |  99.90 |
|     1002 | US      |  20.50 |
|     1003 | CN      |  15.50 |
|     1004 | JP      | 200.00 |
+----------+---------+--------+
```

### 示例 4：使用 Bitmap 进行精确去重计数

```SQL
CREATE TABLE uv_base (
    k INT, uid INT
) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 2;

INSERT INTO uv_base VALUES (1, 10), (1, 20), (1, 20), (2, 30);

CREATE MATERIALIZED VIEW mv_uv
DISTRIBUTED BY HASH(k) BUCKETS 2
REFRESH DEFERRED MANUAL
PROPERTIES ("refresh_mode" = "INCREMENTAL")
AS SELECT k, bitmap_union(to_bitmap(uid)) AS uv FROM uv_base GROUP BY k;

REFRESH MATERIALIZED VIEW mv_uv WITH SYNC MODE;

INSERT INTO uv_base VALUES (1, 30), (2, 30), (2, 40);
REFRESH MATERIALIZED VIEW mv_uv WITH SYNC MODE;

SELECT k, bitmap_count(uv) AS dc FROM mv_uv ORDER BY k;
```

```Plain
+------+------+
| k    | dc   |
+------+------+
|    1 |    3 |
|    2 |    2 |
+------+------+
```

该结果与 `count(distinct)` 等价：

```SQL
SELECT COUNT(*) FROM (
    SELECT k, bitmap_count(uv) FROM mv_uv
    EXCEPT SELECT k, COUNT(DISTINCT uid) FROM uv_base GROUP BY k) t;   -- 0
```

如需近似去重计数，将 `bitmap_union(to_bitmap(uid))` 替换为 `hll_union(hll_hash(uid))`，并在查询时使用 `hll_cardinality()`。

### 示例 5：AGGREGATE KEY 基表上的严格上卷

```SQL
CREATE TABLE base_agg (
    region VARCHAR(8),
    city   VARCHAR(8),
    s      BIGINT SUM,
    mx     BIGINT MAX,
    mn     BIGINT MIN
) AGGREGATE KEY(region, city)
DISTRIBUTED BY HASH(region) BUCKETS 1;

INSERT INTO base_agg VALUES ('CN','BJ',10,100,5), ('CN','BJ',20,50,8), ('US','NY',30,200,1);

CREATE MATERIALIZED VIEW mv_agg
DISTRIBUTED BY HASH(region) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES ("refresh_mode" = "INCREMENTAL")
AS SELECT region, city, SUM(s) AS s_sum, MAX(mx) AS mx_max, MIN(mn) AS mn_min
   FROM base_agg
   GROUP BY region, city;

REFRESH MATERIALIZED VIEW mv_agg WITH SYNC MODE;

INSERT INTO base_agg VALUES ('CN','BJ',5,150,2), ('JP','TK',7,7,7);
REFRESH MATERIALIZED VIEW mv_agg WITH SYNC MODE;
SELECT * FROM mv_agg ORDER BY region, city;
```

```Plain
+--------+------+-------+--------+--------+
| region | city | s_sum | mx_max | mn_min |
+--------+------+-------+--------+--------+
| CN     | BJ   |    35 |    150 |      2 |
| JP     | TK   |     7 |      7 |      7 |
| US     | NY   |    30 |    200 |      1 |
+--------+------+-------+--------+--------+
```

`GROUP BY region`（上卷到键列的子集）同样合法。但是，除 `GROUP BY region, city` 之外的任何表达式，以及 `COUNT(*)` / `AVG()`，都会被拒绝。

### 示例 6：PRIMARY KEY 基表上的 UPDATE / DELETE 撤回

这是原生表上增量物化视图最具差异化的能力之一。

```SQL
CREATE TABLE pk_base (
    id INT NOT NULL,
    g  INT,
    v  BIGINT
) PRIMARY KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES ('enable_change_data_capture' = 'true');

INSERT INTO pk_base VALUES (1,10,100),(2,10,200),(3,20,300),(4,20,400),(5,30,500);

CREATE MATERIALIZED VIEW mv_pk
DISTRIBUTED BY HASH(g) BUCKETS 2
REFRESH DEFERRED MANUAL
PROPERTIES ("refresh_mode" = "INCREMENTAL")
AS SELECT g, SUM(v) AS sv, MAX(v) AS mx, MIN(v) AS mn, COUNT(*) AS c
   FROM pk_base
   GROUP BY g;

REFRESH MATERIALIZED VIEW mv_pk WITH SYNC MODE;
SELECT * FROM mv_pk ORDER BY g;
```

```Plain
+------+------+------+------+------+
| g    | sv   | mx   | mn   | c    |
+------+------+------+------+------+
|   10 |  300 |  200 |  100 |    2 |
|   20 |  700 |  400 |  300 |    2 |
|   30 |  500 |  500 |  500 |    1 |
+------+------+------+------+------+
```

`UPDATE` 会正确撤回旧值并重新计算 `MAX`：

```SQL
UPDATE pk_base SET v = 250 WHERE id = 2;
REFRESH MATERIALIZED VIEW mv_pk WITH SYNC MODE;
-- g=10 变为 sv=350, mx=250, mn=100, c=2
```

`DELETE` 会正确移除对应行。如果整个分组变为空，物化视图中相应的行也会被移除：

```SQL
DELETE FROM pk_base WHERE id = 4;
REFRESH MATERIALIZED VIEW mv_pk WITH SYNC MODE;
-- g=20 变为 sv=300, mx=300, mn=300, c=1

DELETE FROM pk_base WHERE id = 5;
REFRESH MATERIALIZED VIEW mv_pk WITH SYNC MODE;
-- g=30 对应的行被完全移除
```

当分组列被修改、行在分组之间迁移时，新旧两个分组都会被更新：

```SQL
UPDATE pk_base SET g = 20 WHERE id = 1;
REFRESH MATERIALIZED VIEW mv_pk WITH SYNC MODE;
```

### 示例 7：带基表变更触发的分区物化视图

```SQL
CREATE TABLE events (
    id BIGINT, dt DATE, v BIGINT
) DUPLICATE KEY(id, dt)
PARTITION BY RANGE(dt) (
    PARTITION p1 VALUES LESS THAN ('2026-02-01'),
    PARTITION p2 VALUES LESS THAN ('2026-03-01'),
    PARTITION p3 VALUES LESS THAN ('2026-04-01')
)
DISTRIBUTED BY HASH(id) BUCKETS 2;

CREATE MATERIALIZED VIEW mv_events
PARTITION BY dt
DISTRIBUTED BY HASH(id) BUCKETS 2
REFRESH ASYNC                                   -- 每次基表导入后自动触发增量刷新
PROPERTIES ("refresh_mode" = "INCREMENTAL")
AS SELECT id, dt, v FROM events;

INSERT INTO events VALUES (1,'2026-01-10',10), (2,'2026-02-10',20);
-- 无需手动执行 REFRESH，刷新会在导入事务提交后自动触发。
```

验证触发模式：

```SQL
SELECT TABLE_NAME, REFRESH_TRIGGER, REFRESH_POLICY
FROM information_schema.materialized_views
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'mv_events';
```

```Plain
+-----------+----------------------+----------------------+
| TABLE_NAME| REFRESH_TRIGGER      | REFRESH_POLICY       |
+-----------+----------------------+----------------------+
| mv_events | ON_BASE_TABLE_CHANGE | ON_BASE_TABLE_CHANGE |
+-----------+----------------------+----------------------+
```

## 刷新增量物化视图

### 首次刷新是一次完整基线

**首次刷新会执行一次完整的 PCT 全量初始化（bootstrap）**，以建立增量的起始点。真正的增量刷新从第二次刷新开始。

因此，在 `information_schema.task_runs` 中同时看到 `PCT SUCCESS` 和 `INCREMENTAL SUCCESS` 是**预期行为**，而不是错误：

```SQL
SELECT DISTINCT get_json_string(EXTRA_MESSAGE, '$.refreshMode') AS executed_mode, STATE
FROM information_schema.task_runs
WHERE TASK_NAME = (SELECT TASK_NAME FROM information_schema.materialized_views
                   WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '<mv>')
  AND get_json_double(EXTRA_MESSAGE, '$.processStartTime') > 0
ORDER BY 1, 2;
```

```Plain
+---------------+---------+
| executed_mode | STATE   |
+---------------+---------+
| INCREMENTAL   | SUCCESS |
| PCT           | SUCCESS |
+---------------+---------+
```

初始化期间的并发写入不会被重复计数。系统会将读取固定（pin）在一个冻结的位点上，以提供精确一次（exactly-once）语义。

### 三种刷新触发模式

共有三种刷新触发模式：

**基表变更触发**

- **DDL 语法**：不带 `EVERY` 的 `REFRESH ASYNC`
- **说明**：每次基表事务提交后自动触发一次增量刷新。**该模式仅适用于原生表**，Iceberg 基表不支持。可以使用物化视图属性 `excluded_trigger_tables` 排除特定的基表。

**定时刷新**

- **DDL 语法**：`REFRESH ASYNC [START('<t>')] EVERY(INTERVAL <n> <unit>)`
- **说明**：最小刷新间隔由 FE 配置项 `materialized_view_min_refresh_interval` 控制（默认 60 秒）。

**手动刷新**

- **DDL 语法**：`REFRESH [DEFERRED] MANUAL` + `REFRESH MATERIALIZED VIEW <mv>`
- **说明**：添加 `WITH SYNC MODE` 可以同步等待刷新完成。


### 无变更时跳过刷新

如果所有基表都没有发生变化，则不会生成刷新计划。任务运行状态为 `SKIPPED`，原因为 `MV_UP_TO_DATE`，但新鲜度时间戳仍会向前推进。

### 事务与重试

增量刷新本质上是执行以下语句：

```SQL
INSERT INTO <mv> SELECT <incrementally rewritten query>
```

**推进增量位点与写入数据在同一个事务中提交。** 因此：

- 刷新失败 ⇒ 位点不会推进 ⇒ 下一次刷新会重新消费同一个窗口，从而提供**幂等重试，不会重复或丢失数据**。
- 增量刷新只有一次重试机会。`max_mv_refresh_failure_retry_times` 仅适用于 PCT 刷新。锁超时有单独的 `max_mv_refresh_try_lock_failure_retry_times` 设置，默认值为 3。
- 当连续失败次数超过 `max_task_consecutive_fail_count`（默认 10）后，刷新任务会被挂起，物化视图也会被置为未激活状态。

### 批处理（当前对原生表无效）

FE 配置项 `mv_max_rows_per_refresh`（默认 1 亿行）和 `mv_max_bytes_per_refresh`（默认 20 GB）原本用于将累积的增量变更拆分为多个批次。

## 查询增量物化视图

增量物化视图**必须被直接查询**：

```SQL
SELECT * FROM mv_orders WHERE region = 'CN';
```

## 监控与故障排查

增量物化视图的可观测性分为三个层级。

### 第一层：物化视图级别

```SQL
SELECT TABLE_NAME, REFRESH_MODE, REFRESH_TRIGGER, REFRESH_POLICY,
       IS_ACTIVE, INACTIVE_REASON,
       QUERY_REWRITE_STATUS, QUERY_REWRITE_STATUS_REASON,
       LAST_REFRESH_STATE, LAST_REFRESH_FINISHED_TIME,
       LAST_FRESHNESS_CONFIRMED_AT, BASE_TABLE_REFRESH_VERSION_TIMES,
       LAST_REFRESH_ERROR_MESSAGE, TASK_NAME
FROM information_schema.materialized_views
WHERE TABLE_SCHEMA = DATABASE() AND REFRESH_MODE = 'INCREMENTAL';
```

- `LAST_FRESHNESS_CONFIRMED_AT`：最近一次确认新鲜度的时间。当刷新因为没有变更而被跳过时，该时间同样会推进。
- `BASE_TABLE_REFRESH_VERSION_TIMES`：每张基表的数据版本时间戳，可用于评估端到端的新鲜度延迟。

### 第二层：刷新作业级别

```SQL
SELECT JOB_ID, REFRESH_TRIGGER, REFRESH_STATE,
       SUBMIT_TIME, FINISH_TIME, DURATION_TIME,
       IMV_SOURCE_VERSION_RANGE,
       IMV_SOURCE_TIMESTAMP_RANGE,
       IMV_SOURCE_PINNED_SNAPSHOT_ID_MAP,
       ERROR_CODE, ERROR_MESSAGE, FAILED_QUERY_ID
FROM information_schema.materialized_view_refresh_jobs
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '<mv>'
ORDER BY SUBMIT_TIME DESC LIMIT 20;
```

- `IMV_SOURCE_VERSION_RANGE`：本次作业针对每张基表消费的位点范围。JSON 键使用包含 catalog 在内的完整可读名称。例如：

  ```Plain
  {"default_catalog.mydb.base_tbl":{"start":"1024","end":"1088"}}
  ```

  `start == end` 表示该基表在本次作业中没有发生变化。对于首次刷新，`start` 会显示为字面量 `MIN`。

- `IMV_SOURCE_TIMESTAMP_RANGE`：上述两个端点对应的提交时间戳，以 epoch 毫秒表示。**如果某个 Bookmark 已被回收，则对应端点的时间戳无法解析，该基表的条目会被省略。** 如需长期审计，请依赖 `task_runs`。

- 当本次作业没有消费任何增量范围时（例如 PCT 初始化作业），上述三列均为 `NULL`，而不是 `{}`。

### 第三层：单次运行级别

```SQL
SELECT QUERY_ID, STATE, PROCESS_TIME, FINISH_TIME,
       get_json_string(EXTRA_MESSAGE, '$.refreshMode')           AS executed_mode,
       get_json_string(EXTRA_MESSAGE, '$.imvSourceVersionRange') AS ver_range,
       ERROR_MESSAGE
FROM information_schema.task_runs
WHERE JOB_ID = '<job_id>'
ORDER BY CREATE_TIME;
```

验证增量窗口是否连续、没有缺口：

```SQL
WITH r AS (
  SELECT regexp_extract(get_json_string(EXTRA_MESSAGE,'$.imvSourceVersionRange'),'"start": *"([0-9]+)"',1) AS s,
         regexp_extract(get_json_string(EXTRA_MESSAGE,'$.imvSourceVersionRange'),'"end": *"([0-9]+)"',1)   AS e
  FROM information_schema.task_runs
  WHERE TASK_NAME = '<task_name>'
    AND get_json_string(EXTRA_MESSAGE,'$.refreshMode') = 'INCREMENTAL'
    AND get_json_string(EXTRA_MESSAGE,'$.imvSourceVersionRange') <> '{}'
)
SELECT (SELECT COUNT(*) FROM r)                         AS incremental_runs,
       (SELECT COUNT(*) FROM r a JOIN r b ON a.e = b.s) AS adjacent_pairs,
       (SELECT COUNT(*) FROM r WHERE s = e)             AS degenerate_windows;
```

### 判断失败是否可恢复

```SQL
SELECT QUERY_ID, CREATE_TIME, STATE, ERROR_CODE,
       ERROR_MESSAGE LIKE '%CDC-ERROR-1 (CHANGE_NOT_TRACKABLE):%'        AS permanently_broken_be,
       ERROR_MESSAGE LIKE '%do not support non-append-only base changes%' AS permanently_broken_fe,
       ERROR_MESSAGE
FROM information_schema.task_runs
WHERE `DATABASE` = DATABASE()
  AND (DEFINITION LIKE '%<mv>%' OR EXTRA_MESSAGE LIKE '%<mv>%')
  AND STATE = 'FAILED'
ORDER BY CREATE_TIME DESC LIMIT 20;
```

- 如果任一标记为 `1`，说明增量链已永久性中断。该物化视图已被置为未激活状态，**必须删除并重新创建**。
- 如果两者都为 `0`，该失败通常是可重试的，例如内存溢出或锁超时。物化视图仍保持激活状态，解决根因后重新刷新即可。

### 相关指标

- `mv_global_count{refresh_mode, status}`：按刷新模式和激活状态统计异步物化视图的数量。
- `mv_global_refresh_jobs_total` / `..._success_jobs_total` / `..._failed_jobs_total` / `mv_global_refresh_duration` / `mv_global_refresh_pending_jobs`。
- `mv_global_query_mv_usage_total{usage_type, refresh_mode}`：`usage_type = DIRECT` 专门表示增量物化视图的使用模式，可用于验证业务查询是否已实际切换到该物化视图。
- **Bookmark 相关指标**，是原生表上增量物化视图特有的、用于监控保留成本的指标：`bookmark_count`、`bookmark_reference_count`、`bookmark_max_active_age_ms` 等。`bookmark_max_active_age_ms` 持续增长，说明某个物化视图长时间未被刷新，导致旧的基表文件无法被回收。

## 管理与恢复

### 支持的操作

| 操作                                                    | 是否支持                                         | 说明                                                  |
| ------------------------------------------------------------ | ------------------------------------------------- | ------------------------------------------------------------ |
| `REFRESH MATERIALIZED VIEW <mv>`                             | 是                                               | 异步提交一次刷新                             |
| `REFRESH MATERIALIZED VIEW <mv> WITH SYNC MODE`              | 是                                               | 同步等待刷新完成                           |
| `REFRESH ... PARTITION START(...) END(...)`                  | **否**                                            | `Partition refresh is not supported for materialized views with refresh_mode=INCREMENTAL. Please refresh the whole materialized view instead.` |
| `REFRESH ... FORCE`                                          | **否**                                            | `FORCE refresh is not supported for materialized views with refresh_mode=INCREMENTAL. Please drop and re-create the materialized view instead.` |
| `ALTER MATERIALIZED VIEW ... SET("refresh_mode"=...)`        | **仅允许设置为相同的值**                               | 拒绝在不同模式之间切换                          |
| `ALTER MATERIALIZED VIEW ... REFRESH ASYNC EVERY(...)` / `... REFRESH MANUAL` | 是                                               | 保留增量位点；下一次刷新仍为增量刷新 |
| `ALTER MATERIALIZED VIEW ... REFRESH ASYNC`（切换为基表变更触发） | 支持，但**会丢失增量位点** | 会重建刷新上下文并清空已记录的增量位点。`ALTER` 之后触发的第一次刷新是一次**完整的 PCT 全量初始化**。切换为基表变更触发时，请预留一个全量刷新的时间窗口。 |
| `ALTER MATERIALIZED VIEW ... ACTIVE / INACTIVE`              | 是                                               | 参见下文说明                                  |
| `DROP MATERIALIZED VIEW`                                     | 是                                               | 释放该物化视图在基表上持有的所有 Bookmark，使旧版本可以被回收 |
| 同一张基表上存在多个增量物化视图 | 是                                               | 每个物化视图各自维护独立的位点     |

### 会中断增量链的基表操作

| 基表操作                                                | 结果                                                       | 关键错误信息                                            |
| ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 在 DUPLICATE KEY 基表上执行 `DELETE`                       | 刷新失败 + 物化视图被置为未激活（**不可恢复**） | `CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): CDC for DUP_KEYS does not support delete` |
| `ALTER TABLE ... DROP PARTITION`                             | 刷新失败（**不可恢复**）                            | `non-append-only change on base table`                       |
| `TRUNCATE TABLE [PARTITION p]`                               | 刷新失败（**不可恢复**）                            | 同上                                       |
| `INSERT OVERWRITE`                                           | 刷新失败（**不可恢复**）                            | 同上                                       |
| `DROP TABLE`（基表）                                    | 物化视图被置为未激活                                | `INACTIVE_REASON` 中包含 `base-table dropped`              |
| `ALTER TABLE ... RENAME`                                     | 物化视图被置为未激活                                | 包含 `base-table renamed`                              |
| `ALTER TABLE a SWAP WITH b`                                  | 物化视图被置为未激活                                | 包含 `base-table swapped`                               |
| `ALTER TABLE ... MODIFY COLUMN`（修改类型）                | 刷新失败 + 物化视图被置为未激活                | `column schema not compatible`                               |
| 修改分布列 / 排序键                      | 物化视图被置为未激活                                | `INACTIVE_REASON` 以 `base-table optimized:` 开头        |
| PRIMARY KEY 表恢复（`recover`）                       | 对应窗口永久失败                   | `Change data capture does not support primary key recover`   |
| 跨集群复制写入                             | 对应窗口永久失败                   | `Change data capture does not support replication`           |
| CDC 被禁用期间的窗口                           | 对应窗口永久失败                   | `CHANGES window on tablet <id> spans version <v> which was not recorded` |
| 版本祖先链已被回收                     | 对应窗口永久失败                   | `CHANGES ancestor chain on tablet <id> cannot reach base version <v>` |
| **在已启用 CDC 的 PRIMARY KEY 基表上执行 `DELETE` / `UPDATE`** | **正常的增量维护**                           | —                                                            |
| **当物化视图未引用该列时执行 `ADD COLUMN` / `DROP COLUMN`** | **不会中断链路；增量刷新继续正常进行**  | —                                                            |
| **仅修改分桶数量（`DISTRIBUTED BY ... BUCKETS n`）** | **不会中断链路；增量刷新继续正常进行**  | —                                                            |

### 恢复

一旦增量链被永久中断：

```Plain
Cannot incrementally refresh materialized view <mv>: <reason>.
INCREMENTAL materialized views do not support non-append-only base changes
(DELETE / OVERWRITE / DROP PARTITION / snapshot expiration / table replacement).
Drop and recreate the materialized view to recover.
```

物化视图会被置为未激活状态，`INACTIVE_REASON` 被设置为：

```Plain
incremental refresh broken by non-append-only base change: <mv>
```

系统**不会**在后台自动重新激活该物化视图。

## 成本与开销

| 成本                           | 说明                                                  |
| ------------------------------ | ------------------------------------------------------------ |
| **存储放大**      | 隐藏列 `__ROW_ID__` 和 `__AGG_STATE_*` 会增加存储用量。基准测试显示放大倍数约为 2.7 倍。 |
| **垃圾回收延迟** | 物化视图持有的 Bookmark 会把基表的最小保留版本拉低到仍被引用的最旧版本。**只要物化视图存在且未被刷新，旧的基表文件就无法被回收。** 因此长时间不刷新会导致存储持续增长。请监控 `bookmark_max_active_age_ms`。 |
| **PRIMARY KEY 写入开销** | 启用 CDC 后，每次导入都会记录用于定位变更的额外元数据页。这些结构不会随导入不断累积，只代表每次导入产生的增量元数据开销。禁用 CDC 时开销为零。 |
| **有限的追溯范围**   | BE 配置项 `cloud_native_tablet_metadata_ancestors_recorded`（默认 5）决定了每个 tablet 保留多少个历史元数据版本，从而决定增量刷新能够回溯多远。调大该值可以扩展追溯范围，但会带来额外的元数据存储开销。 |
| **迁移成本**             | 不支持查询改写，因此业务 SQL 必须修改为直接查询该物化视图。 |

## 决策树

```Plain
集群是否为存算分离集群？
├─ 否 → 使用异步物化视图（PCT）
└─ 是 → 现有业务 SQL 是否可以修改为直接查询物化视图？
         ├─ 否 → 使用异步物化视图（PCT，支持查询改写）
         └─ 是 → 基表是否存在定期的 DROP PARTITION / TRUNCATE / INSERT OVERWRITE 操作？
                  ├─ 是 → 使用异步物化视图（PCT），或重新设计这些操作
                  └─ 否 → 查询是否只使用受支持的算子和聚合函数？
                           ├─ 否 → 使用异步物化视图（PCT）
                           └─ 是 → 基表的数据模型是什么？
                                    ├─ UNIQUE KEY → 不支持；更换模型或使用 PCT
                                    ├─ AGGREGATE KEY → 查询能否表示为严格上卷？
                                    │                   ├─ 是 → 使用增量物化视图
                                    │                   └─ 否 → 使用 PCT
                                    ├─ PRIMARY KEY → 启用 CDC 并使用增量物化视图
                                    └─ DUPLICATE KEY → 使用增量物化视图
```

## 常见误解

| 误解                                                | 实际行为                                              |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| “增量物化视图是异步物化视图的升级版本，可以完全取代它。” | 不对。它是一种专用模式，其 SQL 支持范围比 PCT 窄得多，并且不支持查询改写。 |
| “物化视图创建后，现有查询会自动变快。” | 不对。必须显式修改业务 SQL，使其查询该物化视图。 |
| “在 PRIMARY KEY 基表上执行 `DELETE` 会中断增量链。” | 不对。在已启用 CDC 的 PRIMARY KEY 基表上，行级 `DELETE` / `UPDATE` 是正常支持的。分区级 / 表级的破坏性操作才会中断增量链。 |
| “在任务运行中看到 `PCT`，说明增量配置没有生效。” | 不对。首次刷新是一次全量初始化，因此会使用 `PCT`。同时看到 `PCT` 和 `INCREMENTAL` 是预期行为。 |
| “`materialized_view_refresh_jobs.REFRESH_MODE` 表示实际执行的模式。” | 不对。这是配置的模式，实际执行模式记录在 `task_runs.EXTRA_MESSAGE.$.refreshMode` 中。 |
| “可以先以 PCT 模式创建物化视图，之后再通过 `ALTER` 改为 INCREMENTAL。” | 不对。`refresh_mode` 无法通过 `ALTER` 跨模式修改，请删除并重新创建物化视图。 |
| “将 `default_mv_refresh_mode` 改为 `incremental`，会让所有新创建的物化视图都变为增量物化视图。” | 不对。创建流程只读取显式指定的 `refresh_mode` 属性。修改该配置可能导致物化视图“声称自己是增量的，但实际使用 PCT”，同时还会禁用查询改写。 |
| “使用 `ALTER ... REFRESH ASYNC` 将物化视图从定时刷新改为基表变更触发是无损的。” | 不对。该操作会清空增量位点，并在下一次刷新时触发一次全量初始化。 |
| “调大 `mv_max_rows_per_refresh` 可以控制每次刷新处理的数据量。”                     | 该参数当前对内部表基表没有效果。每次刷新都会消费全部累积的窗口。 |
| “增量刷新总是更便宜。”                     | 这取决于具体的工作负载。如果写入天然分散在许多小分区中，或者历史分区不再变化，PCT 本身的成本可能已经很低。请针对您的工作负载进行基准测试。 |

## 参数参考

### 表属性（基表）

#### `enable_change_data_capture`

- 类型：Boolean
- 默认值：`false`
- 说明：只能在存算分离集群的 PRIMARY KEY 表上设置。启用后该表可以作为增量物化视图的基表，并支持 `UPDATE` / `DELETE` 撤回。可以通过 `ALTER TABLE ... SET(...)` 以异步元数据作业的方式在线启用。

### 物化视图属性

#### `refresh_mode`

- 类型：Enum
- 默认值：`PCT`
- 说明：将其设置为 `INCREMENTAL` 即可声明一个增量物化视图。必须在创建时显式指定，并且无法通过 `ALTER` 语句跨模式切换。

#### `excluded_trigger_tables`

- 类型：String
- 默认值：空字符串
- 说明：从基表变更触发中排除的基表名称列表。仅在 `REFRESH ASYNC` 下生效。

#### `partition_refresh_number` / `partition_refresh_strategy` / `auto_refresh_partitions_limit`

- 说明：这些属性对增量物化视图没有效果。

#### `enable_query_rewrite` 及其他查询改写相关属性

- 说明：这些属性对增量物化视图没有效果，查询改写始终处于禁用状态。

### FE 配置项

#### `default_mv_refresh_mode`

- 类型：String
- 默认值：`pct`
- 说明：有效取值为 `PCT` / `INCREMENTAL`。**不会决定新创建的物化视图是否实际使用增量维护**；创建流程只检查显式指定的 `refresh_mode` 属性。将其改为 `incremental` 可能导致未显式指定该属性的物化视图“声称自己是增量的，但实际使用 PCT”。请保持默认值。


#### `mv_max_rows_per_refresh`

- 类型：long
- 默认值：`100000000`
- 说明：每个增量批次的最大行数。**当前对原生表没有效果。**

#### `mv_max_bytes_per_refresh`

- 类型：long
- 默认值：`21474836480`
- 说明：每个增量批次的最大字节数（20 GB）。**当前对原生表没有效果。**

#### `mv_refresh_try_lock_timeout_ms`

- 类型：int
- 默认值：`30000`
- 说明：生成刷新计划时的锁获取超时时间。

#### `max_mv_refresh_try_lock_failure_retry_times`

- 类型：int
- 默认值：`3`
- 说明：锁超时后的重试次数。

#### `max_mv_refresh_failure_retry_times`

- 类型：int
- 默认值：`1`
- 说明：**仅适用于 PCT 刷新**。增量刷新始终只重试一次。

#### `max_task_consecutive_fail_count`

- 类型：int
- 默认值：`10`
- 说明：当连续失败次数超过该值后，挂起任务并将物化视图置为未激活状态。

#### `max_mv_task_run_meta_message_values_length`

- 类型：int
- 默认值：`8`
- 说明：`EXTRA_MESSAGE` 中集合类型字段的截断长度。**如果基表数量超过 8 张，可观测性数据会不完整。**

#### `materialized_view_min_refresh_interval`

- 类型：int
- 默认值：`60` 秒
- 说明：`EVERY(INTERVAL ...)` 的最小间隔。

#### `enable_mv_automatic_active_check`

- 类型：boolean
- 默认值：`true`
- 说明：启用后台自动重新激活。增量链中断的原因不在此范围内。

#### `bookmark_reference_max_ttl_ms`

- 类型：long
- 默认值：`-1`（禁用）
- 说明：集群级别 Bookmark 引用的最大存活时间。**设置为正值可能会回收物化视图所需的 Bookmark，导致刷新失败，请谨慎使用。**

#### `enable_bookmark_meta_functions`

- 类型：boolean
- 默认值：`false`
- 说明：启用诸如 `bookmark_create()` / `bookmark_release()` 等诊断函数，仅用于故障排查。

### BE 配置项

#### `cloud_native_tablet_metadata_ancestors_recorded`

- 类型：int
- 默认值：`5`
- 说明：每个 tablet 保留的历史元数据祖先版本数量，决定了增量刷新的追溯范围。调大该值可以让刷新回溯得更远，但会带来额外的元数据存储开销。

## 能力边界

### 前提条件

创建增量物化视图需要同时满足以下所有条件：

- 集群为**存算分离集群**，基表为云原生表或 Iceberg 表。
- 基表类型受支持：DUPLICATE KEY 表直接支持；PRIMARY KEY 表需要启用 CDC；AGGREGATE KEY 表仅支持严格上卷；不支持 UNIQUE KEY。
- 查询只使用受支持的算子，参见[支持的算子](#支持的算子)。
- 所有聚合函数都在允许列表中，参见[支持的聚合函数](#支持的聚合函数)。

### 基表类型

| 基表类型                           | 是否支持 | 说明                                                  |
| ----------------------------------------- | --------- | ------------------------------------------------------------ |
| 存算分离集群中的云原生表 | 是       | 本文档的主要关注对象。                              |
| 存算一体集群中的原生表    | 否        | 错误：`IVMAnalyzer does not support table type: OLAP`。      |
| Iceberg 表（只追加）               | 是       | 参见 [CREATE MATERIALIZED VIEW](../../sql-reference/sql-statements/materialized_view/CREATE_MATERIALIZED_VIEW.md)。 |
| Hive / Hudi / Delta Lake / Paimon / JDBC  | 否        | 错误：`IVMAnalyzer does not support table type: <TYPE>`。    |
| 逻辑视图                              | 否        | 错误：`IVMAnalyzer does not support inner relation type: ViewRelation`。 |
| 物化视图（MV on MV）              | 否        | 错误：`IVMAnalyzer does not support table type: CLOUD_NATIVE_MATERIALIZED_VIEW`。 |
| CTE（WITH 子句）                         | 否        | 错误：`IVMAnalyzer does not support inner relation type: CTERelation`。 |
| 临时表                           | 否        | 错误：`Materialized view can't base on temporary table`。    |

### 数据模型

#### DUPLICATE KEY 基表

**支持只追加（append-only）写入。** 无需额外配置。

在基表上执行 `DELETE` 会导致刷新失败，并将物化视图置为未激活状态。错误信息包含：

```Plain
CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): CDC for DUP_KEYS does not support delete
```

原因在于 DUPLICATE KEY 表没有 delete vector。行没有可寻址的行标识符，因此系统无法将某一行的“消失”传播到物化视图中。

#### PRIMARY KEY 基表

**支持针对 `INSERT` / `UPDATE` / `DELETE` 的行级增量维护。** 这是原生表上的增量物化视图相较于 Iceberg 最重要的差异化能力。

基表必须通过将表属性 `enable_change_data_capture` 设置为 `true` 来启用 CDC：

```SQL
-- 建表时启用 CDC
CREATE TABLE pk_base (
    id INT NOT NULL,
    g  INT,
    v  BIGINT
) PRIMARY KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES ('enable_change_data_capture' = 'true');

-- 或者为已有表启用（异步元数据变更作业，需等待完成）
ALTER TABLE pk_base SET ('enable_change_data_capture' = 'true');
```

如果未启用 CDC，创建增量物化视图会失败，报错如下：

```Plain
IVM on cloud-native PRIMARY KEY base 'pk_base' requires change data capture to be
enabled on the base table: ... Enable it with
ALTER TABLE pk_base SET ('enable_change_data_capture' = 'true').
```

PRIMARY KEY 基表还有以下额外限制：

| 限制                                                  | 关键错误信息                                            |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| 纯投影/过滤查询中不能混用非 PRIMARY KEY 基表 | `IVM over a cloud-native PRIMARY KEY base requires every base to be a cloud-native PRIMARY KEY table, but '<t>' is not` |
| 不能混用非 PRIMARY KEY 基表后再进行聚合 | `IVM retractable aggregate requires every base to be a cloud-native PRIMARY KEY table` |
| 物化视图定义中不支持 `ORDER BY (...)` | `IVMAnalyzer does not yet support ORDER BY for a materialized view over a cloud-native PRIMARY KEY base` |
| JOIN 物化视图的输出列必须是可排序的；不支持 JSON / MAP / BITMAP / HLL / PERCENTILE / VARIANT | `IVMAnalyzer does not support a join materialized view with a non-orderable output column type` |
| 派生表上不支持 `SELECT *`              | `IVMAnalyzer does not support SELECT * over a derived table on a cloud-native PRIMARY KEY base; list the columns explicitly` |
| 派生表不支持显式列别名列表，例如 `t(a, b)` | `IVMAnalyzer does not support a derived table with an explicit column alias list` |
| `UNION ALL` 不能混用可撤回分支和只追加分支  | `IVMAnalyzer does not support a UNION ALL that mixes a retractable cloud-native PRIMARY KEY branch with an append-only branch` |
| `UNION ALL` 分支内部不能出现 `GROUP BY` / `DISTINCT` | `IVMAnalyzer does not support a GROUP BY or DISTINCT branch in a UNION ALL materialized view` |

以下 PRIMARY KEY 查询形态已验证支持：单表查询、PK INNER JOIN PK、PK CROSS JOIN PK、PK 表上的聚合、PK 表上的一层派生表，以及 PK UNION ALL PK。

#### AGGREGATE KEY 基表

**仅支持严格上卷。** 该数据模型的限制最多。

原因在于增量数据流中包含的是**聚合前**的原始行，而普通查询看到的是合并后的结果。只有当物化视图中的每个分组都能对应到一个完整的合并分组，并且聚合结果在合并操作下保持不变时，增量维护才是安全的。

适用以下八项约束：

| 约束                                                   | 关键错误信息                                            |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| 必须是唯一的 `FROM` 数据源；不允许 JOIN / UNION / 子查询 | `is only supported when the base table is the sole FROM source (no JOIN, UNION, or subquery)` |
| 必须包含 `GROUP BY`；不支持纯投影物化视图 | `requires GROUP BY: the CDC delta stream emits raw pre-merge rowset rows` |
| `GROUP BY` 只能包含**普通列引用**      | `only supports GROUP BY plain column references, got: <expr>` |
| `GROUP BY` 列必须是**聚合键列的子集** | `requires GROUP BY columns to be a subset of the aggregate-key columns` |
| `WHERE` 只能过滤键列                          | `does not support a WHERE predicate on value column '<c>': the predicate is evaluated on raw pre-merge delta rows` |
| 聚合函数的参数必须是**单个普通列引用**（`SUM(s * 2)` 会被拒绝，尽管它在 DUPLICATE KEY 表上是合法的） | `requires aggregate over a single plain column reference, got: <expr>` |
| 聚合函数必须逐列匹配基表列的聚合类型 | `requires MV aggregate to match the base column's aggregation type` |
| 不能对键列或 `REPLACE` / `REPLACE_IF_NOT_NULL` 列应用聚合 | `does not support aggregate on aggregate-key column '<c>'`; `does not support aggregate on REPLACE/REPLACE_IF_NOT_NULL column '<c>': replace semantics is order-dependent` |

AGGREGATE KEY 基表仅支持以下六个聚合函数：`sum`、`max`、`min`、`bitmap_union`、`hll_union` 和 `percentile_union`。

`count` / `count(*)` / `avg` / `ndv` / `approx_count_distinct` / `array_agg` / `bool_or` / `bitmap_agg` 在 AGGREGATE KEY 基表上会被拒绝，尽管它们在 DUPLICATE KEY 基表上是受支持的：

```Plain
IVM on cloud-native AGGREGATE KEY base '<t>' does not support aggregate 'count';
AGG_KEYS base only accepts delta-rollup-compatible aggregates [sum, max, min,
bitmap_union, hll_union, percentile_union]
```

#### UNIQUE KEY 基表

**不支持**

```Plain
IVM on cloud-native UNIQUE_KEYS base '<t>' is not supported: the CDC delta stream is
append-only and cannot maintain replace/upsert semantics, so the MV would retain
stale rows after a key update
```

#### 混合数据模型 JOIN

| 组合                                       | 是否支持 |
| ------------------------------------------------- | --------- |
| PRIMARY KEY ⋈ PRIMARY KEY（两侧均已启用 CDC） | 是       |
| DUPLICATE KEY ⋈ DUPLICATE KEY                     | 是       |
| PRIMARY KEY ⋈ DUPLICATE KEY                       | 否        |
| PRIMARY KEY ⋈ DUPLICATE KEY + 聚合         | 否        |
| AGGREGATE KEY ⋈ 任意表                         | 否        |

### 支持的算子

| 算子                                                     | 是否支持 | 说明                                                  |
| ------------------------------------------------------------ | --------- | ------------------------------------------------------------ |
| `SELECT` 中的标量表达式                               | 是       | `CAST`、`CASE WHEN`、算术运算、字符串/日期函数等 |
| `WHERE` 过滤                                            | 是       | 对于 AGGREGATE KEY 基表仅限于键列         |
| `GROUP BY` 聚合                                       | 是       | 必须包含 `GROUP BY`；不支持没有 `GROUP BY` 的全局聚合 |
| `GROUP BY` 序号（`GROUP BY 1`）                            | 是       | 自动处理                                                  |
| `GROUP BY` 表达式                                       | 是       | 对于 AGGREGATE KEY 基表仅限于普通列引用 |
| 不含聚合的 `GROUP BY`（去重）               | 是       | 等价于按键去重                                        |
| `SELECT DISTINCT`                                            | 是       | 会自动改写为等价的 `GROUP BY`，AGGREGATE KEY 基表除外 |
| 仅引用分组键的 `HAVING`                      | 是       |                                                              |
| INNER JOIN                                                   | 是       | 能够正确处理任意一侧或两侧的变更            |
| CROSS JOIN                                                   | 是       |                                                              |
| UNION ALL                                                    | 是       | 分支中不能包含聚合                                       |
| 派生表 / 子查询                                  | 部分支持 | 所有基表模型都只支持一层。内层查询只能包含投影 / 过滤 / JOIN。内层查询中的聚合、`DISTINCT` 或 `UNION` 会被拒绝 |
| **LEFT / RIGHT / FULL OUTER JOIN**                           | 否        | `IVMAnalyzer does not support join type: LEFT OUTER JOIN`    |
| **SEMI / ANTI JOIN**                                         | 否        | `IVMAnalyzer does not support join type: LEFT SEMI JOIN`     |
| **UNION（去重）**                                    | 否        | `IVMAnalyzer only supports UNION ALL, but got: DISTINCT`     |
| **INTERSECT / EXCEPT**                                       | 否        | `IVMAnalyzer can only handle UnionRelation, but got: IntersectRelation` |
| **窗口 / 分析函数**                              | 否        | `IVMAnalyzer does not support window functions`              |
| **查询中的 `ORDER BY`**                                  | 否        | `IVMAnalyzer does not support order by clause`               |
| **`LIMIT`**                                                  | 否        | 创建会失败，报错为 `IVM rewrite failed to fully resolve incremental markers` |
| **包含聚合函数的 `HAVING`**                  | 否        | `IVMAnalyzer does not support HAVING with aggregate functions` |
| **GROUPING SETS / ROLLUP / CUBE / GROUP BY ALL**             | 否        | `IVMAnalyzer does not support <GROUPING_SETS｜ROLLUP｜CUBE｜GROUP_BY_ALL> for incremental view maintenance` |
| **没有 `GROUP BY` 的全局聚合**                    | 否        | `IVMAnalyzer requires group by expressions for incremental view maintenance.` |
| **去重聚合**（`count(distinct x)` 等）         | 否        | `IVMAnalyzer does not support distinct aggregate functions`  |
| **CTE（`WITH`）**                                             | 否        | `IVMAnalyzer does not support inner relation type: CTERelation` |
| **非确定性函数**（`rand()` / `now()` / `uuid()`） | 否        | 物化视图的通用限制                        |
| **Time Travel 查询**                                      | 否        | 物化视图的通用限制                        |

### 支持的聚合函数

| 函数                | 支持的参数类型                                     |
| ----------------------- | ------------------------------------------------------------ |
| `count`                 | `count(*)` 或 `count(col)`（任意类型，最多一个参数）  |
| `sum`                   | 整数、浮点数、DECIMAL                             |
| `avg`                   | 整数、浮点数、DECIMAL                             |
| `min` / `max`           | 整数、浮点数、DECIMAL、DATE、DATETIME、字符串     |
| `array_agg`             | 一个参数；**不支持** `array_agg(col ORDER BY k)` |
| `bool_or`               | BOOLEAN                                                      |
| `approx_count_distinct` | 一个参数                                                |
| `ndv`                   | 一个参数                                                |
| `bitmap_agg`            | 一个参数                                                |
| `bitmap_union`          | BITMAP，例如 `bitmap_union(to_bitmap(col))`               |
| `hll_union`             | HLL，例如 `hll_union(hll_hash(col))`                      |
| `percentile_union`      | PERCENTILE，例如 `percentile_union(percentile_hash(col))` |

常见的不支持函数包括 `count(distinct x)`、`multi_distinct_count`、`stddev`、`variance`、`stddev_samp`、`percentile_approx`、`group_concat`、`any_value`、`max_by`、`min_by`、`retention`、`window_funnel`、`bitmap_union_count`、`hll_union_agg`、`covar_*`、`corr`，以及所有聚合类 UDF。

错误格式如下：

```Plain
IVMAnalyzer does not support aggregate function: stddev. Supported functions:
[count, sum, avg, min, max, array_agg, bool_or, approx_count_distinct, ndv,
bitmap_agg, bitmap_union, hll_union, percentile_union]
```

### 物化视图的结构

增量物化视图**始终是 PRIMARY KEY 表**，用户无法选择其他模型。这会带来几个用户可见的副作用：

- **隐藏列 `__ROW_ID__`**：物化视图的主键，用于标识每一行，有两种可能的来源：

  - **由查询派生**：当存在 `GROUP BY` / `DISTINCT`，或基表为 PRIMARY KEY 时，该值由分组键或主键列编码而成，类型为 `VARCHAR`。
  - **自增生成**：在纯追加投影/过滤场景中，该值为 `BIGINT AUTO_INCREMENT`，由存储引擎填充。
- **隐藏列 `__AGG_STATE_<聚合表达式>`**：每个可终结（finalizable）的聚合函数都会额外存储一个中间状态列。`bitmap_union`、`hll_union` 和 `percentile_union` 在直接作为输出列使用时会被折叠，不会产生额外的列。
- **存储放大**：这些隐藏列会增加存储用量。在基准测试中，存储用量为 318 MB，而 PCT 为 117 MB，放大约为 **2.7 倍**。请相应预留额外容量。

分区、分布和排序键：

| 子句           | 说明                                                  |
| ---------------- | ------------------------------------------------------------ |
| `PARTITION BY`   | 支持。使用标准的物化视图分区校验规则，可以采用与基表不同的粒度，例如基表按天分区、物化视图按月分区。 |
| `DISTRIBUTED BY` | **会被自动归一化**。如果省略该子句，且存算分离集群中启用了 `enable_range_distribution`，则使用 Range 分布；Range 分布没有用户可写的语法。否则，会被归一化为基于**所有键列**的 HASH 分布，仅保留显式指定的分桶数量。显式指定的哈希列和 `RANDOM` 会被忽略。 |
| `ORDER BY (...)` | 非 PRIMARY KEY 基表支持；**PRIMARY KEY 基表不支持**。 |

## 错误参考

| 错误信息包含                                       | 含义与处理方式                                            |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| `does not support table type: OLAP`                          | 不支持存算一体集群，没有变通方案。 |
| `does not support table type: CLOUD_NATIVE_MATERIALIZED_VIEW` | 不支持 MV on MV。                                   |
| `does not support inner relation type: ViewRelation` / `CTERelation` | 逻辑视图 / CTE 不能作为基础关系使用，请将其展开为表引用。 |
| `requires change data capture to be enabled`                 | PRIMARY KEY 基表需要执行 `ALTER TABLE ... SET ('enable_change_data_capture' = 'true')`。 |
| `UNIQUE_KEYS base ... is not supported`                      | 不支持 UNIQUE KEY 基表，请考虑改用 PRIMARY KEY 模型。 |
| `AGGREGATE KEY base ... requires GROUP BY`                   | AGGREGATE KEY 基表必须使用上卷；不支持纯投影物化视图。 |
| `AGGREGATE KEY base ... does not support aggregate 'count'`  | AGGREGATE KEY 基表仅支持 `sum` / `max` / `min` / `bitmap_union` / `hll_union` / `percentile_union`。 |
| `does not support join type: LEFT OUTER JOIN`                | 仅支持 INNER / CROSS JOIN。                       |
| `only supports UNION ALL`                                    | 不支持 `UNION`（去重）、`INTERSECT` 和 `EXCEPT`。 |
| `does not support window functions`                          | 不支持窗口函数。                          |
| `does not support order by clause`                           | 查询体中不能出现 `ORDER BY`。                  |
| `does not support HAVING with aggregate functions`           | `HAVING` 只能引用分组键。                   |
| `requires group by expressions`                              | 聚合需要 `GROUP BY`；不支持全局聚合。 |
| `does not support distinct aggregate functions`              | 请使用 `bitmap_union(to_bitmap(col))` 替代 `count(distinct col)`。 |
| `does not support aggregate function: <name>`                | 该函数不在允许列表中。参见[支持的聚合函数](#支持的聚合函数)。 |
| `every base to be a cloud-native PRIMARY KEY table`          | PRIMARY KEY 基表不能与非 PRIMARY KEY 基表混用。 |
| `IVM rewrite failed to fully resolve incremental markers`    | 查询中包含无法增量维护的算子，例如 `LIMIT` 或未解析的相关子查询。 |
| `Failed to generate IVM refresh plan at CREATE time`         | 创建期间编译失败，实际原因会出现在该消息之后。 |
| `Invalid refresh_mode`                                       | `refresh_mode` 仅接受 `PCT` / `INCREMENTAL`。           |
| `Altering refresh_mode from ... is not supported`            | 不支持跨模式的 `ALTER`，请删除并重新创建物化视图。 |
| `Partition refresh is not supported` / `FORCE refresh is not supported` | 增量物化视图不支持分区刷新或 FORCE 刷新。 |
| `do not support non-append-only base changes`                | 增量链已永久中断，请删除并重新创建物化视图。 |
| `CDC-ERROR-1 (CHANGE_NOT_TRACKABLE)`                         | 该变更无法被追踪，例如 DUPLICATE KEY 表上的 `DELETE`、CDC 出现缺口，或祖先链已被回收。增量链已永久中断，请删除并重新创建物化视图。 |
| `column schema not compatible`                               | 基表列类型变更导致 Schema 不兼容，请删除并重新创建物化视图。 |

## 相关文档

- [CREATE MATERIALIZED VIEW](../../sql-reference/sql-statements/materialized_view/CREATE_MATERIALIZED_VIEW.md)
- [REFRESH MATERIALIZED VIEW](../../sql-reference/sql-statements/materialized_view/REFRESH_MATERIALIZED_VIEW.md)
- [ALTER MATERIALIZED VIEW](../../sql-reference/sql-statements/materialized_view/ALTER_MATERIALIZED_VIEW.md)
- [异步物化视图](./async_mv.mdx)
- [information_schema.materialized_views](../../sql-reference/information_schema/materialized_views.md)
- [information_schema.materialized_view_refresh_jobs](../../sql-reference/information_schema/materialized_view_refresh_jobs.md)
