---
displayed_sidebar: docs
keywords: ['session','variable']
---

# 系统变量

StarRocks 提供多个系统变量（system variables），方便您根据业务情况进行调整。本文介绍 StarRocks 支持的变量。您可以在 MySQL 客户端通过命令 [SHOW VARIABLES](sql-statements/cluster-management/config_vars/SHOW_VARIABLES.md) 查看当前变量。也可以通过 [SET](sql-statements/cluster-management/config_vars/SET.md) 命令动态设置或者修改变量。您可以设置变量在系统全局 (global) 范围内生效、仅在当前会话 (session) 中生效、或者仅在单个查询语句中生效。

StarRocks 中的变量参考 MySQL 中的变量设置，但**部分变量仅用于兼容 MySQL 客户端协议，并不产生其在 MySQL 数据库中的实际意义**。

> **说明**
>
> 任何用户都有权限通过 SHOW VARIABLES 查看变量。任何用户都有权限设置变量在 Session 级别生效。只有拥有 System 级 OPERATE 权限的用户才可以设置变量为全局生效。设置全局生效后，后续所有新的会话都会使用新配置，当前会话仍然使用老的配置。

## 查看变量

可以通过 `SHOW VARIABLES [LIKE 'xxx'];` 查看所有或指定的变量。例如：

```SQL

-- 查看系统中所有变量。
SHOW VARIABLES;

-- 查看符合匹配规则的变量。
SHOW VARIABLES LIKE '%time_zone%';
```

## 变量层级和类型

StarRocks 支持三种类型（层级）的变量：全局变量、Session 变量和 `SET_VAR` Hint。它们的层级关系如下：

* 全局变量在全局级别生效，可以被 Session 变量和 `SET_VAR` Hint 覆盖。
* Session 变量仅在当前会话中生效，可以被 `SET_VAR` Hint 覆盖。
* `SET_VAR` Hint 仅在当前查询语句中生效。

## 设置变量

### 设置变量全局生效或在会话中生效

变量一般可以设置为**全局**生效或**仅当前会话**生效。设置为全局生效后，**后续所有新的会话**连接中会使用新设置的值，当前会话还会继续使用之前设置的值；设置为仅当前会话生效时，变量仅对当前会话产生作用。

通过 `SET <var_name> = xxx;` 语句设置的变量仅在当前会话生效。如：

```SQL
SET query_mem_limit = 137438953472;

SET forward_to_master = true;

SET time_zone = "Asia/Shanghai";
```

通过 `SET GLOBAL <var_name> = xxx;` 语句设置的变量全局生效。如：

 ```SQL
SET GLOBAL query_mem_limit = 137438953472;
 ```

以下变量仅支持全局生效，不支持设置为会话级别生效。您必须使用 `SET GLOBAL <var_name> = xxx;`，不能使用 `SET <var_name> = xxx;`，否则返回错误。

* activate_all_roles_on_login
* character_set_database
* cngroup_low_watermark_cpu_used_permille
* cngroup_low_watermark_running_query_count
* cngroup_resource_usage_fresh_ratio
* cngroup_schedule_mode
* default_rowset_type
* enable_group_level_query_queue
* enable_query_history
* enable_query_queue_load
* enable_query_queue_select
* enable_query_queue_statistic
* enable_table_name_case_insensitive
* enable_tde
* init_connect
* language
* license
* lower_case_table_names
* performance_schema
* query_cache_size
* query_history_keep_seconds
* query_history_load_interval_seconds
* query_queue_concurrency_limit
* query_queue_cpu_used_permille_limit
* query_queue_driver_high_water
* query_queue_driver_low_water
* query_queue_fresh_resource_usage_interval_ms
* query_queue_max_queued_queries
* query_queue_mem_used_pct_limit
* query_queue_pending_timeout_second
* system_time_zone
* version
* version_comment


Session 级变量既可以设置全局生效也可以设置 session 级生效。

此外，变量设置也支持常量表达式，如：

```SQL
SET query_mem_limit = 10 * 1024 * 1024 * 1024;
```

```SQL
SET forward_to_master = concat('tr', 'u', 'e');
```

### 设置变量在单个查询语句中生效

在一些场景中，可能需要对某些查询专门设置变量。可以使用 SET_VAR 提示 (Hint) 在查询中设置仅在单个语句内生效的会话变量。

当前，StarRocks 支持在以下语句中使用 `SET_VAR` Hint：

- SELECT
- INSERT（自 v3.1.12 和 v3.2.0 起支持）
- UPDATE（自 v3.1.12 和 v3.2.0 起支持）
- DELETE（自 v3.1.12 和 v3.2.0 起支持）

`SET_VAR` 只能跟在以上关键字之后，必须以 `/*+` 开头，以 `*/` 结束。

举例：

```sql
SELECT /*+ SET_VAR(query_mem_limit = 8589934592) */ name FROM people ORDER BY name;

SELECT /*+ SET_VAR(query_timeout = 1) */ sleep(3);

UPDATE /*+ SET_VAR(insert_timeout=100) */ tbl SET c1 = 2 WHERE c1 = 1;

DELETE /*+ SET_VAR(query_mem_limit = 8589934592) */
FROM my_table PARTITION p1
WHERE k1 = 3;

INSERT /*+ SET_VAR(insert_timeout = 10000000) */
INTO insert_wiki_edit
    SELECT * FROM FILES(
        "path" = "s3://inserttest/parquet/insert_wiki_edit_append.parquet",
        "format" = "parquet",
        "aws.s3.access_key" = "XXXXXXXXXX",
        "aws.s3.secret_key" = "YYYYYYYYYY",
        "aws.s3.region" = "us-west-2"
);
```

StarRocks 同时支持在单个语句中设置多个变量，参考如下示例：

```sql
SELECT /*+ SET_VAR
  (
  exec_mem_limit = 515396075520,
  query_timeout=10000000,
  batch_size=4096,
  parallel_fragment_exec_instance_num=32
  )
  */ * FROM TABLE;
```

### 设置变量为用户属性

您可以通过 [ALTER USER](../sql-reference/sql-statements/account-management/ALTER_USER.md) 将 Session 变量设置为用户属性该功能自 v3.3.3 起支持。

示例：

```SQL
-- 设置用户 jack 的 Session 变量 `query_timeout` 为 `600`。
ALTER USER 'jack' SET PROPERTIES ('session.query_timeout' = '600');
```

## 支持的变量

本节以字母顺序对变量进行解释。带 `global` 标记的变量为全局变量，仅支持全局生效。其余变量既可以设置全局生效，也可设置会话级别生效。

### activate_all_roles_on_login (global)

* 描述：用于控制是否在用户登录时默认激活所有角色（包括默认角色和授予的角色）。
  * 开启后，在用户登录时默认激活所有角色，优先级高于通过 [SET DEFAULT ROLE](sql-statements/account-management/SET_DEFAULT_ROLE.md) 设置的角色。
  * 如果不开启，则会默认激活 SET DEFAULT ROLE 中设置的角色。
* 默认值：false，表示不开启。
* 引入版本：v3.0

如果要在当前会话中激活一个角色，可以使用 [SET ROLE](sql-statements/account-management/SET_ROLE.md)。

### auto_increment_increment

* 描述：用于兼容 MySQL 客户端。无实际作用。
* 默认值：1
* 类型：Int

### big_query_profile_threshold

* 描述：用于设定大查询的阈值。当会话变量 `enable_profile` 设置为 `false` 且查询时间超过 `big_query_profile_threshold` 设定的阈值时，则会生成 Profile。

  NOTE：在 v3.1.5 至 v3.1.7 以及 v3.2.0 至 v3.2.2 中，引入了 `big_query_profile_second_threshold` 参数，用于设定大查询的阈值。而在 v3.1.8、v3.2.3 及后续版本中，此参数被 `big_query_profile_threshold` 替代，以便提供更加灵活的配置选项。
* 默认值：0
* 单位：秒
* 类型：String
* 引入版本：v3.1

### catalog（3.2.4 及以后）

* 描述：用于指定当前会话所在的 Catalog。
* 默认值：default_catalog
* 类型：String
* 引入版本：v3.2.4

### cbo_cte_force_reuse_node_count

* **范围**: 会话  
* **描述**: 控制 CTE 生产者树中节点数的阈值，当超过该阈值时强制重用 CTE 而不是完全转换/内联它。优化器度量生产者节点数（来自 `cteContext.getCteNodeCount`），当该计数大于配置的阈值时，RelationTransformer 会创建一个 consume 运算符以重用已注册的 CTE 表达式（从而避免访问并构建完整的生产者执行计划）。这可以减少对非常大的 CTE 生产者树的优化器时间。取值为 0 会禁用强制重用行为，并无论生产者大小如何都强制正常的转换/内联。  
* **默认值**: 2000  
* **数据类型**: int  
* **引入版本**: v3.5.3

### cbo_disabled_rules

* **描述**: 仅限会话的字符串，包含以逗号分隔的一组要禁用的优化器规则枚举名称。每个名称必须与 RuleType 枚举常量完全匹配（区分大小写），并且只有以 `TF_`（转换规则）或 `GP_`（组合并规则）开头的规则名称才能被禁用。语句分析器（`SetStmtAnalyzer.validateCboDisabledRules`）会验证该列表并拒绝未知名称或非 `TF_`/`GP_` 前缀的名称。在运行时，`OptimizerOptions.applyDisableRuleFromSessionVariable` 读取 `cbo_disabled_rules`，`parseDisabledRules` 会拆分并修剪条目，将有效的枚举名称转换为 RuleType 并清除相应的规则开关，从而使优化器跳过这些规则。优化器解析路径可能会对未知名称记录警告并忽略它们，但尝试 SET 一个无效值将引发语义错误。使用空字符串表示不禁用任何规则。示例：
  SET `cbo_disabled_rules` = 'TF_JOIN_COMMUTATIVITY,GP_PRUNE_COLUMNS';
* **默认值**: `""`
* **类型**: String
* **引入版本**: -

### cbo_enable_intersect_add_distinct

* **描述**：启用一个 CBO 转换，当优化器认为有利时，尝试在 `LOGICAL_INTERSECT` 的每个输入分支上插入全局 DISTINCT（聚合）。启用后，`IntersectAddDistinctRule` 会为每个子节点创建一个 `LogicalAggregationOperator`（AggType.GLOBAL），调用统计估算，并使用 `StatisticsEstimateCoefficient.MEDIUM_AGGREGATE_EFFECT_COEFFICIENT` 将估算的去重行数与原始子节点行数进行比较。只有当估算的去重行数为正且相对原始行数足够小时才保留该聚合（即 estimatedAggRows &gt; 0、originalRows &gt; 0 且 estimatedAggRows * coefficient &lt;= originalRows）。禁用时，规则将返回原始的 `INTERSECT` 而不插入 DISTINCT。该开关为会话级，通过 `SessionVariable.isCboEnableIntersectAddDistinct()` / `setCboEnableIntersectAddDistinct(...)` 访问。
* **作用域**：会话
* **默认值**：`true`
* **类型**：boolean
* **引入版本**：v3.3.12, v3.4.2, v3.5.0

### cbo_enable_low_cardinality_optimize

* 描述：是否开启低基数全局字典优化。开启后，查询 STRING 列时查询速度会有 3 倍左右提升。
* 默认值：true

### cbo_enable_low_cardinality_optimize_for_join

* **描述**: 会话开关，启用将连接重写为对字符串列使用字典编码（low-cardinality）表示的功能。启用时，优化器（参见 `DecodeRewriter.visitPhysicalHashJoin`）会尝试在连接的 ON 谓词、投影和谓词中将字符串列替换为其字典 ID 对应列，以避免代价高昂的字符串比较。收集器（`DecodeCollector.visitPhysicalJoin`）也使用此标志来决定是否为低基数列提取连接相等组；注意收集器当前仅对广播型连接应用此优化（当双方都是 shuffle 时会跳过重写）。若禁用，则与连接相关的低基数重写被关闭，相关的 ON 列会被标记以防止重写。该开关也通过 `SessionVariable.isEnableLowCardinalityOptimizeForJoin()` 暴露。
* **作用域**: Session
* **默认值**: `true`
* **类型**: boolean
* **引入版本**: -

### cbo_eq_base_type

* 描述：用来指定 DECIMAL 类型和 STRING 类型的数据比较时的强制类型，默认按照 `DECIMAL` 类型进行比较，可选 `VARCHAR`（按数值进行比较）。**该变量仅在进行 `=` 和 `!=` 比较时生效。**
* 类型：String
* 引入版本：v2.5.14

### cbo_json_v2_dict_opt

* 描述：是否为 JSON v2 路径改写生成的 Flat JSON 字符串子列启用低基数字典优化。开启后，优化器可以为这些子列构建并使用全局字典，从而加速字符串表达式、GROUP BY、JOIN 等操作。
* 默认值：true
* 类型：Boolean

### cbo_json_v2_rewrite

* 描述：是否在优化器中启用 JSON v2 路径改写。开启后，会将 JSON 函数（如 `get_json_*`）改写为直接访问 Flat JSON 子列，从而启用谓词下推、列裁剪以及字典优化。
* 默认值：true
* 类型：Boolean

### cbo_push_down_distinct_limit

* **描述**: 会话级阈值（行数限制），优化器在将全局 DISTINCT 聚合转换为两阶段（本地 + 全局）聚合时使用。在 SplitTwoPhaseAggRule 重写过程中，如果查询是带 LIMIT 的纯 GROUP BY 且查询 LIMIT 小于该变量，则优化器会将该 LIMIT 下推到本地聚合阶段（将本地 agg limit 设置为查询 LIMIT），以减少全局阶段处理的数据。具体地，该规则检查与 `aggOp.getLimit() &lt; context.getSessionVariable().cboPushDownDistinctLimit()` 等价的条件，并据此决定是否设置非默认的 localAggLimit。较小的值会使此下推不那么激进；较大的值会使其更激进。参见 `SplitTwoPhaseAggRule` 的使用位置。
* **范围**: Session
* **默认值**: `4096`
* **数据类型**: long
* **引入版本**: v3.4.1, v3.5.0

### cbo_push_down_topn_limit

* **范围**: Session
* **描述**: 优化器在考虑将 TopN 操作下推到更低的计划节点时，会检查的最大 TopN LIMIT。优化器会将 TopN 的 LIMIT 与此会话变量比较（例如 PushDownTopNToPreAggRule、PushDownTopNBelowUnionRule 和 PushDownTopNBelowOuterJoinRule 中的规则）。如果 TopN 没有 LIMIT 或其 LIMIT &gt; 此值，则不执行下推。此变量仅用于限制数值型 LIMIT 的检查；其他规则条件（例如存在 OFFSET、依赖聚合结果或连接/并集约束）仍由各个变换规则强制执行。将此值设置为 0 会在通常的正 LIMIT 情况下有效地禁用 TopN 下推。
* **默认值**: `1000`
* **类型**: long
* **引入版本**: v3.2.0

### character_set_database（global）

* 描述：StarRocks 数据库支持的字符集，当前仅支持 UTF8 编码（`utf8`）。
* 默认值：utf8
* 类型：String

### collation_server

* **描述**: 会话范围的服务器排序规则名称。此变量设置当未提供显式排序规则时，SQL 层用于字符串比较、排序（ORDER BY）、模式匹配（LIKE）和其他对排序规则敏感操作的默认排序规则。它存储在 SessionVariable 对象的 `collationServer` 字段中，并与 `character_set_server`、`collation_connection` 和 `collation_database` 一起工作。可接受的值为 MySQL 风格的 collation 标识符（例如 `utf8_general_ci`）。更改它只影响当前会话的与区域设置相关的行为。
* **范围**: Session
* **默认值**: `utf8_general_ci`
* **类型**: String
* **引入版本**: v3.2.0

### computation_fragment_scheduling_policy

* **作用域**: Session
* **默认值**: `COMPUTE_NODES_ONLY`
* **类型**: String
* **描述**: 控制查询 planner/scheduler 在放置 computation fragments 时可选择的节点类型。有效值（不区分大小写）为：
  * `compute_nodes_only` — 仅在 compute 节点上调度 fragments（默认）
  * `all_nodes` — 允许在 compute 节点和 BE/backend 节点上调度
  会话的 SET 语句设置器会验证该值（SetStmtAnalyzer），在执行 SET 语句时对于不支持的值会抛出 SemanticException。SessionVariable 的设置器也会验证并对无效名称抛出 IllegalArgumentException；有效值会映射到枚举 SessionVariableConstants.ComputationFragmentSchedulingPolicy 并以大写形式存储。getter 返回相应的枚举，如果存储的值缺失或无法识别，则回退到 `COMPUTE_NODES_ONLY`。
* **引入版本**: v3.2.7

### connector_io_tasks_per_scan_operator

* 描述：外表查询时每个 Scan 算子能同时下发的 I/O 任务的最大数量。目前外表查询时会使用自适应算法来调整并发 I/O 任务的数量，通过 `enable_connector_adaptive_io_tasks` 开关来控制，默认打开。
* 默认值：16
* 类型：Int
* 引入版本：v2.5

### connector_sink_compression_codec

* 描述：用于指定写入 Hive 表或 Iceberg 表时以及使用 Files() 导出数据时的压缩算法。有效值：`uncompressed`、`snappy`、`lz4`、`zstd`、`gzip`。
* 默认值：uncompressed
* 类型：String
* 引入版本：v3.2.3

### connector_sink_target_max_file_size

* 描述: 指定将数据写入 Hive 表或 Iceberg 表或使用 Files() 导出数据时目标文件的最大大小。该限制并不一定精确，只作为尽可能的保证。
* 单位：Bytes
* 默认值: 1073741824
* 类型: Long
* 引入版本: v3.3.0

### count_distinct_column_buckets

* 描述：group-by-count-distinct 查询中为 count distinct 列设置的分桶数。该变量只有在 `enable_distinct_column_bucketization` 设置为 `true` 时才会生效。
* 默认值：1024
* 引入版本：v2.5

### custom_query_id (session)

* **描述**: 用于将某些外部标识绑定到当前查询。可以在执行查询前通过 `SET SESSION custom_query_id = 'my-query-id';` 设置。查询结束后该值会被重置。该值可用于 `KILL QUERY 'my-query-id'`。在审计日志中可作为 `customQueryId` 字段找到该值。
* **默认值**: ""
* **数据类型**: String
* **引入版本**: v3.4.0

### datacache_sharing_work_period

- 描述：Cache Sharing 功能的生效时长。每次群集扩展操作后，如果启用了缓存共享功能，只有在这段时间内的请求才会尝试访问其他节点的缓存数据。
- 默认值：600
- 单位：秒
- 引入版本：v3.5.1

### default_rowset_type (global)

全局变量，仅支持全局生效。用于设置计算节点存储引擎默认的存储格式。当前支持的存储格式包括：alpha/beta。

### default_table_compression

* 描述：存储表格数据时使用的默认压缩算法，支持 LZ4、Zstandard（或 zstd）、zlib 和 Snappy。如果您建表时在 PROPERTIES 设置了 `compression`，则 `compression` 指定的压缩算法生效。
* 默认值：lz4_frame
* 类型：String
* 引入版本：v3.0

### disable_colocate_join

* 描述：控制是否启用 Colocate Join 功能。默认值为 false，表示启用该功能。true 表示禁用该功能。当该功能被禁用后，查询规划将不会尝试执行 Colocate Join。
* 默认值：false

### disable_join_reorder

* **范围**: 会话
* **描述**: 当设置为 `true` 时，基于成本的优化器会跳过所有的连接重排序阶段。具体而言，planner 不会应用 `ReorderJoinRule`、连接转换规则、外连接转换规则，或在允许重排序时通常会添加的相关连接交换性展开。`QueryOptimizer` 和 `SPMOptimizer` 中的代码路径会检查此标志，并在评估诸如 innerCrossJoinNode < `CBO_MAX_REORDER_NODE` 和外连接是否可重排序等条件时对重排序进行保护。可用于强制优化器保持原始连接顺序（用于调试、稳定计划或当重排序产生更差的计划时）。SessionVariable 上可用的辅助方法包括 `isDisableJoinReorder()`、`disableJoinReorder()`、`enableJoinReorder()` 和 `enableJoinReorder(boolean)`。
* **默认值**: `false`
* **数据类型**: boolean
* **引入版本**: v3.2.0

### div_precision_increment

* 描述：用于兼容 MySQL 客户端，无实际作用。
* 默认值：4
* 类型：Int

### dynamic_overwrite

* 描述：是否为 INSERT OVERWRITE 语句覆盖写分区表时启用 [Dynamic Overwrite](./sql-statements/loading_unloading/INSERT.md#dynamic-overwrite) 语义。有效值：
  * `true`：启用 Dynamic Overwrite。
  * `false`：禁用 Dynamic Overwrite 并使用默认语义。
* 默认值：false
* 引入版本：v3.4.0

### enable_adaptive_sink_dop

* 描述：是否开启导入自适应并行度。开启后 INSERT INTO 和 Broker Load 自动设置导入并行度，保持和 `pipeline_dop` 一致。新部署的 2.5 版本默认值为 `true`，从 2.4 版本升级上来为 `false`。
* 默认值：false
* 引入版本：v2.5

### enable_bucket_aware_execution_on_lake

* 描述：是否针对数据湖（如 Iceberg 表）查询启用 Bucket-aware 执行。启用后，系统通过利用分桶信息来优化查询执行，减少数据 Shuffle 并提高性能。此优化对分桶表的 Join 和 Aggregation 特别有效。
* 默认值：true
* 数据类型：Boolean
* 引入版本：v4.0

### enable_cbo_based_mv_rewrite

* 描述：是否在 CBO 阶段启用物化视图改写，这可以最大化查询改写成功的可能性（例如，当物化视图和查询之间的连接顺序不同时），但这会增加优化器阶段的执行时间。
* 默认值：true
* 引入版本：v3.5.5，v4.0.1

### enable_cbo_table_prune

* **范围**: 会话
* **描述**: 启用针对基数保持连接的由 CBO 驱动的表裁剪。设置为 true 时，优化器（包括 QueryOptimizer 和 SPMOptimizer）将在 memo 优化规则集中加入 CboTablePruneRule，前提是用于 CBO 裁剪的连接节点数量 &lt; 10。此标志仅控制在 memo 优化期间是否考虑该裁剪规则；它不会改变裁剪规则的内部阈值或连接重排行为。使用 `enable_rbo_table_prune` 启用 RBO 变体，使用 `enable_table_prune_on_update` 控制对 UPDATE 语句的裁剪。
* **默认值**: `false`
* **数据类型**: boolean
* **引入版本**: v3.2.0

### enable_connector_adaptive_io_tasks

* 描述：外表查询时是否使用自适应策略来调整 I/O 任务的并发数。默认打开。如果未开启自适应策略，可以通过 `connector_io_tasks_per_scan_operator` 变量来手动设置外表查询时的 I/O 任务并发数。
* 默认值：true
* 引入版本：v2.5

### enable_datacache_async_populate_mode

* 描述：是否使用异步方式进行 Data Cache 填充。系统默认使用同步方式进行填充，即在查询数据时同步填充进行缓存填充。
* 默认值：false
* 引入版本：v3.2.7

### enable_datacache_io_adaptor

* 描述：是否开启 Data Cache I/O 自适应开关。`true` 表示开启。开启后，系统会根据当前磁盘 I/O 负载自动将一部分缓存请求路由到远端存储来减少磁盘压力。
* 默认值：true
* 引入版本：v3.3.0

### enable_datacache_sharing

- 描述：是否启用 Cache Sharing。设置为 `true` 可启用该功能。Cache Sharing 能够在本地缓存未命中时通过网络访问其他节点上的缓存数据，这有助于减少集群扩展过程中缓存失效造成的性能抖动。只有当 FE 参数 `enable_trace_historical_node` 设置为 `true` 时，此变量才会生效。
- 默认值：true
- 引入版本：v3.5.1

### enable_distinct_agg_over_window

* **范围**: 会话
* **描述**: 控制一个优化器重写，该重写将对 window 函数计算的 DISTINCT 聚合转换为等价的空值安全相等连接（null-safe-equality join）计划。启用时，QueryOptimizer 会先运行准备性重写（合并/拆分 projection 规则和逻辑属性推导），并应用 DistinctAggregationOverWindowRule，将计划中出现的 DISTINCT aggregation-over-window 模式（计划中存在 LogicalWindowOperator）替换为基于 join 的实现，这可能更高效或启用进一步优化。禁用时，优化器跳过此重写，保留 DISTINCT-over-window 表达式不变。该变量与 `optimize_distinct_agg_over_framed_window` 交互，后者管理 framed-window 特定的优化。
* **默认值**: `true`
* **数据类型**: boolean
* **引入版本**: -

### enable_distinct_column_bucketization

* 描述：是否在 group-by-count-distinct 查询中开启对 count distinct 列的分桶优化。在类似 `select a, count(distinct b) from t group by a;` 的查询中，如果 group by 列 a 为低基数列，count distinct 列 b 为高基数列且发生严重数据倾斜时，会引发查询性能瓶颈。可以通过对 count distinct 列进行分桶来平衡数据，规避数据倾斜。

  该变量需要与 `count_distinct_column_buckets` 配合使用。

  您也可以通过添加 `skew` hint 来开启 count distinct 列的分桶优化，例如 `select a,count(distinct [skew] b) from t group by a;`。
* 默认值：false，表示不开启。
* 引入版本：v2.5

### enable_force_rule_based_mv_rewrite

* 描述：在优化器的 RBO（rule-based optimization）阶段是否针对多表查询启用查询改写。启用此功能将提高查询改写的鲁棒性。但如果查询未命中物化视图，则会增加优化耗时。
* 默认值：true
* 引入版本：v3.3

### enable_gin_filter

* 描述：查询时是否使用[全文倒排索引](../table_design/indexes/inverted_index.md)。
* 默认值：true
* 引入版本：v3.3.0

### enable_global_runtime_filter

* 描述：Global runtime filter 开关。Runtime Filter（简称 RF）在运行时对数据进行过滤，过滤通常发生在 Join 阶段。当多表进行 Join 时，往往伴随着谓词下推等优化手段进行数据过滤，以减少 Join 表的数据扫描以及 shuffle 等阶段产生的 IO，从而提升查询性能。StarRocks 中有两种 RF，分别是 Local RF 和 Global RF。Local RF 应用于 Broadcast Hash Join 场景。Global RF 应用于 Shuffle Join 场景。
* 默认值 `true`，表示打开 global runtime filter 开关。关闭该开关后, 不生成 Global RF, 但是依然会生成 Local RF。

### enable_group_by_compressed_key

* 描述：是否利用准确的统计信息来压缩 GROUP BY Key 列。有效值：`true` 和 `false`。
* 默认值：true
* 引入版本：v4.0

### enable_group_execution

* **作用域**: Session
* **描述**: 启用将兼容的 fragment 分组为 colocated 执行组以减少 shuffle 并启用 colocated 执行的 planner 优化。启用时，PlanFragmentBuilder 会将 ExecGroups 标记为 colocate 组并在 colocate join 时合并左右组，为聚合阶段（除非存在本地 shuffle）启用 group execution，并为 set 操作设置 colocate 行为。该变量在构建计划的各处被访问（例如聚合、join 和 set-operation 处理）。注意：当启用 runtime adaptive DOP 时，该功能会隐式禁用，因为 runtime DOP 要求 join probe 等待 build，且与 group execution 冲突。相关会话配置：`group_execution_group_scale`、`group_execution_max_groups`、`group_execution_min_scan_rows`。
* **默认值**: `true`
* **类型**: boolean
* **引入版本**: v3.3.0, v3.4.0, v3.5.0

### enable_group_level_query_queue (global)

* 描述：是否开启资源组粒度的[查询队列](../administration/management/resource_management/query_queues.md)。
* 默认值：false，表示不开启。
* 引入版本：v3.1.4

### enable_insert_partial_update

* **描述**：是否为主键表的 INSERT 语句启用部分更新（Partial Update）。当设置为 `true`（默认）时，如果 INSERT 语句只指定了部分列（少于表中所有非生成列），系统会执行部分更新，即仅更新指定列，并保留其他列的现有值。当设置为 `false` 时，系统会对未指定的列使用默认值，而不是保留已有值。此功能特别适用于对主键表的特定列进行更新，而不影响其他列的值。
* **默认值**：true
* **引入版本**：v3.3.20、v3.4.9、v3.5.8、v4.0.2

### enable_insert_strict

* 描述：是否在使用 INSERT from FILES() 导入数据时启用严格模式。有效值：`true` 和 `false`（默认值）。启用严格模式时，系统仅导入合格的数据行，过滤掉不合格的行，并返回不合格行的详细信息。更多信息请参见 [严格模式](../loading/load_concept/strict_mode.md)。在早于 v3.4.0 的版本中，当 `enable_insert_strict` 设置为 `true` 时，INSERT 作业会在出现不合格行时失败。
* 默认值：true

### enable_lake_tablet_internal_parallel

* 描述：是否开启存算分离集群内云原生表的 Tablet 并行 Scan.
* 默认值：true
* 类型：Boolean
* 引入版本：v3.3.0

### enable_load_profile

* **作用域**: 会话
* **描述**: 当设置为 true 时，会话在加载作业完成后请求生成并收集运行时配置文件（runtime profile）。加载协调器会检查该会话变量（例如，StreamLoadTask 使用 `coord.isEnableLoadProfile()` 来决定是否调用 `collectProfile(false)`），并且它控制流式/加载操作期间与配置文件相关的行为（在 DefaultCoordinator、StreamLoadMgr、StreamLoadPlanner、QueryRuntimeProfile、StatisticUtils 中使用）。启用此选项会导致 FE 为加载收集/上传额外的分析信息，这会产生额外的工作和 I/O；它不会影响由其他配置文件变量控制的常规查询分析。
* **默认值**: `false`
* **类型**: boolean
* **引入版本**: v3.2.0

### enable_materialized_view_agg_pushdown_rewrite

* 描述：是否为物化视图查询改写启用聚合函数下推。如果设置为 `true`，聚合函数将在查询执行期间下推至 Scan Operator，并在执行 Join Operator 之前被物化视图改写。此举可以缓解 Join 操作导致的数据膨胀，从而提高查询性能。有关此功能的具体场景和限制的详细信息，请参见 [聚合函数下推](../using_starrocks/async_mv/use_cases/query_rewrite_with_materialized_views.md#聚合下推)。
* 默认值：false
* 引入版本：v3.3.0

### enable_materialized_view_for_insert

* 描述：是否允许 StarRocks 改写 INSERT INTO SELECT 语句中的查询。
* 默认值：false，即默认关闭该场景下的物化视图查询改写。
* 引入版本：v2.5.18, v3.0.9, v3.1.7, v3.2.2

### enable_materialized_view_text_match_rewrite

* 描述：是否启用基于文本的物化视图改写。当此项设置为 `true` 时，优化器将查询与现有的物化视图进行比较。如果物化视图定义的抽象语法树与查询或其子查询的抽象语法树匹配，则会对查询进行改写。
* 默认值：true
* 引入版本：v3.2.5，v3.3.0

### enable_materialized_view_union_rewrite

* 描述：是否启用物化视图 UNION 改写。如果此项设置为 true，则系统在物化视图的谓词不能满足查询的谓词时，会尝试使用 UNION ALL 来补偿谓词。
* 默认值：true
* 引入版本：v2.5.20，v3.1.9，v3.2.7，v3.3.0

### enable_metadata_profile

* 描述：是否为 Iceberg Catalog 的元数据收集查询开启 Profile。
* 默认值：true
* 引入版本：v3.3.3

### enable_multicolumn_global_runtime_filter

* 描述：多列 Global runtime filter 开关。默认值为 false，表示关闭该开关。

  对于 Broadcast 和 Replicated Join 类型之外的其他 Join，当 Join 的等值条件有多个的情况下：
  * 如果该选项关闭: 则只会产生 Local RF。
  * 如果该选项打开, 则会生成 multi-part GRF, 并且该 GRF 需要携带 multi-column 作为 partition-by 表达式.

* 默认值：false

### enable_parquet_reader_bloom_filter

* 默认值：true
* 类型：Boolean
* 单位：-
* 描述：是否在读取 Parquet 文件时启用 Bloom Filter 优化。
  * `true`（默认）：读取 Parquet 文件时启用 Bloom Filter 优化。
  * `false`：读取 Parquet 文件时禁用 Bloom Filter 优化。
* 引入版本：v3.5.0

### enable_parquet_reader_page_index

* 默认值：true
* 类型：Boolean
* 单位：-
* 描述：是否在读取 Parquet 文件时启用 Page Index 优化。
  * `true`（默认）：读取 Parquet 文件时启用 Page Index 优化。
  * `false`：读取 Parquet 文件时禁用 Page Index 优化。
* 引入版本：v3.5.0

### enable_partition_hash_join

* 描述: 是否启用自适应 Partition Hash Join。
* 默认值: true
* 引入版本: v3.4

### enable_phased_scheduler

* 描述: 是否启用多阶段调度。当启用多阶段调度时，系统将根据 Fragment 之间的依赖关系进行调度。例如，系统将首先调度 Shuffle Join 的 Build Side Fragment ，然后调度 Probe Side Fragment （注意，与分阶段调度不同，多阶段调度仍处于 MPP 执行模式下）。启用多阶段调度可显著降低大量 UNION ALL 查询的内存使用量。
* 默认值: false
* 引入版本: v3.3

### enable_pipeline_engine

* 描述：是否启用 Pipeline 执行引擎。`true`：启用（默认），`false`：不启用。
* 默认值：true

### enable_plan_advisor

* 描述：是否为慢查询或手动标记查询开启 Query Feedback 功能。
* 默认值：true
* 引入版本：v3.4.0

### enable_predicate_reorder

* **范围**: 会话
* **描述**: 启用后，优化器会应用 PredicateReorderRule 对算子内的合取谓词（AND）根据估计选择性进行重排序。该规则从子节点统计信息（或者对于 PhysicalOlapScanOperator，从表列统计信息）构建 Statistics 对象，然后使用 DefaultPredicateSelectivityEstimator 估计每个合取项的选择性。合取项按选择性从小到大排序（选择性估计越低越优先评估），生成新的复合 AND 谓词。仅当谓词为 `CompoundPredicateOperator` 时才会运行重写，并且该重写通过 `PredicateReorderRule.rewrite(...)` 被调用，并受 `SessionVariable.isEnablePredicateReorder()` 保护。如果所需统计信息不可用或谓词不是复合谓词，则不会进行重排序。SessionVariable 提供 `isEnablePredicateReorder()`、`enablePredicateReorder()` 和 `disablePredicateReorder()` 访问器。
* **默认值**: `false`
* **数据类型**: boolean
* **引入版本**: v3.2.0

### enable_profile

用于设置是否需要查看查询的 profile。默认为 `false`，即不需要查看 profile。2.5 版本之前，该变量名称为 `is_report_success`，2.5 版本之后更名为 `enable_profile`。

默认情况下，只有在查询发生错误时，BE 才会发送 profile 给 FE，用于查看错误。正常结束的查询不会发送 profile。发送 profile 会产生一定的网络开销，对高并发查询场景不利。当用户希望对一个查询的 profile 进行分析时，可以将这个变量设为 `true` 后，发送查询。查询结束后，可以通过在当前连接的 FE 的 web 页面（地址：fe_host:fe_http_port/query）查看 profile。该页面会显示最近 100 条开启了 `enable_profile` 的查询的 profile。

### enable_query_cache

* 描述：是否开启 Query Cache。取值范围：true 和 false。true 表示开启，false 表示关闭（默认值）。开启该功能后，只有当查询满足[Query Cache](../using_starrocks/caching/query_cache.md#应用场景) 所述条件时，才会启用 Query Cache。
* 默认值：false
* 引入版本：v2.5

### enable_query_queue_load (global)

* 描述：用于控制是否为导入任务启用查询队列。
* 默认值：false
* 类型：Boolean

### enable_query_queue_select (global)

* 描述：用于控制是否为 SELECT 查询启用查询队列。
* 默认值：false
* 类型：Boolean

### enable_query_queue_statistic (global)

* 描述：用于控制是否为统计信息查询启用查询队列。
* 默认值：false
* 类型：Boolean

### enable_query_tablet_affinity

* 描述：用于控制在多次查询同一个 tablet 时是否倾向于选择固定的同一个副本。

  如果待查询的表中存在大量 tablet，开启该特性会对性能有提升，因为会更快的将 tablet 的元信息以及数据缓存在内存中。但是，如果查询存在一些热点 tablet，开启该特性可能会导致性能有所退化，因为该特性倾向于将一个热点 tablet 的查询调度到相同的 BE 上，在高并发的场景下无法充分利用多台 BE 的资源。
* 默认值：`false`，表示使用原来的机制，即每次查询会从多个副本中选择一个。
* 类型：Boolean
* 引入版本：v2.5.6、v3.0.8、v3.1.4、v3.2.0

### enable_query_trigger_analyze

* 默认值：true
* 类型：Boolean
* 单位：-
* 描述：是否开启查询触发 ANALYZE 外表任务。
* 引入版本：v3.4.0

### enable_rbo_table_prune

* **范围**: 会话
* **描述**: 启用后，在查询优化器中为基数保持（cardinality-preserving）的连接激活基于规则的表裁剪。QueryOptimizer 在 pruneTables() 中检查此标志，当为 true 且存在可裁剪的连接时，会运行一系列重写规则（分区裁剪、投影合并、`UniquenessBasedTablePruneRule`、连接重排，在设置了 `enable_table_prune_on_update` 时可选地运行 `PrimaryKeyUpdateTableRule`），最后应用 `RboTablePruneRule` 来移除或简化可适用连接的不必要表输入。启用此选项还会在优化器上下文中禁用连接等价推导（调用 setEnableJoinEquivalenceDerive(false)），这会改变优化期间连接谓词/等价的推导方式。适用于规则基（树重写）裁剪在基数保持的连接模式下能产生更好裁剪效果的场景。
* **默认值**: `false`
* **数据类型**: boolean
* **引入版本**: v3.2.0

### enable_rewrite_simple_agg_to_meta_scan

* **作用域**: Session
* **描述**: 启用优化器转换，将符合条件的简单聚合查询重写为在元数据（例如 zonemap/列统计）上执行的 LogicalMetaScan + 聚合，以避免扫描基础数据。启用后，RewriteSimpleAggToMetaScanRule 可能会将诸如 MIN/MAX/COUNT（以及 COLUMN_SIZE / COLUMN_COMPRESSED_SIZE）之类的查询替换为 MetaScan 和 基于 SUM 的聚合，从而在适合的 DUPLICATE_KEY OLAP 表上大幅减少 I/O。该规则仅在下列所有条件满足时生效：
  * 目标表为 DUP_KEYS 且不存在删除；
  * 无 GROUP BY 键、无 HAVING、无查询级别过滤器且无 LIMIT；
  * 仅使用 MIN、MAX、COUNT（非 distinct）、COLUMN_SIZE 或 COLUMN_COMPRESSED_SIZE；
  * 聚合器参数为单个原始列且无表达式，并且存在 zonemap 索引（对 min/max 不支持字符串/复杂类型）；
  * 无分区修剪或不兼容的模式更改（在具有模式演进的 shared-data 模式下，min/max 被禁用）。
  注意：某些组件（例如在生成带提示的基线计划时的 SPMPlanBuilder）会临时禁用此变量以确保计划生成的一致性。
* **默认值**: `true`
* **数据类型**: boolean
* **引入版本**: v3.2.0

### enable_rewrite_sum_by_associative_rule

* **作用域**: Session
* **描述**: 控制一个优化器重写规则，该规则将形如 sum(expr +/- const) 的聚合表达式转换为 sum(expr) +/- count(expr_other) * const，以避免每行投影开销。该规则仅在聚合为 `SUM`（非 `DISTINCT`，且非 DecimalV2）时适用；SUM 参数的预聚合投影必须是一个 `ADD` 或 `SUBTRACT` 调用，并且至少有一个常量操作数。重写器将：
    * 在底层类型完全兼容且存在匹配的 SUM 签名时移除冗余的 cast，
    * 生成新的聚合（对非常量部分使用 SUM，对用于乘法的参数使用 COUNT），
    * 保留无法重写的原始聚合。
  对于分组聚合，如果重写不会减少唯一聚合函数的数量（以避免增加开销），规则会跳过重写。优化器在尝试重写前会检查 `context.getSessionVariable().isEnableRewriteSumByAssociativeRule()`。
* **默认值**: `true`
* **数据类型**: boolean
* **引入版本**: v3.2.0

### enable_runtime_adaptive_dop

* **范围**: 会话
* **描述**: 启用后，允许 pipeline 执行引擎为满足条件的计划片段在运行时激活自适应并行度 (DOP)。在构建计划（在 PlanFragmentBuilder 中）时，如果未使用查询缓存且 `enable_runtime_adaptive_dop` 为 true，则报告 `canUseRuntimeAdaptiveDop()` 的片段将应用 `enableAdaptiveDop()`，以便其 DOP 可以根据观测到的执行指标在运行时调整。此设置仅在 `enable_pipeline_engine` 也为 true 时生效。启用此选项将自动禁用 `enable_pipeline_level_multi_partitioned_rf`（setter 会清除该标志）。该变量值会记录在查询运行时 profile 中以供审计。
* **默认值**: `false`
* **类型**: boolean
* **引入版本**: v3.2.0

### enable_scan_datacache

* 描述：是否开启 Data Cache 特性。该特性开启之后，StarRocks 通过将外部存储系统中的热数据缓存成多个 block，加速数据查询和分析。更多信息，参见 [Data Cache](../data_source/data_cache.md)。该特性从 2.5 版本开始支持。在 3.2 之前各版本中，对应变量为 `enable_scan_block_cache`。
* 默认值：true
* 引入版本：v2.5

### enable_short_circuit

* 描述：是否启用短路径查询。默认值：`false`。如果将其设置为 `true`，当[查询满足条件](../table_design/hybrid_table.md#查询数据)（用于评估是否为点查）：WHERE 子句的条件列必须包含所有主键列，并且运算符为 `=` 或者 `IN`，则该查询才会走短路径。
* 默认值：false
* 引入版本：v3.2.3

### enable_sort_aggregate

* 描述：是否开启 sorted streaming 聚合。`true` 表示开启 sorted streaming 聚合功能，对流中的数据进行排序。
* 默认值：false
* 引入版本：v2.5

### enable_spill

* 描述：是否启用中间结果落盘。默认值：`false`。如果将其设置为 `true`，StarRocks 会将中间结果落盘，以减少在查询中处理聚合、排序或连接算子时的内存使用量。
* 默认值：false
* 引入版本：v3.0

### enable_spill_to_remote_storage

* 描述：是否启用将中间结果落盘至对象存储。如果设置为 `true`，当本地磁盘的用量达到上限后，StarRocks 将中间结果落盘至 `spill_storage_volume` 中指定的存储卷中。有关更多信息，请参阅 [将中间结果落盘至对象存储](../administration/management/resource_management/spill_to_disk.md#preview-将中间结果落盘至对象存储)。
* 默认值：false
* 引入版本：v3.3.0

### enable_spm_rewrite

* 描述：是否启用 SQL Plan Manager (SPM) 查询改写功能。启用此功能后，StarRocks 将自动将相应的查询改写为绑定的查询计划，以提升查询性能和稳定性。
* 默认值：false

### enable_strict_order_by

* 描述：是否校验 ORDER BY 引用列是否有歧义。设置为默认值 `TRUE` 时，如果查询中的输出列存在不同的表达式使用重复别名的情况，且按照该别名进行排序，查询会报错，例如 `select distinct t1.* from tbl1 t1 order by t1.k1;`。该行为和 2.3 及之前版本的逻辑一致。如果取值为 `FALSE`，采用宽松的去重机制，把这类查询作为有效 SQL 处理。
* 默认值：true
* 引入版本：v2.5.18，v3.1.7

### enable_sync_materialized_view_rewrite

* 描述：是否启用基于同步物化视图的查询改写。
* 默认值：true
* 引入版本：v3.1.11，v3.2.5

### enable_tablet_internal_parallel

* 描述：是否开启自适应 Tablet 并行扫描，使用多个线程并行分段扫描一个 Tablet，可以减少 Tablet 数量对查询能力的限制。
* 默认值：true
* 引入版本：v2.3

### enable_topn_runtime_filter

* 描述: 是否启用 TopN Runtime Filter。如果启用此功能，对于 ORDER BY LIMIT 查询，将动态构建一个 Runtime Filter 并将其下推到 Scan 阶段进行过滤。
* 默认值: true
* 引入版本: v3.3

### enable_view_based_mv_rewrite

* 描述：是否为基于逻辑视图创建的物化视图启用查询改写。如果此项设置为 `true`，则逻辑视图被用作统一节点进行查询改写，从而获得更好的性能。如果此项设置为 `false`，则系统将针对逻辑视图的查询展开变为针对物理表或物化视图的查询，然后进行改写。
* 默认值：false
* 引入版本：v3.1.9，v3.2.5，v3.3.0

### enable_wait_dependent_event

* 描述: 在同一 Fragment 内，Pipeline 是否等待依赖的 Operator 完成执行后再继续执行。例如，在 Left Join 查询中，当启用此功能时，Probe Side Operator 会在 Build Side Operator 完成执行后再开始执行。启用此功能可减少内存使用，但可能增加查询延迟。然而，对于在CTE中重用的查询，启用此功能可能增加内存使用。
* 默认值: false
* 引入版本: v3.3

### enable_write_hive_external_table

* 描述：是否开启往 Hive 的 External Table 写数据的功能。
* 默认值：false
* 引入版本：v3.2

### event_scheduler

* 描述：用于兼容 MySQL 客户端。无实际作用。
* 默认值：OFF
* 类型：String

### forward_to_leader

用于设置是否将一些命令转发到 Leader FE 节点执行。默认为 `false`，即不转发。StarRocks 中存在多个 FE 节点，其中一个为 Leader 节点。通常用户可以连接任意 FE 节点进行全功能操作。但部分信息查看指令只有从 Leader FE 节点才能获取详细信息。别名 `forward_to_master`。

如 `SHOW BACKENDS;` 命令，如果不转发到 Leader FE 节点，则仅能看到节点是否存活等一些基本信息，而转发到 Leader FE 则可以获取包括节点启动时间、最后一次心跳时间等更详细的信息。

当前受该参数影响的命令如下：

* SHOW FRONTENDS;

  转发到 Leader 可以查看最后一次心跳信息。

* SHOW BACKENDS;

  转发到 Leader 可以查看启动时间、最后一次心跳信息、磁盘容量信息。

* SHOW BROKER;

  转发到 Leader 可以查看启动时间、最后一次心跳信息。

* SHOW TABLET;
* ADMIN SHOW REPLICA DISTRIBUTION;
* ADMIN SHOW REPLICA STATUS;

  转发到 Leader 可以查看 Leader FE 元数据中存储的 tablet 信息。正常情况下，不同 FE 元数据中 tablet 信息应该是一致的。当出现问题时，可以通过这个方法比较当前 FE 和 Leader FE 元数据的差异。

* SHOW PROC;

  转发到 Leader 可以查看 Leader FE 元数据中存储的相关 PROC 的信息。主要用于元数据比对。

### group_concat_max_len

* 描述：[group_concat](sql-functions/string-functions/group_concat.md) 函数返回的字符串的最大长度，单位为字符。
* 默认值：1024
* 最小值：4
* 类型：Long

### hash_join_push_down_right_table

* 描述：用于控制在 Join 查询中是否可以使用针对右表的过滤条件来过滤左表的数据，可以减少 Join 过程中需要处理的左表的数据量。取值为 `true` 时表示允许该操作，系统将根据实际情况决定是否能对左表进行过滤；取值为 `false` 表示禁用该操作。
* 默认值：true

### historical_nodes_min_update_interval

- 描述：历史节点记录两次更新之间的最小间隔。如果集群的节点在短时间内频繁变化（即小于此变量中设置的值），一些中间状态将不会被记录为有效的历史节点快照。历史节点是 Cache Sharing 功能在集群扩展时选择正确缓存节点的主要依据。
- 默认值：600
- 单位：秒
- 引入版本：v3.5.1

### init_connect (global)

用于兼容 MySQL 客户端。无实际作用。

### innodb_read_only

* **范围**: Session
* **描述**: 以系统变量 `innodb_read_only` 暴露的会话范围布尔标志。它存储在会话对象中（`SessionVariable` 的字段 `innodbReadOnly`），并可通过 `isInnodbReadOnly()` / `setInnodbReadOnly()` 访问。该变量指示会话是否应将 InnoDB 视为只读，以兼容 MySQL 客户端和工具。StarRocks 本身不会在存储层自动强制此只读语义；其他组件必须显式检查此会话标志以遵循只读语义。
* **默认值**: `true`
* **类型**: boolean
* **引入版本**: v3.2.0

### insert_max_filter_ratio

* 描述：INSERT 导入作业的最大容忍率，即导入作业能够容忍的因数据质量不合格而过滤掉的数据行所占的最大比例。当不合格行数比例超过该限制时，导入作业失败。默认值：`0`。范围：[0, 1]。
* 默认值：0
* 引入版本：v3.4.0

### insert_timeout

* 描述：INSERT 作业的超时时间。单位：秒。从 v3.4.0 版本开始，`insert_timeout` 作用于所有涉及 INSERT 的操作（例如，UPDATE、DELETE、CTAS、物化视图刷新、统计信息收集和 PIPE），替代原本的 `query_timeout`。
* 默认值：14400
* 引入版本：v3.4.0

### interactive_timeout

* 描述：用于兼容 MySQL 客户端。无实际作用。
* 默认值：1024
* 单位：秒
* 类型：Int

### io_tasks_per_scan_operator

* 每个 Scan 算子能同时下发的 I/0 任务的数量。如果使用远端存储系统（比如 HDFS 或 S3）且时延较长，可以增加该值。但是值过大会增加内存消耗。
* 默认值：4
* 类型：Int
* 引入版本：v2.5

### jit_level

* 描述：表达式 JIT 编译的启用级别。有效值：
  * `1`：系统为可编译表达式自适应启用 JIT 编译。
  * `-1`：对所有可编译的非常量表达式启用 JIT 编译。
  * `0`：禁用 JIT 编译。如果该功能返回任何错误，您可以手动禁用。
* 默认值：1
* 数据类型：Int
* 引入版本：-

### lake_bucket_assign_mode

* 描述：数据湖表查询的分桶分配模式。此变量控制系统执行查询期间启用 Bucket-aware 执行时如何将分桶分配给工作节点。有效值：
  * `balance`：在工作节点间均匀分配分桶以实现负载均衡，以获取更好的性能。
  * `elastic`：使用一致性哈希将分桶分配给工作节点，可以在弹性环境中提供更好的负载分配。
* 默认值：balance
* 数据类型：String
* 引入版本：v4.0

### language (global)

用于兼容 MySQL 客户端。无实际作用。

### license (global)

* 描述：显示 StarRocks 的 license。无其他作用。
* 默认值：Apache License 2.0
* 类型：String

### load_mem_limit

* 描述：用于指定导入操作的内存限制，单位为 Byte。默认值为 0，即表示不使用该变量，而采用 `query_mem_limit` 作为导入操作的内存限制。

  这个变量仅用于 INSERT 操作。因为 INSERT 操作涉及查询和导入两个部分，如果用户不设置此变量，则查询和导入操作各自的内存限制均为 `query_mem_limit`。否则，INSERT 的查询部分内存限制为 `query_mem_limit`，而导入部分限制为 `load_mem_limit`。

  其他导入方式，如 Broker Load，STREAM LOAD 的内存限制依然使用 `query_mem_limit`。

* 默认值：0
* 单位：Byte

### log_rejected_record_num（3.1 及以后）

* 描述：指定最多允许记录多少条因数据质量不合格而过滤掉的数据行数。取值范围：`0`、`-1`、大于 0 的正整数。
* 取值为 `0` 表示不记录过滤掉的数据行。
* 取值为 `-1` 表示记录所有过滤掉的数据行。
* 取值为大于 0 的正整数（比如 `n`）表示每个 BE 节点上最多可以记录 `n` 条过滤掉的数据行。

### low_cardinality_optimize_on_lake

* 默认值：true
* 类型：Boolean
* 单位：-
* 描述：是否在数据湖查询中启用低基数优化。有效值：
  * `true`（默认）：在数据湖查询中启用低基数优化。
  * `false`: 在数据湖查询中禁用低基数优化。
* 引入版本：v3.5.0

### low_cardinality_optimize_v2

* **作用域**: Session
* **描述**: 会话级别的开关，用于选择优化器使用的低基数优化实现。启用时，优化器会应用在 `LowCardinalityRewriteRule` 中实现的 V2 重写（该规则收集解码候选并使用 decode/encode 操作符重写计划）。禁用时，优化器回退到传统路径（`AddDecodeNodeForDictStringRule`）或跳过 V2 特定的重写。规划器会检查 `isEnableLowCardinalityOptimize()` 和此变量来决定是否运行 V2 重写。可以通过 `setUseLowCardinalityOptimizeV2` 在每个会话中更改。
* **默认值**: `true`
* **数据类型**: boolean
* **引入版本**: v3.3.0, v3.4.0, v3.5.0

### lower_case_table_names (global)

用于兼容 MySQL 客户端，无实际作用。StarRocks 中的表名是大小写敏感的。

### materialized_view_rewrite_mode（3.2 及以后）

指定异步物化视图的查询改写模式。有效值：

* `disable`：禁用异步物化视图的自动查询改写。
* `default`（默认值）：启用异步物化视图的自动查询改写，并允许优化器根据 Cost 决定是否可以使用物化视图改写查询。如果查询无法改写，则直接查询基表中的数据。
* `default_or_error`：启用异步物化视图的自动查询改写，并允许优化器根据 Cost 决定是否可以使用物化视图改写查询。如果查询无法改写，将返回错误。
* `force`：启用异步物化视图的自动查询改写，并且优化器优先使用物化视图改写查询。如果查询无法改写，则直接查询基表中的数据。
* `force_or_error`：启用异步物化视图的自动查询改写，并且优化器优先使用物化视图改写查询。如果查询无法改写，将返回错误。

### materialized_view_subuqery_text_match_max_count

* 描述：指定系统比对查询的子查询是否与物化视图定义匹配的最大次数。
* 默认值：4
* 引入版本：v3.2.5，v3.3.0

### max_allowed_packet

* 描述：用于兼容 JDBC 连接池 C3P0。该变量值决定了服务端发送给客户端或客户端发送给服务端的最大 packet 大小。当客户端报 `PacketTooBigException` 异常时，可以考虑调大该值。
* 默认值：33554432 (32 MB)
* 单位：Byte
* 类型：Int

### max_pushdown_conditions_per_column

* 描述：该变量的具体含义请参阅 [BE 配置项](../administration/management/BE_configuration.md)中 `max_pushdown_conditions_per_column` 的说明。
* 默认值：`-1`，表示使用 `be.conf` 中的配置值。如果设置大于 0，则忽略 `be.conf` 中的配置值。
* 类型：Int

### max_scan_key_num

* 描述：该变量的具体含义请参阅 [BE 配置项](../administration/management/BE_configuration.md)中 `max_scan_key_num` 的说明。
* 默认值：`-1`，表示使用 `be.conf` 中的配置值。如果设置大于 0，则忽略 `be.conf` 中的配置值。

### metadata_collect_query_timeout

* 描述：Iceberg Catalog 元数据收集阶段的超时时间。
* 单位： 秒
* 默认值：60
* 引入版本：v3.3.3

### nested_mv_rewrite_max_level

* 描述：可用于查询改写的嵌套物化视图的最大层数。
* 取值范围：[1, +∞)。取值为 `1` 表示只可使用基于基表创建的物化视图用于查询改写。
* 默认值：`3`
* 类型：Int

### net_buffer_length

* 描述：用于兼容 MySQL 客户端。无实际作用。
* 默认值：16384
* 类型：Int

### net_read_timeout

* 描述：用于兼容 MySQL 客户端。无实际作用。
* 默认值：60
* 单位：秒
* 类型：Int

### net_write_timeout

* 描述：用于兼容 MySQL 客户端。无实际作用。
* 默认值：60
* 单位：秒
* 类型：Int

### new_planner_optimize_timeout

* 描述：查询优化器的超时时间。一般查询中 Join 过多时容易出现超时。超时后会报错并停止查询，影响查询性能。您可以根据查询的具体情况调大该参数配置，也可以将问题上报给 StarRocks 技术支持进行排查。
* 默认值：3000
* 单位：毫秒

### optimizer_materialized_view_timelimit

* 描述：指定一个物化视图改写规则可消耗的最大时间。当达到阈值时，将不再使用该规则进行查询改写。
* 默认值：1000
* 单位：毫秒
* 类型：Long

### orc_use_column_names

* 描述：设置通过 Hive Catalog 读取 ORC 文件时，列的对应方式。默认值是 `false`，即按照 Hive 表中列的顺序对应。如果设置为 `true`，则按照列名称对应。
* 引入版本：v3.1.10

### parallel_exchange_instance_num

用于设置执行计划中，一个上层节点接收下层节点数据所使用的 exchange node 数量。默认为 -1，即表示 exchange node 数量等于下层节点执行实例的个数（默认行为）。当设置大于 0，并且小于下层节点执行实例的个数，则 exchange node 数量等于设置值。

在一个分布式的查询执行计划中，上层节点通常有一个或多个 exchange node 用于接收来自下层节点在不同 BE 上的执行实例的数据。通常 exchange node 数量等于下层节点执行实例数量。

在一些聚合查询场景下，如果底层需要扫描的数据量较大，但聚合之后的数据量很小，则可以尝试修改此变量为一个较小的值，可以降低此类查询的资源开销。如在 DUPLICATE KEY 明细表上进行聚合查询的场景。

### parallel_fragment_exec_instance_num

针对扫描节点，设置其在每个 BE 节点上执行实例的个数。默认为 1。

一个查询计划通常会产生一组 scan range，即需要扫描的数据范围。这些数据分布在多个 BE 节点上。一个 BE 节点会有一个或多个 scan range。默认情况下，每个 BE 节点的一组 scan range 只由一个执行实例处理。当机器资源比较充裕时，可以将增加该变量，让更多的执行实例同时处理一组 scan range，从而提升查询效率。

而 scan 实例的数量决定了上层其他执行节点，如聚合节点，join 节点的数量。因此相当于增加了整个查询计划执行的并发度。修改该参数会对大查询效率提升有帮助，但较大数值会消耗更多的机器资源，如CPU、内存、磁盘I/O。

### partial_update_mode

* 描述：控制部分更新的模式，有效值为：

  * `auto`（默认值），表示由系统通过分析更新语句以及其涉及的列，自动判断执行部分更新时使用的模式。
  * `column`，指定使用列模式执行部分更新，比较适用于涉及少数列并且大量行的部分列更新场景。

  详细信息，请参见[UPDATE](sql-statements/table_bucket_part_index/UPDATE.md#列模式的部分更新自-31)。
* 默认值：auto
* 引入版本：v3.1

### performance_schema (global)

用于兼容 8.0.16 及以上版本的 MySQL JDBC。无实际作用。

### phased_scheduler_max_concurrency

* 描述: 多阶段调度器调度 Leaf Node Fragment 的并发度。例如，默认值表示在大量 UNION ALL Scan 查询中，最多允许同时调度两个 Scan Fragment。
* 默认值: 2
* 引入版本: v3.3

### pipeline_dop

* 描述：一个 Pipeline 实例的并行数量。可通过设置实例的并行数量调整查询并发度。默认值为 0，即系统自适应调整每个 pipeline 的并行度。该变量还控制着 OLAP 表上导入作业的并行度。您也可以设置为大于 0 的数值，通常为 BE 节点 CPU 物理核数的一半。从 3.0 版本开始，支持根据查询并发度自适应调节 `pipeline_dop`。
* 默认值：0
* 类型：Int

### pipeline_profile_level

* 描述：用于控制 profile 的等级。一个 profile 通常有 5 个层级：Fragment、FragmentInstance、Pipeline、PipelineDriver、Operator。不同等级下，profile 的详细程度有所区别：
  * 0：在此等级下，StarRocks 会合并 profile，只显示几个核心指标。
  * 1：默认值。在此等级下，StarRocks 会对 profile 进行简化处理，将同一个 pipeline 的指标做合并来缩减层级。
  * 2：在此等级下，StarRocks 会保留 Profile 所有的层级，不做简化。该设置下 profile 的体积会非常大，特别是 SQL 较复杂时，因此不推荐该设置。
* 默认值：1
* 类型：Int

### pipeline_sink_dop

* 描述：用于向 Iceberg 表、Hive 表导入数据以及通过 INSERT INTO FILES() 导出数据的 Sink 并行数量。该参数用于调整这些导入作业的并发性。默认值为 0，即系统自适应调整并行度。您也可以将此变量设置为大于 0 的值。
* 默认值：0
* 类型：Int

### plan_mode

* 描述：Iceberg Catalog 元数据获取方案模式。详细信息，参考 [Iceberg Catalog 元数据获取方案](../data_source/catalog/iceberg/iceberg_catalog.md#附录元数据周期性后台刷新方案)。有效值：
  * `auto`：系统自动选择方案。
  * `local`：使用本地缓存方案。
  * `distributed`：使用分布式方案。
* 默认值：auto
* 引入版本：v3.3.3

#### enable_iceberg_column_statistics

* 描述：是否获取列统计信息，例如 `min`、`max`、`null count`、`row size` 和 `ndv`（如果存在 puffin 文件）。当此项设置为 `false` 时，仅收集行数信息。
* 默认值：false
* 引入版本：v3.4

### populate_datacache_mode

* 描述：StarRocks 从外部存储系统读取数据时，控制数据缓存填充行为。有效值包括：
  * `auto`（默认）：系统自动根据查询的特点，选择性进行缓存。
  * `always`：总是缓存数据。 
  * `never` 永不缓存数据。
* 默认值：auto
* 引入版本：v3.3.2

### prefer_compute_node

* 描述：将部分执行计划调度到 CN 节点执行。
* 默认值：false
* 引入版本：v2.4

### query_cache_agg_cardinality_limit

* 描述：GROUP BY 聚合的高基数上限。GROUP BY 聚合的输出预估超过该行数, 则不启用 cache。
* 默认值：5000000
* 类型：Long
* 引入版本：v2.5

### query_cache_entry_max_bytes

* 描述：Cache 项的字节数上限，触发 Passthrough 模式的阈值。当一个 Tablet 上产生的计算结果的字节数或者行数超过 `query_cache_entry_max_bytes` 或 `query_cache_entry_max_rows` 指定的阈值时，则查询采用 Passthrough 模式执行。当 `query_cache_entry_max_bytes` 或 `query_cache_entry_max_rows` 取值为 0 时, 即便 Tablet 产生结果为空，也采用 Passthrough 模式。
* 取值范围：0 ~ 9223372036854775807
* 默认值：4194304
* 单位：字节
* 类型：Long
* 引入版本：v2.5

### query_cache_entry_max_rows

* 描述：Cache 项的行数上限，见 `query_cache_entry_max_bytes` 描述。
* 默认值：409600
* 类型：Long
* 引入版本：v2.5

### query_cache_size (global)

用于兼容 MySQL 客户端。无实际作用。

### query_cache_type

 用于兼容 JDBC 连接池 C3P0。无实际作用。

### query_mem_limit

* 描述：用于设置每个 BE 节点上单个查询的内存限制。该项仅在启用 Pipeline Engine 后生效。设置为 `0` 表示没有限制。
* 单位：字节
* 默认值：`0`

### query_queue_concurrency_limit (global)

* 描述：单个 BE 节点中并发查询上限。仅在设置为大于 `0` 后生效。设置为 `0` 表示没有限制。
* 默认值：`0`
* 单位：-
* 类型：Int

### query_queue_cpu_used_permille_limit (global)

* 描述：单个 BE 节点中内存使用千分比上限（即 CPU 使用率）。仅在设置为大于 `0` 后生效。设置为 `0` 表示没有限制。
* 默认值：0。
* 取值范围：[0, 1000]

### query_queue_max_queued_queries (global)

* 描述：队列中查询数量的上限。当达到此阈值时，新增查询将被拒绝执行。仅在设置为大于 `0` 后生效。设置为 `0` 表示没有限制。
* 默认值：1024。

### query_queue_mem_used_pct_limit (global)

* 描述：单个 BE 节点中内存使用百分比上限。仅在设置为大于 `0` 后生效。设置为 `0` 表示没有限制。
* 默认值：`0`
* 取值范围：[0, 1]

### query_queue_pending_timeout_second (global)

* 描述：队列中单个查询的最大超时时间。当达到此阈值时，该查询将被拒绝执行。
* 默认值：300
* 单位：秒

### query_timeout

* 描述：用于设置查询超时时间，单位为秒。该变量会作用于当前连接中所有的查询语句。从 v3.4.0 起，`query_timeout` 不再适用于涉及 INSERT 的操作（例如，UPDATE、DELETE、CTAS、物化视图刷新、统计数据收集和 PIPE）。
* 默认值：300 （5 分钟）
* 单位：秒
* 类型：Int
* 取值范围：1 ~ 259200

### range_pruner_max_predicate

* 描述：设置进行 Range 分区裁剪时，最多能使用的 IN 谓词的个数，默认值：100。如果超过该值，会扫描全部 tablet，降低查询性能。
* 默认值：100
* 引入版本：v3.0

### runtime_filter_on_exchange_node

* 描述：GRF 成功下推跨过 Exchange 算子后，是否在 Exchange Node 上放置 GRF。当一个 GRF 下推跨越 Exchange 算子，最终安装在 Exchange 算子更下层的算子上时，Exchange 算子本身是不放置 GRF 的，这样可以避免重复性使用 GRF 过滤数据而增加计算时间。但是 GRF 的投递本身是 try-best 模式，如果 query 执行时，Exchange 下层的算子接收 GRF 失败，而 Exchange 本身又没有安装 GRF，数据无法被过滤，会造成性能衰退.

  该选项打开（设置为 `true`）时，GRF 即使下推跨过了 Exchange 算子, 依然会在 Exchange 算子上放置 GRF 并生效。
* 默认值：false

### runtime_join_filter_push_down_limit

* 描述：生成 Bloomfilter 类型的 Local RF 的 Hash Table 的行数阈值。超过该阈值, 则不产生 Local RF。该变量避免产生过大 Local RF。取值为整数，表示行数。
* 默认值：1024000
* 类型：Long

### runtime_profile_report_interval

* 描述：Runtime Profile 的上报间隔。
* 默认值：10
* 单位：秒
* 类型：Int
* 引入版本：v3.1.0

### scan_olap_partition_num_limit

* 描述：在SQL执行计划中, 单表允许的最大扫描分区数.
* 默认值：0 (无限制)
* 引入版本：v3.3.9

### skip_local_disk_cache

* **作用域**: Session
* **描述**: 会话级布尔变量，指示 frontend 在标记 scan ranges 时让 backend 跳过本地磁盘缓存。当设置为 `true` 时，OlapScanNode（在构建 TScanRange/TInternalScanRange 时）会在发送到 BE 的每个 scan range 上设置 `skip_disk_cache` 标志，导致后端的存储引擎在该查询中避免从本地磁盘缓存读取或向其填充数据。此设置按会话生效，影响该会话后续的扫描。通常在绕过缓存以获得新鲜读取或避免缓存污染时，与 `skip_page_cache` 一起使用。
* **默认值**: `false`
* **类型**: boolean
* **引入版本**: v3.3.9, v3.4.0, v3.5.0

### spill_encode_level

* **范围**: Session
* **默认值**: `7`
* **数据类型**: int
* **描述**: 控制写入 spill 文件的数据所使用的编码/压缩级别。其位含义遵循与 `transmission_encode_level` 相同的约定：
  * bit 2 (value 2) — 对类整数列启用整数类型编码（streamvbyte）。
  * bit 4 (value 4) — 对二进制/字符串列启用压缩（lz4）。
  * bit 1 (value 1) — 启用自适应编码（基于压缩比按列决定）。
  示例解释：
  * `7` (默认) = 1|2|4：自适应地对整数和字符串/二进制列进行编码（自适应决策使用压缩比 &lt; 0.9）。
  * `6` = 2|4：强制对整数和字符串/二进制列进行编码（不进行自适应决策）。
  * `2` 或 `4`：分别仅选择性启用整数或仅启用字符串编码。
  当启用 spill（见 `enable_spill`）时，此变量有意义，FE 会从会话读取该值以决定如何对溢出的数据进行编码以降低磁盘/IO 成本，并可能以 CPU 交换 IO。有关实现细节和原理，参见 `transmission_encode_level`。
* **引入版本**: v3.2.0

### spill_mode (3.0 及以后)

中间结果落盘的执行方式。默认值：`auto`。有效值包括：

* `auto`：达到内存使用阈值时，会自动触发落盘。
* `force`：无论内存使用情况如何，StarRocks 都会强制落盘所有相关算子的中间结果。

此变量仅在变量 `enable_spill` 设置为 `true` 时生效。

### spill_storage_volume

* 描述：用于存储触发落盘的查询的中间结果的存储卷。有关更多信息，请参阅 [将中间结果落盘至对象存储](../administration/management/resource_management/spill_to_disk.md#preview-将中间结果落盘至对象存储)。
* 默认值：空字符串
* 引入版本：v3.3.0

### sql_dialect

* 描述：设置生效的 SQL 语法。例如，执行 `set sql_dialect = 'trino';` 命令可以切换为 Trino 语法，这样您就可以在查询中使用 Trino 特有的 SQL 语法和函数。

  > **注意**
  >
  > 设置使用 Trino 语法后，查询默认对大小写不敏感。因此，您在 StarRocks 内建库、表时必须使用小写的库、表名称，否则查询会失败。
* 默认值：StarRocks
* 类型：String
* 引入版本：v3.0

### sql_mode

用于指定 SQL 模式，以适应某些 SQL 方言。有效值包括：

* `PIPES_AS_CONCAT`：管道符号 `|` 用于连接字符串。例如：`select 'hello ' || 'world'`。
* `ONLY_FULL_GROUP_BY` (默认值)：SELECT LIST 中只能包含 GROUP BY 列或者聚合函数。
* `ALLOW_THROW_EXCEPTION`：类型转换报错而不是返回 NULL。
* `FORBID_INVALID_DATE`：禁止非法的日期。
* `MODE_DOUBLE_LITERAL`：将浮点类型解释为 DOUBLE 而非 DECIMAL。
* `SORT_NULLS_LAST`：排序后，将 NULL 值放到最后。
* `ERROR_IF_OVERFLOW`：运算溢出时，报错而不是返回 NULL，目前仅 DECIMAL 支持这一行为。
* `GROUP_CONCAT_LEGACY`：使用 2.5 及以前的 `group_concat` 的语法。该选项从 3.0.9，3.1.6 开始支持。

不同模式之间可以独立设置，您可以单独开启某一个模式，例如：

```SQL
set sql_mode = 'PIPES_AS_CONCAT';
```

或者，您也可以同时设置多个模式，例如：

```SQL
set sql_mode = 'PIPES_AS_CONCAT,ERROR_IF_OVERFLOW,GROUP_CONCAT_LEGACY';
```

### sql_safe_updates

用于兼容 MySQL 客户端。无实际作用。

### sql_select_limit

* 描述：用于限制查询返回的结果集的最大行数，可以防止因查询返回过多的数据而导致内存不足或网络拥堵等问题。
* 默认值：无限制
* 类型：Long

### storage_engine

指定系统使用的存储引擎。StarRocks 支持的引擎类型包括：

* olap (默认值)：StarRocks 系统自有引擎。
* mysql：使用 MySQL 外部表。
* broker：通过 Broker 程序访问外部表。
* ELASTICSEARCH 或者 es：使用 Elasticsearch 外部表。
* HIVE：使用 Hive 外部表。
* ICEBERG：使用 Iceberg 外部表。从 2.1 版本开始支持。
* HUDI: 使用 Hudi 外部表。从 2.2 版本开始支持。
* jdbc: 使用 JDBC 外部表。从2.3 版本开始支持。

### streaming_preaggregation_mode

用于设置多阶段聚合时，group-by 第一阶段的预聚合方式。如果第一阶段本地预聚合效果不好，则可以关闭预聚合，走流式方式，把数据简单序列化之后发出去。取值含义如下：

* `auto`（默认值）：先探索本地预聚合，如果预聚合效果好，则进行本地预聚合；否则切换成流式。默认值，建议保留。
* `force_preaggregation`: 不进行探索，直接进行本地预聚合。
* `force_streaming`: 不进行探索，直接做流式。

### system_time_zone (global)

显示当前系统时区。不可更改。

### time_zone

用于设置当前会话的时区。时区会对某些时间函数的结果产生影响。

### transaction_read_only

* 描述：用于兼容 MySQL 5.8 以上客户端，无实际作用。别名 `tx_read_only`。该变量用于指定事务访问模式。取值 `ON` 表示只读。取值 `OFF` 表示可读可写。
* 默认值：OFF
* 引入版本：v2.5.18, v3.0.9, v3.1.7

### transmission_encode_level

* **描述**: 控制列级传输编码（用于数据交换，例如 RPC/exchange）。该变量为位掩码：
  * bit 1 (`1`): 启用自适应编码（根据测得的压缩比选择编码方式），
  * bit 2 (`2`): 使用 StreamVByte 对整数及兼容整数的类型进行编码，
  * bit 4 (`4`): 对二进制/字符串列使用 LZ4 压缩。
  示例：`7` (1|2|4) 为数字和字符串启用自适应编码，并在编码比率 &lt; 0.9 时选择编码；`6` (2|4) 强制对数字和字符串进行编码。JSON 和 object 列类型尚不支持。该设置影响每个会话的传输行为，并以 `TRANSMISSION_ENCODE_LEVEL` 暴露。
* **范围**: 会话
* **默认值**: `7`
* **类型**: int
* **引入版本**: v3.2.0

### tx_isolation

用于兼容 MySQL 客户端，无实际作用。别名 `transaction_isolation`。

### use_compute_nodes

* 描述：用于设置使用 CN 节点的数量上限。该设置只会在 `prefer_compute_node=true` 时才会生效。`-1`，表示使用所有 CN 节点。`0` 表示不使用 CN 节点。
* 默认值：-1
* 类型：Int
* 引入版本：v2.4

### version (global)

MySQL 服务器的版本，取值等于 FE 参数 `mysql_server_version`。

### version_comment (global)

用于显示 StarRocks 的版本，不可更改。

### wait_timeout

* 描述：用于设置客户端与 StarRocks 数据库交互时的最大空闲时长。如果一个空闲连接在该时长内与 StarRocks 数据库没有任何交互，StarRocks 会主动断开这个连接。
* 默认值：28800（即 8 小时）
* 单位：秒
* 类型：Int


