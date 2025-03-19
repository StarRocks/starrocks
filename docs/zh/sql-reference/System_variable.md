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
* default_rowset_type
* enable_query_queue_select
* enable_query_queue_statistic
* enable_query_queue_load
* init_connect
* lower_case_table_names
* license
* language
* query_cache_size
* query_queue_fresh_resource_usage_interval_ms
* query_queue_concurrency_limit
* query_queue_mem_used_pct_limit
* query_queue_cpu_used_permille_limit
* query_queue_pending_timeout_second
* query_queue_max_queued_queries
* system_time_zone
* version_comment
* version

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

UPDATE /*+ SET_VAR(query_timeout=100) */ tbl SET c1 = 2 WHERE c1 = 1;

DELETE /*+ SET_VAR(query_mem_limit = 8589934592) */
FROM my_table PARTITION p1
WHERE k1 = 3;

INSERT /*+ SET_VAR(query_timeout = 10000000) */
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

### autocommit

* 描述：用于兼容 MySQL 客户端。无实际作用。
* 默认值：true

### chunk_size

用于指定在查询执行过程中，各个节点传输的单个数据包的行数。默认一个数据包的行数为 4096 行，即源端节点每产生 4096 行数据后，打包发给目的节点。较大的行数，会在扫描大数据量场景下提升查询的吞吐率，但可能会在小查询场景下增加查询延迟。同时，也会增加查询的内存开销。建议设置范围 1024 至 4096。

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

### cbo_decimal_cast_string_strict

* 描述：用于优化器控制 DECIMAL 类型转为 STRING 类型的行为。取值为 `true` 时，使用 v2.5.x及之后版本的处理逻辑，执行严格转换（按 Scale 截断补 `0`）；取值为 `false`时，保留 v2.5.x 之前版本的处理逻辑（按有效数字处理）。默认值是 `true`。
* 默认值：true
* 引入版本：v2.5.14

### cbo_enable_low_cardinality_optimize

* 描述：是否开启低基数全局字典优化。开启后，查询 STRING 列时查询速度会有 3 倍左右提升。
* 默认值：true

### cbo_eq_base_type

* 描述：用来指定 DECIMAL 类型和 STRING 类型的数据比较时的强制类型，默认按照 `VARCHAR` 类型进行比较，可选 `DECIMAL`（按数值进行比较）。**该变量仅在进行 `=` 和 `!=` 比较时生效。**
* 类型：String
* 引入版本：v2.5.14

### cbo_materialized_view_rewrite_related_mvs_limit

* 描述：用于指定查询在 Plan 阶段最多拥有的候选物化视图个数。
* 默认值：64
* 类型：Int
* 引入版本：v3.1.9, v3.2.5

### cbo_prune_subfield

* 描述：是否开启 JSON 子列裁剪。需要配合 BE 动态参数 `enable_json_flat` 一起使用，单独使用可能会导致 JSON 性能变慢。
* 默认值：false
* 引入版本：v3.3.0

### enable_sync_materialized_view_rewrite

* 描述：是否启用基于同步物化视图的查询改写。
* 默认值：true
* 引入版本：v3.1.11，v3.2.5

### enable_datacache_async_populate_mode

* 描述：是否使用异步方式进行 Data Cache 填充。系统默认使用同步方式进行填充，即在查询数据时同步填充进行缓存填充。
* 默认值：false
* 引入版本：v3.2.7

### query_including_mv_names

* 描述：指定需要在查询执行过程中包含的异步物化视图的名称。您可以使用此变量来限制候选物化视图的数量，并提高优化器中的查询改写性能。此项优先于 `query_excluding_mv_names` 生效。
* 默认值：空字符串
* 类型：String
* 引入版本：v3.1.11，v3.2.5

### query_excluding_mv_names

* 描述：指定需要在查询执行过程中排除的异步物化视图的名称。您可以使用此变量来限制候选物化视图的数量，并提高优化器中的查询改写性能。`query_including_mv_names` 优先于此项生效。
* 默认值：空字符串
* 类型：String
* 引入版本：v3.1.11，v3.2.5

### optimizer_materialized_view_timelimit

* 描述：指定一个物化视图改写规则可消耗的最大时间。当达到阈值时，将不再使用该规则进行查询改写。
* 默认值：1000
* 单位：毫秒
* 类型：Long

### enable_materialized_view_agg_pushdown_rewrite

* 描述：是否为物化视图查询改写启用聚合函数下推。如果设置为 `true`，聚合函数将在查询执行期间下推至 Scan Operator，并在执行 Join Operator 之前被物化视图改写。此举可以缓解 Join 操作导致的数据膨胀，从而提高查询性能。有关此功能的具体场景和限制的详细信息，请参见 [聚合函数下推](../using_starrocks/async_mv/use_cases/query_rewrite_with_materialized_views.md#聚合下推)。
* 默认值：false
* 引入版本：v3.3.0

### enable_materialized_view_text_match_rewrite

* 描述：是否启用基于文本的物化视图改写。当此项设置为 `true` 时，优化器将查询与现有的物化视图进行比较。如果物化视图定义的抽象语法树与查询或其子查询的抽象语法树匹配，则会对查询进行改写。
* 默认值：true
* 引入版本：v3.2.5，v3.3.0

### materialized_view_subuqery_text_match_max_count

* 描述：指定系统比对查询的子查询是否与物化视图定义匹配的最大次数。
* 默认值：4
* 引入版本：v3.2.5，v3.3.0

### enable_force_rule_based_mv_rewrite

* 描述：在优化器的 RBO（rule-based optimization）阶段是否针对多表查询启用查询改写。启用此功能将提高查询改写的鲁棒性。但如果查询未命中物化视图，则会增加优化耗时。
* 默认值：true
* 引入版本：v3.3

### enable_view_based_mv_rewrite

* 描述：是否为基于逻辑视图创建的物化视图启用查询改写。如果此项设置为 `true`，则逻辑视图被用作统一节点进行查询改写，从而获得更好的性能。如果此项设置为 `false`，则系统将针对逻辑视图的查询展开变为针对物理表或物化视图的查询，然后进行改写。
* 默认值：false
* 引入版本：v3.1.9，v3.2.5，v3.3.0

### enable_materialized_view_union_rewrite

* 描述：是否启用物化视图 UNION 改写。如果此项设置为 true，则系统在物化视图的谓词不能满足查询的谓词时，会尝试使用 UNION ALL 来补偿谓词。
* 默认值：true
* 引入版本：v2.5.20，v3.1.9，v3.2.7，v3.3.0

### enable_materialized_view_plan_cache

* 描述：是否开启物化视图查询计划缓存，用于提高物化视图查询改写性能。默认值是 `true`，即开启物化视图查询计划缓存。
* 默认值：true
* 引入版本：v2.5.13，v3.0.7，v3.1.4，v3.2.0，v3.3.0

### enable_plan_advisor

* 描述：是否为慢查询或手动标记查询开启 Query Feedback 功能。
* 默认值：true
* 引入版本：v3.4.0

### enable_plan_analyzer

* 描述：是否为所有查询开启 Query Feedback 功能。该变量仅在 `enable_plan_advisor` 为 `true` 是生效。
* 默认值：false
* 引入版本：v3.4.0

### follower_query_forward_mode

* 描述：用于指定将查询语句路由到 Leader FE 或 Follower FE 节点。

  有效值:
  * `default`: 将查询语句路由到 Leader FE 或 Follower FE 节点，取决于 Follower FE 节点的回放进度。如果 Follower FE 节点未完成回放，查询将会被路由至 Leader FE 节点。反之，查询会被优先路由至 Follower FE 节点。
  * `leader`: 将查询语句路由到 Leader FE 节点。
  * `follower`: 将查询语句路由到 Follower FE 节点。
* 默认值：default
* 类型：String
* 引入版本：v2.5.20，v3.1.9，v3.2.7，v3.3.0

### character_set_database（global）

* 描述：StarRocks 数据库支持的字符集，当前仅支持 UTF8 编码（`utf8`）。
* 默认值：utf8
* 类型：String

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

### disable_streaming_preaggregations (已弃用)

控制是否开启流式预聚合。默认为 `false`，即开启。3.1 版本已弃用。

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

<!--
### enable_collect_table_level_scan_stats (Invisible to users)

解决升级中的兼容问题，用户不可见。

默认值：`true`。
-->

### enable_connector_adaptive_io_tasks

* 描述：外表查询时是否使用自适应策略来调整 I/O 任务的并发数。默认打开。如果未开启自适应策略，可以通过 `connector_io_tasks_per_scan_operator` 变量来手动设置外表查询时的 I/O 任务并发数。
* 默认值：true
* 引入版本：v2.5

### enable_distinct_column_bucketization

* 描述：是否在 group-by-count-distinct 查询中开启对 count distinct 列的分桶优化。在类似 `select a, count(distinct b) from t group by a;` 的查询中，如果 group by 列 a 为低基数列，count distinct 列 b 为高基数列且发生严重数据倾斜时，会引发查询性能瓶颈。可以通过对 count distinct 列进行分桶来平衡数据，规避数据倾斜。

  该变量需要与 `count_distinct_column_buckets` 配合使用。

  您也可以通过添加 `skew` hint 来开启 count distinct 列的分桶优化，例如 `select a,count(distinct [skew] b) from t group by a;`。
* 默认值：false，表示不开启。
* 引入版本：v2.5

### enable_gin_filter

* 描述：查询时是否使用[全文倒排索引](../table_design/indexes/inverted_index.md)。
* 默认值：true
* 引入版本：v3.3.0

### enable_group_level_query_queue (global)

* 描述：是否开启资源组粒度的[查询队列](../administration/management/resource_management/query_queues.md)。
* 默认值：false，表示不开启。
* 引入版本：v3.1.4

### enable_iceberg_metadata_cache

* 描述：是否缓存 Iceberg 表指针和分区名相关的数据。在 3.2.1 到 3.2.3 版本，该参数默认值统一为 `true`。自 3.2.4 版本起，如果 Iceberg 集群的元数据服务为 AWS Glue，该参数默认值仍为 `true`，如果 Iceberg 集群的元数据服务为 Hive Metastore（简称 HMS）或其他，则该参数默认值变更为 `false`。
* 引入版本：v3.2.1

### enable_metadata_profile

* 描述：是否为 Iceberg Catalog 的元数据收集查询开启 Profile。
* 默认值：true
* 引入版本：v3.3.3

### plan_mode

* 描述：Iceberg Catalog 元数据获取方案模式。详细信息，参考 [Iceberg Catalog 元数据获取方案](../data_source/catalog/iceberg/iceberg_catalog.md#附录元数据周期性后台刷新方案)。有效值：
  * `auto`：系统自动选择方案。
  * `local`：使用本地缓存方案。
  * `distributed`：使用分布式方案。
* 默认值：auto
* 引入版本：v3.3.3

### metadata_collect_query_timeout

* 描述：Iceberg Catalog 元数据收集阶段的超时时间。
* 单位： 秒
* 默认值：60
* 引入版本：v3.3.3

### enable_insert_strict

* 描述：是否在使用 INSERT from FILES() 导入数据时启用严格模式。有效值：`true` 和 `false`（默认值）。启用严格模式时，系统仅导入合格的数据行，过滤掉不合格的行，并返回不合格行的详细信息。更多信息请参见 [严格模式](../loading/load_concept/strict_mode.md)。在早于 v3.4.0 的版本中，当 `enable_insert_strict` 设置为 `true` 时，INSERT 作业会在出现不合格行时失败。
* 默认值：true

### insert_max_filter_ratio

* 描述：INSERT 导入作业的最大容忍率，即导入作业能够容忍的因数据质量不合格而过滤掉的数据行所占的最大比例。当不合格行数比例超过该限制时，导入作业失败。默认值：`0`。范围：[0, 1]。
* 默认值：0
* 引入版本：v3.4.0

### insert_timeout

* 描述：INSERT 作业的超时时间。单位：秒。从 v3.4.0 版本开始，`insert_timeout` 作用于所有涉及 INSERT 的操作（例如，UPDATE、DELETE、CTAS、物化视图刷新、统计信息收集和 PIPE），替代原本的 `query_timeout`。
* 默认值：14400
* 引入版本：v3.4.0

### enable_materialized_view_for_insert

* 描述：是否允许 StarRocks 改写 INSERT INTO SELECT 语句中的查询。
* 默认值：false，即默认关闭该场景下的物化视图查询改写。
* 引入版本：v2.5.18, v3.0.9, v3.1.7, v3.2.2

### enable_rule_based_materialized_view_rewrite

* 描述：是否开启基于规则的物化视图查询改写功能，主要用于处理单表查询改写。
* 默认值：true
* 引入版本：v2.5

### enable_short_circuit

* 描述：是否启用短路径查询。默认值：`false`。如果将其设置为 `true`，当[查询满足条件](../table_design/hybrid_table.md#查询数据)（用于评估是否为点查）：WHERE 子句的条件列必须包含所有主键列，并且运算符为 `=` 或者 `IN`，则该查询才会走短路径。
* 默认值：false
* 引入版本：v3.2.3

### enable_spill

* 描述：是否启用中间结果落盘。默认值：`false`。如果将其设置为 `true`，StarRocks 会将中间结果落盘，以减少在查询中处理聚合、排序或连接算子时的内存使用量。
* 默认值：false
* 引入版本：v3.0

### enable_spill_to_remote_storage

* 描述：是否启用将中间结果落盘至对象存储。如果设置为 `true`，当本地磁盘的用量达到上限后，StarRocks 将中间结果落盘至 `spill_storage_volume` 中指定的存储卷中。有关更多信息，请参阅 [将中间结果落盘至对象存储](../administration/management/resource_management/spill_to_disk.md#preview-将中间结果落盘至对象存储)。
* 默认值：false
* 引入版本：v3.3.0

### enable_strict_order_by

* 描述：是否校验 ORDER BY 引用列是否有歧义。设置为默认值 `TRUE` 时，如果查询中的输出列存在不同的表达式使用重复别名的情况，且按照该别名进行排序，查询会报错，例如 `select distinct t1.* from tbl1 t1 order by t1.k1;`。该行为和 2.3 及之前版本的逻辑一致。如果取值为 `FALSE`，采用宽松的去重机制，把这类查询作为有效 SQL 处理。
* 默认值：true
* 引入版本：v2.5.18，v3.1.7

### enable_profile

用于设置是否需要查看查询的 profile。默认为 `false`，即不需要查看 profile。2.5 版本之前，该变量名称为 `is_report_success`，2.5 版本之后更名为 `enable_profile`。

默认情况下，只有在查询发生错误时，BE 才会发送 profile 给 FE，用于查看错误。正常结束的查询不会发送 profile。发送 profile 会产生一定的网络开销，对高并发查询场景不利。当用户希望对一个查询的 profile 进行分析时，可以将这个变量设为 `true` 后，发送查询。查询结束后，可以通过在当前连接的 FE 的 web 页面（地址：fe_host:fe_http_port/query）查看 profile。该页面会显示最近 100 条开启了 `enable_profile` 的查询的 profile。

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

### enable_lake_tablet_internal_parallel

* 描述：是否开启存算分离集群内云原生表的 Tablet 并行 Scan.
* 默认值：false
* 类型：Boolean
* 引入版本：v3.3.0

### tablet_internal_parallel_mode

* 描述：Tablet 内部并行 Scan 策略。有效值:
  * `auto`: 在 BE 或 CN 节点需要扫描的 Tablet 数小于 DOP 时，系统根据预估的 Tablet 大小自动判断是否需要并行 Scan。
  * `force_split`: 强制对 Tablet 进行拆分和并行扫描。
* 默认值：auto
* 类型：String
* 引入版本：v2.5.0

### enable_scan_datacache

* 描述：是否开启 Data Cache 特性。该特性开启之后，StarRocks 通过将外部存储系统中的热数据缓存成多个 block，加速数据查询和分析。更多信息，参见 [Data Cache](../data_source/data_cache.md)。该特性从 2.5 版本开始支持。在 3.2 之前各版本中，对应变量为 `enable_scan_block_cache`。
* 默认值：false
* 引入版本：v2.5

### populate_datacache_mode

* 描述：StarRocks 从外部存储系统读取数据时，控制数据缓存填充行为。有效值包括：
  * `auto`（默认）：系统自动根据查询的特点，选择性进行缓存。
  * `always`：总是缓存数据。 
  * `never` 永不缓存数据。
* 默认值：auto
* 引入版本：v3.3.2

### enable_datacache_io_adaptor

* 描述：是否开启 Data Cache I/O 自适应开关。`true` 表示开启。开启后，系统会根据当前磁盘 I/O 负载自动将一部分缓存请求路由到远端存储来减少磁盘压力。
* 默认值：true
* 引入版本：v3.3.0

### enable_file_metacache

* 描述：是否启用远端文件元数据缓存（Footer Cache）。`true` 表示开启。Footer Cache 通过将解析后生成 Footer 对象直接缓存在内存中，在后续访问相同文件 Footer 时，可以直接从缓存中获得该对象句柄进行使用，避免进行重复解析。该功能依赖 Data Cache 的内存缓存，因此需要保证 BE 参数 `datacache_enable` 为 `true` 且为 `datacache_mem_size` 配置一个合理值后才会生效。
* 默认值：true
* 引入版本：v3.3.0

### enable_tablet_internal_parallel

* 描述：是否开启自适应 Tablet 并行扫描，使用多个线程并行分段扫描一个 Tablet，可以减少 Tablet 数量对查询能力的限制。
* 默认值：true
* 引入版本：v2.3

### enable_query_cache

* 描述：是否开启 Query Cache。取值范围：true 和 false。true 表示开启，false 表示关闭（默认值）。开启该功能后，只有当查询满足[Query Cache](../using_starrocks/caching/query_cache.md#应用场景) 所述条件时，才会启用 Query Cache。
* 默认值：false
* 引入版本：v2.5

### enable_adaptive_sink_dop

* 描述：是否开启导入自适应并行度。开启后 INSERT INTO 和 Broker Load 自动设置导入并行度，保持和 `pipeline_dop` 一致。新部署的 2.5 版本默认值为 `true`，从 2.4 版本升级上来为 `false`。
* 默认值：false
* 引入版本：v2.5

### enable_pipeline_engine

* 描述：是否启用 Pipeline 执行引擎。`true`：启用（默认），`false`：不启用。
* 默认值：true

### enable_sort_aggregate

* 描述：是否开启 sorted streaming 聚合。`true` 表示开启 sorted streaming 聚合功能，对流中的数据进行排序。
* 默认值：false
* 引入版本：v2.5

### enable_global_runtime_filter

* 描述：Global runtime filter 开关。Runtime Filter（简称 RF）在运行时对数据进行过滤，过滤通常发生在 Join 阶段。当多表进行 Join 时，往往伴随着谓词下推等优化手段进行数据过滤，以减少 Join 表的数据扫描以及 shuffle 等阶段产生的 IO，从而提升查询性能。StarRocks 中有两种 RF，分别是 Local RF 和 Global RF。Local RF 应用于 Broadcast Hash Join 场景。Global RF 应用于 Shuffle Join 场景。
* 默认值 `true`，表示打开 global runtime filter 开关。关闭该开关后, 不生成 Global RF, 但是依然会生成 Local RF。

### enable_multicolumn_global_runtime_filter

* 描述：多列 Global runtime filter 开关。默认值为 false，表示关闭该开关。

  对于 Broadcast 和 Replicated Join 类型之外的其他 Join，当 Join 的等值条件有多个的情况下：
  * 如果该选项关闭: 则只会产生 Local RF。
  * 如果该选项打开, 则会生成 multi-part GRF, 并且该 GRF 需要携带 multi-column 作为 partition-by 表达式.

* 默认值：false

### enable_strict_type

* 描述：是否对所有复合谓词以及 WHERE 子句中的表达式进行隐式转换。
* 默认值：false
* 引入版本：v3.1

### enable_write_hive_external_table

* 描述：是否开启往 Hive 的 External Table 写数据的功能。
* 默认值：false
* 引入版本：v3.2

### enable_query_trigger_analyze

* 默认值：true
* 类型：Boolean
* 单位：-
* 描述：是否开启查询触发 ANALYZE 任务。
* 引入版本：v3.4.0

### event_scheduler

* 描述：用于兼容 MySQL 客户端。无实际作用。
* 默认值：OFF
* 类型：String

### force_streaming_aggregate（已弃用）

用于控制聚合节点是否启用流式聚合计算策略。默认为 false，表示不启用该策略。

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

### init_connect (global)

用于兼容 MySQL 客户端。无实际作用。

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

### lower_case_table_names (global)

用于兼容 MySQL 客户端，无实际作用。StarRocks 中的表名是大小写敏感的。

### materialized_view_rewrite_mode（3.2 及以后）

指定异步物化视图的查询改写模式。有效值：

* `disable`：禁用异步物化视图的自动查询改写。
* `default`（默认值）：启用异步物化视图的自动查询改写，并允许优化器根据 Cost 决定是否可以使用物化视图改写查询。如果查询无法改写，则直接查询基表中的数据。
* `default_or_error`：启用异步物化视图的自动查询改写，并允许优化器根据 Cost 决定是否可以使用物化视图改写查询。如果查询无法改写，将返回错误。
* `force`：启用异步物化视图的自动查询改写，并且优化器优先使用物化视图改写查询。如果查询无法改写，则直接查询基表中的数据。
* `force_or_error`：启用异步物化视图的自动查询改写，并且优化器优先使用物化视图改写查询。如果查询无法改写，将返回错误。

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

### pipeline_dop

* 描述：一个 Pipeline 实例的并行数量。可通过设置实例的并行数量调整查询并发度。默认值为 0，即系统自适应调整每个 pipeline 的并行度。您也可以设置为大于 0 的数值，通常为 BE 节点 CPU 物理核数的一半。从 3.0 版本开始，支持根据查询并发度自适应调节 `pipeline_dop`。
* 默认值：0
* 类型：Int

### pipeline_profile_level

* 描述：用于控制 profile 的等级。一个 profile 通常有 5 个层级：Fragment、FragmentInstance、Pipeline、PipelineDriver、Operator。不同等级下，profile 的详细程度有所区别：
  * 0：在此等级下，StarRocks 会合并 profile，只显示几个核心指标。
  * 1：默认值。在此等级下，StarRocks 会对 profile 进行简化处理，将同一个 pipeline 的指标做合并来缩减层级。
  * 2：在此等级下，StarRocks 会保留 Profile 所有的层级，不做简化。该设置下 profile 的体积会非常大，特别是 SQL 较复杂时，因此不推荐该设置。
* 默认值：1
* 类型：Int

### prefer_compute_node

* 描述：将部分执行计划调度到 CN 节点执行。
* 默认值：false
* 引入版本：v2.4

### query_cache_size (global)

用于兼容 MySQL 客户端。无实际作用。

### query_cache_type

 用于兼容 JDBC 连接池 C3P0。无实际作用。

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

### query_cache_agg_cardinality_limit

* 描述：GROUP BY 聚合的高基数上限。GROUP BY 聚合的输出预估超过该行数, 则不启用 cache。
* 默认值：5000000
* 类型：Long
* 引入版本：v2.5

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

* 描述：用于设置查询超时时间，单位为秒。该变量会作用于当前连接中所有的查询语句。自 v3.4.0 起，`query_timeout` 不再作用于 INSERT 语句。
* 默认值：300 （5 分钟）
* 单位：秒
* 类型：Int
* 取值范围：1 ~ 259200

### range_pruner_max_predicate

* 描述：设置进行 Range 分区裁剪时，最多能使用的 IN 谓词的个数，默认值：100。如果超过该值，会扫描全部 tablet，降低查询性能。
* 默认值：100
* 引入版本：v3.0

### resource_group

暂不使用。

### rewrite_count_distinct_to_bitmap_hll (已弃用)

是否将 Bitmap 和 HLL 类型的 count distinct 查询改写为 bitmap_union_count 和 hll_union_agg。

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

### spill_mode (3.0 及以后)

中间结果落盘的执行方式。默认值：`auto`。有效值包括：

* `auto`：达到内存使用阈值时，会自动触发落盘。
* `force`：无论内存使用情况如何，StarRocks 都会强制落盘所有相关算子的中间结果。

此变量仅在变量 `enable_spill` 设置为 `true` 时生效。

### spill_storage_volume

* 描述：用于存储触发落盘的查询的中间结果的存储卷。有关更多信息，请参阅 [将中间结果落盘至对象存储](../administration/management/resource_management/spill_to_disk.md#preview-将中间结果落盘至对象存储)。
* 默认值：空字符串
* 引入版本：v3.3.0

### SQL_AUTO_IS_NULL

用于兼容 JDBC 连接池 C3P0，无实际作用。默认值：false。

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

### statistic_collect_parallel

* 描述：用于调整 BE 上能并发执行的统计信息收集任务的个数，默认值为 1，可以调大该数值来加快采集任务的执行速度。
* 默认值：1
* 类型：Int

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

### trace_log_mode

* 描述：用于控制 Query Trace Profile 的 Logs 的输出位置。有效值包括：
  * `command`：在执行 TRACE LOGS 后作为 **Explain String** 返回。
  * `file`：在 FE 日志文件 **fe.log** 中以 `FileLogTracer` 为类名返回。

  有关 Query Trace Profile 的更多信息，请参阅 [Query Trace Profile](../developers/trace-tools/query_trace_profile.md)。

* 默认值：`command`
* 类型：String
* 引入版本：v3.2.0

### transaction_read_only

* 描述：用于兼容 MySQL 5.8 以上客户端，无实际作用。别名 `tx_read_only`。该变量用于指定事务访问模式。取值 `ON` 表示只读。取值 `OFF` 表示可读可写。
* 默认值：OFF
* 引入版本：v2.5.18, v3.0.9, v3.1.7

### tx_isolation

用于兼容 MySQL 客户端，无实际作用。别名 `transaction_isolation`。

### use_compute_nodes

* 描述：用于设置使用 CN 节点的数量上限。该设置只会在 `prefer_compute_node=true` 时才会生效。`-1`，表示使用所有 CN 节点。`0` 表示不使用 CN 节点。
* 默认值：-1
* 类型：Int
* 引入版本：v2.4

### use_v2_rollup

用于控制查询使用 segment v2 存储格式的 Rollup 索引获取数据。该变量用于上线 segment v2 的时进行验证使用。其他情况不建议使用。

### vectorized_engine_enable (2.4 版本开始弃用)

用于控制是否使用向量化引擎执行查询。值为 true 时表示使用向量化引擎，否则使用非向量化引擎。默认值为 true。2.4 版本开始默认打开，所以弃用该变量。

### version (global)

MySQL 服务器的版本，取值等于 FE 参数 `mysql_server_version`。

### version_comment (global)

用于显示 StarRocks 的版本，不可更改。

### wait_timeout

* 描述：用于设置客户端与 StarRocks 数据库交互时的最大空闲时长。如果一个空闲连接在该时长内与 StarRocks 数据库没有任何交互，StarRocks 会主动断开这个连接。
* 默认值：28800（即 8 小时）
* 单位：秒
* 类型：Int

### orc_use_column_names

* 描述：设置通过 Hive Catalog 读取 ORC 文件时，列的对应方式。默认值是 `false`，即按照 Hive 表中列的顺序对应。如果设置为 `true`，则按照列名称对应。
* 引入版本：v3.1.10
