---
displayed_sidebar: "Chinese"
---

# 系统变量

StarRocks 提供多个系统变量（system variables），方便您根据业务情况进行调整。本文介绍 StarRocks 支持的变量。您可以在 MySQL 客户端通过命令 [SHOW VARIABLES](../sql-reference/sql-statements/Administration/SHOW_VARIABLES.md) 查看当前变量。也可以通过 [SET](../sql-reference/sql-statements/Administration/SET.md) 命令动态设置或者修改变量。您可以设置变量在系统全局 (global) 范围内生效、仅在当前会话 (session) 中生效、或者仅在单个查询语句中生效。

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
SET exec_mem_limit = 137438953472;

SET forward_to_master = true;

SET time_zone = "Asia/Shanghai";
```

通过 `SET GLOBAL <var_name> = xxx;` 语句设置的变量全局生效。如：

```SQL
SET GLOBAL query_mem_limit = 137438953472;
```

以下变量仅支持全局生效，不支持设置为会话级别生效。您必须使用 `SET GLOBAL <var_name> = xxx;`，不能使用 `SET <var_name> = xxx;`，否则返回错误。

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

在一些场景中，可能需要对某些查询专门设置变量。可以使用 SET_VAR 提示 (hint) 在查询中设置仅在单个语句内生效的会话变量。举例：

```sql
SELECT /*+ SET_VAR(exec_mem_limit = 8589934592) */ name FROM people ORDER BY name;

SELECT /*+ SET_VAR(query_timeout = 1) */ sleep(3);
```

> **注意**
>
> `SET_VAR` 只能跟在 SELECT 关键字之后，必须以 `/*+` 开头，以 `*/` 结束。

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

## 支持的变量

本节以字母顺序对变量进行解释。带 `global` 标记的变量为全局变量，仅支持全局生效。其余变量既可以设置全局生效，也可设置会话级别生效。

### auto_increment_increment

用于兼容 MySQL 客户端。无实际作用。默认值为 1。

### autocommit

用于兼容 MySQL 客户端。无实际作用。默认值为 true。

### batch_size

用于指定在查询执行过程中，各个节点传输的单个数据包的行数。默认一个数据包的行数为 1024 行，即源端节点每产生 1024 行数据后，打包发给目的节点。较大的行数，会在扫描大数据量场景下提升查询的吞吐率，但可能会在小查询场景下增加查询延迟。同时，也会增加查询的内存开销。建议设置范围 1024 至 4096。

### cbo_enable_low_cardinality_optimize

是否开启低基数全局字典优化。开启后，查询 STRING 列时查询速度会有 3 倍左右提升。默认值：true。

### character_set_database（global）

StarRocks 数据库支持的字符集，当前仅支持 UTF8 编码 （`utf8`）。

### connector_io_tasks_per_scan_operator（2.5 及以后）

外表查询时每个 Scan 算子能同时下发的 I/O 任务的最大数量。取值为整数，默认值 16。目前外表查询时会使用自适应算法来调整并发 I/O 任务的数量，通过 `enable_connector_adaptive_io_tasks` 开关来控制，默认打开。

### count_distinct_column_buckets（2.5 及以后）

group-by-count-distinct 查询中为 count distinct 列设置的分桶数。该变量只有在 `enable_distinct_column_bucketization` 设置为 `true` 时才会生效。默认值：1024。

### default_rowset_type (global)

全局变量，仅支持全局生效。用于设置计算节点存储引擎默认的存储格式。当前支持的存储格式包括：alpha/beta。

### disable_colocate_join

控制是否启用 Colocate Join 功能。默认为 false，表示启用该功能。true 表示禁用该功能。当该功能被禁用后，查询规划将不会尝试执行 Colocate Join。

### disable_streaming_preaggregations

控制是否开启流式预聚合。默认为 `false`，即开启。

### div_precision_increment

用于兼容 MySQL 客户端，无实际作用。

### enable_connector_adaptive_io_tasks（2.5 及以后）

外表查询时是否使用自适应策略来调整 I/O 任务的并发数。默认打开。如果未开启自适应策略，可以通过 `connector_io_tasks_per_scan_operator` 变量来手动设置外表查询时的 I/O 任务并发数。

### enable_distinct_column_bucketization（2.5 及以后）

是否在 group-by-count-distinct 查询中开启对 count distinct 列的分桶优化。在类似 `select a, count(distinct b) from t group by a;` 的查询中，如果 group by 列 a 为低基数列，count distinct 列 b 为高基数列且发生严重数据倾斜时，会引发查询性能瓶颈。可以通过对 count distinct 列进行分桶来平衡数据，规避数据倾斜。

默认值：false，表示不开启。该变量需要与 `count_distinct_column_buckets` 配合使用。

您也可以通过添加 `skew` hint 来开启 count distinct 列的分桶优化，例如 `select a,count(distinct [skew] b) from t group by a;`。

### enable_insert_strict

用于设置通过 INSERT 语句进行数据导入时，是否开启严格模式 (Strict Mode)。默认为 `true`，即开启严格模式。关于该模式的介绍，可以参阅[严格模式](../loading/load_concept/strict_mode.md)。

### enable_materialized_view_rewrite_for_insert (2.5.18 and later)

是否允许 StarRocks 改写 INSERT INTO SELECT 语句中的查询。默认为 `false`，即默认关闭该场景下的物化视图查询改写。

### enable_materialized_view_union_rewrite（2.5 及以后）

是否开启物化视图 Union 改写。默认值：`true`。

### enable_rule_based_materialized_view_rewrite（2.5 及以后）

是否开启基于规则的物化视图查询改写功能，主要用于处理单表查询改写。默认值：`true`。

### enable_strict_order_by

是否校验 ORDER BY 引用列是否有歧义。设置为默认值 `TRUE` 时，如果查询中的输出列存在不同的表达式使用重复别名的情况，且按照该别名进行排序，查询会报错，例如 `select distinct t1.* from tbl1 t1 order by t1.k1;`。该行为和 2.3 及之前版本的逻辑一致。如果取值为 `FALSE`，采用宽松的去重机制，把这类查询作为有效 SQL 处理。

该变量从 2.5.18，3.1.7 版本开始支持。

### enable_profile

用于设置是否需要查看查询的 profile。默认为 `false`，即不需要查看 profile。2.5 版本之前，该变量名称为 `is_report_success`，2.5 版本之后更名为 `enable_profile`。

默认情况下，只有在查询发生错误时，BE 才会发送 profile 给 FE，用于查看错误。正常结束的查询不会发送 profile。发送 profile 会产生一定的网络开销，对高并发查询场景不利。当用户希望对一个查询的 profile 进行分析时，可以将这个变量设为 `true` 后，发送查询。查询结束后，可以通过在当前连接的 FE 的 web 页面（地址：fe_host:fe_http_port/query）查看 profile。该页面会显示最近 100 条开启了 `enable_profile` 的查询的 profile。

### enable_query_queue_load (global)

布尔值，用于控制是否为导入任务启用查询队列。默认值：`false`。

### enable_query_queue_select (global)

布尔值，用于控制是否为 SELECT 查询启用查询队列。默认值：`false`。

### enable_query_queue_statistic (global)

布尔值，用于控制是否为统计信息查询启用查询队列。默认值：`false`。

### enable_query_tablet_affinity（2.5 及以后）

布尔值，用于控制在多次查询同一个 tablet 时是否倾向于选择固定的同一个副本。

如果待查询的表中存在大量 tablet，开启该特性会对性能有提升，因为会更快的将 tablet 的元信息以及数据缓存在内存中。但是，如果查询存在一些热点 tablet，开启该特性可能会导致性能有所退化，因为该特性倾向于将一个热点 tablet 的查询调度到相同的 BE 上，在高并发的场景下无法充分利用多台 BE 的资源。

默认值：`false`，表示使用原来的机制，即每次查询会从多个副本中选择一个。自 2.5.6、3.0.8、3.1.4 版本起，StarRocks 支持该参数。

### enable_scan_block_cache（2.5 及以后）

是否开启 Data Cache 特性。该特性开启之后，StarRocks 通过将外部存储系统中的热数据缓存成多个 block，加速数据查询和分析。更多信息，参见 [Data Cache](../data_source/data_cache.md)。该特性从 2.5 版本开始支持。

### enable_populate_block_cache（2.5 及以后）

StarRocks 从外部存储系统读取数据时，是否将数据进行缓存。如果只想读取，不进行缓存，可以将该参数设置为 `false`。默认值为 `true`。

### enable_tablet_internal_parallel

是否开启自适应 Tablet 并行扫描，使用多个线程并行分段扫描一个 Tablet，可以减少 Tablet 数量对查询能力的限制。默认值为 `true`。自 2.3 版本起，StarRocks 支持该参数。

### enable_query_cache (2.5 及以后)

是否开启 Query Cache。取值范围：true 和 false。true 表示开启，false 表示关闭（默认值）。开启该功能后，只有当查询满足[Query Cache](../using_starrocks/query_cache.md#应用场景) 所述条件时，才会启用 Query Cache。

### enable_adaptive_sink_dop (2.5 及以后)

是否开启导入自适应并行度。开启后 INSERT INTO 和 Broker Load 自动设置导入并行度，保持和 `pipeline_dop` 一致。新部署的 2.5 版本默认值为 `true`，从 2.4 版本升级上来为 `false`。

### enable_pipeline_engine

是否启用 Pipeline 执行引擎。true：启用（默认），false：不启用。

### enable_sort_aggregate (2.5 及以后)

是否开启 sorted streaming 聚合。`true` 表示开启 sorted streaming 聚合功能，对流中的数据进行排序。

### enable_global_runtime_filter

Global runtime filter 开关。Runtime Filter（简称 RF）在运行时对数据进行过滤，过滤通常发生在 Join 阶段。当多表进行 Join 时，往往伴随着谓词下推等优化手段进行数据过滤，以减少 Join 表的数据扫描以及 shuffle 等阶段产生的 IO，从而提升查询性能。StarRocks 中有两种 RF，分别是 Local RF 和 Global RF。Local RF 应用于 Broadcast Hash Join 场景。Global RF 应用于 Shuffle Join 场景。

默认值 `true`，表示打开 global runtime filter 开关。关闭该开关后, 不生成 Global RF, 但是依然会生成 Local RF。

### enable_multicolumn_global_runtime_filter

多列 Global runtime filter 开关。默认值为 false，表示关闭该开关。

对于 Broadcast 和 Replicated Join 类型之外的其他 Join，当 Join 的等值条件有多个的情况下：

* 如果该选项关闭: 则只会产生 Local RF。
* 如果该选项打开, 则会生成 multi-part GRF, 并且该 GRF 需要携带 multi-column 作为 partition-by 表达式.

### event_scheduler

用于兼容 MySQL 客户端。无实际作用。

### force_streaming_aggregate

用于控制聚合节点是否启用流式聚合计算策略。默认为 false，表示不启用该策略。

### forward_to_master

用于设置是否将一些命令转发到 Leader FE 节点执行。默认为 false，即不转发。StarRocks 中存在多个 FE 节点，其中一个为 Leader 节点。通常用户可以连接任意 FE 节点进行全功能操作。但部分信息查看指令只有从 Leader FE 节点才能获取详细信息。

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

### hash_join_push_down_right_table

用于控制在 Join 查询中是否可以使用针对右表的过滤条件来过滤左表的数据，可以减少 Join 过程中需要处理的左表的数据量。取值为 true 时表示允许该操作，系统将根据实际情况决定是否能对左表进行过滤；取值为 false 表示禁用该操作。默认值为 true。

### init_connect (global)

用于兼容 MySQL 客户端。无实际作用。

### interactive_timeout

用于兼容 MySQL 客户端。无实际作用。

### io_tasks_per_scan_operator (2.5 及以后)

每个 Scan 算子能同时下发的 I/0 任务的数量。如果使用远端存储系统（比如 HDFS 或 S3）且时延较长，可以增加该值。但是值过大会增加内存消耗。

取值为整数。默认值：4。

### language (global)

用于兼容 MySQL 客户端。无实际作用。

### license (global)

显示 StarRocks 的 license。无其他作用。

### load_mem_limit

用于指定导入操作的内存限制，单位为 Byte。默认值为 0，即表示不使用该变量，而采用 `query_mem_limit` 作为导入操作的内存限制。

这个变量仅用于 INSERT 操作。因为 INSERT 操作涉及查询和导入两个部分，如果用户不设置此变量，则查询和导入操作各自的内存限制均为 `query_mem_limit`。否则，INSERT 的查询部分内存限制为 `query_mem_limit`，而导入部分限制为 `load_mem_limit`。

其他导入方式，如 Broker Load，STREAM LOAD 的内存限制依然使用 `query_mem_limit`。

### lower_case_table_names (global)

用于兼容 MySQL 客户端，无实际作用。StarRocks 中的表名是大小写敏感的。

### max_allowed_packet

用于兼容 JDBC 连接池 C3P0。该变量值决定了服务端发送给客户端或客户端发送给服务端的最大 packet 大小，单位为 Byte，默认值为 32 MB。当客户端报 `PacketTooBigException` 异常时，可以考虑调大该值。

### max_pushdown_conditions_per_column

该变量的具体含义请参阅 [BE 配置项](../administration/Configuration.md#配置-be-动态参数)中 `max_pushdown_conditions_per_column` 的说明。该变量默认值为 -1，表示使用 `be.conf` 中的配置值。如果设置大于 0，则忽略 `be.conf` 中的配置值。

### max_scan_key_num

该变量的具体含义请参阅 [BE 配置项](../administration/Configuration.md#配置-be-动态参数)中 `max_scan_key_num` 的说明。该变量默认值为 -1，表示使用 `be.conf` 中的配置值。如果设置大于 0，则忽略 `be.conf` 中的配置值。

### nested_mv_rewrite_max_level

可用于查询改写的嵌套物化视图的最大层数。类型：INT。取值范围：[1, +∞)。默认值：`3`。取值为 `1` 表示只可使用基于基表创建的物化视图用于查询改写。

### net_buffer_length

用于兼容 MySQL 客户端。无实际作用。

### net_read_timeout

用于兼容 MySQL 客户端。无实际作用。

### net_write_timeout

用于兼容 MySQL 客户端。无实际作用。

### new_planner_optimize_timeout

查询优化器的超时时间。一般查询中 Join 过多时容易出现超时。超时后会报错并停止查询，影响查询性能。您可以根据查询的具体情况调大该参数配置，也可以将问题上报给 StarRocks 技术支持进行排查。

单位：毫秒。默认值：3000。

### parallel_exchange_instance_num

用于设置执行计划中，一个上层节点接收下层节点数据所使用的 exchange node 数量。默认为 -1，即表示 exchange node 数量等于下层节点执行实例的个数（默认行为）。当设置大于 0，并且小于下层节点执行实例的个数，则 exchange node 数量等于设置值。

在一个分布式的查询执行计划中，上层节点通常有一个或多个 exchange node 用于接收来自下层节点在不同 BE 上的执行实例的数据。通常 exchange node 数量等于下层节点执行实例数量。

在一些聚合查询场景下，如果底层需要扫描的数据量较大，但聚合之后的数据量很小，则可以尝试修改此变量为一个较小的值，可以降低此类查询的资源开销。如在 DUPLICATE KEY 明细表上进行聚合查询的场景。

### parallel_fragment_exec_instance_num

针对扫描节点，设置其在每个 BE 节点上执行实例的个数。默认为 1。

一个查询计划通常会产生一组 scan range，即需要扫描的数据范围。这些数据分布在多个 BE 节点上。一个 BE 节点会有一个或多个 scan range。默认情况下，每个 BE 节点的一组 scan range 只由一个执行实例处理。当机器资源比较充裕时，可以将增加该变量，让更多的执行实例同时处理一组 scan range，从而提升查询效率。

而 scan 实例的数量决定了上层其他执行节点，如聚合节点，join 节点的数量。因此相当于增加了整个查询计划执行的并发度。修改该参数会对大查询效率提升有帮助，但较大数值会消耗更多的机器资源，如CPU、内存、磁盘I/O。

### performance_schema

用于兼容 8.0.16 及以上版本的 MySQL JDBC。无实际作用。

### pipeline_dop

一个 Pipeline 实例的并行数量。可通过设置实例的并行数量调整查询并发度。默认值为 0，即系统自适应调整每个 pipeline 的并行度。您也可以设置为大于 0 的数值，通常为 BE 节点 CPU 物理核数的一半。从 3.0 版本开始，支持根据查询并发度自适应调节 `pipeline_dop`。

### pipeline_profile_level

用于控制 profile 的等级。一个 profile 通常有 5 个层级：Fragment、FragmentInstance、Pipeline、PipelineDriver、Operator。不同等级下，profile 的详细程度有所区别：

* 0：在此等级下，StarRocks 会合并 profile，只显示几个核心指标。
* 1：默认值。在此等级下，StarRocks 会对 profile 进行简化处理，将同一个 pipeline 的指标做合并来缩减层级。
* 2：在此等级下，StarRocks 会保留 Profile 所有的层级，不做简化。该设置下 profile 的体积会非常大，特别是 SQL 较复杂时，因此不推荐该设置。

### prefer_compute_node

将部分执行计划调度到 CN 节点执行。默认为 false。该变量从 2.4 版本开始支持。

### query_cache_size (global)

用于兼容 MySQL 客户端。无实际作用。

### query_cache_type

 用于兼容 JDBC 连接池 C3P0。无实际作用。

### query_cache_entry_max_bytes (2.5 及以后)

Cache 项的字节数上限，触发 Passthrough 模式的阈值。取值范围：0 ~ 9223372036854775807。默认值：4194304。当一个 Tablet 上产生的计算结果的字节数或者行数超过 `query_cache_entry_max_bytes` 或 `query_cache_entry_max_rows` 指定的阈值时，则查询采用 Passthrough 模式执行。当 `query_cache_entry_max_bytes` 或 `query_cache_entry_max_rows` 取值为 0 时, 即便 Tablet 产生结果为空，也采用 Passthrough 模式。

### query_cache_entry_max_rows (2.5 及以后)

Cache 项的行数上限，见 `query_cache_entry_max_bytes` 描述。默认值：409600。

### query_cache_agg_cardinality_limit (2.5 及以后)

GROUP BY 聚合的高基数上限。GROUP BY 聚合的输出预估超过该行数, 则不启用 cache。默认值：5000000。

### query_mem_limit

用于设置每个 BE 节点上查询的内存限制。单位：Byte。默认值为 `0`，表示没有限制。该项仅在启用 Pipeline Engine 后生效。

### query_queue_concurrency_limit (global)

单个 BE 节点中并发查询上限。仅在设置为大于 `0` 后生效。默认值：`0`。

### query_queue_cpu_used_permille_limit (global)

单个 BE 节点中内存使用千分比上限（即 CPU 使用率 ### 1000）。仅在设置为大于 `0` 后生效。默认值：`0`。取值范围：[0, 1000]

### query_queue_max_queued_queries (global)

队列中查询数量的上限。当达到此阈值时，新增查询将被拒绝执行。仅在设置为大于 `0` 后生效。默认值：`1024`。

### query_queue_mem_used_pct_limit (global)

单个 BE 节点中内存使用百分比上限。仅在设置为大于 `0` 后生效。默认值：`0`。取值范围：[0, 1]

### query_queue_pending_timeout_second (global)

队列中单个查询的最大超时时间。当达到此阈值时，该查询将被拒绝执行。默认值：`300`。单位：秒。

### query_timeout

用于设置查询超时时间，单位为秒。该变量会作用于当前连接中所有的查询语句，以及 INSERT 语句。默认为 300 秒，即 5 分钟。取值范围：1 ~ 259200。

### resource_group

暂不使用。

### rewrite_count_distinct_to_bitmap_hll

是否将 Bitmap 和 HLL 类型的 count distinct 查询重写为 bitmap_union_count 和 hll_union_agg。

### runtime_filter_on_exchange_node

GRF 成功下推跨过 Exchange 算子后，是否在 Exchange Node 上放置 GRF。当一个 GRF 下推跨越 Exchange 算子，最终安装在 Exchange 算子更下层的算子上时，Exchange 算子本身是不放置 GRF 的，这样可以避免重复性使用 GRF 过滤数据而增加计算时间。但是 GRF 的投递本身是 try-best 模式，如果 query 执行时，Exchange 下层的算子接收 GRF 失败，而 Exchange 本身又没有安装 GRF，数据无法被过滤，会造成性能衰退.

该选项打开（设置为 `true`）时，GRF 即使下推跨过了 Exchange 算子, 依然会在 Exchange 算子上放置 GRF 并生效。默认值为 `false`。

### runtime_join_filter_push_down_limit

生成 Bloomfilter 类型的 Local RF 的 Hash Table 的行数阈值。超过该阈值, 则不产生 Local RF。该变量避免产生过大 Local RF。取值为整数，表示行数。默认值：1024000。

### SQL_AUTO_IS_NULL

用于兼容 JDBC 连接池 C3P0。无实际作用。默认值：false。

### sql_mode

用于指定 SQL 模式，以适应某些 SQL 方言。有效值包括：

* `PIPES_AS_CONCAT`：管道符号 `|` 用于连接字符串。例如：`select 'hello ' || 'world'`。
* `ONLY_FULL_GROUP_BY` (默认值)：SELECT LIST 中只能包含 GROUP BY 列或者聚合函数。
* `ALLOW_THROW_EXCEPTION`：类型转换报错而不是返回 NULL。
* `FORBID_INVALID_DATE`：禁止非法的日期。
* `MODE_DOUBLE_LITERAL`：将浮点类型解释为 DOUBLE 而非 DECIMAL。
* `SORT_NULLS_LAST`：排序后，将 NULL 值放到最后。
* `ERROR_IF_OVERFLOW`：运算溢出时，报错而不是返回 NULL，目前仅 DECIMAL 支持这一行为。

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

用于兼容 MySQL 客户端。无实际作用。

### storage_engine

指定系统使用的存储引擎。StarRocks 支持的引擎类型包括：

* olap：StarRocks 系统自有引擎。
* mysql：使用 MySQL 外部表。
* broker：通过 Broker 程序访问外部表。
* ELASTICSEARCH 或者 es：使用 Elasticsearch 外部表。
* HIVE：使用 Hive 外部表。
* ICEBERG：使用 Iceberg 外部表。从 2.1 版本开始支持。
* HUDI: 使用 Hudi 外部表。从 2.2 版本开始支持。
* jdbc: 使用 JDBC 外部表。从2.3 版本开始支持。

### streaming_preaggregation_mode

用于设置多阶段聚合时，group-by 第一阶段的预聚合方式。如果第一阶段本地预聚合效果不好，则可以关闭预聚合，走流式方式，把数据简单序列化之后发出去。取值含义如下：

* `auto`：先探索本地预聚合，如果预聚合效果好，则进行本地预聚合；否则切换成流式。默认值，建议保留。
* `force_preaggregation`: 不进行探索，直接进行本地预聚合。
* `force_streaming`: 不进行探索，直接做流式。

### system_time_zone (global)

显示当前系统时区。不可更改。

### time_zone

用于设置当前会话的时区。时区会对某些时间函数的结果产生影响。

### tx_isolation

用于兼容 MySQL 客户端。无实际作用。

### use_compute_nodes

用于设置使用 CN 节点的数量上限。该设置只会在 `prefer_compute_node=true` 时才会生效。

-1 表示使用所有 CN 节点，0 表示不使用 CN 节点。默认值为 -1。该变量从 2.4 版本开始支持。

### use_v2_rollup

用于控制查询使用 segment v2 存储格式的 Rollup 索引获取数据。该变量用于上线 segment v2 的时进行验证使用。其他情况不建议使用。

### vectorized_engine_enable (2.4 版本开始弃用)

用于控制是否使用向量化引擎执行查询。值为 true 时表示使用向量化引擎，否则使用非向量化引擎。默认值为 true。2.4 版本开始默认打开，所以弃用该变量。

### version (global)

MySQL 服务器的版本。

### version_comment (global)

用于显示 StarRocks 的版本不，可更改。

### wait_timeout

用于设置空闲连接的连接时长，单位为秒。当一个空闲连接在该时长内与 StarRocks 没有任何交互，则 StarRocks 会主动断开这个链接。默认为 8 小时。
