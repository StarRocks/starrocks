---
displayed_sidebar: "Chinese"
---

# FE 配置项

FE 参数分为动态参数和静态参数。动态参数可通过 SQL 命令进行在线配置和调整，方便快捷。**需要注意通过 SQL 命令所做的动态设置在重启 FE 后会失效。如果想让设置长期生效，建议同时修改 fe.conf 文件。**

静态参数必须在 FE 配置文件 **fe.conf** 中进行配置和调整。**调整完成后，需要重启 FE 使变更生效。**

参数是否为动态参数可通过 [ADMIN SHOW CONFIG](../sql-reference/sql-statements/Administration/ADMIN_SHOW_CONFIG.md) 返回结果中的 `IsMutable` 列查看。`TRUE` 表示动态参数。

静态和动态参数均可通过 **fe.conf** 文件进行修改。

## 查看 FE 配置项

FE 启动后，您可以在 MySQL 客户端执行 ADMIN SHOW FRONTEND CONFIG 命令来查看参数配置。如果您想查看具体参数的配置，执行如下命令：

```SQL
 ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"];
 ```

详细的命令返回字段解释，参见 [ADMIN SHOW CONFIG](../sql-reference/sql-statements/Administration/ADMIN_SHOW_CONFIG.md)。

> **注意**
>
> 只有拥有 `cluster_admin` 角色的用户才可以执行集群管理相关命令。

## 配置 FE 动态参数

您可以通过 [ADMIN SET FRONTEND CONFIG](../sql-reference/sql-statements/Administration/ADMIN_SET_CONFIG.md) 命令在线修改 FE 动态参数。

```sql
ADMIN SET FRONTEND CONFIG ("key" = "value");
```

> **注意**
>
> 动态设置的配置项，在 FE 重启之后会恢复成 **fe.conf** 文件中的配置或者默认值。如果需要让配置长期生效，建议设置完之后同时修改 **fe.conf** 文件，防止重启后修改失效。

本节对 FE 动态参数做了如下分类：

- [Log](#log)
- [元数据与集群管理](#元数据与集群管理)
- [Query engine](#query-engine)
- [导入和导出](#导入和导出)
- [存储](#存储)
- [其他动态参数](#其他动态参数)

### Log

#### qe_slow_log_ms

- 含义：Slow query 的认定时长。如果查询的响应时间超过此阈值，则会在审计日志 `fe.audit.log` 中记录为 slow query。
- 单位：毫秒
- 默认值：5000

### 元数据与集群管理

#### catalog_try_lock_timeout_ms

- 含义：全局锁（global lock）获取的超时时长。
- 单位：毫秒
- 默认值：5000

#### edit_log_roll_num

- 含义：该参数用于控制日志文件的大小，指定了每写多少条元数据日志，执行一次日志滚动操作来为这些日志生成新的日志文件。新日志文件会写入到 BDBJE Database。
- 默认值：50000

#### ignore_unknown_log_id

- 含义：是否忽略未知的 logID。当 FE 回滚到低版本时，可能存在低版本 BE 无法识别的 logID。<br />如果为 TRUE，则 FE 会忽略这些 logID；否则 FE 会退出。
- 默认值：FALSE

#### ignore_materialized_view_error

- 含义：是否忽略因物化视图错误导致的元数据异常。如果 FE 因为物化视图错误导致的元数据异常而无法启动，您可以通过将该参数设置为 `true` 以忽略错误。
- 默认值：FALSE
- 引入版本：2.5.10

#### ignore_meta_check

- 含义：是否忽略元数据落后的情形。如果为 true，非主 FE 将忽略主 FE 与其自身之间的元数据延迟间隙，即使元数据延迟间隙超过 meta_delay_toleration_second，非主 FE 仍将提供读取服务。<br />当您尝试停止 Master FE 较长时间，但仍希望非 Master FE 可以提供读取服务时，该参数会很有帮助。
- 默认值：FALSE

#### meta_delay_toleration_second

- 含义：FE 所在 StarRocks 集群中，非 Leader FE 能够容忍的元数据落后的最大时间。<br />如果非 Leader FE 上的元数据与 Leader FE 上的元数据之间的延迟时间超过该参数取值，则该非 Leader FE 将停止服务。
- 单位：秒
- 默认值：300

#### drop_backend_after_decommission

- 含义：BE 被下线后，是否删除该 BE。true 代表 BE 被下线后会立即删除该 BE。False 代表下线完成后不删除 BE。
- 默认值：TRUE

#### enable_collect_query_detail_info

- 含义：是否收集查询的 profile 信息。设置为 true 时，系统会收集查询的 profile。设置为 false 时，系统不会收集查询的 profile。
- 默认值：FALSE

#### enable_background_refresh_connector_metadata

- 含义：是否开启 Hive 元数据缓存周期性刷新。开启后，StarRocks 会轮询 Hive 集群的元数据服务（Hive Metastore 或 AWS Glue），并刷新经常访问的 Hive 外部数据目录的元数据缓存，以感知数据更新。`true` 代表开启，`false` 代表关闭。
- 默认值：v3.0 为 TRUE；v2.5 为 FALSE

#### background_refresh_metadata_interval_millis

- 含义：接连两次 Hive 元数据缓存刷新之间的间隔。
- 单位：毫秒
- 默认值：600000
- 引入版本：2.5.5

#### background_refresh_metadata_time_secs_since_last_access_secs

- 含义：Hive 元数据缓存刷新任务过期时间。对于已被访问过的 Hive Catalog，如果超过该时间没有被访问，则停止刷新其元数据缓存。对于未被访问过的 Hive Catalog，StarRocks 不会刷新其元数据缓存。
- 默认值：86400
- 单位：秒
- 引入版本：2.5.5

#### enable_statistics_collect_profile

- 含义：统计信息查询时是否生成 Profile。您可以将此项设置为 `true`，以允许 StarRocks 为系统统计查询生成 Profile。
- 默认值：false
- 引入版本：3.1.5

### Query engine

#### max_allowed_in_element_num_of_delete

- 含义：DELETE 语句中 IN 谓词最多允许的元素数量。
- 默认值：10000

#### enable_materialized_view

- 含义：是否允许创建物化视图。
- 默认值：TRUE

#### enable_decimal_v3

- 含义：是否开启 Decimal V3。
- 默认值：TRUE

#### enable_sql_blacklist

- 含义：是否开启 SQL Query 黑名单校验。如果开启，在黑名单中的 Query 不能被执行。
- 默认值：FALSE

#### dynamic_partition_check_interval_seconds

- 含义：动态分区检查的时间周期。如果有新数据生成，会自动生成分区。
- 单位：秒
- 默认值：600

#### dynamic_partition_enable

- 含义：是否开启动态分区功能。打开后，您可以按需为新数据动态创建分区，同时 StarRocks 会⾃动删除过期分区，从而确保数据的时效性。
- 默认值：TRUE

#### http_slow_request_threshold_ms

- 含义：如果一条 HTTP 请求的时间超过了该参数指定的时长，会生成日志来跟踪该请求。
- 单位：毫秒
- 默认值：5000
- 引入版本：2.5.15，3.1.5

#### max_partitions_in_one_batch

- 含义：批量创建分区时，分区数目的最大值。
- 默认值：4096

#### max_query_retry_time

- 含义：FE 上查询重试的最大次数。
- 默认值：2

#### max_create_table_timeout_second

- 含义：建表的最大超时时间。
- 单位：秒
- 默认值：600

#### max_running_rollup_job_num_per_table

- 含义：每个 Table 执行 Rollup 任务的最大并发度。
- 默认值：1

#### max_planner_scalar_rewrite_num

- 含义：优化器重写 ScalarOperator 允许的最大次数。
- 默认值：100000

#### enable_statistic_collect

- 含义：是否采集统计信息，该开关默认打开。
- 默认值：TRUE

#### enable_collect_full_statistic

- 含义：是否开启自动全量统计信息采集，该开关默认打开。
- 默认值：TRUE

#### statistic_auto_collect_ratio

- 含义：自动统计信息的健康度阈值。如果统计信息的健康度小于该阈值，则触发自动采集。
- 默认值：0.8

#### statistic_max_full_collect_data_size

- 含义：自动统计信息采集的最大分区大小。<br />如果超过该值，则放弃全量采集，转为对该表进行抽样采集。
- 单位：GB
- 默认值：100

#### statistic_collect_interval_sec

- 含义：自动定期采集任务中，检测数据更新的间隔时间，默认为 5 分钟。
- 单位：秒
- 默认值：300

#### statistic_auto_analyze_start_time

- 含义：用于配置自动全量采集的起始时间。取值范围：`00:00:00` ~ `23:59:59`。
- 类型：STRING
- 默认值：00:00:00
- 引入版本：2.5.0

#### statistic_auto_analyze_end_time

- 含义：用于配置自动全量采集的结束时间。取值范围：`00:00:00` ~ `23:59:59`。
- 类型：STRING
- 默认值：23:59:59
- 引入版本：2.5.0

#### statistic_sample_collect_rows

- 含义：最小采样行数。如果指定了采集类型为抽样采集（SAMPLE），需要设置该参数。<br />如果参数取值超过了实际的表行数，默认进行全量采集。
- 默认值：200000

#### histogram_buckets_size

- 含义：直方图默认分桶数。
- 默认值：64

#### histogram_mcv_size

- 含义：直方图默认 most common value 的数量。
- 默认值：100

#### histogram_sample_ratio

- 含义：直方图默认采样比例。
- 默认值：0.1

#### histogram_max_sample_row_count

- 含义：直方图最大采样行数。
- 默认值：10000000

#### statistics_manager_sleep_time_sec

- 含义：统计信息相关元数据调度间隔周期。系统根据这个间隔周期，来执行如下操作：<ul><li>创建统计信息表；</li><li>删除已经被删除的表的统计信息；</li><li>删除过期的统计信息历史记录。</li></ul>
- 单位：秒
- 默认值：60

#### statistic_update_interval_sec

- 含义：统计信息内存 Cache 失效时间。
- 单位：秒
- 默认值：24 \* 60 \* 60

#### statistic_analyze_status_keep_second

- 含义：统计信息采集任务的记录保留时间，默认为 3 天。
- 单位：秒
- 默认值：259200

#### statistic_collect_concurrency

- 含义：手动采集任务的最大并发数，默认为 3，即最多可以有 3 个手动采集任务同时运行。超出的任务处于 PENDING 状态，等待调度。
- 默认值：3

#### statistic_auto_collect_small_table_rows

- 含义：自动收集中，用于判断外部数据源下的表 (Hive, Iceberg, Hudi) 是否为小表的行数门限。
- 默认值：10000000
- 引入版本：v3.2

#### enable_local_replica_selection

- 含义：是否选择本地副本进行查询。本地副本可以减少数据传输的网络时延。<br />如果设置为 true，优化器优先选择与当前 FE 相同 IP 的 BE 节点上的 tablet 副本。设置为 false 表示选择可选择本地或非本地副本进行查询。
- 默认值：FALSE

#### max_distribution_pruner_recursion_depth

- 含义：分区裁剪允许的最大递归深度。增加递归深度可以裁剪更多元素但同时增加 CPU 资源消耗。
- 默认值：100

#### enable_udf

- 含义：是否开启 UDF。
- 默认值：FALSE

### 导入和导出

#### max_broker_load_job_concurrency

- 含义：StarRocks 集群中可以并行执行的 Broker Load 作业的最大数量。本参数仅适用于 Broker Load。取值必须小于 `max_running_txn_num_per_db`。从 2.5 版本开始，该参数默认值从 `10` 变为 `5`。参数别名 `async_load_task_pool_size`。
- 默认值：5

#### load_straggler_wait_second

- 含义：控制 BE 副本最大容忍的导入落后时长，超过这个时长就进行克隆。
- 单位：秒
- 默认值：300

#### desired_max_waiting_jobs

- 含义：最多等待的任务数，适用于所有的任务，建表、导入、schema change。<br />如果 FE 中处于 PENDING 状态的作业数目达到该值，FE 会拒绝新的导入请求。该参数配置仅对异步执行的导入有效。从 2.5 版本开始，该参数默认值从 100 变为 1024。
- 默认值：1024

#### max_load_timeout_second

- 含义：导入作业的最大超时时间，适用于所有导入。
- 单位：秒
- 默认值：259200

#### min_load_timeout_second

- 含义：导入作业的最小超时时间，适用于所有导入。
- 单位：秒
- 默认值：1

#### max_running_txn_num_per_db

- 含义：StarRocks 集群每个数据库中正在运行的导入相关事务的最大个数，默认值为 `1000`。自 3.1 版本起，默认值由 100 变为 1000。<br />当数据库中正在运行的导入相关事务超过最大个数限制时，后续的导入不会执行。如果是同步的导入作业请求，作业会被拒绝；如果是异步的导入作业请求，作业会在队列中等待。不建议调大该值，会增加系统负载。
- 默认值：1000

#### load_parallel_instance_num

- 含义：单个 BE 上每个作业允许的最大并发实例数。自 3.1 版本起弃用。
- 默认值：1

#### disable_load_job

- 含义：是否禁用任何导入任务，集群出问题时的止损措施。
- 默认值：FALSE

#### history_job_keep_max_second

- 含义：历史任务最大的保留时长，例如 schema change 任务。
- 单位：秒
- 默认值：604800

#### label_keep_max_num

- 含义：一定时间内所保留导入任务的最大数量。超过之后历史导入作业的信息会被删除。
- 默认值：1000

#### label_keep_max_second

- 含义：已经完成、且处于 FINISHED 或 CANCELLED 状态的导入作业记录在 StarRocks 系统 label 的保留时长，默认值为 3 天。<br />该参数配置适用于所有模式的导入作业。- 单位：秒。设定过大将会消耗大量内存。
- 默认值：259200

#### max_routine_load_task_concurrent_num

- 含义：每个 Routine Load 作业最大并发执行的 task 数。
- 默认值：5

#### max_routine_load_task_num_per_be

- 含义：每个 BE 并发执行的 Routine Load 导入任务数量上限。从 3.1.0 版本开始，参数默认值从 5 变为 16，并且不再需要小于等于 BE 的配置项 `routine_load_thread_pool_size`（已废弃）。
- 默认值：16

#### max_routine_load_batch_size

- 含义：每个 Routine Load task 导入的最大数据量。
- 单位：字节
- 默认值：4294967296

#### routine_load_task_consume_second

- 含义：集群内每个 Routine Load 导入任务消费数据的最大时间。<br />自 v3.1.0 起，Routine Load 导入作业 [job_properties](../sql-reference/sql-statements/data-manipulation/CREATE_ROUTINE_LOAD.md#job_properties) 新增参数 `task_consume_second`，作用于单个 Routine Load 导入作业内的导入任务，更加灵活。
- 单位：秒
- 默认值：15

#### routine_load_task_timeout_second

- 含义：集群内每个 Routine Load 导入任务超时时间，- 单位：秒。<br />自 v3.1.0 起，Routine Load 导入作业 [job_properties](../sql-reference/sql-statements/data-manipulation/CREATE_ROUTINE_LOAD.md#job_properties) 新增参数 `task_timeout_second`，作用于单个 Routine Load 导入作业内的任务，更加灵活。
- 单位：秒
- 默认值：60

#### routine_load_unstable_threshold_second

- 含义：Routine Load 导入作业的任一导入任务消费延迟，即正在消费的消息时间戳与当前时间的差值超过该阈值，且数据源中存在未被消费的消息，则导入作业置为 UNSTABLE 状态。
- 单位：秒
- 默认值：3600

#### max_tolerable_backend_down_num

- 含义：允许的最大故障 BE 数。如果故障的 BE 节点数超过该阈值，则不能自动恢复 Routine Load 作业。
- 默认值：0

#### period_of_auto_resume_min

- 含义：自动恢复 Routine Load 的时间间隔。
- 单位：分钟
- 默认值：5

#### spark_load_default_timeout_second

- 含义：Spark 导入的超时时间。
- 单位：秒
- 默认值：86400

#### spark_home_default_dir

- 含义：Spark 客户端根目录。
- 默认值：`StarRocksFE.STARROCKS_HOME_DIR + "/lib/spark2x"`

#### stream_load_default_timeout_second

- 含义：Stream Load 的默认超时时间。
- 单位：秒
- 默认值：600

#### max_stream_load_timeout_second

- 含义：Stream Load 的最大超时时间。
- 单位：秒
- 默认值：259200

#### insert_load_default_timeout_second

- 含义：Insert Into 语句的超时时间。
- 单位：秒
- 默认值：3600

#### broker_load_default_timeout_second

- 含义：Broker Load 的超时时间。
- 单位：秒
- 默认值：14400

#### min_bytes_per_broker_scanner

- 含义：单个 Broker Load 任务最大并发实例数。
- 单位：字节
- 默认值：67108864

#### max_broker_concurrency

- 含义：单个 Broker Load 任务最大并发实例数。从 3.1 版本起，StarRocks 不再支持该参数。
- 默认值：100

#### export_max_bytes_per_be_per_task

- 含义：单个导出任务在单个 BE 上导出的最大数据量。
- 单位：字节
- 默认值：268435456

#### export_running_job_num_limit

- 含义：导出作业最大的运行数目。
- 默认值：5

#### export_task_default_timeout_second

- 含义：导出作业的超时时长。
- 单位：秒。
- 默认值：7200

#### empty_load_as_error

- 含义：导入数据为空时，是否返回报错提示 `all partitions have no load data`。取值：<br /> - **TRUE**：当导入数据为空时，则显示导入失败，并返回报错提示 `all partitions have no load data`。<br /> - **FALSE**：当导入数据为空时，则显示导入成功，并返回 `OK`，不返回报错提示。
- 默认值：TRUE

#### enable_sync_publish

- 含义：是否在导入事务 publish 阶段同步执行 apply 任务，仅适用于主键表。取值：
  - `TRUE`（默认）：导入事务 publish 阶段同步执行 apply 任务，即 apply 任务完成后才会返回导入事务 publish 成功，此时所导入数据真正可查。因此当导入任务一次导入的数据量比较大，或者导入频率较高时，开启该参数可以提升查询性能和稳定性，但是会增加导入耗时。
  - `FALSE`：在导入事务 publish 阶段异步执行 apply 任务，即在导入事务 publish 阶段 apply 任务提交之后立即返回导入事务 publish 成功，然而此时导入数据并不真正可查。这时并发的查询需要等到 apply 任务完成或者超时，才能继续执行。因此当导入任务一次导入的数据量比较大，或者导入频率较高时，关闭该参数会影响查询性能和稳定性。
- 默认值：TRUE
- 引入版本：v3.2.0

#### external_table_commit_timeout_ms

- 含义：发布写事务到 StarRocks 外表的超时时长，单位为毫秒。默认值 `10000` 表示超时时长为 10 秒。
- 单位：毫秒
- 默认值：10000

### 存储

#### default_replication_num

- 含义：用于配置分区默认的副本数。如果建表时指定了 `replication_num` 属性，则该属性优先生效；如果建表时未指定 `replication_num`，则配置的 `default_replication_num` 生效。建议该参数的取值不要超过集群内 BE 节点数。
- 默认值：3

#### enable_strict_storage_medium_check

- 含义：建表时，是否严格校验存储介质类型。<br />为 true 时表示在建表时，会严格校验 BE 上的存储介质。比如建表时指定 `storage_medium = HDD`，而 BE 上只配置了 SSD，那么建表失败。<br />为 FALSE 时则忽略介质匹配，建表成功。
- 默认值：FALSE

#### enable_auto_tablet_distribution

- 含义：是否开启自动设置分桶功能。<ul><li>设置为 `true` 表示开启，您在建表或新增分区时无需指定分桶数目，StarRocks 自动决定分桶数量。自动设置分桶数目的策略，请参见[设置分桶数量](../table_design/Data_distribution.md#设置分桶数量)。</li><li>设置为 `false` 表示关闭，您在建表时需要手动指定分桶数量。<br />新增分区时，如果您不指定分桶数量，则新分区的分桶数量继承建表时候的分桶数量。当然您也可以手动指定新增分区的分桶数量。</li></ul>
- 默认值：TRUE
- 引入版本：2.5.6

#### storage_usage_soft_limit_percent

- 含义：如果 BE 存储目录空间使用率超过该值且剩余空间小于 `storage_usage_soft_limit_reserve_bytes`，则不能继续往该路径 clone tablet。
- 默认值：90

#### storage_usage_soft_limit_reserve_bytes

- 含义：默认 200 GB，单位为 Byte，如果 BE 存储目录下剩余空间小于该值且空间使用率超过 `storage_usage_soft_limit_percent`，则不能继续往该路径 clone tablet。
- 单位：字节
- 默认值：200 \* 1024 \* 1024 \* 1024

#### catalog_trash_expire_second

- 含义：删除表/数据库之后，元数据在回收站中保留的时长，超过这个时长，数据就不可以再恢复。
- 单位：秒
- 默认值：86400

#### alter_table_timeout_second

- 含义：Schema change 超时时间。
- 单位：秒
- 默认值：86400

#### enable_fast_schema_evolution

- 含义：是否开启集群内所有表的 fast schema evolution，取值：`TRUE` 或 `FALSE`（默认）。开启后增删列时可以提高 schema change 速度并降低资源使用。
  > **NOTE**
  >
  > - StarRocks 存算分离集群不支持该参数。
  > - 如果您需要为某张表设置该配置，例如关闭该表的 fast schema evolution，则可以在建表时设置表属性 [`fast_schema_evolution`](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md#设置-fast-schema-evolution)。
- 默认值：FALSE
- 引入版本：3.2.0

#### recover_with_empty_tablet

- 含义：在 tablet 副本丢失/损坏时，是否使用空的 tablet 代替。<br />这样可以保证在有 tablet 副本丢失/损坏时，query 依然能被执行（但是由于缺失了数据，结果可能是错误的）。默认为 false，不进行替代，查询会失败。
- 默认值：FALSE

#### tablet_create_timeout_second

- 含义：创建 tablet 的超时时长。自 3.1 版本起，默认值由 1 改为 10。
- 单位：秒
- 默认值：10

#### tablet_delete_timeout_second

- 含义：删除 tablet 的超时时长。
- 单位：秒
- 默认值：2

#### check_consistency_default_timeout_second

- 含义：副本一致性检测的超时时间
- 单位：秒
- 默认值：600

#### tablet_sched_slot_num_per_path

- 含义：一个 BE 存储目录能够同时执行 tablet 相关任务的数目。参数别名 `schedule_slot_num_per_path`。从 2.5 版本开始，该参数默认值从 2.4 版本的 `4` 变为 `8`。
- 默认值：8

#### tablet_sched_max_scheduling_tablets

- 含义：可同时调度的 tablet 的数量。如果正在调度的 tablet 数量超过该值，跳过 tablet 均衡和修复检查。
- 默认值：10000

#### tablet_sched_disable_balance

- 含义：是否禁用 Tablet 均衡调度。参数别名 `disable_balance`。
- 默认值：FALSE

#### tablet_sched_disable_colocate_balance

- 含义：是否禁用 Colocate Table 的副本均衡。参数别名 `disable_colocate_balance`。
- 默认值：FALSE

#### tablet_sched_max_balancing_tablets

- 含义：正在均衡的 tablet 数量的最大值。如果正在均衡的 tablet 数量超过该值，跳过 tablet 重新均衡。参数别名 `max_balancing_tablets`。
- 默认值：500

#### tablet_sched_balance_load_disk_safe_threshold

- 含义：判断 BE 磁盘使用率是否均衡的百分比阈值。如果所有 BE 的磁盘使用率低于该值，认为磁盘使用均衡。当有 BE 磁盘使用率超过该阈值时，如果最大和最小 BE 磁盘使用率之差高于 10%，则认为磁盘使用不均衡，会触发 Tablet 重新均衡。参数别名`balance_load_disk_safe_threshold`。
- 默认值：0.5

#### tablet_sched_balance_load_score_threshold

- 含义：用于判断 BE 负载是否均衡的百分比阈值。如果一个 BE 的负载低于所有 BE 的平均负载，且差值大于该阈值，则认为该 BE 处于低负载状态。相反，如果一个 BE 的负载比平均负载高且差值大于该阈值，则认为该 BE 处于高负载状态。参数别名 `balance_load_score_threshold`。
- 默认值：0.1

#### tablet_sched_repair_delay_factor_second

- 含义：FE 进行副本修复的间隔。参数别名 `tablet_repair_delay_factor_second`。
- 单位：秒
- 默认值：60

#### tablet_sched_min_clone_task_timeout_sec

- 含义：克隆 Tablet 的最小超时时间。
- 单位：秒
- 默认值：3 \* 60

#### tablet_sched_max_clone_task_timeout_sec

- 含义：克隆 Tablet 的最大超时时间。参数别名 `max_clone_task_timeout_sec`。
- 单位：秒
- 默认值：2 \* 60 \* 60

#### tablet_sched_max_not_being_scheduled_interval_ms

- 含义：克隆 Tablet 调度时，如果超过该时间一直未被调度，则将该 Tablet 的调度优先级升高，以尽可能优先调度。
- 单位：毫秒
- 默认值：15 \* 60 \* 100

### 存算分离相关动态参数

#### lake_compaction_score_selector_min_score

- 含义：触发 Compaction 操作的 Compaction Score 阈值。当一个表分区的 Compaction Score 大于或等于该值时，系统会对该分区执行 Compaction 操作。
- 默认值： 10.0
- 引入版本：v3.1.0

Compaction Score 代表了一个表分区是否值得进行 Compaction 的评分，您可以通过 [SHOW PARTITIONS](../sql-reference/sql-statements/data-manipulation/SHOW_PARTITIONS.md) 语句返回中的 `MaxCS` 一列的值来查看某个分区的 Compaction Score。Compaction Score 和分区中的文件数量有关系。文件数量过多将影响查询性能，因此系统后台会定期执行 Compaction 操作来合并小文件，减少文件数量。

#### lake_compaction_max_tasks

- 含义：允许同时执行的 Compaction 任务数。
- 默认值：-1
- 引入版本：v3.1.0

系统依据分区中 Tablet 数量来计算 Compaction 任务数。如果一个分区有 10 个 Tablet，那么对该分区作一次 Compaciton 就会创建 10 个 Compaction 任务。如果正在执行中的 Compaction 任务数超过该阈值，系统将不会创建新的 Compaction 任务。将该值设置为 `0` 表示禁止 Compaction，设置为 `-1` 表示系统依据自适应策略自动计算该值。

#### lake_compaction_history_size

- 含义：在 Leader FE 节点内存中保留多少条最近成功的 Compaction 任务历史记录。您可以通过 `SHOW PROC '/compactions'` 命令查看最近成功的 Compaction 任务记录。请注意，Compaction 历史记录是保存在 FE 进程内存中的，FE 进程重启后历史记录会丢失。
- 默认值：12
- 引入版本：v3.1.0

#### lake_compaction_fail_history_size

- 含义：在 Leader FE 节点内存中保留多少条最近失败的 Compaction 任务历史记录。您可以通过 `SHOW PROC '/compactions'` 命令查看最近失败的 Compaction 任务记录。请注意，Compaction 历史记录是保存在 FE 进程内存中的，FE 进程重启后历史记录会丢失。
- 默认值：12
- 引入版本：v3.1.0

#### lake_publish_version_max_threads

- 含义：发送生效版本（Publish Version）任务的最大线程数。
- 默认值：512
- 引入版本：v3.2.0

#### lake_autovacuum_parallel_partitions

- 含义：最多可以同时对多少个表分区进行垃圾数据清理（AutoVacuum，即在 Compaction 后进行的垃圾文件回收）。
- 默认值：8
- 引入版本：v3.1.0

#### lake_autovacuum_partition_naptime_seconds

- 含义：对同一个表分区进行垃圾数据清理的最小间隔时间。
- 单位：秒
- 默认值：180
- 引入版本：v3.1.0

#### lake_autovacuum_grace_period_minutes

- 含义：保留历史数据版本的时间范围。此时间范围内的历史数据版本不会被自动清理。您需要将该值设置为大于最大查询时间，以避免正在访问中的数据被删除导致查询失败。
- 单位：分钟
- 默认值：5
- 引入版本：v3.1.0

#### lake_autovacuum_stale_partition_threshold

- 含义：如果某个表分区在该阈值范围内没有任何更新操作(导入、删除或 Compaction)，将不再触发该分区的自动垃圾数据清理操作。
- 单位：小时
- 默认值：12
- 引入版本：v3.1.0

#### lake_enable_ingest_slowdown

- 含义：是否开启导入限速功能。开启导入限速功能后，当某个表分区的 Compaction Score 超过了 `lake_ingest_slowdown_threshold`，该表分区上的导入任务将会被限速。
- 默认值：false
- 引入版本：v3.2.0

#### lake_ingest_slowdown_threshold

- 含义：触发导入限速的 Compaction Score 阈值。只有当 `lake_enable_ingest_slowdown` 设置为 `true` 后，该配置项才会生效。
- 默认值：100
- 引入版本：v3.2.0

> **说明**
>
> 当 `lake_ingest_slowdown_threshold` 比配置项 `lake_compaction_score_selector_min_score` 小时，实际生效的阈值会是 `lake_compaction_score_selector_min_score`。

#### lake_ingest_slowdown_ratio

- 含义：导入限速比例。
- 默认值：0.1
- 引入版本：v3.2.0

数据导入任务可以分为数据写入和数据提交（COMMIT）两个阶段，导入限速是通过延迟数据提交来达到限速的目的的，延迟比例计算公式为：`(compaction_score - lake_ingest_slowdown_threshold) * lake_ingest_slowdown_ratio`。例如，数据写入阶段耗时为 5 分钟，`lake_ingest_slowdown_ratio` 为 0.1，Compaction Score 比 `lake_ingest_slowdown_threshold` 多 10，那么延迟提交的时间为 `5 * 10 * 0.1 = 5` 分钟，相当于写入阶段的耗时由 5 分钟增加到了 10 分钟，平均导入速度下降了一倍。

> **说明**
>
> - 如果一个导入任务同时向多个分区写入，那么会取所有分区的 Compaction Score 的最大值来计算延迟提交时间。
> - 延迟提交的时间是在第一次尝试提交时计算的，一旦确定便不会更改，延迟时间一到，只要 Compaction Score 不超过 `lake_compaction_score_upper_bound`，系统都会执行数据提交（COMMIT）操作。
> - 如果延迟之后的提交时间超过了导入任务的超时时间，那么导入任务会直接失败。

#### lake_compaction_score_upper_bound

- 含义：表分区的 Compaction Score 的上限, `0` 表示没有上限。只有当 `lake_enable_ingest_slowdown` 设置为 `true` 后，该配置项才会生效。当表分区 Compaction Score 达到或超过该上限后，所有涉及到该分区的导入任务将会被无限延迟提交，直到 Compaction Score 降到该值以下或者任务超时。
- 默认值：0
- 引入版本：v3.2.0

### 其他动态参数

#### plugin_enable

- 含义：是否开启了插件功能。只能在 Leader FE 安装/卸载插件。
- 默认值：TRUE

#### max_small_file_number

- 含义：允许存储小文件数目的最大值。
- 默认值：100

#### max_small_file_size_bytes

- 含义：存储文件的大小上限。
- 单位：字节
- 默认值：1024 \* 1024

#### agent_task_resend_wait_time_ms

- 含义：Agent task 重新发送前的等待时间。当代理任务的创建时间已设置，并且距离现在超过该值，才能重新发送代理任务，。<br />该参数防止过于频繁的代理任务发送。
- 单位：毫秒
- 默认值：5000

#### backup_job_default_timeout_ms

- 含义：Backup 作业的超时时间
- 单位：毫秒
- 默认值：86400*1000

#### enable_experimental_mv

- 含义：是否开启异步物化视图功能。`TRUE` 表示开启。从 2.5.2 版本开始，该功能默认开启。2.5.2 版本之前默认值为 `FALSE`。
- 默认值：TRUE

#### authentication_ldap_simple_bind_base_dn

- 含义：检索用户时，使用的 Base DN，用于指定 LDAP 服务器检索用户鉴权信息的起始点。
- 默认值：空字符串

#### authentication_ldap_simple_bind_root_dn

- 含义：检索用户时，使用的管理员账号的 DN。
- 默认值：空字符串

#### authentication_ldap_simple_bind_root_pwd

- 含义：检索用户时，使用的管理员账号的密码。
- 默认值：空字符串

#### authentication_ldap_simple_server_host

- 含义：LDAP 服务器所在主机的主机名。
- 默认值：空字符串

#### authentication_ldap_simple_server_port

- 含义：LDAP 服务器的端口。
- 默认值：389

#### authentication_ldap_simple_user_search_attr

- 含义：LDAP 对象中标识用户的属性名称。
- 默认值：uid

#### max_upload_task_per_be

- 含义：单次 BACKUP 操作下，系统向单个 BE 节点下发的最大上传任务数。设置为小于或等于 0 时表示不限制任务数。该参数自 v3.1.0 起新增。
- 默认值：0

#### max_download_task_per_be

- 含义：单次 RESTORE 操作下，系统向单个 BE 节点下发的最大下载任务数。设置为小于或等于 0 时表示不限制任务数。该参数自 v3.1.0 起新增。
- 默认值：0

#### allow_system_reserved_names

- 含义：是否允许用户创建以 `__op` 或 `__row` 开头命名的列。TRUE 表示启用此功能。请注意，在 StarRocks 中，这样的列名被保留用于特殊目的，创建这样的列可能导致未知行为，因此系统默认禁止使用这类名字。该参数自 v3.2.0 起新增。
- 默认值: FALSE

#### enable_backup_materialized_view

- 含义：在数据库的备份操作中，是否对数据库中的异步物化视图进行备份。如果设置为 `false`，将跳过对异步物化视图的备份。该参数自 v3.2.0 起新增。
- 默认值: TRUE

#### enable_colocate_mv_index

- 含义：在创建同步物化视图时，是否将同步物化视图的索引与基表加入到相同的 Colocate Group。如果设置为 `true`，TabletSink 将加速同步物化视图的写入性能。该参数自 v3.2.0 起新增。
- 默认值: TRUE

#### enable_mv_automatic_active_check

- 含义：是否允许系统自动检查和重新激活异步物化视图。启用此功能后，系统将会自动激活因基表（或视图）Schema Change 或重建而失效（Inactive）的物化视图。请注意，此功能不会激活由用户手动设置为 Inactive 的物化视图。此项功能支持从 v3.1.6 版本开始。
- 默认值: TRUE

##### jdbc_meta_default_cache_enable

- 含义：JDBC Catalog 元数据缓存是否开启的默认值。当设置为TRUE时，新创建的 JDBC Catalog 会默认开启元数据缓存。
- 默认值：FALSE

##### jdbc_meta_default_cache_expire_sec

- 含义：JDBC Catalog 元数据缓存的默认过期时间。当 jdbc_meta_default_cache_enable 设置为 TRUE 时，新创建的 JDBC Catalog 会默认设置元数据缓存的过期时间。
- 单位：秒
- 默认值：600

##### default_mv_refresh_immediate

- 含义：创建异步物化视图后，是否立即刷新该物化视图。当设置为 `true` 时，异步物化视图创建后会立即刷新。
- 默认值：TRUE
- 引入版本：v3.2.3

## 配置 FE 静态参数

以下 FE 配置项为静态参数，不支持在线修改，您需要在 `fe.conf` 中修改并重启 FE。

本节对 FE 静态参数做了如下分类：

- [Log](#log-fe-静态)
- [Server](#server-fe-静态)
- [元数据与集群管理](#元数据与集群管理fe-静态)
- [Query engine](#query-enginefe-静态)
- [导入和导出](#导入和导出fe-静态)
- [存储](#存储fe-静态)
- [其他动态参数](#其他动态参数)

### Log (FE 静态)

#### log_roll_size_mb

- 含义：日志文件的大小。
- 单位：MB
- 默认值：1024，表示每个日志文件的大小为 1 GB。

#### sys_log_dir

- 含义：系统日志文件的保存目录。
- 默认值：`StarRocksFE.STARROCKS_HOME_DIR + "/log"`

#### sys_log_level

- 含义：系统日志的级别，从低到高依次为 `INFO`、`WARN`、`ERROR`、`FATAL`。
- 默认值：INFO

#### sys_log_verbose_modules

- 含义：打印系统日志的模块。如果设置参数取值为 `org.apache.starrocks.catalog`，则表示只打印 Catalog 模块下的日志。
- 默认值：空字符串

#### sys_log_roll_interval

- 含义：系统日志滚动的时间间隔。取值范围：`DAY` 和 `HOUR`。<br />取值为 `DAY` 时，日志文件名的后缀为 `yyyyMMdd`。取值为 `HOUR` 时，日志文件名的后缀为 `yyyyMMddHH`。
- 默认值：DAY

#### sys_log_delete_age

- 含义：系统日志文件的保留时长。默认值 `7d` 表示系统日志文件可以保留 7 天，保留时长超过 7 天的系统日志文件会被删除。
- 默认值：`7d`

#### sys_log_roll_num

- 含义：每个 `sys_log_roll_interval` 时间段内，允许保留的系统日志文件的最大数目。
- 默认值：10

#### audit_log_dir

- 含义：审计日志文件的保存目录。
- 默认值：`StarRocksFE.STARROCKS_HOME_DIR + "/log"`

#### audit_log_roll_num

- 含义：每个 `audit_log_roll_interval` 时间段内，允许保留的审计日志文件的最大数目。
- 默认值：90

#### audit_log_modules

- 含义：打印审计日志的模块。默认打印 slow_query 和 query 模块的日志。可以指定多个模块，模块名称之间用英文逗号加一个空格分隔。
- 默认值：slow_query, query

#### audit_log_roll_interval

- 含义：审计日志滚动的时间间隔。取值范围：`DAY` 和 `HOUR`。<br />取值为 `DAY` 时，日志文件名的后缀为 `yyyyMMdd`。取值为 `HOUR` 时，日志文件名的后缀为 `yyyyMMddHH`。
- 默认值：DAY

#### audit_log_delete_age

- 含义：审计日志文件的保留时长。- 默认值 `30d` 表示审计日志文件可以保留 30 天，保留时长超过 30 天的审计日志文件会被删除。
- 默认值：`30d`

#### dump_log_dir

- 含义：Dump 日志文件的保存目录。
- 默认值：`StarRocksFE.STARROCKS_HOME_DIR + "/log"`

#### dump_log_modules

- 含义：打印 Dump 日志的模块。默认打印 query 模块的日志。可以指定多个模块，模块名称之间用英文逗号加一个空格分隔。
- 默认值：query

#### dump_log_roll_interval

- 含义：Dump 日志滚动的时间间隔。取值范围：`DAY` 和 `HOUR`。<br />取值为 `DAY` 时，日志文件名的后缀为 `yyyyMMdd`。取值为 `HOUR` 时，日志文件名的后缀为 `yyyyMMddHH`。
- 默认值：DAY

#### dump_log_roll_num

- 含义：每个 `dump_log_roll_interval` 时间内，允许保留的 Dump 日志文件的最大数目。
- 默认值：10

#### dump_log_delete_age

- 含义：Dump 日志文件的保留时长。- 默认值 `7d` 表示 Dump 日志文件可以保留 7 天，保留时长超过 7 天的 Dump 日志文件会被删除。
- 默认值：`7d`

### Server (FE 静态)

#### frontend_address

- 含义：FE 节点的 IP 地址。
- 默认值：0.0.0.0

#### priority_networks

- 含义：为那些有多个 IP 地址的服务器声明一个选择策略。 <br />请注意，最多应该有一个 IP 地址与此列表匹配。这是一个以分号分隔格式的列表，用 CIDR 表示法，例如 `10.10.10.0/24`。 如果没有匹配这条规则的ip，会随机选择一个。
- 默认值：空字符串

#### http_port

- 含义：FE 节点上 HTTP 服务器的端口。
- 默认值：8030

#### http_worker_threads_num

- 含义：Http Server 用于处理 HTTP 请求的线程数。如果配置为负数或 0 ，线程数将设置为 CPU 核数的 2 倍。
- 默认值：0
- 引入版本：2.5.18，3.0.10，3.1.7，3.2.2

#### http_backlog_num

- 含义：HTTP 服务器支持的 Backlog 队列长度。
- 默认值：1024

#### cluster_name

- 含义：FE 所在 StarRocks 集群的名称，显示为网页标题。
- 默认值：StarRocks Cluster

#### rpc_port

- 含义：FE 节点上 Thrift 服务器的端口。
- 默认值：9020

#### thrift_backlog_num

- 含义：Thrift 服务器支持的 Backlog 队列长度。
- 默认值：1024

#### thrift_server_max_worker_threads

- 含义：Thrift 服务器支持的最大工作线程数。
- 默认值：4096

#### thrift_client_timeout_ms

- 含义：Thrift 客户端链接的空闲超时时间，即链接超过该时间无新请求后则将链接断开。
- 单位：毫秒。
- 默认值：5000

#### thrift_server_queue_size

- 含义：Thrift 服务器 pending 队列长度。如果当前处理线程数量超过了配置项 `thrift_server_max_worker_threads` 的值，则将超出的线程加入 pending 队列。
- 默认值：4096

#### brpc_idle_wait_max_time

- 含义：bRPC 的空闲等待时间。单位：毫秒。
- 默认值：10000

#### query_port

- 含义：FE 节点上 MySQL 服务器的端口。
- 默认值：9030

#### mysql_service_nio_enabled

- 含义：是否开启 MySQL 服务器的异步 I/O 选项。
- 默认值：TRUE

#### mysql_service_io_threads_num

- 含义：MySQL 服务器中用于处理 I/O 事件的最大线程数。
- 默认值：4

#### mysql_nio_backlog_num

- 含义：MySQL 服务器支持的 Backlog 队列长度。
- 默认值：1024

#### max_mysql_service_task_threads_num

- 含义：MySQL 服务器中用于处理任务的最大线程数。
- 默认值：4096

#### mysql_server_version

- 含义：MySQL 服务器的版本。修改该参数配置会影响以下场景中返回的版本号：1. `select version();` 2. Handshake packet 版本 3. 全局变量 `version` 的取值 (`show variables like 'version';`)
- 默认值：5.1.0

#### max_connection_scheduler_threads_num

- 含义：连接调度器支持的最大线程数。
- 默认值：4096

#### qe_max_connection

- 含义：FE 支持的最大连接数，包括所有用户发起的连接。
- 默认值：1024

#### check_java_version

- 含义：检查已编译的 Java 版本与运行的 Java 版本是否兼容。<br />如果不兼容，则上报 Java 版本不匹配的异常信息，并终止启动。
- 默认值：TRUE

### 元数据与集群管理（FE 静态）

#### meta_dir

- 含义：元数据的保存目录。
- 默认值：`StarRocksFE.STARROCKS_HOME_DIR + "/meta"`

#### heartbeat_mgr_threads_num

- 含义：Heartbeat Manager 中用于发送心跳任务的最大线程数。
- 默认值：8

#### heartbeat_mgr_blocking_queue_size

- 含义：Heartbeat Manager 中存储心跳任务的阻塞队列大小。
- 默认值：1024

#### metadata_failure_recovery

- 含义：是否强制重置 FE 的元数据。请谨慎使用该配置项。
- 默认值：FALSE

#### edit_log_port

- 含义：FE 所在 StarRocks 集群中各 Leader FE、Follower FE、Observer FE 之间通信用的端口。
- 默认值：9010

#### edit_log_type

- 含义：编辑日志的类型。取值只能为 `BDB`。
- 默认值：BDB

#### bdbje_heartbeat_timeout_second

- 含义：FE 所在 StarRocks 集群中 Leader FE 和 Follower FE 之间的 BDB JE 心跳超时时间。
- 单位：秒。
- 默认值：30

#### bdbje_lock_timeout_second

- 含义：BDB JE 操作的锁超时时间。
- 单位：秒。
- 默认值：1

#### max_bdbje_clock_delta_ms

- 含义：FE 所在 StarRocks 集群中 Leader FE 与非 Leader FE 之间能够容忍的最大时钟偏移。
- 单位：毫秒。
- 默认值：5000

#### txn_rollback_limit

- 含义：允许回滚的最大事务数。
- 默认值：100

#### bdbje_replica_ack_timeout_second

- 含义：FE 所在 StarRocks 集群中，元数据从 Leader FE 写入到多个 Follower FE 时，Leader FE 等待足够多的 Follower FE 发送 ACK 消息的超时时间。当写入的元数据较多时，可能返回 ACK 的时间较长，进而导致等待超时。如果超时，会导致写元数据失败，FE 进程退出，此时可以适当地调大该参数取值。
- 单位：秒
- 默认值：10

#### master_sync_policy

- 含义：FE 所在 StarRocks 集群中，Leader FE 上的日志刷盘方式。该参数仅在当前 FE 为 Leader 时有效。取值范围： <ul><li>`SYNC`：事务提交时同步写日志并刷盘。</li><li> `NO_SYNC`：事务提交时不同步写日志。</li><li> `WRITE_NO_SYNC`：事务提交时同步写日志，但是不刷盘。 </li></ul>如果您只部署了一个 Follower FE，建议将其设置为 `SYNC`。 如果您部署了 3 个及以上 Follower FE，建议将其与下面的 `replica_sync_policy` 均设置为 `WRITE_NO_SYNC`。
- 默认值：SYNC

#### replica_sync_policy

- 含义：FE 所在 StarRocks 集群中，Follower FE 上的日志刷盘方式。取值范围： <ul><li>`SYNC`：事务提交时同步写日志并刷盘。</li><li> `NO_SYNC`：事务提交时不同步写日志。</li><li> `WRITE_NO_SYNC`：事务提交时同步写日志，但是不刷盘。</li></ul>
- 默认值：SYNC

#### replica_ack_policy

- 含义：判定日志是否有效的策略，默认是多数 Follower FE 返回确认消息，就认为生效。
- 默认值：SIMPLE_MAJORITY

#### cluster_id

- 含义：FE 所在 StarRocks 集群的 ID。具有相同集群 ID 的 FE 或 BE 属于同一个 StarRocks 集群。取值范围：正整数。- 默认值 `-1` 表示在 Leader FE 首次启动时随机生成一个。
- 默认值：-1

### Query engine（FE 静态）

#### publish_version_interval_ms

- 含义：两个版本发布操作之间的时间间隔。
- 单位：毫秒
- 默认值：10

#### statistic_cache_columns

- 含义：缓存统计信息表的最大行数。
- 默认值：100000

### 导入和导出（FE 静态）

#### load_checker_interval_second

- 含义：导入作业的轮询间隔。
- 单位：秒。
- 默认值：5

#### transaction_clean_interval_second

- 含义：已结束事务的清理间隔。建议清理间隔尽量短，从而确保已完成的事务能够及时清理掉。
- 单位：秒。
- 默认值：30

#### label_clean_interval_second

- 含义：作业标签的清理间隔。建议清理间隔尽量短，从而确保历史作业的标签能够及时清理掉。
- 单位：秒。
- 默认值：14400

#### spark_dpp_version

- 含义：Spark DPP 特性的版本。
- 默认值：1.0.0

#### spark_resource_path

- 含义：Spark 依赖包的根目录。
- 默认值：空字符串

#### spark_launcher_log_dir

- 含义：Spark 日志的保存目录。
- 默认值：`sys_log_dir + "/spark_launcher_log"`

#### yarn_client_path

- 含义：Yarn 客户端的根目录。
- 默认值：`StarRocksFE.STARROCKS_HOME_DIR + "/lib/yarn-client/hadoop/bin/yarn"`

#### yarn_config_dir

- 含义：Yarn 配置文件的保存目录。
- 默认值：`StarRocksFE.STARROCKS_HOME_DIR + "/lib/yarn-config"`

#### export_checker_interval_second

- 含义：导出作业调度器的调度间隔。
- 默认值：5

#### export_task_pool_size

- 含义：导出任务线程池的大小。
- 默认值：5

#### broker_client_timeout_ms

- 含义：Broker RPC 的默认超时时间。
- 单位：毫秒
- 默认值 10s

### 存储（FE 静态）


#### tablet_sched_storage_cooldown_second

- 含义：从 Table 创建时间点开始计算，自动降冷的时延。降冷是指从 SSD 介质迁移到 HDD 介质。<br />参数别名为 `storage_cooldown_second`。默认值 `-1` 表示不进行自动降冷。如需启用自动降冷功能，请显式设置参数取值大于 0。
- 单位：秒。
- 默认值：-1

#### tablet_stat_update_interval_second

- 含义：FE 向每个 BE 请求收集 Tablet 统计信息的时间间隔。
- 单位：秒。
- 默认值：300

### StarRocks 存算分离集群

#### run_mode

- 含义：StarRocks 集群的运行模式。有效值：`shared_data` 和 `shared_nothing` (默认)。`shared_data` 表示在存算分离模式下运行 StarRocks。`shared_nothing` 表示在存算一体模式下运行 StarRocks。<br />**注意**<br />StarRocks 集群不支持存算分离和存算一体模式混合部署。<br />请勿在集群部署完成后更改 `run_mode`，否则将导致集群无法再次启动。不支持从存算一体集群转换为存算分离集群，反之亦然。
- 默认值：shared_nothing

#### cloud_native_meta_port

- 含义：云原生元数据服务监听端口。
- 默认值：6090

#### cloud_native_storage_type

- 含义：您使用的存储类型。在存算分离模式下，StarRocks 支持将数据存储在 HDFS 、Azure Blob（自 v3.1.1 起支持）、以及兼容 S3 协议的对象存储中（例如 AWS S3、Google GCP、阿里云 OSS 以及 MinIO）。有效值：`S3`（默认）、`AZBLOB` 和 `HDFS`。如果您将此项指定为 `S3`，则必须添加以 `aws_s3` 为前缀的配置项。如果您将此项指定为 `AZBLOB`，则必须添加以 `azure_blob` 为前缀的配置项。如果将此项指定为 `HDFS`，则只需指定 `cloud_native_hdfs_url`。
- 默认值：S3

#### cloud_native_hdfs_url

- 含义：HDFS 存储的 URL，例如 `hdfs://127.0.0.1:9000/user/xxx/starrocks/`。

#### aws_s3_path

- 含义：用于存储数据的 S3 存储空间路径，由 S3 存储桶的名称及其下的子路径（如有）组成，如 `testbucket/subpath`。

#### aws_s3_region

- 含义：需访问的 S3 存储空间的地区，如 `us-west-2`。

#### aws_s3_endpoint

- 含义：访问 S3 存储空间的连接地址，如 `https://s3.us-west-2.amazonaws.com`。

#### aws_s3_use_aws_sdk_default_behavior

- 含义：是否使用 AWS SDK 默认的认证凭证。有效值：`true` 和 `false` (默认)。
- 默认值：false

#### aws_s3_use_instance_profile

- 含义：是否使用 Instance Profile 或 Assumed Role 作为安全凭证访问 S3。有效值：`true` 和 `false` (默认)。<ul><li>如果您使用 IAM 用户凭证（Access Key 和 Secret Key）访问 S3，则需要将此项设为 `false`，并指定 `aws_s3_access_key` 和 `aws_s3_secret_key`。</li><li>如果您使用 Instance Profile 访问 S3，则需要将此项设为 `true`。</li><li>如果您使用 Assumed Role 访问 S3，则需要将此项设为 `true`，并指定 `aws_s3_iam_role_arn`。</li><li>如果您使用外部 AWS 账户通过 Assumed Role 认证访问 S3，则需要额外指定 `aws_s3_external_id`。</li></ul>
- 默认值：false

#### aws_s3_access_key

- 含义：访问 S3 存储空间的 Access Key。

#### aws_s3_secret_key

- 含义：访问 S3 存储空间的 Secret Key。

#### aws_s3_iam_role_arn

- 含义：有访问 S3 存储空间权限 IAM Role 的 ARN。

#### aws_s3_external_id

- 含义：用于跨 AWS 账户访问 S3 存储空间的外部 ID。

#### azure_blob_path

- 含义：用于存储数据的 Azure Blob Storage 路径，由存 Storage Account 中的容器名称和容器下的子路径（如有）组成，如 `testcontainer/subpath`。

#### azure_blob_endpoint

- 含义：Azure Blob Storage 的链接地址，如 `https://test.blob.core.windows.net`。

#### azure_blob_shared_key

- 含义：访问 Azure Blob Storage 的 Shared Key。

#### azure_blob_sas_token

- 含义：访问 Azure Blob Storage 的共享访问签名（SAS）。
- 默认值：N/A

### 其他静态参数

#### plugin_dir

- 含义：插件的安装目录。
- 默认值：`STARROCKS_HOME_DIR/plugins`

#### small_file_dir

- 含义：小文件的根目录。
- 默认值：`StarRocksFE.STARROCKS_HOME_DIR + "/small_files"`

#### max_agent_task_threads_num

- 含义：代理任务线程池中用于处理代理任务的最大线程数。
- 默认值：4096

#### auth_token

- 含义：用于内部身份验证的集群令牌。为空则在 Leader FE 第一次启动时随机生成一个。
- 默认值：空字符串

#### tmp_dir

- 含义：临时文件的保存目录，例如备份和恢复过程中产生的临时文件。<br />这些过程完成以后，所产生的临时文件会被清除掉。
- 默认值：`StarRocksFE.STARROCKS_HOME_DIR + "/temp_dir"`

#### locale

- 含义：FE 所使用的字符集。
- 默认值：zh_CN.UTF-8

#### hive_meta_load_concurrency

- 含义：Hive 元数据支持的最大并发线程数。
- 默认值：4

#### hive_meta_cache_refresh_interval_s

- 含义：刷新 Hive 外表元数据缓存的时间间隔。
- 单位：秒。
- 默认值：7200

#### hive_meta_cache_ttl_s

- 含义：Hive 外表元数据缓存的失效时间。
- 单位：秒。
- 默认值：86400

#### hive_meta_store_timeout_s

- 含义：连接 Hive Metastore 的超时时间。
- 单位：秒。
- 默认值：10

#### es_state_sync_interval_second

- 含义：FE 获取 Elasticsearch Index 和同步 StarRocks 外部表元数据的时间间隔。
- 单位：秒
- 默认值：10

#### enable_auth_check

- 含义：是否开启鉴权检查功能。取值范围：`TRUE` 和 `FALSE`。`TRUE` 表示开该功能。`FALSE`表示关闭该功能。
- 默认值：TRUE

#### enable_metric_calculator

- 含义：是否开启定期收集指标 (Metrics) 的功能。取值范围：`TRUE` 和 `FALSE`。`TRUE` 表示开该功能。`FALSE`表示关闭该功能。
- 默认值：TRUE

#### jdbc_connection_pool_size

- 含义：访问JDBC Catalog, JDBC Connection Pool容量上限
- 默认值：8

#### jdbc_minimum_idle_connections

- 含义：访问JDBC Catalog, JDBC Connection Pool中处于idle状态的最低数量
- 默认值：1

#### jdbc_connection_idle_timeout_ms

- 含义：访问JDBC Catalog, 超过这个时间的连接被认为是idle状态
- 单位：毫秒
- 默认值：600000