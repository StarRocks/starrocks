# 配置参数

本文介绍如何配置 StarRocks FE 节点、BE 节点、Broker 以及系统参数，并介绍相关参数。

## FE 配置项

FE 参数分为动态参数和静态参数。动态参数可通过 SQL 命令进行在线配置和调整，方便快捷。**需要注意通过 SQL 命令所做的动态设置在重启 FE 后会失效。如果想让设置长期生效，建议同时修改 fe.conf 文件。**

静态参数必须在 FE 配置文件 **fe.conf** 中进行配置和调整。**调整完成后，需要重启 FE 使变更生效。**

参数是否为动态参数可通过 [ADMIN SHOW CONFIG](../sql-reference/sql-statements/Administration/ADMIN%20SHOW%20CONFIG.md) 返回结果中的 `IsMutable` 列查看。`TRUE` 表示动态参数。

静态和动态参数均可通过 **fe.conf** 文件进行修改。

### 查看 FE 配置项

FE 启动后，您可以在 MySQL 客户端执行 ADMIN SHOW FRONTEND CONFIG 命令来查看参数配置。如果您想查看具体参数的配置，执行如下命令：

```SQL
 ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"];
 ```

详细的命令返回字段解释，参见 [ADMIN SHOW CONFIG](../sql-reference/sql-statements/Administration/ADMIN%20SHOW%20CONFIG.md)。

> **注意**
>
> 只有拥有 `cluster_admin` 角色的用户才可以执行集群管理相关命令。

### 配置 FE 动态参数

您可以通过 [ADMIN SET FRONTEND CONFIG](../sql-reference/sql-statements/Administration/ADMIN%20SET%20CONFIG.md) 命令在线修改 FE 动态参数。

```sql
ADMIN SET FRONTEND CONFIG ("key" = "value");
```

> **注意**
>
> 动态设置的配置项，在 FE 重启之后会恢复成 **fe.conf** 文件中的配置或者默认值。如果需要让配置长期生效，建议设置完之后同时修改 **fe.conf** 文件，防止重启后修改失效。

#### Log

|配置项|默认值|描述|
|---|---|---|
|qe_slow_log_ms|5000|Slow query 的认定时长，单位为 ms。如果查询的响应时间超过此阈值，则会在审计日志 `fe.audit.log` 中记录为 slow query。|

#### 元数据与集群管理

|配置项|默认值|描述|
|---|---|---|
|catalog_try_lock_timeout_ms|5000|全局锁（global lock）获取的超时时长，单位为 ms。|
|edit_log_roll_num|50000|该参数用于控制日志文件的大小，指定了每写多少条元数据日志，执行一次日志滚动操作来为这些日志生成新的日志文件。新日志文件会写入到 BDBJE Database。|
|ignore_unknown_log_id|FALSE|是否忽略未知的 logID。当 FE 回滚到低版本时，可能存在低版本 BE 无法识别的 logID。<br>如果为 TRUE，则 FE 会忽略这些 logID；否则 FE 会退出。|
|ignore_meta_check|FALSE|是否忽略元数据落后的情形。如果为 true，非主 FE 将忽略主 FE 与其自身之间的元数据延迟间隙，即使元数据延迟间隙超过 meta_delay_toleration_second，非主 FE 仍将提供读取服务。<br>当您尝试停止 Master FE 较长时间，但仍希望非 Master FE 可以提供读取服务时，该参数会很有帮助。|
|meta_delay_toleration_second | 300  | FE 所在 StarRocks 集群中，非 Leader FE 能够容忍的元数据落后的最大时间。单位：秒。<br>如果非 Leader FE 上的元数据与 Leader FE 上的元数据之间的延迟时间超过该参数取值，则该非 Leader FE 将停止服务。 |
|drop_backend_after_decommission|TRUE|BE 被下线后，是否删除该 BE。true 代表 BE 被下线后会立即删除该 BE。False 代表下线完成后不删除 BE。|
|enable_collect_query_detail_info|FALSE|是否收集查询的 profile 信息。设置为 true 时，系统会收集查询的 profile。设置为 false 时，系统不会收集查询的 profile。|
|enable_background_refresh_connector_metadata| v3.0 为 TRUE<br />v2.5 为 FALSE |是否开启 Hive 元数据缓存周期性刷新。开启后，StarRocks 会轮询 Hive 集群的元数据服务（Hive Metastore 或 AWS Glue），并刷新经常访问的 Hive 外部数据目录的元数据缓存，以感知数据更新。`true` 代表开启，`false` 代表关闭。自 v2.5.5 起支持。 |
|background_refresh_metadata_interval_millis| 600000 |接连两次 Hive 元数据缓存刷新之间的间隔。单位：毫秒。自 v2.5.5 起支持。|
|background_refresh_metadata_time_secs_since_last_access_secs|86400|Hive 元数据缓存刷新任务过期时间。对于已被访问过的 Hive Catalog，如果超过该时间没有被访问，则停止刷新其元数据缓存。对于未被访问过的 Hive Catalog，StarRocks 不会刷新其元数据缓存。单位：秒。自 v2.5.5 起支持。|

#### Query engine

|配置项|默认值|描述|
|---|---|---|
|max_allowed_in_element_num_of_delete|10000|DELETE 语句中 IN 谓词最多允许的元素数量。|
|enable_materialized_view|TRUE|是否允许创建物化视图。|
|enable_decimal_v3|TRUE|是否开启 Decimal V3。|
|enable_sql_blacklist|FALSE|是否开启 SQL Query 黑名单校验。如果开启，在黑名单中的 Query 不能被执行。|
|dynamic_partition_check_interval_seconds|600|动态分区检查的时间周期。如果有新数据生成，会自动生成分区。|
|dynamic_partition_enable|TRUE|是否开启动态分区功能。打开后，您可以按需为新数据动态创建分区，同时 StarRocks 会⾃动删除过期分区，从而确保数据的时效性。|
|max_partitions_in_one_batch|4096|批量创建分区时，分区数目的最大值。|
|max_query_retry_time|2|FE 上查询重试的最大次数。|
|max_create_table_timeout_second|600|建表最大超时时间，单位为秒。|
|max_running_rollup_job_num_per_table|1|每个 Table 执行 Rollup 任务的最大并发度。|
|max_planner_scalar_rewrite_num|100000|优化器重写 ScalarOperator 允许的最大次数。|
|enable_statistic_collect|TRUE|是否采集统计信息，该开关默认打开。|
|enable_collect_full_statistic|TRUE|是否开启自动全量统计信息采集，该开关默认打开。|
|statistic_auto_collect_ratio|0.8|自动统计信息的健康度阈值。如果统计信息的健康度小于该阈值，则触发自动采集。|
|statistic_max_full_collect_data_size|100|自动统计信息采集的最大分区大小。单位：GB。<br>如果超过该值，则放弃全量采集，转为对该表进行抽样采集。|
|statistic_collect_interval_sec|300|自动定期采集任务中，检测数据更新的间隔时间，默认为 5 分钟。单位：秒。|
| statistic_auto_analyze_start_time | STRING     | 00:00:00        | 用于配置自动全量采集的起始时间。取值范围：`00:00:00` ~ `23:59:59`。从 2.5 开始支持。|
| statistic_auto_analyze_end_time | STRING     | 23:59:59        | 用于配置自动全量采集的结束时间。取值范围：`00:00:00` ~ `23:59:59`。从 2.5 开始支持。|
|statistic_sample_collect_rows|200000|最小采样行数。如果指定了采集类型为抽样采集（SAMPLE），需要设置该参数。<br>如果参数取值超过了实际的表行数，默认进行全量采集。|
|histogram_buckets_size|64|直方图默认分桶数。|
|histogram_mcv_size|100|直方图默认 most common value 的数量。|
|histogram_sample_ratio|0.1|直方图默认采样比例。|
|histogram_max_sample_row_count|10000000|直方图最大采样行数。|
|statistics_manager_sleep_time_sec|60|统计信息相关元数据调度间隔周期。单位：秒。系统根据这个间隔周期，来执行如下操作：<ul><li>创建统计信息表；</li><li>删除已经被删除的表的统计信息；</li><li>删除过期的统计信息历史记录。</li></ul>|
|statistic_update_interval_sec|24 \* 60 \* 60|统计信息内存 Cache 失效时间。单位：秒。|
|statistic_analyze_status_keep_second|259200|统计信息采集任务的记录保留时间，默认为 3 天。单位：秒。|
|statistic_collect_concurrency|3|手动采集任务的最大并发数，默认为 3，即最多可以有 3 个手动采集任务同时运行。超出的任务处于 PENDING 状态，等待调度。|
|enable_local_replica_selection|FALSE|是否选择本地副本进行查询。本地副本可以减少数据传输的网络时延。<br>如果设置为 true，优化器优先选择与当前 FE 相同 IP 的 BE 节点上的 tablet 副本。设置为 false 表示选择可选择本地或非本地副本进行查询。默认为 false。|
|max_distribution_pruner_recursion_depth|100|分区裁剪允许的最大递归深度。增加递归深度可以裁剪更多元素但同时增加 CPU 资源消耗。|
|enable_udf                  | FALSE | 是否开启 UDF。                             |

#### 导入和导出

|配置项|默认值|描述|
|---|---|---|
|max_broker_load_job_concurrency|2|StarRocks 集群中可以并行执行的 Broker Load 任务的最大数量。本参数仅适用于 Broker Load。取值必须小于 `max_running_txn_num_per_db`。从 2.5 版本开始，该参数默认值从 `10` 变为 `2`。参数别名 `async_load_task_pool_size`。 |
|load_straggler_wait_second|300|控制 BE 副本最大容忍的导入落后时长，超过这个时长就进行克隆，单位为秒。|
|desired_max_waiting_jobs|1024|最多等待的任务数，适用于所有的任务，建表、导入、schema change。<br>如果 FE 中处于 PENDING 状态的作业数目达到该值，FE 会拒绝新的导入请求。该参数配置仅对异步执行的导入有效。从 2.5 版本开始，该参数默认值从 100 变为 1024。|
|max_load_timeout_second|259200|导入作业的最大超时时间，适用于所有导入，单位为秒。|
|min_load_timeout_second|1|导入作业的最小超时时间，适用于所有导入，单位为秒。|
|max_running_txn_num_per_db|100|StarRocks 集群每个数据库中正在运行的导入作业的最大个数，默认值为 100。<br>当数据库中正在运行的导入作业超过最大个数限制时，后续的导入不会执行。如果是同步的导入作业，作业会被拒绝；如果是异步的导入作业，作业会在队列中等待。不建议调大该值，会增加系统负载。|
|load_parallel_instance_num|1|单个 BE 上每个作业允许的最大并发实例数。|
|disable_load_job|FALSE|是否禁用任何导入任务，集群出问题时的止损措施。|
|history_job_keep_max_second|604800|历史任务最大的保留时长，例如 schema change 任务，单位为秒。|
|label_keep_max_num|1000|一定时间内所保留导入任务的最大数量。超过之后历史导入作业的信息会被删除。|
|label_keep_max_second|259200|已经完成、且处于 FINISHED 或 CANCELLED 状态的导入作业记录在 StarRocks 系统 label 的保留时长，默认值为 3 天。<br>该参数配置适用于所有模式的导入作业。单位为秒。设定过大将会消耗大量内存。|
|max_routine_load_job_num|100|最大的 Routine Load 作业数。|
|max_routine_load_task_concurrent_num|5|每个 Routine Load 作业最大并发执行的 task 数。|
|max_routine_load_task_num_per_be|5|每个 BE 最大并发执行的 Routine Load task 数，需要小于等于 BE 的配置项 `routine_load_thread_pool_size`。|
|max_routine_load_batch_size|4294967296|每个 Routine Load task 导入的最大数据量，单位为 Byte。|
|routine_load_task_consume_second|15|每个 Routine Load task 消费数据的最大时间，单位为秒。|
|routine_load_task_timeout_second|60|每个 Routine Load task 超时时间，单位为秒。|
|max_tolerable_backend_down_num|0|允许的最大故障 BE 数。如果故障的 BE 节点数超过该阈值，则不能自动恢复 Routine Load 作业。|
|period_of_auto_resume_min|5|自动恢复 Routine Load 的时间间隔，单位为分钟。|
|spark_load_default_timeout_second|86400|Spark 导入的超时时间，单位为秒。|
|spark_home_default_dir|StarRocksFE.STARROCKS_HOME_DIR + "/lib/spark2x"|Spark 客户端根目录。|
|stream_load_default_timeout_second|600|Stream Load 的默认超时时间，单位为秒。|
|max_stream_load_timeout_second|259200|Stream Load 的最大超时时间，单位为秒。|
|insert_load_default_timeout_second|3600|Insert Into 语句的超时时间，单位为秒。|
|broker_load_default_timeout_second|14400|Broker Load 的超时时间，单位为秒。|
|min_bytes_per_broker_scanner|67108864|单个 Broker Load 任务最大并发实例数，单位为 Byte。|
|max_broker_concurrency|100|单个 Broker Load 任务最大并发实例数。|
|export_max_bytes_per_be_per_task|268435456|单个导出任务在单个 BE 上导出的最大数据量，单位为 Byte。|
|export_running_job_num_limit|5|导出作业最大的运行数目。|
|export_task_default_timeout_second|7200|导出作业的超时时长，单位为秒。|
|empty_load_as_error|TRUE|导入数据为空时，是否返回报错提示 `all partitions have no load data`。取值：<br> - **TRUE**：当导入数据为空时，则显示导入失败，并返回报错提示 `all partitions have no load data`。<br> - **FALSE**：当导入数据为空时，则显示导入成功，并返回 `OK`，不返回报错提示。|
| external_table_commit_timeout_ms|10000|发布写事务到 StarRocks 外表的超时时长，单位为毫秒。默认值 `10000` 表示超时时长为 10 秒。 |

#### 存储

|配置项|默认值|描述|
|---|---|---|
|enable_strict_storage_medium_check|FALSE|建表时，是否严格校验存储介质类型。<br>为 true 时表示在建表时，会严格校验 BE 上的存储介质。比如建表时指定 `storage_medium = HDD`，而 BE 上只配置了 SSD，那么建表失败。<br>为 FALSE 时则忽略介质匹配，建表成功。|
|enable_auto_tablet_distribution| TRUE| 是否开启自动设置分桶功能。<ul><li>设置为 `true` 表示开启，您在建表或新增分区时无需指定分桶数目，StarRocks 自动决定分桶数量。自动设置分桶数目的策略，请参见[确定分桶数量)](../table_design/Data_distribution.md#确定分桶数量)。</li><li>设置为 `false` 表示关闭，您在建表时需要手动指定分桶数量。<br>新增分区时，如果您不指定分桶数量，则新分区的分桶数量继承建表时候的分桶数量。当然您也可以手动指定新增分区的分桶数量。</li></ul>自 2.5.6 版本起，StarRocks 支持设置该参数。  |
|storage_usage_soft_limit_percent|90|如果 BE 存储目录空间使用率超过该值且剩余空间小于 `storage_usage_soft_limit_reserve_bytes`，则不能继续往该路径 clone tablet。|
|storage_usage_soft_limit_reserve_bytes|200 \* 1024 \* 1024 \* 1024|默认 200GB，单位为 Byte，如果 BE 存储目录下剩余空间小于该值且空间使用率超过 `storage_usage_soft_limit_percent`，则不能继续往该路径 clone tablet。|
|catalog_trash_expire_second|86400|删除表/数据库之后，元数据在回收站中保留的时长，超过这个时长，数据就不可以在恢复，单位为秒。|
|alter_table_timeout_second|86400|Schema change 超时时间，单位为秒。|
|recover_with_empty_tablet|FALSE|在 tablet 副本丢失/损坏时，是否使用空的 tablet 代替。<br>这样可以保证在有 tablet 副本丢失/损坏时，query 依然能被执行（但是由于缺失了数据，结果可能是错误的）。默认为 false，不进行替代，查询会失败。|
|tablet_create_timeout_second|10|创建 tablet 的超时时长，单位为秒。|
|tablet_delete_timeout_second|2|删除 tablet 的超时时长，单位为秒。|
|check_consistency_default_timeout_second|600|副本一致性检测的超时时间，单位为秒。|
|tablet_sched_slot_num_per_path|8|一个 BE 存储目录能够同时执行 tablet 相关任务的数目。参数别名 `schedule_slot_num_per_path`。从 2.5 版本开始，该参数默认值从 2.4 版本的 `4` 变为 `8`。|
|tablet_sched_max_scheduling_tablets|2000|可同时调度的 tablet 的数量。如果正在调度的 tablet 数量超过该值，跳过 tablet 均衡和修复检查。|
|tablet_sched_disable_balance|FALSE|是否禁用 Tablet 均衡调度。参数别名 `disable_balance`。|
|tablet_sched_disable_colocate_balance|FALSE|是否禁用 Colocate Table 的副本均衡。参数别名 `disable_colocate_balance`。|
|tablet_sched_max_balancing_tablets|100|正在均衡的 tablet 数量的最大值。如果正在均衡的 tablet 数量超过该值，跳过 tablet 重新均衡。参数别名 `max_balancing_tablets`。|
|tablet_sched_balance_load_disk_safe_threshold|0.5|判断 BE 磁盘使用率是否均衡的阈值。只有 `tablet_sched_balancer_strategy` 设置为 `disk_and_tablet`时，该参数才生效。<br>如果所有 BE 的磁盘使用率低于 50%，认为磁盘使用均衡。<br>对于 disk_and_tablet 策略，如果最大和最小 BE 磁盘使用率之差高于 10%，认为磁盘使用不均衡，会触发 tablet 重新均衡。参数别名`balance_load_disk_safe_threshold`。|
|tablet_sched_balance_load_score_threshold|0.1|用于判断 BE 负载是否均衡。只有 `tablet_sched_balancer_strategy` 设置为 `be_load_score`时，该参数才生效。<br>负载比平均负载低 10% 的 BE 处于低负载状态，比平均负载高 10% 的 BE 处于高负载状态。参数别名 `balance_load_score_threshold`。|
|tablet_sched_repair_delay_factor_second|60|FE 进行副本修复的间隔，单位为秒。参数别名 `tablet_repair_delay_factor_second`。|
|tablet_sched_min_clone_task_timeout_sec|3 \* 60|克隆 Tablet 的最小超时时间，单位为秒。|
|tablet_sched_max_clone_task_timeout_sec|2 \* 60 \* 60|克隆 Tablet 的最大超时时间，单位为秒。参数别名 `max_clone_task_timeout_sec`。|

#### 其他动态参数

|配置项|默认值|描述|
|---|---|---|
|plugin_enable|TRUE|是否开启了插件功能。只能在 Leader FE 安装/卸载插件。|
|max_small_file_number|100|允许存储小文件数目的最大值。|
|max_small_file_size_bytes|1024 \* 1024|存储文件的大小上限，单位为 Byte。|
|agent_task_resend_wait_time_ms|5000|Agent task 重新发送前的等待时间。当代理任务的创建时间已设置，并且距离现在超过该值，才能重新发送代理任务，单位为 ms。<br>该参数防止过于频繁的代理任务发送。|
|backup_job_default_timeout_ms|86400*1000|Backup 作业的超时时间，单位为 ms。|
|enable_experimental_mv|TRUE|是否开启异步物化视图功能。`TRUE` 表示开启。从 2.5.2 版本开始，该功能默认开启。2.5.2 版本之前默认值为 `FALSE`。|
|authentication_ldap_simple_bind_base_dn    | 空字符串 | 检索用户时，使用的 Base DN，用于指定 LDAP 服务器检索用户鉴权信息的起始点。 |
|authentication_ldap_simple_bind_root_dn    | 空字符串 | 检索用户时，使用的管理员账号的 DN。                          |
|authentication_ldap_simple_bind_root_pwd   | 空字符串 | 检索用户时，使用的管理员账号的密码。                         |
|authentication_ldap_simple_server_host     | 空字符串 | LDAP 服务器所在主机的主机名。                                |
|authentication_ldap_simple_server_port     | 389     | LDAP 服务器的端口。                                          |
|authentication_ldap_simple_user_search_attr| uid     | LDAP 对象中标识用户的属性名称。                              |

### 配置 FE 静态参数

以下 FE 配置项为静态参数，不支持在线修改，您需要在 `fe.conf` 中修改并重启 FE。

#### Log

| 配置项                  | 默认值                                  | 描述                                                         |
| ----------------------- | --------------------------------------- | ------------------------------------------------------------ |
| log_roll_size_mb        | 1024                                    | 日志文件的大小。单位：MB。默认值 `1024` 表示每个日志文件的大小为 1 GB。 |
| sys_log_dir             | StarRocksFE.STARROCKS_HOME_DIR + "/log" | 系统日志文件的保存目录。                                     |
| sys_log_level           | INFO                                    | 系统日志的级别，从低到高依次为 `INFO`、`WARN`、`ERROR`、`FATAL`。 |
| sys_log_verbose_modules | 空字符串                                | 打印系统日志的模块。如果设置参数取值为 `org.apache.starrocks.catalog`，则表示只打印 Catalog 模块下的日志。 |
| sys_log_roll_interval   | DAY                                     | 系统日志滚动的时间间隔。取值范围：`DAY` 和 `HOUR`。<br>取值为 `DAY` 时，日志文件名的后缀为 `yyyyMMdd`。取值为 `HOUR` 时，日志文件名的后缀为 `yyyyMMddHH`。 |
| sys_log_delete_age      | 7d                                      | 系统日志文件的保留时长。默认值 `7d` 表示系统日志文件可以保留 7 天，保留时长超过 7 天的系统日志文件会被删除。 |
| sys_log_roll_num        | 10                                      | 每个 `sys_log_roll_interval` 时间段内，允许保留的系统日志文件的最大数目。 |
| audit_log_dir           | StarRocksFE.STARROCKS_HOME_DIR + "/log" | 审计日志文件的保存目录。                                     |
| audit_log_roll_num      | 90                                      | 每个 `audit_log_roll_interval` 时间段内，允许保留的审计日志文件的最大数目。 |
| audit_log_modules       | slow_query, query                       | 打印审计日志的模块。默认打印 slow_query 和 query 模块的日志。可以指定多个模块，模块名称之间用英文逗号加一个空格分隔。 |
| audit_log_roll_interval | DAY                                     | 审计日志滚动的时间间隔。取值范围：`DAY` 和 `HOUR`。<br>取值为 `DAY` 时，日志文件名的后缀为 `yyyyMMdd`。取值为 `HOUR` 时，日志文件名的后缀为 `yyyyMMddHH`。 |
| audit_log_delete_age    | 30d                                     | 审计日志文件的保留时长。默认值 `30d` 表示审计日志文件可以保留 30 天，保留时长超过 30 天的审计日志文件会被删除。 |
| dump_log_dir            | StarRocksFE.STARROCKS_HOME_DIR + "/log" | Dump 日志文件的保存目录。                                    |
| dump_log_modules        | query                                   | 打印 Dump 日志的模块。默认打印 query 模块的日志。可以指定多个模块，模块名称之间用英文逗号加一个空格分隔。 |
| dump_log_roll_interval  | DAY                                     | Dump 日志滚动的时间间隔。取值范围：`DAY` 和 `HOUR`。<br>取值为 `DAY` 时，日志文件名的后缀为 `yyyyMMdd`。取值为 `HOUR` 时，日志文件名的后缀为 `yyyyMMddHH`。 |
| dump_log_roll_num       | 10                                      | 每个 `dump_log_roll_interval` 时间内，允许保留的 Dump 日志文件的最大数目。 |
| dump_log_delete_age     | 7d                                      | Dump 日志文件的保留时长。默认值 `7d` 表示 Dump 日志文件可以保留 7 天，保留时长超过 7 天的 Dump 日志文件会被删除。 |

#### Server

| 配置项                               | 默认值            | 描述                                                         |
| ------------------------------------ | ----------------- | ------------------------------------------------------------ |
| frontend_address                     | 0.0.0.0           | FE 节点的 IP 地址。                                          |
| priority_networks                    | 空字符串          | 为那些有多个 IP 地址的服务器声明一个选择策略。 <br>请注意，最多应该有一个 IP 地址与此列表匹配。这是一个以分号分隔格式的列表，用 CIDR 表示法，例如 `10.10.10.0/24`。 如果没有匹配这条规则的ip，会随机选择一个。 |
| http_port                            | 8030              | FE 节点上 HTTP 服务器的端口。                                |
| http_backlog_num                     | 1024              | HTTP 服务器支持的 Backlog 队列长度。                         |
| cluster_name                         | StarRocks Cluster | FE 所在 StarRocks 集群的名称，显示为网页标题。               |
| rpc_port                             | 9020              | FE 节点上 Thrift 服务器的端口。                              |
| thrift_backlog_num                   | 1024              | Thrift 服务器支持的 Backlog 队列长度。                       |
| thrift_server_max_worker_threads     | 4096              | Thrift 服务器支持的最大工作线程数。                          |
| thrift_client_timeout_ms             | 5000              | Thrift 客户端链接的空闲超时时间，即链接超过该时间无新请求后则将链接断开。单位：ms。|
| thrift_server_queue_size             | 4096              | Thrift 服务器 pending 队列长度。如果当前处理线程数量超过了配置项 `thrift_server_max_worker_threads` 的值，则将超出的线程加入 pending 队列。|
| brpc_idle_wait_max_time              | 10000             | BRPC 的空闲等待时间。单位：ms。                              |
| query_port                           | 9030              | FE 节点上 MySQL 服务器的端口。                               |
| mysql_service_nio_enabled            | TRUE              | 是否开启 MySQL 服务器的异步 I/O 选项。                       |
| mysql_service_io_threads_num         | 4                 | MySQL 服务器中用于处理 I/O 事件的最大线程数。                |
| mysql_nio_backlog_num                | 1024              | MySQL 服务器支持的 Backlog 队列长度。                        |
| max_mysql_service_task_threads_num   | 4096              | MySQL 服务器中用于处理任务的最大线程数。                     |
| max_connection_scheduler_threads_num | 4096              | 连接调度器支持的最大线程数。                                 |
| qe_max_connection                    | 1024              | FE 支持的最大连接数，包括所有用户发起的连接。                |
| check_java_version                   | TRUE              | 检查已编译的 Java 版本与运行的 Java 版本是否兼容。<br>如果不兼容，则上报 Java 版本不匹配的异常信息，并终止启动。 |

#### 元数据与集群管理

| 配置项                            | 默认值                                   | 描述                                                         |
| --------------------------------- | ---------------------------------------- | ------------------------------------------------------------ |
| meta_dir                          | StarRocksFE.STARROCKS_HOME_DIR + "/meta" | 元数据的保存目录。                                           |
| heartbeat_mgr_threads_num         | 8                                        | Heartbeat Manager 中用于发送心跳任务的最大线程数。           |
| heartbeat_mgr_blocking_queue_size | 1024                                     | Heartbeat Manager 中存储心跳任务的阻塞队列大小。             |
| metadata_failure_recovery         | FALSE                                    | 是否强制重置 FE 的元数据。请谨慎使用该配置项。               |
| edit_log_port                     | 9010                                     | FE 所在 StarRocks 集群中各 Leader FE、Follower FE、Observer FE 之间通信用的端口。 |
| edit_log_type                     | BDB                                      | 编辑日志的类型。取值只能为 `BDB`。                           |
| bdbje_heartbeat_timeout_second    | 30                                       | FE 所在 StarRocks 集群中 Leader FE 和 Follower FE 之间的 BDB JE 心跳超时时间。单位：秒。 |
| bdbje_lock_timeout_second         | 1                                        | BDB JE 操作的锁超时时间。单位：秒。                          |
| max_bdbje_clock_delta_ms          | 5000                                     | FE 所在 StarRocks 集群中 Leader FE 与非 Leader FE 之间能够容忍的最大时钟偏移。单位：ms。 |
| txn_rollback_limit                | 100                                      | 允许回滚的最大事务数。                                       |
| bdbje_replica_ack_timeout_second  | 10                                       | FE 所在 StarRocks 集群中，元数据从 Leader FE 写入到多个 Follower FE 时，Leader FE 等待足够多的 Follower FE 发送 ACK 消息的超时时间。单位：秒。当写入的元数据较多时，可能返回 ACK 的时间较长，进而导致等待超时。如果超时，会导致写元数据失败，FE 进程退出，此时可以适当地调大该参数取值。 |
| master_sync_policy                | SYNC                                     | FE 所在 StarRocks 集群中，Leader FE 上的日志刷盘方式。该参数仅在当前 FE 为 Leader 时有效。取值范围： <ul><li>`SYNC`：事务提交时同步写日志并刷盘。</li><li> `NO_SYNC`：事务提交时不同步写日志。</li><li> `WRITE_NO_SYNC`：事务提交时同步写日志，但是不刷盘。 </li></ul>如果您只部署了一个 Follower FE，建议将其设置为 `SYNC`。 如果您部署了 3 个及以上 Follower FE，建议将其与下面的 `replica_sync_policy` 均设置为 `WRITE_NO_SYNC`。 |
| replica_sync_policy               | SYNC                                     | FE 所在 StarRocks 集群中，Follower FE 上的日志刷盘方式。取值范围： <ul><li>`SYNC`：事务提交时同步写日志并刷盘。</li><li> `NO_SYNC`：事务提交时不同步写日志。</li><li> `WRITE_NO_SYNC`：事务提交时同步写日志，但是不刷盘。</li></ul> |
| replica_ack_policy                | SIMPLE_MAJORITY                          | 判定日志是否有效的策略，默认是多数 Follower FE 返回确认消息，就认为生效。 |
| cluster_id                        | -1                                       | FE 所在 StarRocks 集群的 ID。具有相同集群 ID 的 FE 或 BE 属于同一个 StarRocks 集群。取值范围：正整数。默认值 `-1` 表示在 Leader FE 首次启动时随机生成一个。 |

#### Query engine

| 配置项                      | 默认值 | 描述                                       |
| --------------------------- | ------ | ------------------------------------------ |
| publish_version_interval_ms | 10     | 两个版本发布操作之间的时间间隔。单位：ms。 |
| statistic_cache_columns     | 100000 | 缓存统计信息表的最大行数。                 |

#### 导入和导出

| 配置项                            | 默认值                                                       | 描述                                                         |
| --------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| load_checker_interval_second      | 5                                                            | 导入作业的轮询间隔。单位：秒。                               |
| transaction_clean_interval_second | 30                                                           | 已结束事务的清理间隔。单位：秒。建议清理间隔尽量短，从而确保已完成的事务能够及时清理掉。 |
| label_clean_interval_second       | 14400                                                        | 作业标签的清理间隔。单位：秒。建议清理间隔尽量短，从而确保历史作业的标签能够及时清理掉。 |
| spark_dpp_version                 | 1.0.0                                                        | Spark DPP 特性的版本。                                       |
| spark_resource_path               | 空字符串                                                     | Spark 依赖包的根目录。                                       |
| spark_launcher_log_dir            | sys_log_dir + "/spark_launcher_log"                          | Spark 日志的保存目录。                                       |
| yarn_client_path                  | StarRocksFE.STARROCKS_HOME_DIR + "/lib/yarn-client/hadoop/bin/yarn" | Yarn 客户端的根目录。                                        |
| yarn_config_dir                   | StarRocksFE.STARROCKS_HOME_DIR + "/lib/yarn-config"          | Yarn 配置文件的保存目录。                                    |
| export_checker_interval_second    | 5                                                            | 导出作业调度器的调度间隔。                                   |
| export_task_pool_size             | 5                                                            | 导出任务线程池的大小。                                       |
| broker_client_timeout_ms          | 10000                                                        | Broker RPC 的默认超时时间，单位：毫秒，默认值 10s。                 |

#### 存储

| 配置项    | 默认值    | 描述         |
| -------- | -------- | -----------  |
| tablet_sched_balancer_strategy | disk_and_tablet | Tablet 均衡策略。参数别名为 `tablet_balancer_strategy`。取值范围：`disk_and_tablet` 和 `be_load_score`。 |
| tablet_sched_storage_cooldown_second | -1         | 从 Table 创建时间点开始计算，自动降冷的时延。降冷是指从 SSD 介质迁移到 HDD 介质。<br>参数别名为 `storage_cooldown_second`。单位：秒。默认值 `-1` 表示不进行自动降冷。如需启用自动降冷功能，请显式设置参数取值大于 0。 |
| tablet_stat_update_interval_second| 300         | FE 向每个 BE 请求收集 Tablet 统计信息的时间间隔。单位：秒。  |

#### StarRocks 存算分离集群

| 配置项                               | 默认值          | 描述                                                         |
| ----------------------------------- | -------------- | ------------------------------------------------------------ |
| run_mode                            | shared_nothing | StarRocks 集群的运行模式。有效值：`shared_data` 和 `shared_nothing` (默认)。`shared_data` 表示在存算分离模式下运行 StarRocks。`shared_nothing` 表示以普通模式运行 StarRocks。<br />**注意**<br />StarRocks 集群不支持存算分离和普通模式混合部署。<br />请勿在集群部署完成后更改 `run_mode`，否则将导致集群无法再次启动。不支持从普通集群转换为存算分离集群，反之亦然。 |
| cloud_native_meta_port              | 6090           | 云原生元数据服务监听端口。默认值：`6090`。                         |
| cloud_native_storage_type           | S3             | 您使用的存储类型。有效值：`S3`（默认）和 `HDFS`。如果您将此项指定为 `S3`，则必须添加以 `aws_s3` 为前缀的配置项。如果将此项指定为 `HDFS`，则只需指定 `cloud_native_hdfs_url`。 |
| cloud_native_hdfs_url               | N/A            | HDFS 存储的 URL，例如 `hdfs://127.0.0.1:9000/user/xxx/starrocks/`。 |
| aws_s3_path                         | N/A            | 用于存储数据的 S3 存储空间路径，由 S3 存储桶的名称及其下的子路径（如有）组成，如 `testbucket/subpath`。 |
| aws_s3_region                       | N/A            | 需访问的 S3 存储空间的地区，如 `us-west-2`。                      |
| aws_s3_endpoint                     | N/A            | 访问 S3 存储空间的连接地址，如 `https://s3.us-west-2.amazonaws.com`。 |
| aws_s3_use_aws_sdk_default_behavior | false          | 是否使用 AWS SDK 默认的认证凭证。有效值：`true` 和 `false` (默认)。 |
| aws_s3_use_instance_profile         | false          | 是否使用 Instance Profile 或 Assumed Role 作为安全凭证访问 S3。有效值：`true` 和 `false` (默认)。<ul><li>如果您使用 IAM 用户凭证（Access Key 和 Secret Key）访问 S3，则需要将此项设为 `false`，并指定 `aws_s3_access_key` 和 `aws_s3_secret_key`。</li><li>如果您使用 Instance Profile 访问 S3，则需要将此项设为 `true`。</li><li>如果您使用 Assumed Role 访问 S3，则需要将此项设为 `true`，并指定 `aws_s3_iam_role_arn`。</li><li>如果您使用外部 AWS 账户通过 Assumed Role 认证访问 S3，则需要额外指定 `aws_s3_external_id`。</li></ul> |
| aws_s3_access_key                   | N/A            | 访问 S3 存储空间的 Access Key。                                 |
| aws_s3_secret_key                   | N/A            | 访问 S3 存储空间的 Secret Key。                                 |
| aws_s3_iam_role_arn                 | N/A            | 有访问 S3 存储空间权限 IAM Role 的 ARN。                         |
| aws_s3_external_id                  | N/A            | 用于跨 AWS 账户访问 S3 存储空间的外部 ID。                        |

#### 其他静态参数

| 配置项                             | 默认值                                          | 描述                                                         |
| ---------------------------------- | ----------------------------------------------- | ------------------------------------------------------------ |
| plugin_dir                         | STARROCKS_HOME_DIR/plugins                      | 插件的安装目录。                                             |
| small_file_dir                     | StarRocksFE.STARROCKS_HOME_DIR + "/small_files" | 小文件的根目录。                                             |
| max_agent_task_threads_num         | 4096                                            | 代理任务线程池中用于处理代理任务的最大线程数。               |
| auth_token                         | 空字符串                                        | 用于内部身份验证的集群令牌。为空则在 Leader FE 第一次启动时随机生成一个。 |
| tmp_dir                            | StarRocksFE.STARROCKS_HOME_DIR + "/temp_dir"    | 临时文件的保存目录，例如备份和恢复过程中产生的临时文件。<br>这些过程完成以后，所产生的临时文件会被清除掉。 |
| locale                             | zh_CN.UTF-8                                     | FE 所使用的字符集。                                          |
| hive_meta_load_concurrency         | 4                                               | Hive 元数据支持的最大并发线程数。                            |
| hive_meta_cache_refresh_interval_s | 7200                                            | 刷新 Hive 外表元数据缓存的时间间隔。单位：秒。               |
| hive_meta_cache_ttl_s              | 86400                                           | Hive 外表元数据缓存的失效时间。单位：秒。                    |
| hive_meta_store_timeout_s          | 10                                              | 连接 Hive Metastore 的超时时间。单位：秒。                   |
| es_state_sync_interval_second      | 10                                              | FE 获取 Elasticsearch Index 和同步 StarRocks 外部表元数据的时间间隔。单位：秒。 |
| enable_auth_check                  | TRUE                                            | 是否开启鉴权检查功能。取值范围：`TRUE` 和 `FALSE`。`TRUE` 表示开该功能。`FALSE`表示关闭该功能。                                               |
| enable_metric_calculator           | TRUE                                            | 是否开启定期收集指标 (Metrics) 的功能。取值范围：`TRUE` 和 `FALSE`。`TRUE` 表示开该功能。`FALSE`表示关闭该功能。                      |

## BE 配置项

部分 BE 节点配置项为动态参数，您可以通过命令在线修改。其他配置项为静态参数，需要通过修改 **be.conf** 文件后重启 BE 服务使相关修改生效。

### 查看 BE 配置项

您可以通过以下命令查看 BE 配置项：

```shell
curl http://<BE_IP>:<BE_HTTP_PORT>/varz
```

### 配置 BE 动态参数

您可以通过 `curl` 命令在线修改 BE 节点动态参数。

```shell
curl -XPOST http://be_host:http_port/api/update_config?configuration_item=value
```

以下是 BE 动态参数列表。

| 配置项                                                | 默认值      | 单位   | 描述                                                         |
| ----------------------------------------------------- | ----------- | ------ | ------------------------------------------------------------ |
| report_task_interval_seconds                          | 10          | second | 汇报单个任务的间隔。建表，删除表，导入，schema change 都可以被认定是任务。 |
| report_disk_state_interval_seconds                    | 60          | second | 汇报磁盘状态的间隔。汇报各个磁盘的状态，以及其中数据量等。   |
| report_tablet_interval_seconds                        | 60          | second | 汇报 tablet 的间隔。汇报所有的 tablet 的最新版本。           |
| report_workgroup_interval_seconds                     | 5           | second | 汇报 workgroup 的间隔。汇报所有 workgroup 的最新版本。           |
| max_download_speed_kbps                               | 50000       | KB/s   | 单个 HTTP 请求的最大下载速率。这个值会影响 BE 之间同步数据副本的速度。 |
| download_low_speed_limit_kbps                         | 50          | KB/s   | 单个 HTTP 请求的下载速率下限。如果在 `download_low_speed_time` 秒内下载速度一直低于`download_low_speed_limit_kbps`，那么请求会被终止。 |
| download_low_speed_time                               | 300         | second | 见 `download_low_speed_limit_kbps`。                             |
| status_report_interval                                | 5           | second | 查询汇报 profile 的间隔，用于 FE 收集查询统计信息。          |
| scanner_thread_pool_thread_num                        | 48          | N/A    | 存储引擎并发扫描磁盘的线程数，统一管理在线程池中。           |
| thrift_client_retry_interval_ms                       | 100         | ms     | Thrift client 默认的重试时间间隔。                            |
| scanner_thread_pool_queue_size                        | 102400      | N/A    | 存储引擎支持的扫描任务数。                                   |
| scanner_row_num                                       | 16384       | N/A    | 每个扫描线程单次执行最多返回的数据行数。                     |
| max_scan_key_num                                      | 1024        | N/A    | 查询最多拆分的 scan key 数目。                               |
| max_pushdown_conditions_per_column                    | 1024        | N/A    | 单列上允许下推的最大谓词数量，如果超出数量限制，谓词不会下推到存储层。 |
| exchg_node_buffer_size_bytes                          | 10485760    | Byte   | Exchange 算子中，单个查询在接收端的 buffer 容量。<br>这是一个软限制，如果数据的发送速度过快，接收端会触发反压来限制发送速度。 |
| column_dictionary_key_ratio_threshold                 | 0           | %      | 字符串类型的取值比例，小于这个比例采用字典压缩算法。         |
| memory_limitation_per_thread_for_schema_change        | 2           | GB     | 单个 schema change 任务允许占用的最大内存。                  |
| update_cache_expire_sec                               | 360         | second | Update Cache 的过期时间。                                     |
| file_descriptor_cache_clean_interval                  | 3600        | second | 文件句柄缓存清理的间隔，用于清理长期不用的文件句柄。         |
| disk_stat_monitor_interval                            | 5           | second | 磁盘健康状态检测的间隔。                                     |
| unused_rowset_monitor_interval                        | 30          | second | 清理过期 Rowset 的时间间隔。                                 |
| max_percentage_of_error_disk                          | 0           | %      | 磁盘错误达到一定比例，BE 退出。                              |
| default_num_rows_per_column_file_block                | 1024        | N/A    | 每个 row block 最多存放的行数。                                |
| pending_data_expire_time_sec                          | 1800        | second | 存储引擎保留的未生效数据的最大时长。                         |
| inc_rowset_expired_sec                                | 1800        | second | 导入生效的数据，存储引擎保留的时间，用于增量克隆。           |
| tablet_rowset_stale_sweep_time_sec                    | 1800        | second | 失效 rowset 的清理间隔。                                       |
| snapshot_expire_time_sec                              | 172800      | second | 快照文件清理的间隔，默认 48 个小时。                         |
| trash_file_expire_time_sec                            | 259200      | second | 回收站清理的间隔，默认 72 个小时。                           |
| base_compaction_check_interval_seconds                | 60          | second | BaseCompaction 线程轮询的间隔。                              |
| min_base_compaction_num_singleton_deltas              | 5           | N/A    | 触发 BaseCompaction 的最小 segment 数。                            |
| max_base_compaction_num_singleton_deltas              | 100         | N/A    | 单次 BaseCompaction 合并的最大 segment 数。                        |
| base_compaction_interval_seconds_since_last_operation | 86400       | second | 上一轮 BaseCompaction 距今的间隔，是触发 BaseCompaction 条件之一。 |
| cumulative_compaction_check_interval_seconds          | 1           | second | CumulativeCompaction 线程轮询的间隔。                        |
| update_compaction_check_interval_seconds              | 60          | second | Primary key 模型 Update compaction 的检查间隔。              |
| min_compaction_failure_interval_sec                   | 120         | second | Tablet Compaction 失败之后，再次被调度的间隔。               |
| periodic_counter_update_period_ms                     | 500         | ms     | Counter 统计信息的间隔。                                     |
| load_error_log_reserve_hours                          | 48          | hour   | 导入数据信息保留的时长。                                     |
| streaming_load_max_mb                                 | 10240       | MB     | 流式导入单个文件大小的上限。                                 |
| streaming_load_max_batch_size_mb                      | 100         | MB     | 流式导入单个 JSON 文件大小的上限。                             |
| memory_maintenance_sleep_time_s                       | 10          | second | 触发 ColumnPool GC 任务的时间间隔。StarRocks 会周期运行 GC 任务，尝试将空闲内存返还给操作系统。 |
| write_buffer_size                                     | 104857600   | Byte   | MemTable 在内存中的 buffer 大小，超过这个限制会触发 flush。      |
| tablet_stat_cache_update_interval_second              | 300         | second | Tablet Stat Cache 的更新间隔。                                |
| result_buffer_cancelled_interval_time                 | 300         | second | BufferControlBlock 释放数据的等待时间。                         |
| thrift_rpc_timeout_ms                                 | 5000        | ms     | Thrift 超时的时长，单位为 ms。                               |
| txn_commit_rpc_timeout_ms                             | 20000       | ms     | Txn 超时的时长，单位为 ms。                                  |
| max_consumer_num_per_group                            | 3           | N/A    | Routine load 中，每个consumer group 内最大的 consumer 数量。     |
| max_memory_sink_batch_count                           | 20          | N/A    | Scan cache 的最大缓存批次数量。                                 |
| scan_context_gc_interval_min                          | 5           | Minute | Scan context 的清理间隔。                                     |
| path_gc_check_step                                    | 1000        | N/A    | 单次连续 scan 最大的文件数量。                                   |
| path_gc_check_step_interval_ms                        | 10          | ms     | 多次连续 scan 文件间隔时间。                                     |
| path_scan_interval_second                             | 86400       | second | gc 线程清理过期数据的间隔时间。                                 |
| storage_flood_stage_usage_percent                     | 95          | %      | 如果空间使用率超过该值且剩余空间小于 `storage_flood_stage_left_capacity_bytes`，会拒绝 Load 和 Restore 作业。        |
| storage_flood_stage_left_capacity_bytes               | 107374182400| Byte   | 如果剩余空间小于该值且空间使用率超过 `storage_flood_stage_usage_percent`，会拒绝 Load 和 Restore 作业，默认 100GB。 |
| tablet_meta_checkpoint_min_new_rowsets_num            | 10          | N/A    | 自上次 TabletMeta Checkpoint 至今新创建的 rowset 数量。        |
| tablet_meta_checkpoint_min_interval_secs              | 600         | second | TabletMeta Checkpoint 线程轮询的时间间隔。         |
| max_runnings_transactions_per_txn_map                 | 100         | N/A    | 每个分区内部同时运行的最大事务数量。            |
| tablet_max_pending_versions                           | 1000        | N/A    | PrimaryKey 表允许 committed 未 apply 的最大版本数。                 |
| max_hdfs_file_handle                                  | 1000        | N/A    | 最多可以打开的 HDFS 文件句柄数量。                             |
| be_exit_after_disk_write_hang_second                  | 60          | second | 磁盘挂起后触发 BE 进程退出的等待时间。                       |
| min_cumulative_compaction_failure_interval_sec       | 30          | second | Cumulative Compaction 失败后的最小重试间隔。                      |
| size_tiered_level_num                                 | 7           | N/A    | Size-tiered Compaction 策略的 level 数量。每个 level 最多保留一个 rowset，因此稳定状态下最多会有和 level 数相同的 rowset。 |
| size_tiered_level_multiple                            | 5           | N/A    | Size-tiered Compaction 策略中，相邻两个 level 之间相差的数据量的倍数。 |
| size_tiered_min_level_size                            | 131072      | Byte   | Size-tiered Compaction 策略中，最小 level 的大小，小于此数值的 rowset 会直接触发 compaction。 |
| storage_page_cache_limit | 20% | N/A | PageCache 的容量，STRING，可写为容量大小，例如： `20G`、`20480M`、`20971520K` 或 `21474836480B`。也可以写为 PageCache 占系统内存的比例，例如，`20%`。该参数仅在 `disable_storage_page_cache` 为 `false` 时生效。|
|max_compaction_concurrency|-1| N/A | Compaction 线程数上限（即 BaseCompaction + CumulativeCompaction 的最大并发）。该参数防止 Compaction 占用过多内存。 -1 代表没有限制。0 表示不允许 compaction。|
| internal_service_async_thread_num | 10 | N/A | 单个 BE 上与 Kafka 交互的线程池大小。当前 Routine Load FE 与 Kafka 的交互需经由 BE 完成，而每个 BE 上实际执行操作的是一个单独的线程池。当 Routine Load 任务较多时，可能会出现线程池线程繁忙的情况，可以调整该配置。|

### 配置 BE 静态参数

以下 BE 配置项为静态参数，不支持在线修改，您需要在 **be.conf** 中修改并重启 BE 服务。

|配置项|默认值|描述|
|---|---|---|
|be_port|9060|BE 上 thrift server 的端口，用于接收来自 FE 的请求。|
|brpc_port|8060|BRPC 的端口，可以查看 BRPC 的一些网络统计信息。|
|brpc_num_threads|-1|BRPC 的 bthreads 线程数量，-1 表示和 CPU 核数一样。|
|priority_networks|空字符串|以 CIDR 形式 10.10.10.0/24 指定 BE IP 地址，适用于机器有多个 IP，需要指定优先使用的网络。|
|starlet_port|9070|StarRocks 存算分离集群用于 BE 心跳服务的端口。 |
|heartbeat_service_port|9050|心跳服务端口（thrift），用户接收来自 FE 的心跳。|
|heartbeat_service_thread_count|1|心跳线程数。|
|create_tablet_worker_count|3|创建 tablet 的线程数。|
|drop_tablet_worker_count|3|删除 tablet 的线程数。|
|push_worker_count_normal_priority|3|导入线程数，处理 NORMAL 优先级任务。|
|push_worker_count_high_priority|3|导入线程数，处理 HIGH 优先级任务。|
|transaction_publish_version_worker_count|0|生效版本的最大线程数。当该参数被设置为小于或等于 `0` 时，系统默认使用 CPU 核数的一半，以避免因使用固定值而导致在导入并行较高时线程资源不足。自 2.5 版本起，默认值由 `8` 变更为 `0`。|
|clear_transaction_task_worker_count|1|清理事务的线程数。|
|alter_tablet_worker_count|3|进行 schema change 的线程数。|
|clone_worker_count|3|克隆的线程数。|
|storage_medium_migrate_count|1|介质迁移的线程数，SATA 迁移到 SSD。|
|check_consistency_worker_count|1|计算 tablet 的校验和 (checksum)。|
|sys_log_dir|${STARROCKS_HOME}/log|存放日志的地方，包括 INFO，WARNING，ERROR，FATAL 等日志。|
|user_function_dir|${STARROCKS_HOME}/lib/udf|UDF 程序存放的路径。|
|small_file_dir|${STARROCKS_HOME}/lib/small_file|保存文件管理器下载的文件的目录。|
|sys_log_level|INFO|日志级别，INFO < WARNING < ERROR < FATAL。|
|sys_log_roll_mode|SIZE-MB-1024|日志拆分的大小，每 1G 拆分一个日志。|
|sys_log_roll_num|10|日志保留的数目。|
|sys_log_verbose_modules|空字符串|日志打印的模块。有效值为 BE 的 namespace，包括 `starrocks`、`starrocks::debug`、`starrocks::fs`、`starrocks::io`、`starrocks::lake`、`starrocks::pipeline`、`starrocks::query_cache`、`starrocks::stream` 以及 `starrocks::workgroup`。|
|sys_log_verbose_level|10|日志显示的级别，用于控制代码中 VLOG 开头的日志输出。|
|log_buffer_level|空字符串|日志刷盘的策略，默认保持在内存中。|
|num_threads_per_core|3|每个 CPU core 启动的线程数。|
|compress_rowbatches|TRUE|BE 之间 RPC 通信是否压缩 RowBatch，用于查询层之间的数据传输。|
|serialize_batch|FALSE|BE 之间 RPC 通信是否序列化 RowBatch，用于查询层之间的数据传输。|
|file_descriptor_cache_clean_interval|3600|文件句柄缓存清理的间隔，用于清理长期不用的文件句柄|
|storage_root_path|${STARROCKS_HOME}/storage|存储数据的目录以及存储介质类型，多块盘配置使用分号 `;` 隔开。<br>如果为 SSD 磁盘，需在路径后添加 `,medium:ssd`，如果为 HDD 磁盘，需在路径后添加 `,medium:hdd`。例如：`/data1,medium:hdd;/data2,medium:ssd`。|
|max_length_for_bitmap_function|1000000|bitmap 函数输入值的最大长度。单位：字节。|
|max_length_for_to_base64|200000|to_base64() 函数输入值的最大长度。单位：字节。|
|max_percentage_of_error_disk|0|磁盘错误达到一定比例，BE 退出。|
|max_tablet_num_per_shard|1024|每个 shard 的 tablet 数目，用于划分 tablet，防止单个目录下 tablet 子目录过多。|
|max_garbage_sweep_interval|3600|磁盘进行垃圾清理的最大间隔。|
|min_garbage_sweep_interval|180|磁盘进行垃圾清理的最小间隔。|
|file_descriptor_cache_capacity|16384|文件句柄缓存的容量。|
|min_file_descriptor_number|60000|BE 进程的文件句柄 limit 要求的下线。|
|index_stream_cache_capacity|10737418240|BloomFilter/Min/Max 等统计信息缓存的容量。|
|disable_storage_page_cache|FALSE|是否开启 PageCache。开启 PageCache 后，查询结果会缓存在 Cache 中，对于查询重复性高的场景，会大幅提升查询效率。`true` 表示不开启。自 2.4 版本起，该参数默认值由 `true` 变更为 `false`。|
|base_compaction_num_threads_per_disk|1|每个磁盘 BaseCompaction 线程的数目。|
|base_cumulative_delta_ratio|0.3|BaseCompaction 触发条件之一：Cumulative 文件大小达到 Base 文件的比例。|
|max_compaction_concurrency|-1|BaseCompaction + CumulativeCompaction 的最大并发， -1 代表没有限制。|
|compaction_trace_threshold|60|单次 Compaction 打印 trace 的时间阈值，如果单次 compaction 时间超过该阈值就打印 trace，单位为秒。|
|be_http_port|8040|HTTP Server 端口。|
|webserver_num_workers|48|HTTP Server 线程数。|
|load_data_reserve_hours|4|小批量导入生成的文件保留的时。|
|number_tablet_writer_threads|16|流式导入的线程数。|
|streaming_load_rpc_max_alive_time_sec|1200|流式导入 RPC 的超时时间。|
|fragment_pool_thread_num_min|64|最小查询线程数，默认启动 64 个线程。|
|fragment_pool_thread_num_max|4096|最大查询线程数。|
|fragment_pool_queue_size|2048|单节点上能够处理的查询请求上限。|
|enable_token_check|TRUE|Token 开启检验。|
|enable_prefetch|TRUE|查询提前预取。|
|load_process_max_memory_limit_bytes|107374182400|单节点上所有的导入线程占据的内存上限，100GB。|
|load_process_max_memory_limit_percent|30|单节点上所有的导入线程占据的内存上限比例。|
|sync_tablet_meta|FALSE|存储引擎是否开 sync 保留到磁盘上。|
|routine_load_thread_pool_size|10|Routine Load 的线程池数目。|
|brpc_max_body_size|2147483648|BRPC 最大的包容量，单位为 Byte。|
|tablet_map_shard_size|32|Tablet 分组数。|
|enable_bitmap_union_disk_format_with_set|FALSE|Bitmap 新存储格式，可以优化 bitmap_union 性能。|
|mem_limit|90%|BE 进程内存上限。可设为比例上限（如 "80%"）或物理上限（如 "100GB"）。|
|flush_thread_num_per_store|2|每个 Store 用以 Flush MemTable 的线程数。|
| block_cache_enable     | false | 是否启用 Data Cache。<ul><li>`true`：启用。</li><li>`false`：不启用，为默认值。</li></ul> 如要启用，设置该参数值为 `true`。|
| block_cache_disk_path  | N/A | 磁盘路径。支持添加多个路径，多个路径之间使用分号(;) 隔开。建议 BE 机器有几个磁盘即添加几个路径。配置路径后，StarRocks 会自动创建名为 **cachelib_data** 的文件用于缓存 block。 |
| block_cache_meta_path  | N/A | Block 的元数据存储目录，可自定义。推荐创建在 **$STARROCKS_HOME** 路径下。 |
| block_cache_block_size | 1048576 | 单个 block 大小，单位：字节。默认值为 `1048576`，即 1 MB。   |
| block_cache_mem_size   | 2147483648 | 内存缓存数据量的上限，单位：字节。默认值为 `2147483648`，即 2 GB。推荐将该参数值最低设置成 20 GB。如在开启 Data Cache 期间，存在大量从磁盘读取数据的情况，可考虑调大该参数。 |
| block_cache_disk_size  | 0 | 单个磁盘缓存数据量的上限，单位：字节。举例：在 `block_cache_disk_path` 中配置了 2 个磁盘，并设置 `block_cache_disk_size` 参数值为 `21474836480`，即 20 GB，那么最多可缓存 40 GB 的磁盘数据。默认值为 `0`，即仅使用内存作为缓存介质，不使用磁盘。 |
| jdbc_connection_pool_size  | 8 | JDBC 连接池大小。每个 BE 节点上访问 `jdbc_url` 相同的外表时会共用同一个连接池。 |
| jdbc_minimum_idle_connections  | 1 | JDBC 连接池中最少的空闲连接数量。 |
| jdbc_connection_idle_timeout_ms  | 600000 | JDBC 空闲连接超时时间。如果 JDBC 连接池内的连接空闲时间超过此值，连接池会关闭超过 `jdbc_minimum_idle_connections` 配置项中指定数量的空闲连接。 |
| query_cache_capacity  | 536870912 | 指定 Query Cache 的大小。单位：Byte。默认为 512 MB。最小不低于 4 MB。如果当前的 BE 内存容量无法满足您期望的 Query Cache 大小，可以增加 BE 的内存容量，然后再设置合理的 Query Cache 大小。<br>每个 BE 都有自己私有的 Query Cache 存储空间，BE 只 Populate 或 Probe 自己本地的 Query Cache 存储空间。 |
| enable_event_based_compaction_framework  | TRUE | 是否开启 Event-based Compaction Framework。`true` 代表开启。`false` 代表关闭。开启则能够在 tablet 数比较多或者单个 tablet 数据量比较大的场景下大幅降低 compaction 的开销。 |
| enable_size_tiered_compaction_strategy  | TRUE | 是否开启 Size-tiered Compaction 策略。`true` 代表开启。`false` 代表关闭。 |
