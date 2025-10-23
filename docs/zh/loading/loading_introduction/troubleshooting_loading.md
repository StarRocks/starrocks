---
displayed_sidebar: docs
sidebar_label: Troubleshooting
---

# 数据导入故障排查

本指南旨在帮助数据库管理员和运维工程师通过 SQL 接口监控数据导入作业的状态，而无需依赖外部监控系统。同时，还提供了识别性能瓶颈和排查导入操作异常的指导。

## 术语

**导入作业：** 一个持续的数据导入过程，例如 **Routine Load 作业** 或 **Pipe 作业**。

**导入任务：** 一次性的数据导入过程，通常对应于单个导入事务。示例包括 **Broker Load**、**Stream Load**、**Spark Load** 和 **INSERT INTO**。Routine Load 作业和 Pipe 作业会持续生成任务以执行数据摄取。

## 观察导入作业

观察导入作业有两种方式：

- 使用 SQL 语句 **[SHOW ROUTINE LOAD](../../sql-reference/sql-statements/loading_unloading/routine_load/SHOW_ROUTINE_LOAD.md)** 和 **[SHOW PIPES](../../sql-reference/sql-statements/loading_unloading/pipe/SHOW_PIPES.md)**。
- 使用系统视图 **[information_schema.routine_load_jobs](../../sql-reference/information_schema/routine_load_jobs.md)** 和 **[information_schema.pipes](../../sql-reference/information_schema/pipes.md)**。

## 观察导入任务

导入任务也可以通过两种方式进行监控：

- 使用 SQL 语句 **[SHOW LOAD](../../sql-reference/sql-statements/loading_unloading/SHOW_LOAD.md)** 和 **[SHOW ROUTINE LOAD TASK](../../sql-reference/sql-statements/loading_unloading/routine_load/SHOW_ROUTINE_LOAD_TASK.md)**。
- 使用系统视图 **[information_schema.loads](../../sql-reference/information_schema/loads.md)** 和 **statistics.loads_history**。

### SQL 语句

**SHOW** 语句显示当前数据库中正在进行和最近完成的导入任务，提供任务状态的快速概览。检索到的信息是 **statistics.loads_history** 系统视图的一个子集。

SHOW LOAD 语句返回 Broker Load、Insert Into 和 Spark Load 任务的信息，而 SHOW ROUTINE LOAD TASK 语句返回 Routine Load 任务的信息。

### 系统视图

#### information_schema.loads

**information_schema.loads** 系统视图存储有关最近导入任务的信息，包括活动的和最近完成的任务。StarRocks 定期将数据同步到 **statistics.loads_history** 系统表以进行持久存储。

**information_schema.loads** 提供以下字段：

| 字段                | 描述                                                      |
| -------------------- | ------------------------------------------------------------ |
| ID                   | 全局唯一标识符。                                  |
| LABEL                | 导入作业的标签。                                       |
| PROFILE_ID           | Profile 的 ID，可以通过 `ANALYZE PROFILE` 进行分析。 |
| DB_NAME              | 目标表所属的数据库。              |
| TABLE_NAME           | 目标表。                                            |
| USER                 | 启动导入作业的用户。                         |
| WAREHOUSE            | 导入作业所属的仓库。                 |
| STATE                | 导入作业的状态。有效值：<ul><li>`PENDING`/`BEGIN`：导入作业已创建。</li><li>`QUEUEING`/`BEFORE_LOAD`：导入作业在队列中等待调度。</li><li>`LOADING`：导入作业正在运行。</li><li>`PREPARING`：事务正在预提交。</li><li>`PREPARED`：事务已预提交。</li><li>`COMMITED`：事务已提交。</li><li>`FINISHED`：导入作业成功。</li><li>`CANCELLED`：导入作业失败。</li></ul> |
| PROGRESS             | 导入作业的 ETL 阶段和 LOADING 阶段的进度。 |
| TYPE                 | 导入作业的类型。对于 Broker Load，返回值为 `BROKER`。对于 INSERT，返回值为 `INSERT`。对于 Stream Load，返回值为 `STREAM`。对于 Routine Load，返回值为 `ROUTINE`。 |
| PRIORITY             | 导入作业的优先级。有效值：`HIGHEST`、`HIGH`、`NORMAL`、`LOW` 和 `LOWEST`。 |
| SCAN_ROWS            | 扫描的数据行数。                    |
| SCAN_BYTES           | 扫描的字节数。                        |
| FILTERED_ROWS        | 由于数据质量不佳而被过滤掉的数据行数。 |
| UNSELECTED_ROWS      | 由于 WHERE 子句中指定的条件而被过滤掉的数据行数。 |
| SINK_ROWS            | 加载的数据行数。                     |
| RUNTIME_DETAILS      | 加载运行时元数据。详情请参见 [RUNTIME_DETAILS](#runtime_details)。 |
| CREATE_TIME          | 导入作业创建的时间。格式：`yyyy-MM-dd HH:mm:ss`。示例：`2023-07-24 14:58:58`。 |
| LOAD_START_TIME      | 导入作业 LOADING 阶段的开始时间。格式：`yyyy-MM-dd HH:mm:ss`。示例：`2023-07-24 14:58:58`。 |
| LOAD_COMMIT_TIME     | 加载事务提交的时间。格式：`yyyy-MM-dd HH:mm:ss`。示例：`2023-07-24 14:58:58`。 |
| LOAD_FINISH_TIME     | 导入作业 LOADING 阶段的结束时间。格式：`yyyy-MM-dd HH:mm:ss`。示例：`2023-07-24 14:58:58`。 |
| PROPERTIES           | 导入作业的静态属性。详情请参见 [PROPERTIES](#properties)。 |
| ERROR_MSG            | 导入作业的错误信息。如果导入作业未遇到任何错误，则返回 `NULL`。 |
| TRACKING_SQL         | 可用于查询导入作业跟踪日志的 SQL 语句。仅当导入作业涉及不合格数据行时，才返回 SQL 语句。如果导入作业不涉及任何不合格数据行，则返回 `NULL`。 |
| REJECTED_RECORD_PATH | 可以从中访问导入作业中过滤掉的所有不合格数据行的路径。记录的不合格数据行数由导入作业中配置的 `log_rejected_record_num` 参数决定。可以使用 `wget` 命令访问该路径。如果导入作业不涉及任何不合格数据行，则返回 `NULL`。 |

##### RUNTIME_DETAILS

- 通用指标：

| 指标               | 描述                                                  |
| -------------------- | ------------------------------------------------------------ |
| load_id              | 加载执行计划的全局唯一 ID。               |
| txn_id               | 加载事务 ID。                                         |

- Broker Load、INSERT INTO 和 Spark Load 的特定指标：

| 指标               | 描述                                                  |
| -------------------- | ------------------------------------------------------------ |
| etl_info             | ETL 详情。此字段仅对 Spark Load 作业有效。对于其他类型的导入作业，值将为空。 |
| etl_start_time       | 导入作业 ETL 阶段的开始时间。格式：`yyyy-MM-dd HH:mm:ss`。示例：`2023-07-24 14:58:58`。 |
| etl_start_time       | 导入作业 ETL 阶段的结束时间。格式：`yyyy-MM-dd HH:mm:ss`。示例：`2023-07-24 14:58:58`。 |
| unfinished_backends  | 未完成执行的 BE 列表。                      |
| backends             | 参与执行的 BE 列表。                      |
| file_num             | 读取的文件数量。                                        |
| file_size            | 读取的文件总大小。                                    |
| task_num             | 子任务的数量。                                          |

- Routine Load 的特定指标：

| 指标               | 描述                                                  |
| -------------------- | ------------------------------------------------------------ |
| schedule_interval    | Routine Load 的调度间隔。               |
| wait_slot_time       | Routine Load 任务等待执行槽的时间。 |
| check_offset_time    | Routine Load 任务调度期间检查偏移信息所消耗的时间。 |
| consume_time         | Routine Load 任务读取上游数据所消耗的时间。 |
| plan_time            | 生成执行计划的时间。                      |
| commit_publish_time  | 执行 COMMIT RPC 所消耗的时间。                     |

- Stream Load 的特定指标：

| 指标                 | 描述                                                |
| ---------------------- | ---------------------------------------------------------- |
| timeout                | 导入任务的超时时间。                                    |
| begin_txn_ms           | 开始事务所消耗的时间。                    |
| plan_time_ms           | 生成执行计划的时间。                    |
| receive_data_time_ms   | 接收数据的时间。                                   |
| commit_publish_time_ms | 执行 COMMIT RPC 所消耗的时间。                   |
| client_ip              | 客户端 IP 地址。                                         |

##### PROPERTIES

- Broker Load、INSERT INTO 和 Spark Load 的特定属性：

| 属性               | 描述                                                |
| ---------------------- | ---------------------------------------------------------- |
| timeout                | 导入任务的超时时间。                                    |
| max_filter_ratio       | 由于数据质量不佳而被过滤掉的数据行的最大比例。 |

- Routine Load 的特定属性：

| 属性               | 描述                                                |
| ---------------------- | ---------------------------------------------------------- |
| job_name               | Routine Load 作业名称。                                     |
| task_num               | 实际并行执行的子任务数量。          |
| timeout                | 导入任务的超时时间。                                    |

#### statistics.loads_history

**statistics.loads_history** 系统视图默认存储最近三个月的导入记录。数据库管理员可以通过修改视图的 `partition_ttl` 来调整保留期。**statistics.loads_history** 的模式与 **information_schema.loads** 一致。

## 使用 Load Profiles 识别导入性能问题

**Load Profile** 记录了参与数据导入的所有工作节点的执行细节。它帮助您快速定位 StarRocks 集群中的性能瓶颈。

### 启用 Load Profiles

StarRocks 提供多种方法来启用 Load Profiles，具体取决于导入类型：

#### 对于 Broker Load 和 INSERT INTO

在会话级别启用 Broker Load 和 INSERT INTO 的 Load Profiles：

```sql
SET enable_profile = true;
```

默认情况下，自动为运行时间超过 300 秒的作业启用配置文件。您可以通过以下方式自定义此阈值：

```sql
SET big_query_profile_threshold = 60s;
```

:::note
当 `big_query_profile_threshold` 设置为默认值 `0` 时，默认行为是禁用查询分析。然而，对于导入任务，执行时间超过 300 秒的任务会自动记录配置文件。
:::

StarRocks 还支持 **Runtime Profiles**，它会定期（每 30 秒）报告长时间运行的导入作业的执行指标。您可以通过以下方式自定义报告间隔：

```sql
SET runtime_profile_report_interval = 60;
```

:::note
`runtime_profile_report_interval` 仅指定导入任务的最小报告间隔。实际报告间隔是动态调整的，可能会超过此值。
:::

#### 对于 Stream Load 和 Routine Load

在表级别启用 Stream Load 和 Routine Load 的 Load Profiles：

```sql
ALTER TABLE <table_name> SET ("enable_load_profile" = "true");
```

Stream Load 通常具有高 QPS，因此 StarRocks 允许对 Load Profile 收集进行采样，以避免因过度分析而导致性能下降。您可以通过配置 FE 参数 `load_profile_collect_interval_second` 来调整收集间隔。此设置仅适用于通过表属性启用的 Load Profiles。默认值为 `0`。

```SQL
ADMIN SET FRONTEND CONFIG ("load_profile_collect_interval_second"="30");
```

StarRocks 还允许仅从超过特定时间阈值的导入作业中收集配置文件。您可以通过配置 FE 参数 `stream_load_profile_collect_threshold_second` 来调整此阈值。默认值为 `0`。

```SQL
ADMIN SET FRONTEND CONFIG ("stream_load_profile_collect_threshold_second"="10");
```

### 分析 Load Profiles

Load Profiles 的结构与 Query Profiles 相同。有关详细说明，请参见 [Query Tuning Recipes](../../best_practices/query_tuning/query_profile_tuning_recipes.md)。

您可以通过执行 [ANALYZE PROFILE](../../sql-reference/sql-statements/cluster-management/plan_profile/ANALYZE_PROFILE.md) 来分析 Load Profiles。有关详细说明，请参见 [Analyze text-based Profiles](../../best_practices/query_tuning/query_profile_text_based_analysis.md)。

配置文件提供了详细的操作符指标。关键组件包括 `OlapTableSink` 操作符和 `LoadChannel` 操作符。

#### OlapTableSink 操作符

| 指标            | 描述                                                  |
| ----------------- | ------------------------------------------------------------ |
| IndexNum          | 目标表的同步物化视图数量。 |
| ReplicatedStorage | 是否启用了单领导者复制。                |
| TxnID             | 加载事务 ID。                                         |
| RowsRead          | 从上游操作符读取的数据行数。         |
| RowsFiltered      | 由于数据质量不佳而被过滤掉的数据行数。 |
| RowsReturned      | 加载的数据行数。                     |
| RpcClientSideTime | 客户端统计的数据写入 RPC 所消耗的总时间。 |
| RpcServerSideTime | 服务器端统计的数据写入 RPC 所消耗的总时间。 |
| PrepareDataTime   | 数据格式转换和数据质量检查所消耗的时间。 |
| SendDataTime      | 发送数据所消耗的本地时间，包括数据序列化、压缩和写入发送队列。 |

:::tip
- `OLAP_TABLE_SINK` 中 `PushChunkNum` 的最大值和最小值之间的显著差异表明上游操作符中的数据倾斜，这可能导致写入性能瓶颈。
- `RpcClientSideTime` 等于 `RpcServerSideTime`、网络传输时间和 RPC 框架处理时间的总和。如果 `RpcClientSideTime` 和 `RpcServerSideTime` 之间的差异显著，考虑启用数据压缩以减少传输时间。
- 如果 `RpcServerSideTime` 占用的时间比例较大，可以进一步使用 `LoadChannel` 配置文件进行分析。
:::

#### LoadChannel 操作符

| 指标              | 描述                                                  |
| ------------------- | ------------------------------------------------------------ |
| Address             | BE 节点的 IP 地址或 FQDN。                           |
| LoadMemoryLimit     | 加载的内存限制。                                    |
| PeakMemoryUsage     | 加载的峰值内存使用量。                               |
| OpenCount           | 通道打开的次数，反映了 sink 的总并发性。 |
| OpenTime            | 打开通道所消耗的总时间。                        |
| AddChunkCount       | 加载块的数量，即 `TabletsChannel::add_chunk` 的调用次数。 |
| AddRowNum           | 加载的数据行数。                     |
| AddChunkTime        | 加载块所消耗的总时间，即 `TabletsChannel::add_chunk` 的总执行时间。    |
| WaitFlushTime       | `TabletsChannel::add_chunk` 等待 MemTable 刷新的总时间。 |
| WaitWriterTime      | `TabletsChannel::add_chunk` 等待异步 Delta Writer 执行的总时间。 |
| WaitReplicaTime     | `TabletsChannel::add_chunk` 等待副本同步的总时间。 |
| PrimaryTabletsNum   | 主 tablet 的数量。                                   |
| SecondaryTabletsNum | 次要 tablet 的数量。                                 |

:::tip
如果 `WaitFlushTime` 花费的时间较长，可能表明刷新线程的资源不足。考虑调整 BE 配置 `flush_thread_num_per_store`。
:::

## 最佳实践

### 诊断 Broker Load 性能瓶颈

1. 使用 Broker Load 加载数据：

    ```SQL
    LOAD LABEL click_bench.hits_1713874468 
    (
        DATA INFILE ("s3://test-data/benchmark_data/query_data/click_bench/hits.tbl*") 
        INTO TABLE hits COLUMNS TERMINATED BY "\t" (WatchID,JavaEnable,Title,GoodEvent,EventTime,EventDate,CounterID,ClientIP,RegionID,UserID,CounterClass,OS,UserAgent,URL,Referer,IsRefresh,RefererCategoryID,RefererRegionID,URLCategoryID,URLRegionID,ResolutionWidth,ResolutionHeight,ResolutionDepth,FlashMajor,FlashMinor,FlashMinor2,NetMajor,NetMinor,UserAgentMajor,UserAgentMinor,CookieEnable,JavascriptEnable,IsMobile,MobilePhone,MobilePhoneModel,Params,IPNetworkID,TraficSourceID,SearchEngineID,SearchPhrase,AdvEngineID,IsArtifical,WindowClientWidth,WindowClientHeight,ClientTimeZone,ClientEventTime,SilverlightVersion1,SilverlightVersion2,SilverlightVersion3,SilverlightVersion4,PageCharset,CodeVersion,IsLink,IsDownload,IsNotBounce,FUniqID,OriginalURL,HID,IsOldCounter,IsEvent,IsParameter,DontCountHits,WithHash,HitColor,LocalEventTime,Age,Sex,Income,Interests,Robotness,RemoteIP,WindowName,OpenerName,HistoryLength,BrowserLanguage,BrowserCountry,SocialNetwork,SocialAction,HTTPError,SendTiming,DNSTiming,ConnectTiming,ResponseStartTiming,ResponseEndTiming,FetchTiming,SocialSourceNetworkID,SocialSourcePage,ParamPrice,ParamOrderID,ParamCurrency,ParamCurrencyID,OpenstatServiceName,OpenstatCampaignID,OpenstatAdID,OpenstatSourceID,UTMSource,UTMMedium,UTMCampaign,UTMContent,UTMTerm,FromTag,HasGCLID,RefererHash,URLHash,CLID)
    ) 
    WITH BROKER 
    (
                "aws.s3.access_key" = "<iam_user_access_key>",
                "aws.s3.secret_key" = "<iam_user_secret_key>",
                "aws.s3.region" = "<aws_s3_region>"
    )
    ```

2. 使用 **SHOW PROFILELIST** 检索运行时配置文件列表。

    ```SQL
    MySQL [click_bench]> SHOW PROFILELIST;
    +--------------------------------------+---------------------+----------+---------+----------------------------------------------------------------------------------------------------------------------------------+
    | QueryId                              | StartTime           | Time     | State   | Statement                                                                                                                        |
    +--------------------------------------+---------------------+----------+---------+----------------------------------------------------------------------------------------------------------------------------------+
    | 3df61627-f82b-4776-b16a-6810279a79a3 | 2024-04-23 20:28:26 | 11s850ms | Running | LOAD LABEL click_bench.hits_1713875306 (DATA INFILE ("s3://test-data/benchmark_data/query_data/click_bench/hits.tbl*" ... |
    +--------------------------------------+---------------------+----------+---------+----------------------------------------------------------------------------------------------------------------------------------+
    1 row in set (0.00 sec)
    ```

3. 使用 **ANALYZE PROFILE** 查看运行时配置文件。

    ```SQL
    MySQL [click_bench]> ANALYZE PROFILE FROM '3df61627-f82b-4776-b16a-6810279a79a3';
    +-------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | Explain String                                                                                                                                              |
    +-------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | Summary                                                                                                                                             |
    |     Attention: The transaction of the statement will be aborted, and no data will be actually inserted!!!                           |
    |     Attention: Profile is not identical!!!                                                                                          |
    |     QueryId: 3df61627-f82b-4776-b16a-6810279a79a3                                                                                                   |
    |     Version: default_profile-70fe819                                                                                                                |
    |     State: Running                                                                                                                                  |
    |     Legend: ⏳ for blocked; 🚀 for running; ✅ for finished                                                                                             |
    |     TotalTime: 31s832ms                                                                                                                             |
    |         ExecutionTime: 30s1ms [Scan: 28s885ms (96.28%), Network: 0ns (0.00%), ResultDeliverTime: 7s613ms (25.38%), ScheduleTime: 145.701ms (0.49%)] |
    |         FrontendProfileMergeTime: 3.838ms                                                                                                           |
    |     QueryPeakMemoryUsage: 141.367 MB, QueryAllocatedMemoryUsage: 82.422 GB                                                                          |
    |     Top Most Time-consuming Nodes:                                                                                                                  |
    |         1. FILE_SCAN (id=0)  🚀 : 28s902ms (85.43%)                                                                                              |
    |         2. OLAP_TABLE_SINK 🚀 : 4s930ms (14.57%)                                                                                                      |
    |     Top Most Memory-consuming Nodes:                                                                                                                |
    |     Progress (finished operator/all operator): 0.00%                                                                                                |
    |     NonDefaultVariables:                                                                                                                            |
    |         big_query_profile_threshold: 0s -> 60s                                                                                                      |
    |         enable_adaptive_sink_dop: false -> true                                                                                                     |
    |         enable_profile: false -> true                                                                                                               |
    |         sql_mode_v2: 32 -> 34                                                                                                                       |
    |         use_compute_nodes: -1 -> 0                                                                                                                  |
    | Fragment 0                                                                                                                                          |
    | │   BackendNum: 3                                                                                                                                   |
    | │   InstancePeakMemoryUsage: 128.541 MB, InstanceAllocatedMemoryUsage: 82.422 GB                                                                    |
    | │   PrepareTime: 2.304ms                                                                                                                            |
    | └──OLAP_TABLE_SINK                                                                                                                                  |
    |    │   TotalTime: 4s930ms (14.57%) [CPUTime: 4s930ms]                                                                                               |
    |    │   OutputRows: 14.823M (14823424)                                                                                                               |
    |    │   PartitionType: RANDOM                                                                                                                        |
    |    │   Table: hits                                                                                                                                  |
    |    └──FILE_SCAN (id=0)  🚀                                                                                                                       |
    |           Estimates: [row: ?, cpu: ?, memory: ?, network: ?, cost: ?]                                                                          |
    |           TotalTime: 28s902ms (85.43%) [CPUTime: 17.038ms, ScanTime: 28s885ms]                                                                 |
    |           OutputRows: 14.823M (14823424)                                                                                                       |
    |           Progress (processed rows/total rows): ?                                                                                              |
    |           Detail Timers: [ScanTime = IOTaskExecTime + IOTaskWaitTime]                                                                          |
    |               IOTaskExecTime: 25s612ms [min=19s376ms, max=28s804ms]                                                                            |
    |               IOTaskWaitTime: 63.192ms [min=20.946ms, max=91.668ms]                                                                            |
    |                                                                                                                                                         |
    +-------------------------------------------------------------------------------------------------------------------------------------------------------------+
    40 rows in set (0.04 sec)
    ```

配置文件显示 `FILE_SCAN` 部分耗时近 29 秒，占总 32 秒时长的约 90%。这表明从对象存储读取数据是当前导入过程中的瓶颈。

### 诊断 Stream Load 性能

1. 为目标表启用 Load Profile。

    ```SQL
    mysql> ALTER TABLE duplicate_200_column_sCH SET('enable_load_profile'='true');
    Query OK, 0 rows affected (0.00 sec)
    ```

2. 使用 **SHOW PROFILELIST** 检索配置文件列表。

    ```SQL
    mysql> SHOW PROFILELIST;
    +--------------------------------------+---------------------+----------+----------+-----------+
    | QueryId                              | StartTime           | Time     | State    | Statement |
    +--------------------------------------+---------------------+----------+----------+-----------+
    | 90481df8-afaf-c0fd-8e91-a7889c1746b6 | 2024-09-19 10:43:38 | 9s571ms  | Finished |           |
    | 9c41a13f-4d7b-2c18-4eaf-cdeea3facba5 | 2024-09-19 10:43:37 | 10s664ms | Finished |           |
    | 5641cf37-0af4-f116-46c6-ca7cce149886 | 2024-09-19 10:43:20 | 13s88ms  | Finished |           |
    | 4446c8b3-4dc5-9faa-dccb-e1a71ab3519e | 2024-09-19 10:43:20 | 13s64ms  | Finished |           |
    | 48469b66-3866-1cd9-9f3b-17d786bb4fa7 | 2024-09-19 10:43:20 | 13s85ms  | Finished |           |
    | bc441907-e779-bc5a-be8e-992757e4d992 | 2024-09-19 10:43:19 | 845ms    | Finished |           |
    +--------------------------------------+---------------------+----------+----------+-----------+
    ```

3. 使用 **ANALYZE PROFILE** 查看配置文件。

    ```SQL
    mysql> ANALYZE PROFILE FROM '90481df8-afaf-c0fd-8e91-a7889c1746b6';
    +-----------------------------------------------------------+
    | Explain String                                            |
    +-----------------------------------------------------------+
    | Load:                                                     |
    |   Summary:                                                |
    |      - Query ID: 90481df8-afaf-c0fd-8e91-a7889c1746b6     |
    |      - Start Time: 2024-09-19 10:43:38                    |
    |      - End Time: 2024-09-19 10:43:48                      |
    |      - Query Type: Load                                   |
    |      - Load Type: STREAM_LOAD                             |
    |      - Query State: Finished                              |
    |      - StarRocks Version: main-d49cb08                    |
    |      - Sql Statement                                      |
    |      - Default Db: ingestion_db                           |
    |      - NumLoadBytesTotal: 799008                          |
    |      - NumRowsAbnormal: 0                                 |
    |      - NumRowsNormal: 280                                 |
    |      - Total: 9s571ms                                     |
    |      - numRowsUnselected: 0                               |
    |   Execution:                                              |
    |     Fragment 0:                                           |
    |        - Address: 172.26.93.218:59498                     |
    |        - InstanceId: 90481df8-afaf-c0fd-8e91-a7889c1746b7 |
    |        - TxnID: 1367                                      |
    |        - ReplicatedStorage: true                          |
    |        - AutomaticPartition: false                        |
    |        - InstanceAllocatedMemoryUsage: 12.478 MB          |
    |        - InstanceDeallocatedMemoryUsage: 10.745 MB        |
    |        - InstancePeakMemoryUsage: 9.422 MB                |
    |        - MemoryLimit: -1.000 B                            |
    |        - RowsProduced: 280                                |
    |          - AllocAutoIncrementTime: 348ns                  |
    |          - AutomaticBucketSize: 0                         |
    |          - BytesRead: 0.000 B                             |
    |          - CloseWaitTime: 9s504ms                         |
    |          - IOTaskExecTime: 0ns                            |
    |          - IOTaskWaitTime: 0ns                            |
    |          - IndexNum: 1                                    |
    |          - NumDiskAccess: 0                               |
    |          - OpenTime: 15.639ms                             |
    |          - PeakMemoryUsage: 0.000 B                       |
    |          - PrepareDataTime: 583.480us                     |
    |            - ConvertChunkTime: 44.670us                   |
    |            - ValidateDataTime: 109.333us                  |
    |          - RowsFiltered: 0                                |
    |          - RowsRead: 0                                    |
    |          - RowsReturned: 280                              |
    |          - RowsReturnedRate: 12.049K (12049) /sec         |
    |          - RpcClientSideTime: 28s396ms                    |
    |          - RpcServerSideTime: 28s385ms                    |
    |          - RpcServerWaitFlushTime: 0ns                    |
    |          - ScanTime: 9.841ms                              |
    |          - ScannerQueueCounter: 1                         |
    |          - ScannerQueueTime: 3.272us                      |
    |          - ScannerThreadsInvoluntaryContextSwitches: 0    |
    |          - ScannerThreadsTotalWallClockTime: 0ns          |
    |            - MaterializeTupleTime(*): 0ns                 |
    |            - ScannerThreadsSysTime: 0ns                   |
    |            - ScannerThreadsUserTime: 0ns                  |
    |          - ScannerThreadsVoluntaryContextSwitches: 0      |
    |          - SendDataTime: 2.452ms                          |
    |            - PackChunkTime: 1.475ms                       |
    |            - SendRpcTime: 1.617ms                         |
    |              - CompressTime: 0ns                          |
    |              - SerializeChunkTime: 880.424us              |
    |            - WaitResponseTime: 0ns                        |
    |          - TotalRawReadTime(*): 0ns                       |
    |          - TotalReadThroughput: 0.000 B/sec               |
    |       DataSource:                                         |
    |          - DataSourceType: FileDataSource                 |
    |          - FileScanner:                                   |
    |            - CastChunkTime: 0ns                           |
    |            - CreateChunkTime: 227.100us                   |
    |            - FileReadCount: 3                             |
    |            - FileReadTime: 253.765us                      |
    |            - FillTime: 6.892ms                            |
    |            - MaterializeTime: 133.637us                   |
    |            - ReadTime: 0ns                                |
    |          - ScannerTotalTime: 9.292ms                      |
    +-----------------------------------------------------------+
    76 rows in set (0.00 sec)
    ```

## 附录

### 运维常用 SQL

#### 查询每分钟吞吐量

```SQL
-- overall
select date_trunc('minute', load_finish_time) as t,count(*) as tpm,sum(SCAN_BYTES) as scan_bytes,sum(sink_rows) as sink_rows from _statistics_.loads_history group by t order by t desc limit 10;

-- table
select date_trunc('minute', load_finish_time) as t,count(*) as tpm,sum(SCAN_BYTES) as scan_bytes,sum(sink_rows) as sink_rows from _statistics_.loads_history where table_name = 't' group by t order by t desc limit 10;
```

#### 查询表的 RowsetNum 和 SegmentNum

```SQL
-- overall
select * from information_schema.be_tablets t, information_schema.tables_config c where t.table_id = c.table_id order by num_segment desc limit 5;
select * from information_schema.be_tablets t, information_schema.tables_config c where t.table_id = c.table_id order by num_rowset desc limit 5;

-- table
select * from information_schema.be_tablets t, information_schema.tables_config c where t.table_id = c.table_id and table_name = 't' order by num_segment desc limit 5;
select * from information_schema.be_tablets t, information_schema.tables_config c where t.table_id = c.table_id and table_name = 't' order by num_rowset desc limit 5;
```

- 高 RowsetNum (>100) 表示导入过于频繁。您可以考虑减少频率或增加 Compaction 线程。
- 高 SegmentNum (>100) 表示每次导入的段过多。您可以考虑增加 Compaction 线程或采用随机分布策略。

#### 检查数据倾斜

##### 节点间的数据倾斜

```SQL
-- overall
SELECT tbt.be_id, sum(tbt.DATA_SIZE) FROM information_schema.tables_config tb JOIN information_schema.be_tablets tbt ON tb.TABLE_ID = tbt.TABLE_ID group by be_id;

-- table
SELECT tbt.be_id, sum(tbt.DATA_SIZE) FROM information_schema.tables_config tb JOIN information_schema.be_tablets tbt ON tb.TABLE_ID = tbt.TABLE_ID WHERE tb.table_name = 't' group by be_id;
```

如果检测到节点级别的倾斜，您可以考虑使用更高基数的列作为分布键或采用随机分布策略。

##### tablet 间的数据倾斜

```SQL
select tablet_id,t.data_size,num_row,visible_version,num_version,num_rowset,num_segment,PARTITION_NAME from information_schema.partitions_meta m, information_schema.be_tablets t where t.partition_id = m.partition_id and m.partition_name = 'att' and m.table_name='att' order by t.data_size desc;
```

### 加载的常见监控指标

#### BE Load

这些指标在 Grafana 的 **BE Load** 类别下可用。如果找不到此类别，请确认您正在使用 [最新的 Grafana 仪表板模板](../../administration/management/monitoring/Monitor_and_Alert.md#125-配置-dashboard)。

##### ThreadPool

这些指标有助于分析线程池的状态，例如任务是否积压，或任务在等待中花费的时间。目前，有四个受监控的线程池：

- `async_delta_writer`
- `memtable_flush`
- `segment_replicate_sync`
- `segment_flush`

每个线程池包括以下指标：

| 名称        | 描述                                                                                               |
| ----------- | --------------------------------------------------------------------------------------------------------- |
| **rate**    | 任务处理速率。                                                                                     |
| **pending** | 任务在队列中等待的时间。                                                                    |
| **execute** | 任务执行时间。                                                                                      |
| **total**   | 池中可用的最大线程数。                                                          |
| **util**    | 给定时间段内的池利用率；由于采样不准确，在负载较重时可能超过 100%。 |
| **count**   | 队列中任务的瞬时数量。                                                               |

:::note
- 积压的可靠指标是 **pending duration** 是否持续增加。**workers util** 和 **queue count** 是必要但不充分的指标。
- 如果发生积压，使用 **rate** 和 **execute duration** 确定是由于负载增加还是处理速度变慢。
- **workers util** 有助于评估池的繁忙程度，从而指导调优工作。
:::

##### LoadChannel::add_chunks

这些指标有助于分析 `LoadChannel::add_chunks` 在收到 `BRPC tablet_writer_add_chunks` 请求后的行为。

| 名称              | 描述                                                                             |
| ----------------- | --------------------------------------------------------------------------------------- |
| **rate**          | `add_chunks` 请求的处理速率。                                               |
| **execute**       | `add_chunks` 的平均执行时间。                                                 |
| **wait_memtable** | 主副本的 MemTable 刷新的平均等待时间。                             |
| **wait_writer**   | 主副本的异步 delta writer 执行写入/提交的平均等待时间。 |
| **wait_replica**  | 次要副本完成段刷新的平均等待时间。                     |

:::note
- **latency** 指标等于 `wait_memtable`、`wait_writer` 和 `wait_replica` 的总和。
- 高等待比率表明下游瓶颈，应进一步分析。
:::

##### Async Delta Writer

这些指标有助于分析 **async delta writer** 的行为。

| 名称              | 描述                                       |
| ----------------- | ------------------------------------------------- |
| **rate**          | 写入/提交任务的处理速率。            |
| **pending**       | 在线程池队列中等待的时间。      |
| **execute**       | 处理单个任务的平均时间。            |
| **wait_memtable** | 等待 MemTable 刷新的平均时间。          |
| **wait_replica**  | 等待段同步的平均时间。 |

:::note
- 每个任务的总时间（从上游的角度）等于 **pending** 加上 **execute**。
- **execute** 进一步包括 **wait_memtable** 加上 **wait_replica**。
- 高 **pending** 时间可能表明 **execute** 较慢或线程池规模不足。
- 如果 **wait** 占据 **execute** 的大部分时间，则下游阶段是瓶颈；否则，瓶颈可能在 writer 的逻辑内部。
:::

##### MemTable Flush

这些指标分析 **MemTable flush** 性能。

| 名称            | 描述                                  |
| --------------- | -------------------------------------------- |
| **rate**        | MemTable 的刷新速率。                     |
| **memory-size** | 每秒刷新到内存的数据量。 |
| **disk-size**   | 每秒写入磁盘的数据量。      |
| **execute**     | 任务执行时间。                         |
| **io**          | 刷新任务的 I/O 时间。                  |

:::note
- 通过比较 **rate** 和 **size**，可以确定工作负载是否在变化或是否发生了大规模导入——例如，小 **rate** 但大 **size** 表示大规模导入。
- 可以使用 `memory-size / disk-size` 估算压缩比。
- 还可以通过检查 **io** 时间在 **execute** 中的比例来评估 I/O 是否是瓶颈。
:::

##### Segment Replicate Sync

| 名称        | 描述                                  |
| ----------- | -------------------------------------------- |
| **rate**    | 段同步速率。             |
| **execute** | 同步单个 tablet 副本的时间。 |

##### Segment Flush

这些指标分析 **segment flush** 性能。

| 名称        | 描述                             |
| ----------- | --------------------------------------- |
| **rate**    | 段刷新速率。                     |
| **size**    | 每秒刷新到磁盘的数据量。 |
| **execute** | 任务执行时间。                    |
| **io**      | 刷新任务的 I/O 时间。             |

:::note
- 通过比较 **rate** 和 **size**，可以确定工作负载是否在变化或是否发生了大规模导入——例如，小 **rate** 但大 **size** 表示大规模导入。
- 还可以通过检查 **io** 时间在 **execute** 中的比例来评估 I/O 是否是瓶颈。
:::
