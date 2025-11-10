---
displayed_sidebar: docs
sidebar_label: "Troubleshooting"
---

# Troubleshooting Data Loading

This guide is designed to help DBAs and operation engineers monitor the status of data load jobs through SQL interfaces—without relying on external monitoring systems. It also provides guidance on identifying performance bottlenecks and troubleshooting anomalies during load operations.

## Terminology

**Load Job:** A continuous data load process, such as a **Routine Load Job** or **Pipe Job**.

**Load Task:** A one-time data load process, usually corresponding to a single load transaction. Examples include **Broker Load**, **Stream Load**, **Spark Load**, and **INSERT INTO**. Routine Load jobs and Pipe jobs continuously generate tasks to perform data ingestion.

## Observe load jobs

There are two ways to observe load jobs:

- Using SQL statements **[SHOW ROUTINE LOAD](../../sql-reference/sql-statements/loading_unloading/routine_load/SHOW_ROUTINE_LOAD.md)** and **[SHOW PIPES](../../sql-reference/sql-statements/loading_unloading/pipe/SHOW_PIPES.md)**.
- Using system views **[information_schema.routine_load_jobs](../../sql-reference/information_schema/routine_load_jobs.md)** and **[information_schema.pipes](../../sql-reference/information_schema/pipes.md)**.

## Observe load tasks

Load tasks can also be monitored in two ways:

- Using  SQL statements **[SHOW LOAD](../../sql-reference/sql-statements/loading_unloading/SHOW_LOAD.md)** and **[SHOW ROUTINE LOAD TASK](../../sql-reference/sql-statements/loading_unloading/routine_load/SHOW_ROUTINE_LOAD_TASK.md)**.
- Using system views **[information_schema.loads](../../sql-reference/information_schema/loads.md)** and **statistics.loads_history**.

### SQL statements

The **SHOW** statements display both ongoing and recently completed load tasks for the current database, providing a quick overview of task status. The information retrieved is a subset of the **statistics.loads_history** system view.

SHOW LOAD statements return information of Broker Load, Insert Into, and Spark Load tasks, and SHOW ROUTINE LOAD TASK statements return Routine Load task information.

### System views

#### information_schema.loads

The **information_schema.loads** system view stores information about recent load tasks, including active and recently completed ones. StarRocks periodically synchronizes the data to the **statistics.loads_history** system table for persistent storage.

**information_schema.loads** provides the following fields:

| Field                | Description                                                  |
| -------------------- | ------------------------------------------------------------ |
| ID                   | Globally unique identifier.                                  |
| LABEL                | Label of the load job.                                       |
| PROFILE_ID           | The ID of the Profile, which can be analyzed via `ANALYZE PROFILE`. |
| DB_NAME              | The database to which the target table belongs.              |
| TABLE_NAME           | The target table.                                            |
| USER                 | The user who initiates the load job.                         |
| WAREHOUSE            | The warehouse to which the load job belongs.                 |
| STATE                | The state of the load job. Valid values:<ul><li>`PENDING`/`BEGIN`: The load job is created.</li><li>`QUEUEING`/`BEFORE_LOAD`: The load job is in the queue waiting to be scheduled.</li><li>`LOADING`: The load job is running.</li><li>`PREPARING`: The transaction is being pre-committed.</li><li>`PREPARED`: The transaction has been pre-committed.</li><li>`COMMITED`: The transaction has been committed.</li><li>`FINISHED`: The load job succeeded.</li><li>`CANCELLED`: The load job failed.</li></ul> |
| PROGRESS             | The progress of the ETL stage and LOADING stage of the load job. |
| TYPE                 | The type of the load job. For Broker Load, the return value is `BROKER`. For INSERT, the return value is `INSERT`. For Stream Load, the return value is `STREAM`. For Routine Load Load, the return value is `ROUTINE`. |
| PRIORITY             | The priority of the load job. Valid values: `HIGHEST`, `HIGH`, `NORMAL`, `LOW`, and `LOWEST`. |
| SCAN_ROWS            | The number of data rows that are scanned.                    |
| SCAN_BYTES           | The number of bytes that are scanned.                        |
| FILTERED_ROWS        | The number of data rows that are filtered out due to inadequate data quality. |
| UNSELECTED_ROWS      | The number of data rows that are filtered out due to the conditions specified in the WHERE clause. |
| SINK_ROWS            | The number of data rows that are loaded.                     |
| RUNTIME_DETAILS      | Load runtime metadata. For details, see [RUNTIME_DETAILS](#runtime_details). |
| CREATE_TIME          | The time at which the load job was created. Format: `yyyy-MM-dd HH:mm:ss`. Example: `2023-07-24 14:58:58`. |
| LOAD_START_TIME      | The start time of the LOADING stage of the load job. Format: `yyyy-MM-dd HH:mm:ss`. Example: `2023-07-24 14:58:58`. |
| LOAD_COMMIT_TIME     | The time at which the loading transaction was committed. Format: `yyyy-MM-dd HH:mm:ss`. Example: `2023-07-24 14:58:58`. |
| LOAD_FINISH_TIME     | The end time of the LOADING stage of the load job. Format: `yyyy-MM-dd HH:mm:ss`. Example: `2023-07-24 14:58:58`. |
| PROPERTIES           | The static properties of the load job. For details, see [PROPERTIES](#properties). |
| ERROR_MSG            | The error message of the load job. If the load job did not encounter any error, `NULL` is returned. |
| TRACKING_SQL         | The SQL statement that can be used to query the tracking log of the load job. A SQL statement is returned only when the load job involves unqualified data rows. If the load job does not involve any unqualified data rows, `NULL` is returned. |
| REJECTED_RECORD_PATH | The path from which you can access all the unqualified data rows that are filtered out in the load job. The number of unqualified data rows logged is determined by the `log_rejected_record_num` parameter configured in the load job. You can use the `wget` command to access the path. If the load job does not involve any unqualified data rows, `NULL` is returned. |

##### RUNTIME_DETAILS

- Universal metrics:

| Metric               | Description                                                  |
| -------------------- | ------------------------------------------------------------ |
| load_id              | Globally unique ID of the load execution plan.               |
| txn_id               | Load transaction ID.                                         |

- Specific metrics for Broker Load, INSERT INTO, and Spark Load:

| Metric               | Description                                                  |
| -------------------- | ------------------------------------------------------------ |
| etl_info             | ETL Details. This field is only valid for Spark Load jobs. For other types of load jobs, the value will be empty. |
| etl_start_time       | The start time of the ETL stage of the load job. Format: `yyyy-MM-dd HH:mm:ss`. Example: `2023-07-24 14:58:58`. |
| etl_start_time       | The end time of the ETL stage of the load job. Format: `yyyy-MM-dd HH:mm:ss`. Example: `2023-07-24 14:58:58`. |
| unfinished_backends  | List of BEs with unfinished executions.                      |
| backends             | List of BEs participating in execution.                      |
| file_num             | Number of files read.                                        |
| file_size            | Total size of files read.                                    |
| task_num             | Number of subtasks.                                          |

- Specific metrics for Routine Load:

| Metric               | Description                                                  |
| -------------------- | ------------------------------------------------------------ |
| schedule_interval    | The interval for Routine Load to be scheduled.               |
| wait_slot_time       | Time elapsed while the Routine Load task waits for execution slots. |
| check_offset_time    | Time consumed when checking offset information during Routine Load task scheduling. |
| consume_time         | Time consumed by the Routine Load task to read upstream data. |
| plan_time            | Time for generating the execution plan.                      |
| commit_publish_time  | Time consumed to execute the COMMIT RPC.                     |

- Specific metrics for Stream Load:

| Metric                 | Description                                                |
| ---------------------- | ---------------------------------------------------------- |
| timeout                | Timeout for load tasks.                                    |
| begin_txn_ms           | Time consumed to begin the transaction.                    |
| plan_time_ms           | Time for generating the execution plan.                    |
| receive_data_time_ms   | Time for receiving data.                                   |
| commit_publish_time_ms | Time consumed to execute the COMMIT RPC.                   |
| client_ip              | Client IP address.                                         |

##### PROPERTIES

- Specific properties for Broker Load, INSERT INTO, and Spark Load:

| Property               | Description                                                |
| ---------------------- | ---------------------------------------------------------- |
| timeout                | Timeout for load tasks.                                    |
| max_filter_ratio       | Maximum ratio of data rows that are filtered out due to inadequate data quality. |

- Specific properties for Routine Load:

| Property               | Description                                                |
| ---------------------- | ---------------------------------------------------------- |
| job_name               | Routine Load job name.                                     |
| task_num               | Number of subtasks actually executed in parallel.          |
| timeout                | Timeout for load tasks.                                    |

#### statistics.loads_history

The **statistics.loads_history** system view stores load records for the last three months by default. DBAs can adjust the retention period by modifying the `partition_ttl` of the view. **statistics.loads_history** has the consistent schema with **information_schema.loads**.

## Identify loading performance issues with Load Profiles

A **Load Profile** records execution details of all worker nodes involved in a data load. It helps you quickly pinpoint performance bottlenecks in the StarRocks cluster.

### Enable Load Profiles

StarRocks provides multiple methods to enable Load Profiles, depending on the type of load:

#### For Broker Load and INSERT INTO

Enable Load Profiles for Broker Load and INSERT INTO at session level:

```sql
SET enable_profile = true;
```

By default, profiles are automatically enabled for long-running jobs (longer than 300 seconds). You can customize this threshold by:

```sql
SET big_query_profile_threshold = 60s;
```

:::note
When `big_query_profile_threshold` is set to its default value `0`, the default behavior is to disable Query Profiling for queries. However, for load tasks, profiles are automatically recorded for tasks with execution times exceeding 300 seconds.
:::

StarRocks also supports **Runtime Profiles**, which periodically (every 30 seconds) report execution metrics of long-running load jobs. You can customize the report interval by:

```sql
SET runtime_profile_report_interval = 60;
```

:::note
`runtime_profile_report_interval` specifies only the minimum report interval for load tasks. The actual report interval is dynamically adjusted and may exceed this value.
:::

#### For Stream Load and Routine Load

Enable Load Profiles for Stream Load and Routine Load at table level:

```sql
ALTER TABLE <table_name> SET ("enable_load_profile" = "true");
```

Stream Load typically has high QPS, so StarRocks allows sampling for Load Profile collection to avoid performance degradation from extensive profiling. You can adjust the collection interval by configuring the FE parameter `load_profile_collect_interval_second`. This setting only applies to Load Profiles enabled via table properties. The default value is `0`.

```SQL
ADMIN SET FRONTEND CONFIG ("load_profile_collect_interval_second"="30");
```

StarRocks also allows collecting profiles only from load jobs that exceed a certain time threshold. You can adjust this threshold by configuring the FE parameter `stream_load_profile_collect_threshold_second`. The default value is `0`.

```SQL
ADMIN SET FRONTEND CONFIG ("stream_load_profile_collect_threshold_second"="10");
```

### Analyze Load Profiles

The structure of Load Profiles is identical to that of Query Profiles. For detailed instructions, see [Query Tuning Recipes](../../best_practices/query_tuning/query_profile_tuning_recipes.md).

You can analyze Load Profiles by executing [ANALYZE PROFILE](../../sql-reference/sql-statements/cluster-management/plan_profile/ANALYZE_PROFILE.md). For detailed instructions, see [Analyze text-based Profiles](../../best_practices/query_tuning/query_profile_text_based_analysis.md).

Profiles provide detailed operator metrics. Key components include the `OlapTableSink` operator and the `LoadChannel` operator.

#### OlapTableSink operator

| Metric            | Description                                                  |
| ----------------- | ------------------------------------------------------------ |
| IndexNum          | Number of synchronous materialized views of the target table. |
| ReplicatedStorage | Whether single leader replication is enabled.                |
| TxnID             | Load transaction ID.                                         |
| RowsRead          | Number of data rows read from the upstream operator.         |
| RowsFiltered      | The number of data rows that are filtered out due to inadequate data quality. |
| RowsReturned      | The number of data rows that are loaded.                     |
| RpcClientSideTime | Total time consumed for data writing RPC from client-side statistics. |
| RpcServerSideTime | Total time consumed for data writing RPC from server-side statistics. |
| PrepareDataTime   | Time consumed for data format conversion and data quality checks. |
| SendDataTime      | Local time consumed for sending data, including data serialization, compression, and writing to the send queue. |

:::tip
- The significant variance between the maximum and minimum values of `PushChunkNum` in `OLAP_TABLE_SINK` indicates data skew in the upstream operator, which may cause write performance bottlenecks.
- `RpcClientSideTime` equals the sum of `RpcServerSideTime`, Network transmission time, and RPC framework processing time. If the difference between `RpcClientSideTime` and `RpcServerSideTime` is significant, consider to enable data compression to reduce transmission time.
- If `RpcServerSideTime` accounts for a significant portion of the time spent, further analysis can be conducted using `LoadChannel` Profile.
:::

#### LoadChannel operator

| Metric              | Description                                                  |
| ------------------- | ------------------------------------------------------------ |
| Address             | IP address or FQDN of the BE node.                           |
| LoadMemoryLimit     | Memory Limit for loading.                                    |
| PeakMemoryUsage     | Peak memory usage for loading.                               |
| OpenCount           | The number of times the channel is opened, reflecting the sink's total concurrency. |
| OpenTime            | Total time consumed for the opening channel.                        |
| AddChunkCount       | Number of loading chunks, that is, the number of calls to `TabletsChannel::add_chunk`. |
| AddRowNum           | The number of data rows that are loaded.                     |
| AddChunkTime        | Total time consumed by loading chunks, that is, the total execution time of `TabletsChannel::add_chunk`.    |
| WaitFlushTime       | Total time spent by `TabletsChannel::add_chunk` waiting for MemTable flush. |
| WaitWriterTime      | Total time spent by `TabletsChannel::add_chunk` waiting for Async Delta Writer execution. |
| WaitReplicaTime     | Total time spent by `TabletsChannel::add_chunk` waiting for synchronization from replicas. |
| PrimaryTabletsNum   | Number of primary tablets.                                   |
| SecondaryTabletsNum | Number of secondary tablets.                                 |

:::tip
If `WaitFlushTime` takes an extended period, it may indicate insufficient resources for the flush thread. Consider adjusting the BE configuration `flush_thread_num_per_store`.
:::

## Best practices

### Diagnose Broker Load performance bottleneck

1. Load data using Broker Load:

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

2. Use **SHOW PROFILELIST** to retrieve the list of runtime profiles.

    ```SQL
    MySQL [click_bench]> SHOW PROFILELIST;
    +--------------------------------------+---------------------+----------+---------+----------------------------------------------------------------------------------------------------------------------------------+
    | QueryId                              | StartTime           | Time     | State   | Statement                                                                                                                        |
    +--------------------------------------+---------------------+----------+---------+----------------------------------------------------------------------------------------------------------------------------------+
    | 3df61627-f82b-4776-b16a-6810279a79a3 | 2024-04-23 20:28:26 | 11s850ms | Running | LOAD LABEL click_bench.hits_1713875306 (DATA INFILE ("s3://test-data/benchmark_data/query_data/click_bench/hits.tbl*" ... |
    +--------------------------------------+---------------------+----------+---------+----------------------------------------------------------------------------------------------------------------------------------+
    1 row in set (0.00 sec)
    ```

3. Use **ANALYZE PROFILE** to view the Runtime Profile.

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

The profile shows that the `FILE_SCAN` section took nearly 29 seconds, accounting for approximately 90% of the total 32-second duration. This indicates that reading data from object storage is currently the bottleneck in the loading process.

### Diagnose Stream Load Performance

1. Enable Load Profile for the target table.

    ```SQL
    mysql> ALTER TABLE duplicate_200_column_sCH SET('enable_load_profile'='true');
    Query OK, 0 rows affected (0.00 sec)
    ```

2. Use **SHOW PROFILELIST** to retrieve the list of profiles.

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

3. Use **ANALYZE PROFILE** to view the Profile.

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

## Appendix

### Useful SQL for Operations

:::note
This section only applies to shared-nothing clusters.
:::

#### query the throughput per minute

```SQL
-- overall
select date_trunc('minute', load_finish_time) as t,count(*) as tpm,sum(SCAN_BYTES) as scan_bytes,sum(sink_rows) as sink_rows from _statistics_.loads_history group by t order by t desc limit 10;

-- table
select date_trunc('minute', load_finish_time) as t,count(*) as tpm,sum(SCAN_BYTES) as scan_bytes,sum(sink_rows) as sink_rows from _statistics_.loads_history where table_name = 't' group by t order by t desc limit 10;
```

#### Query RowsetNum and SegmentNum of a table

```SQL
-- overall
select * from information_schema.be_tablets t, information_schema.tables_config c where t.table_id = c.table_id order by num_segment desc limit 5;
select * from information_schema.be_tablets t, information_schema.tables_config c where t.table_id = c.table_id order by num_rowset desc limit 5;

-- table
select * from information_schema.be_tablets t, information_schema.tables_config c where t.table_id = c.table_id and table_name = 't' order by num_segment desc limit 5;
select * from information_schema.be_tablets t, information_schema.tables_config c where t.table_id = c.table_id and table_name = 't' order by num_rowset desc limit 5;
```

- High RowsetNum (>100) indicates too frequent loads. You may consider to reduce frequency or increase Compaction threads.
- High SegmentNum (>100) indicates excessive segments per load. You may consider increase Compaction threads or adopt the random distribution strategy for the table.

#### Check data skew

##### Data skew across nodes

```SQL
-- overall
SELECT tbt.be_id, sum(tbt.DATA_SIZE) FROM information_schema.tables_config tb JOIN information_schema.be_tablets tbt ON tb.TABLE_ID = tbt.TABLE_ID group by be_id;

-- table
SELECT tbt.be_id, sum(tbt.DATA_SIZE) FROM information_schema.tables_config tb JOIN information_schema.be_tablets tbt ON tb.TABLE_ID = tbt.TABLE_ID WHERE tb.table_name = 't' group by be_id;
```

If you detected node-level skew, you may consider to use a higher-cardinality column as the distribution key or adopt the random distribution strategy for the table.

##### Data skew across tablets

```SQL
select tablet_id,t.data_size,num_row,visible_version,num_version,num_rowset,num_segment,PARTITION_NAME from information_schema.partitions_meta m, information_schema.be_tablets t where t.partition_id = m.partition_id and m.partition_name = 'att' and m.table_name='att' order by t.data_size desc;
```

### Common monitoring metrics for loading

#### BE Load

These metrics are available under the **BE Load** category in Grafana. If you cannot find this category, verify that you are using the [latest Grafana dashboard template](../../administration/management/monitoring/Monitor_and_Alert.md#125-configure-dashboard).

##### ThreadPool

These metrics help analyze the status of thread pools — for example, whether tasks are being backlogged, or how long they spend pending. Currently, there are four monitored thread pools:

- `async_delta_writer`
- `memtable_flush`
- `segment_replicate_sync`
- `segment_flush`

Each thread pool includes the following metrics:

| Name        | Description                                                                                               |
| ----------- | --------------------------------------------------------------------------------------------------------- |
| **rate**    | Task processing rate.                                                                                     |
| **pending** | Time tasks spend waiting in the queue.                                                                    |
| **execute** | Task execution time.                                                                                      |
| **total**   | Maximum number of threads available in the pool.                                                          |
| **util**    | Pool utilization over a given period; due to sampling inaccuracy, it may exceed 100% when heavily loaded. |
| **count**   | Instantaneous number of tasks in the queue.                                                               |

:::note
- A reliable indicator for backlog is whether **pending duration** keeps increasing. **workers util** and **queue count** are necessary but not sufficient indicators.
- If a backlog occurs, use **rate** and **execute duration** to determine whether it is due to increased load or slower processing.
- **workers util** helps assess how busy the pool is, which can guide tuning efforts.
:::

##### LoadChannel::add_chunks

These metrics help analyze the behavior of `LoadChannel::add_chunks` after receiving a `BRPC tablet_writer_add_chunks` request.

| Name              | Description                                                                             |
| ----------------- | --------------------------------------------------------------------------------------- |
| **rate**          | Processing rate of `add_chunks` requests.                                               |
| **execute**       | Average execution time of `add_chunks`.                                                 |
| **wait_memtable** | Average wait time for the primary replica’s MemTable flush.                             |
| **wait_writer**   | Average wait time for the primary replica’s async delta writer to perform write/commit. |
| **wait_replica**  | Average wait time for secondary replicas to complete segment flush.                     |

:::note
- The **latency** metric equals the sum of `wait_memtable`, `wait_writer`, and `wait_replica`.
- A high waiting ratio indicates downstream bottlenecks, which should be further analyzed.
:::

##### Async Delta Writer

These metrics help analyze the behavior of the **async delta writer**.

| Name              | Description                                       |
| ----------------- | ------------------------------------------------- |
| **rate**          | Processing rate of write/commit tasks.            |
| **pending**       | Time spent waiting in the thread pool queue.      |
| **execute**       | Average time to process a single task.            |
| **wait_memtable** | Average time waiting for MemTable flush.          |
| **wait_replica**  | Average time waiting for segment synchronization. |

:::note
- The total time per task (from the upstream perspective) equals **pending** plus **execute**.
- **execute** further includes **wait_memtable** plus **wait_replica**.
- A high **pending** time may indicate that **execute** is slow or the thread pool is undersized. 
- If **wait** occupies a large portion of **execute**, downstream stages are the bottleneck; otherwise, the bottleneck is likely within the writer’s logic itself.
:::

##### MemTable Flush

These metrics analyze **MemTable flush** performance.

| Name            | Description                                  |
| --------------- | -------------------------------------------- |
| **rate**        | Flush rate of MemTables.                     |
| **memory-size** | Amount of in-memory data flushed per second. |
| **disk-size**   | Amount of disk data written per second.      |
| **execute**     | Task execution time.                         |
| **io**          | I/O time of the flush task.                  |

:::note
- By comparing **rate** and **size**, you can determine whether the workload is changing or if massive imports are occurring — for example, a small **rate** but large **size** indicates a massive import.
- The compression ratio can be estimated using `memory-size / disk-size`.
- You can also assess if I/O is the bottleneck by checking the proportion of **io** time in **execute**.
:::

##### Segment Replicate Sync

| Name        | Description                                  |
| ----------- | -------------------------------------------- |
| **rate**    | Rate of segment synchronization.             |
| **execute** | Time to synchronize a single tablet replica. |

##### Segment Flush

These metrics analyze **segment flush** performance.

| Name        | Description                             |
| ----------- | --------------------------------------- |
| **rate**    | Segment flush rate.                     |
| **size**    | Amount of disk data flushed per second. |
| **execute** | Task execution time.                    |
| **io**      | I/O time of the flush task.             |

:::note
- By comparing **rate** and **size**, you can determine whether the workload is changing or if large imports are occurring — for example, a small **rate** but large **size** indicates a massive import.
- You can also assess if I/O is the bottleneck by checking the proportion of **io** time in **execute**.
:::
