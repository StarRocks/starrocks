# NLJoin Global Runtime Filter

This directory contains SQL regression coverage for global runtime filters produced by NLJoin. This document records a manual performance good case. It is not executed by the SQL regression framework.

## Test Environment

- Cluster: 1 FE and 1 BE
- Database: `qjc_nl_rf_perf`
- Probe table: `fact_probe_indexed`, 100,000,010 rows
- Build table: `build_range_multi_indexed`, 3 rows
- Warmup: one run with global RF disabled, followed by one run with global RF enabled
- Formal test: five alternating off/on runs
- Scan data cache and page cache disabled

## Test SQL

### Table Schema

`event_day` is the leading sort key of the probe table. StarRocks maintains a ZoneMap index for the column. The explicit bitmap index is retained to make the indexed-column definition visible, although this range runtime filter uses the ZoneMap path.

```sql
CREATE DATABASE IF NOT EXISTS qjc_nl_rf_perf;
USE qjc_nl_rf_perf;

CREATE TABLE fact_probe_indexed (
    event_day DATE NULL,
    id BIGINT NULL,
    payload VARCHAR(512) NULL,
    INDEX idx_event_day (event_day) USING BITMAP
)
ENGINE=OLAP
DUPLICATE KEY(event_day, id)
DISTRIBUTED BY HASH(id) BUCKETS 96
PROPERTIES (
    "compression" = "LZ4",
    "replication_num" = "1"
);

CREATE TABLE build_range_multi_indexed (
    bound_day DATE NULL
)
ENGINE=OLAP
DUPLICATE KEY(bound_day)
DISTRIBUTED BY HASH(bound_day) BUCKETS 1
PROPERTIES (
    "compression" = "LZ4",
    "replication_num" = "1"
);

INSERT INTO build_range_multi_indexed VALUES
    ('2024-03-10'),
    ('2024-03-20'),
    ('2024-03-25');
```

### Data Generation

The probe data was loaded in ten separate date-ordered batches. This gives each rowset a relatively narrow `event_day` range so that the ZoneMap index can reject row ranges using the runtime boundary.

Execute the following statement ten times using the listed `(day_offset, id_offset)` values:

| Batch | `day_offset` | `id_offset` |
| ---: | ---: | ---: |
| 1 | 0 | 0 |
| 2 | 10 | 10000000 |
| 3 | 20 | 20000000 |
| 4 | 30 | 30000000 |
| 5 | 40 | 40000000 |
| 6 | 50 | 50000000 |
| 7 | 60 | 60000000 |
| 8 | 70 | 70000000 |
| 9 | 80 | 80000000 |
| 10 | 90 | 90000000 |

```sql
INSERT INTO fact_probe_indexed
SELECT
    date_add('2024-01-01', INTERVAL <day_offset> + generate_series % 10 DAY),
    generate_series + <id_offset>,
    concat(
        lpad(cast(generate_series + <id_offset> AS VARCHAR), 12, '0'),
        repeat('x', 500)
    )
FROM TABLE(generate_series(1, 10000001));
```

### Query

```sql
USE qjc_nl_rf_perf;

SET enable_profile = true;
SET enable_query_cache = false;
SET query_cache_type = 0;
SET enable_scan_datacache = false;
SET use_page_cache = false;
SET skip_page_cache = true;
SET enable_join_runtime_filter_push_down = true;

SET enable_global_runtime_filter = true;

SELECT sum(length(j.payload))
FROM (
    SELECT event_day, payload
    FROM fact_probe_indexed
    GROUP BY event_day, payload
) j
JOIN build_range_multi_indexed b
ON j.event_day > b.bound_day;
```

## Performance Result

| Case | Average latency | Average peak memory | ZoneMap filtered rows | RF output rows |
| --- | ---: | ---: | ---: | ---: |
| Global RF off | 16.090 s | 63.979 GB | 0 | 0 |
| Global RF on | 5.652 s | 25.717 GB | 67.633M | 31.000M |

All ten formal executions returned `33280003072`. Enabling the global runtime filter reduced average latency by 64.9% and average peak memory by 59.8% in this environment.

## Global RF On Explain

The plan was captured with `enable_global_runtime_filter = true` using `EXPLAIN VERBOSE`:

```text
RESOURCE GROUP: default_wg

PLAN COST
  CPU: 1.670076421007625E10
  Memory: 6.56252486425E8

PLAN FRAGMENT 0(F04)
  Fragment Cost: 36.0
  Output Exprs:20: sum
  Input Partition: UNPARTITIONED
  RESULT SINK

  10:AGGREGATE (merge finalize)
  |  aggregate: sum[([20: sum, BIGINT, true]); args: INT; result: BIGINT; args nullable: true; result nullable: true]
  |  cardinality: 1
  |  
  9:EXCHANGE
     distribution type: GATHER
     cardinality: 1

PLAN FRAGMENT 1(F01)
  Fragment Cost: 8.510386950638124E9

  Input Partition: HASH_PARTITIONED: 1: event_day, 3: payload
  OutPut Partition: UNPARTITIONED
  OutPut Exchange Id: 09

  8:AGGREGATE (update serialize)
  |  aggregate: sum[(length[([3: payload, VARCHAR(512), true]); args: VARCHAR; result: INT; args nullable: true; result nullable: true]); args: INT; result: BIGINT; args nullable: true; result nullable: true]
  |  cardinality: 1
  |  
  7:Project
  |  output columns:
  |  3 <-> [3: payload, VARCHAR(512), true]
  |  cardinality: 41343754
  |  
  6:NESTLOOP JOIN
  |  join op: INNER JOIN
  |  other join predicates: [1: event_day, DATE, true] > [11: bound_day, DATE, true]
  |  build runtime filters:
  |  - filter_id = 0, build_expr = (11: bound_day), remote = true
  |  can local shuffle: false
  |  cardinality: 41343754
  |  
  |----5:EXCHANGE
  |       distribution type: BROADCAST
  |       cardinality: 3
  |    
  3:AGGREGATE (merge finalize)
  |  group by: [1: event_day, DATE, true], [3: payload, VARCHAR(512), true]
  |  cardinality: 27562503
  |  
  2:EXCHANGE
     distribution type: SHUFFLE
     partition exprs: [1: event_day, DATE, true], [3: payload, VARCHAR(512), true]
     cardinality: 52500005
     probe runtime filters:
     - filter_id = 0, probe_expr = (1: event_day)

PLAN FRAGMENT 2(F02)
  Fragment Cost: 6.0

  Input Partition: RANDOM
  OutPut Partition: UNPARTITIONED
  OutPut Exchange Id: 05

  4:OlapScanNode
     table: build_range_multi_indexed, rollup: build_range_multi_indexed
     preAggregation: on
     partitionsRatio=1/1, tabletsRatio=1/1
     tabletList=1341848
     actualRows=3, avgRowSize=1.0
     cardinality: 3

PLAN FRAGMENT 3(F00)
  Fragment Cost: 1.310000131E9
  colocate exec groups: ExecGroup{groupId=2, nodeIds=[0, 1]}

  Input Partition: RANDOM
  OutPut Partition: HASH_PARTITIONED: 1: event_day, 3: payload
  OutPut Exchange Id: 02

  1:AGGREGATE (update serialize)
  |  STREAMING
  |  group by: [1: event_day, DATE, true], [3: payload, VARCHAR(512), true]
  |  cardinality: 52500005
  |  
  0:OlapScanNode
     table: fact_probe_indexed, rollup: fact_probe_indexed
     preAggregation: on
     partitionsRatio=1/1, tabletsRatio=96/96
     tabletList=1341652,1341654,1341656,1341658,1341660,1341662,1341664,1341666,1341668,1341670 ...
     actualRows=100000010, avgRowSize=2.0
     cardinality: 100000010
     probe runtime filters:
     - filter_id = 0, probe_expr = (1: event_day)
```

The same `filter_id = 0` is built by the NLJoin with `remote = true`, crosses the shuffle Exchange, and is attached to the probe-side `OlapScanNode`.

## Global RF On Profile

The complete profile below is from the global-RF-enabled run with query ID `019f690c-b658-7e47-9ce8-a82395a47082`.

<details>
<summary>Complete runtime profile</summary>

```text
Query:
  Summary:
     - Query ID: 019f690c-b658-7e47-9ce8-a82395a47082
     - Start Time: 2026-07-16 03:51:08 (Z)
     - End Time: 2026-07-16 03:51:14 (Z)
     - Total: 5s643ms
     - Query Type: Query
     - Query State: Finished
     - StarRocks Version: feature/support-nljoin-global-rf-77c9779
     - User: root
     - Default Db: qjc_nl_rf_perf
     - Sql Statement: 
SELECT sum(length(j.payload))
FROM (
    SELECT event_day, payload
    FROM fact_probe_indexed
    GROUP BY event_day, payload
) j
JOIN build_range_multi_indexed b
ON j.event_day > b.bound_day

     - Warehouse
     - Sql Dialect: StarRocks
     - Variables: parallel_fragment_exec_instance_num=1,max_parallel_scan_instance_num=-1,pipeline_dop=0,enable_adaptive_sink_dop=true,enable_runtime_adaptive_dop=false,runtime_profile_report_interval=10,resource_group=default_wg
     - NonDefaultSessionVariables: {"skip_page_cache":{"defaultValue":false,"actualValue":true},"use_page_cache":{"defaultValue":true,"actualValue":false},"enable_scan_datacache":{"defaultValue":true,"actualValue":false},"enable_adaptive_sink_dop":{"defaultValue":false,"actualValue":true},"time_zone":{"defaultValue":"UTC","actualValue":"Etc/UTC"},"enable_profile":{"defaultValue":false,"actualValue":true}}
     - Collect Profile Time: 13ms
     - IsProfileAsync: true
  Planner:
     - -- Parser[1] 0
     - -- Total[1] 5ms
     -     -- Analyzer[1] 0
     -         -- Lock[1] 0
     -         -- AnalyzeDatabase[2] 0
     -         -- AnalyzeTemporaryTable[2] 0
     -         -- AnalyzeTable[2] 0
     -     -- Transformer[1] 0
     -     -- Optimizer[1] 3ms
     -         -- MVPreprocess[1] 0
     -         -- MVTextRewrite[1] 0
     -         -- RuleBaseOptimize[1] 2ms
     -         -- CostBaseOptimize[1] 0
     -         -- PhysicalRewrite[1] 0
     -         -- DynamicRewrite[1] 0
     -         -- PlanValidate[1] 0
     -             -- TypeChecker[1] 0
     -             -- ConditionalTypeChecker[1] 0
     -             -- CTEUniqueChecker[1] 0
     -             -- InputDependenciesChecker[1] 0
     -             -- ColumnReuseChecker[1] 0
     -     -- ExecPlanBuild[1] 0
     - -- Pending[1] 0
     - -- Prepare[1] 0
     - -- Deploy[1] 33ms
     -     -- DeployLockInternalTime[1] 33ms
     -         -- DeploySerializeConcurrencyTime[3] 0
     -         -- DeployStageByStageTime[9] 0
     -             -- DeployAsyncSendTime[4] 0
     -         -- DeployWaitTime[9] 31ms
     - -- DeployScanRanges[1] 0
     - DeployDataSize: 26251
    Reason:
  StatsSource:
     - fact_probe_indexed: ANALYZE
     - build_range_multi_indexed: ANALYZE
  Execution:
     - Topology: {"rootId":10,"nodes":[{"id":10,"name":"AGGREGATION","properties":{"sinkIds":[],"displayMem":true},"children":[9]},{"id":9,"name":"EXCHANGE","properties":{"displayMem":true},"children":[8]},{"id":8,"name":"AGGREGATION","properties":{"sinkIds":[9],"displayMem":true},"children":[7]},{"id":7,"name":"PROJECT","properties":{"displayMem":false},"children":[6]},{"id":6,"name":"NEST_LOOP_JOIN","properties":{"displayMem":true},"children":[3,5]},{"id":3,"name":"AGGREGATION","properties":{"displayMem":true},"children":[2]},{"id":5,"name":"EXCHANGE","properties":{"displayMem":true},"children":[4]},{"id":2,"name":"EXCHANGE","properties":{"displayMem":true},"children":[1]},{"id":4,"name":"OLAP_SCAN","properties":{"sinkIds":[5],"displayMem":false},"children":[]},{"id":1,"name":"AGGREGATION","properties":{"sinkIds":[2],"displayMem":true},"children":[0]},{"id":0,"name":"OLAP_SCAN","properties":{"displayMem":false},"children":[]}]}
     - FrontendProfileMergeTime: 4.074ms
     - QueryAllocatedMemoryUsage: 276.458 GB
     - QueryCumulativeCpuTime: 19m15s
     - QueryCumulativeNetworkTime: 425.078ms
     - QueryCumulativeOperatorTime: 10s316ms
     - QueryCumulativeScanTime: 1s496ms
     - QueryDeallocatedMemoryUsage: 267.868 GB
     - QueryExecutionWallTime: 5s630ms
     - QueryPeakMemoryUsagePerNode: 25.770 GB
     - QueryPeakScheduleTime: 5s616ms
     - QuerySpillBytes: 0.000 B
     - QuerySumMemoryUsage: 25.770 GB
     - ResultDeliverTime: 0ns
    Fragment 0:
       - BackendAddresses: 172.20.0.2:9060
       - InstanceIds: 019f690c-b658-7e47-9ce8-a82395a47083
       - EnableEventScheduler: true
       - BackendNum: 1
       - BackendProfileMergeTime: 3.256ms
       - FragmentInstancePrepareTime: 1.451ms
       - InitialProcessDriverCount: 0
       - InitialProcessMem: 9.305 GB
       - InstanceAllocatedMemoryUsage: 2.512 MB
       - InstanceDeallocatedMemoryUsage: 415.758 KB
       - InstanceNum: 1
       - InstancePeakMemoryUsage: 2.106 MB
       - QueryMemoryLimit: -1.000 B
      Pipeline (id=2):
         - IsGroupExecution: false
         - ActiveTime: 93.279us
         - BlockByInputEmpty: 0
         - BlockByOutputFull: 0
         - BlockByPrecondition: 0
         - DegreeOfParallelism: 1
         - DriverTotalTime: 5s616ms
         - PeakDriverQueueSize: 0
         - PendingTime: 0ns
           - InputEmptyTime: 5s616ms
             - FirstInputEmptyTime: 5s616ms
         - ScheduleCount: 1
         - ScheduleTime: 5s616ms
         - TotalDegreeOfParallelism: 1
         - YieldByLocalWait: 0
         - YieldByPreempt: 0
         - YieldByTimeLimit: 0
        RESULT_SINK (plan_node_id=-1):
          CommonMetrics:
             - IsFinalSink
             - OperatorTotalTime: 68.146us
             - OutputChunkBytes: 0.000 B
             - PullChunkNum: 0
             - PullRowNum: 0
             - PullTotalTime: 0ns
             - PushChunkNum: 1
             - PushRowNum: 1
             - PushTotalTime: 56.726us
          UniqueMetrics:
             - SinkType: MYSQL_PROTOCAL
             - AppendChunkTime: 8.124us
               - ExprEvalTime: 5.241us
               - ResultRendTime: 46.047us
               - TupleConvertTime: 1.735us
             - NumSentRows: 1
        CHUNK_ACCUMULATE (plan_node_id=-1):
          CommonMetrics:
             - IsSubordinate
             - OperatorTotalTime: 1.778us
             - OutputChunkBytes: 9.000 B
             - PullChunkNum: 1
             - PullRowNum: 1
             - PullTotalTime: 177ns
             - PushChunkNum: 1
             - PushRowNum: 1
             - PushTotalTime: 740ns
          UniqueMetrics:
        AGGREGATE_BLOCKING_SOURCE (plan_node_id=10):
          CommonMetrics:
             - JoinRuntimeFilterEvaluate: 0
             - JoinRuntimeFilterHashTime: 0ns
             - JoinRuntimeFilterInputRows: 0
             - JoinRuntimeFilterOutputRows: 0
             - JoinRuntimeFilterTime: 0ns
             - OperatorTotalTime: 31.514us
             - OutputChunkBytes: 9.000 B
             - PullChunkNum: 1
             - PullRowNum: 1
             - PullTotalTime: 23.115us
             - PushChunkNum: 0
             - PushRowNum: 0
             - PushTotalTime: 0ns
             - RuntimeFilterNum: 0
             - RuntimeInFilterNum: 0
          UniqueMetrics:
      Pipeline (id=1):
         - IsGroupExecution: false
         - ActiveTime: 416.467us
         - BlockByInputEmpty: 4
         - BlockByOutputFull: 0
         - BlockByPrecondition: 0
         - DegreeOfParallelism: 1
         - DriverTotalTime: 5s616ms
         - PeakDriverQueueSize: 10
         - PendingTime: 0ns
           - InputEmptyTime: 5s615ms
             - FirstInputEmptyTime: 5s72ms
             - FollowupInputEmptyTime: 542.483ms
         - ScheduleCount: 5
         - ScheduleTime: 5s615ms
         - TotalDegreeOfParallelism: 1
         - YieldByLocalWait: 0
         - YieldByPreempt: 0
         - YieldByTimeLimit: 0
        AGGREGATE_BLOCKING_SINK (plan_node_id=10):
          CommonMetrics:
             - OperatorTotalTime: 175.172us
             - OutputChunkBytes: 0.000 B
             - PullChunkNum: 0
             - PullRowNum: 0
             - PullTotalTime: 0ns
             - PushChunkNum: 52
             - PushRowNum: 52
             - PushTotalTime: 135.043us
             - RuntimeFilterNum: 0
             - RuntimeInFilterNum: 0
          UniqueMetrics:
             - AggregateFunctions: sum(20: sum)
             - AggComputeTime: 90.402us
             - AggFuncComputeTime: 84.241us
             - ChunkBufferPeakMem: 0.000 B
             - ChunkBufferPeakSize: 0
             - ExprComputeTime: 75.595us
             - ExprReleaseTime: 16.283us
             - GetResultsTime: 8.106us
             - HashTableMemoryUsage: 4.023 KB
             - HashTableSize: 0
             - InputRowCount: 52
             - PassThroughRowCount: 0
             - ResultAggAppendTime: 0ns
             - ResultGroupByAppendTime: 0ns
             - ResultIteratorTime: 0ns
             - RowsReturned: 0
             - StateAllocate: 0ns
             - StateDestroy: 0ns
             - StreamingTime: 0ns
             - UdafCacheHitCount: 0
             - UdafCachePopulateCount: 0
             - UdafLoadTime: 0ns
        LOCAL_EXCHANGE_SOURCE (plan_node_id=10):
          CommonMetrics:
             - IsSubordinate
             - OperatorTotalTime: 43.585us
             - OutputChunkBytes: 468.000 B
             - PullChunkNum: 52
             - PullRowNum: 52
             - PullTotalTime: 28.533us
             - PushChunkNum: 0
             - PushRowNum: 0
             - PushTotalTime: 0ns
          UniqueMetrics:
      Pipeline (id=0):
         - IsGroupExecution: false
         - ActiveTime: 53.794us
           - __MAX_OF_ActiveTime: 370.556us
           - __MIN_OF_ActiveTime: 12.161us
         - BlockByInputEmpty: 38
           - __MAX_OF_BlockByInputEmpty: 3
           - __MIN_OF_BlockByInputEmpty: 0
         - BlockByOutputFull: 0
         - BlockByPrecondition: 0
         - DegreeOfParallelism: 52
         - DriverTotalTime: 5s616ms
           - __MAX_OF_DriverTotalTime: 5s616ms
           - __MIN_OF_DriverTotalTime: 5s615ms
         - PeakDriverQueueSize: 1.135K (1135)
           - __MAX_OF_PeakDriverQueueSize: 47
           - __MIN_OF_PeakDriverQueueSize: 0
         - PendingTime: 0ns
           - InputEmptyTime: 5s615ms
             - __MAX_OF_InputEmptyTime: 5s615ms
             - __MIN_OF_InputEmptyTime: 5s615ms
             - FirstInputEmptyTime: 5s497ms
               - __MAX_OF_FirstInputEmptyTime: 5s615ms
               - __MIN_OF_FirstInputEmptyTime: 5s72ms
             - FollowupInputEmptyTime: 117.749ms
               - __MAX_OF_FollowupInputEmptyTime: 542.639ms
               - __MIN_OF_FollowupInputEmptyTime: 0ns
         - ScheduleCount: 90
           - __MAX_OF_ScheduleCount: 4
           - __MIN_OF_ScheduleCount: 1
         - ScheduleTime: 5s616ms
           - __MAX_OF_ScheduleTime: 5s616ms
           - __MIN_OF_ScheduleTime: 5s615ms
         - TotalDegreeOfParallelism: 52
         - YieldByLocalWait: 0
         - YieldByPreempt: 0
         - YieldByTimeLimit: 0
        LOCAL_EXCHANGE_SINK (plan_node_id=10):
          CommonMetrics:
             - IsSubordinate
             - OperatorTotalTime: 8.065us
               - __MAX_OF_OperatorTotalTime: 153.721us
               - __MIN_OF_OperatorTotalTime: 391ns
             - OutputChunkBytes: 0.000 B
             - PullChunkNum: 0
             - PullRowNum: 0
             - PullTotalTime: 0ns
             - PushChunkNum: 52
               - __MAX_OF_PushChunkNum: 15
               - __MIN_OF_PushChunkNum: 0
             - PushRowNum: 52
               - __MAX_OF_PushRowNum: 15
               - __MIN_OF_PushRowNum: 0
             - PushTotalTime: 6.696us
               - __MAX_OF_PushTotalTime: 152.404us
               - __MIN_OF_PushTotalTime: 0ns
          UniqueMetrics:
             - Type: Passthrough
             - ShuffleNum: 1
             - LocalExchangePeakMemoryUsage: 160.000 B
             - LocalExchangePeakNumRows: 16
        EXCHANGE_SOURCE (plan_node_id=9):
          CommonMetrics:
             - ConjunctsInputRows: 52
               - __MAX_OF_ConjunctsInputRows: 15
               - __MIN_OF_ConjunctsInputRows: 1
             - ConjunctsOutputRows: 52
               - __MAX_OF_ConjunctsOutputRows: 15
               - __MIN_OF_ConjunctsOutputRows: 1
             - ConjunctsTime: 3.444us
               - __MAX_OF_ConjunctsTime: 5.964us
               - __MIN_OF_ConjunctsTime: 1.126us
             - JoinRuntimeFilterEvaluate: 0
             - JoinRuntimeFilterHashTime: 0ns
             - JoinRuntimeFilterInputRows: 0
             - JoinRuntimeFilterOutputRows: 0
             - JoinRuntimeFilterTime: 0ns
             - OperatorTotalTime: 41.158us
               - __MAX_OF_OperatorTotalTime: 161.007us
               - __MIN_OF_OperatorTotalTime: 14.661us
             - OutputChunkBytes: 468.000 B
               - __MAX_OF_OutputChunkBytes: 135.000 B
               - __MIN_OF_OutputChunkBytes: 0.000 B
             - PullChunkNum: 52
               - __MAX_OF_PullChunkNum: 15
               - __MIN_OF_PullChunkNum: 0
             - PullRowNum: 52
               - __MAX_OF_PullRowNum: 15
               - __MIN_OF_PullRowNum: 0
             - PullTotalTime: 11.927us
               - __MAX_OF_PullTotalTime: 113.184us
               - __MIN_OF_PullTotalTime: 0ns
             - PushChunkNum: 0
             - PushRowNum: 0
             - PushTotalTime: 0ns
             - RuntimeFilterNum: 0
             - RuntimeInFilterNum: 0
          UniqueMetrics:
             - BufferUnplugCount: 3
               - __MAX_OF_BufferUnplugCount: 3
               - __MIN_OF_BufferUnplugCount: 0
             - BytesPassThrough: 1.270 KB
               - __MAX_OF_BytesPassThrough: 50.000 B
               - __MIN_OF_BytesPassThrough: 0.000 B
             - BytesReceived: 0.000 B
             - ClosureBlockCount: 0
             - ClosureBlockTime: 0ns
             - DecompressChunkTime: 0ns
             - DeserializeChunkTime: 0ns
             - PeakBufferMemoryBytes: 1.270 KB
               - __MAX_OF_PeakBufferMemoryBytes: 50.000 B
               - __MIN_OF_PeakBufferMemoryBytes: 0.000 B
             - ReceiverProcessTotalTime: 28.494us
               - __MAX_OF_ReceiverProcessTotalTime: 126.730us
               - __MIN_OF_ReceiverProcessTotalTime: 3.433us
             - RequestReceived: 53
               - __MAX_OF_RequestReceived: 2
               - __MIN_OF_RequestReceived: 1
             - WaitLockTime: 0ns
    Fragment 1:
       - BackendAddresses: 172.20.0.2:9060
       - InstanceIds: 019f690c-b658-7e47-9ce8-a82395a47084
       - EnableEventScheduler: true
       - BackendNum: 1
       - BackendProfileMergeTime: 12.987ms
       - FragmentInstancePrepareTime: 3.879ms
       - InitialProcessDriverCount: 54
       - InitialProcessMem: 9.307 GB
       - InstanceAllocatedMemoryUsage: 185.603 GB
       - InstanceDeallocatedMemoryUsage: 185.469 GB
       - InstanceNum: 1
       - InstancePeakMemoryUsage: 16.973 GB
       - QueryMemoryLimit: -1.000 B
      Pipeline (id=3):
         - IsGroupExecution: false
         - ActiveTime: 303.532us
           - __MAX_OF_ActiveTime: 1.009ms
           - __MIN_OF_ActiveTime: 79.895us
         - BlockByInputEmpty: 0
         - BlockByOutputFull: 0
         - BlockByPrecondition: 0
         - DegreeOfParallelism: 52
         - DriverTotalTime: 5s317ms
           - __MAX_OF_DriverTotalTime: 5s611ms
           - __MIN_OF_DriverTotalTime: 4s924ms
         - PeakDriverQueueSize: 42
           - __MAX_OF_PeakDriverQueueSize: 41
           - __MIN_OF_PeakDriverQueueSize: 0
         - PendingTime: 0ns
           - InputEmptyTime: 5s307ms
             - __MAX_OF_InputEmptyTime: 5s610ms
             - __MIN_OF_InputEmptyTime: 4s924ms
             - FirstInputEmptyTime: 5s307ms
               - __MAX_OF_FirstInputEmptyTime: 5s610ms
               - __MIN_OF_FirstInputEmptyTime: 4s924ms
           - PendingFinishTime: 10.171ms
             - __MAX_OF_PendingFinishTime: 528.912ms
             - __MIN_OF_PendingFinishTime: 0ns
         - ScheduleCount: 52
           - __MAX_OF_ScheduleCount: 1
           - __MIN_OF_ScheduleCount: 1
         - ScheduleTime: 5s317ms
           - __MAX_OF_ScheduleTime: 5s610ms
           - __MIN_OF_ScheduleTime: 4s924ms
         - TotalDegreeOfParallelism: 52
         - YieldByLocalWait: 0
         - YieldByPreempt: 0
         - YieldByTimeLimit: 0
        EXCHANGE_SINK (plan_node_id=9):
          CommonMetrics:
             - OperatorTotalTime: 215.994us
               - __MAX_OF_OperatorTotalTime: 795.764us
               - __MIN_OF_OperatorTotalTime: 45.262us
             - OutputChunkBytes: 0.000 B
             - PullChunkNum: 0
             - PullRowNum: 0
             - PullTotalTime: 0ns
             - PushChunkNum: 52
               - __MAX_OF_PushChunkNum: 1
               - __MIN_OF_PushChunkNum: 1
             - PushRowNum: 52
               - __MAX_OF_PushRowNum: 1
               - __MIN_OF_PushRowNum: 1
             - PushTotalTime: 24.404us
               - __MAX_OF_PushTotalTime: 82.573us
               - __MIN_OF_PushTotalTime: 8.022us
          UniqueMetrics:
             - DestID: 9
             - DestFragments: 019f690cb6587e47-9ce8a82395a47083
             - PartType: UNPARTITIONED
             - ChannelNum: 1
             - BytesPassThrough: 1.270 KB
               - __MAX_OF_BytesPassThrough: 25.000 B
               - __MIN_OF_BytesPassThrough: 25.000 B
             - BytesSent: 0.000 B
             - BytesUnsent: 0.000 B
             - CompressTime: 0ns
             - CompressedBytes: 0.000 B
             - NetworkBandwidth: 0.000 B/sec
             - NetworkTime: 22.777ms
             - OverallThroughput: 0.000 B/sec
             - OverallTime: 686.374ms
             - PassThroughBufferPeakMemoryUsage: 680.000 B
             - RawInputBytes: 0.000 B
             - RequestSent: 0
             - RequestUnsent: 0
             - RpcAvgTime: 429.764us
             - RpcCount: 53
             - SerializeChunkTime: 0ns
             - SerializedBytes: 0.000 B
             - ShuffleChunkAppendCounter: 0
             - ShuffleChunkAppendTime: 0ns
             - ShuffleHashTime: 0ns
             - WaitTime: 704.910us
        AGGREGATE_BLOCKING_SOURCE (plan_node_id=8):
          CommonMetrics:
             - JoinRuntimeFilterEvaluate: 0
             - JoinRuntimeFilterHashTime: 0ns
             - JoinRuntimeFilterInputRows: 0
             - JoinRuntimeFilterOutputRows: 0
             - JoinRuntimeFilterTime: 0ns
             - OperatorTotalTime: 71.816us
               - __MAX_OF_OperatorTotalTime: 270.099us
               - __MIN_OF_OperatorTotalTime: 25.523us
             - OutputChunkBytes: 468.000 B
               - __MAX_OF_OutputChunkBytes: 9.000 B
               - __MIN_OF_OutputChunkBytes: 9.000 B
             - PullChunkNum: 52
               - __MAX_OF_PullChunkNum: 1
               - __MIN_OF_PullChunkNum: 1
             - PullRowNum: 52
               - __MAX_OF_PullRowNum: 1
               - __MIN_OF_PullRowNum: 1
             - PullTotalTime: 64.776us
               - __MAX_OF_PullTotalTime: 256.156us
               - __MIN_OF_PullTotalTime: 24.159us
             - PushChunkNum: 0
             - PushRowNum: 0
             - PushTotalTime: 0ns
             - RuntimeFilterNum: 0
             - RuntimeInFilterNum: 0
          UniqueMetrics:
      Pipeline (id=2):
         - IsGroupExecution: false
         - ActiveTime: 2s206ms
           - __MAX_OF_ActiveTime: 2s512ms
           - __MIN_OF_ActiveTime: 1s808ms
         - BlockByInputEmpty: 52
           - __MAX_OF_BlockByInputEmpty: 1
           - __MIN_OF_BlockByInputEmpty: 1
         - BlockByOutputFull: 0
         - BlockByPrecondition: 0
         - DegreeOfParallelism: 52
         - DriverTotalTime: 5s307ms
           - __MAX_OF_DriverTotalTime: 5s610ms
           - __MIN_OF_DriverTotalTime: 4s924ms
         - OverheadTime: 126.215ms
           - __MAX_OF_OverheadTime: 165.837ms
           - __MIN_OF_OverheadTime: 73.275ms
         - PeakDriverQueueSize: 115
           - __MAX_OF_PeakDriverQueueSize: 9
           - __MIN_OF_PeakDriverQueueSize: 0
         - PendingTime: 0ns
           - InputEmptyTime: 3s96ms
             - __MAX_OF_InputEmptyTime: 3s129ms
             - __MIN_OF_InputEmptyTime: 3s55ms
             - FirstInputEmptyTime: 3s96ms
               - __MAX_OF_FirstInputEmptyTime: 3s129ms
               - __MIN_OF_FirstInputEmptyTime: 3s55ms
           - PreconditionBlockTime: 3.436ms
             - __MAX_OF_PreconditionBlockTime: 3.579ms
             - __MIN_OF_PreconditionBlockTime: 3.304ms
         - ScheduleCount: 1.159K (1159)
           - __MAX_OF_ScheduleCount: 25
           - __MIN_OF_ScheduleCount: 19
         - ScheduleTime: 3s100ms
           - __MAX_OF_ScheduleTime: 3s133ms
           - __MIN_OF_ScheduleTime: 3s59ms
         - TotalDegreeOfParallelism: 52
         - YieldByLocalWait: 0
         - YieldByPreempt: 0
         - YieldByTimeLimit: 1.107K (1107)
           - __MAX_OF_YieldByTimeLimit: 24
           - __MIN_OF_YieldByTimeLimit: 18
        AGGREGATE_BLOCKING_SINK (plan_node_id=8):
          CommonMetrics:
             - OperatorTotalTime: 17.088ms
               - __MAX_OF_OperatorTotalTime: 20.373ms
               - __MIN_OF_OperatorTotalTime: 13.731ms
             - OutputChunkBytes: 0.000 B
             - PullChunkNum: 0
             - PullRowNum: 0
             - PullTotalTime: 0ns
             - PushChunkNum: 16.809K (16809)
               - __MAX_OF_PushChunkNum: 326
               - __MIN_OF_PushChunkNum: 320
             - PushRowNum: 65.000M (65000006)
               - __MAX_OF_PushRowNum: 1.254M (1253867)
               - __MIN_OF_PushRowNum: 1.246M (1245716)
             - PushTotalTime: 17.043ms
               - __MAX_OF_PushTotalTime: 20.347ms
               - __MIN_OF_PushTotalTime: 13.688ms
             - RuntimeFilterNum: 0
             - RuntimeInFilterNum: 0
          UniqueMetrics:
             - AggregateFunctions: sum(length(3: payload))
             - AggComputeTime: 14.466ms
               - __MAX_OF_AggComputeTime: 17.351ms
               - __MIN_OF_AggComputeTime: 11.535ms
             - AggFuncComputeTime: 14.205ms
               - __MAX_OF_AggFuncComputeTime: 17.116ms
               - __MIN_OF_AggFuncComputeTime: 11.308ms
             - ChunkBufferPeakMem: 0.000 B
             - ChunkBufferPeakSize: 0
             - ExprComputeTime: 13.090ms
               - __MAX_OF_ExprComputeTime: 15.824ms
               - __MIN_OF_ExprComputeTime: 10.342ms
             - ExprReleaseTime: 1.586ms
               - __MAX_OF_ExprReleaseTime: 1.938ms
               - __MIN_OF_ExprReleaseTime: 1.273ms
             - GetResultsTime: 20.838us
               - __MAX_OF_GetResultsTime: 113.011us
               - __MIN_OF_GetResultsTime: 5.813us
             - HashTableMemoryUsage: 209.219 KB
               - __MAX_OF_HashTableMemoryUsage: 4.023 KB
               - __MIN_OF_HashTableMemoryUsage: 4.023 KB
             - HashTableSize: 0
             - InputRowCount: 65.000M (65000006)
               - __MAX_OF_InputRowCount: 1.254M (1253867)
               - __MIN_OF_InputRowCount: 1.246M (1245716)
             - PassThroughRowCount: 0
             - ResultAggAppendTime: 0ns
             - ResultGroupByAppendTime: 0ns
             - ResultIteratorTime: 0ns
             - RowsReturned: 0
             - StateAllocate: 0ns
             - StateDestroy: 0ns
             - StreamingTime: 0ns
             - UdafCacheHitCount: 0
             - UdafCachePopulateCount: 0
             - UdafLoadTime: 0ns
        PROJECT (plan_node_id=7):
          CommonMetrics:
             - OperatorTotalTime: 2.006ms
               - __MAX_OF_OperatorTotalTime: 2.537ms
               - __MIN_OF_OperatorTotalTime: 1.619ms
             - OutputChunkBytes: 31.297 GB
               - __MAX_OF_OutputChunkBytes: 618.219 MB
               - __MIN_OF_OutputChunkBytes: 614.200 MB
             - PullChunkNum: 16.809K (16809)
               - __MAX_OF_PullChunkNum: 326
               - __MIN_OF_PullChunkNum: 320
             - PullRowNum: 65.000M (65000006)
               - __MAX_OF_PullRowNum: 1.254M (1253867)
               - __MIN_OF_PullRowNum: 1.246M (1245716)
             - PullTotalTime: 139.690us
               - __MAX_OF_PullTotalTime: 165.899us
               - __MIN_OF_PullTotalTime: 115.670us
             - PushChunkNum: 16.809K (16809)
               - __MAX_OF_PushChunkNum: 326
               - __MIN_OF_PushChunkNum: 320
             - PushRowNum: 65.000M (65000006)
               - __MAX_OF_PushRowNum: 1.254M (1253867)
               - __MIN_OF_PushRowNum: 1.246M (1245716)
             - PushTotalTime: 1.858ms
               - __MAX_OF_PushTotalTime: 2.385ms
               - __MIN_OF_PushTotalTime: 1.472ms
             - RuntimeFilterNum: 0
             - RuntimeInFilterNum: 0
          UniqueMetrics:
             - CommonSubExprComputeTime: 107.545us
               - __MAX_OF_CommonSubExprComputeTime: 174.562us
               - __MIN_OF_CommonSubExprComputeTime: 81.343us
             - ExprComputeTime: 481.577us
               - __MAX_OF_ExprComputeTime: 629.858us
               - __MIN_OF_ExprComputeTime: 317.619us
        NESTLOOP_JOIN_PROBE (plan_node_id=6):
          CommonMetrics:
             - ConjunctsInputRows: 90.000M (90000009)
               - __MAX_OF_ConjunctsInputRows: 1.736M (1735569)
               - __MIN_OF_ConjunctsInputRows: 1.724M (1724190)
             - ConjunctsOutputRows: 65.000M (65000006)
               - __MAX_OF_ConjunctsOutputRows: 1.254M (1253867)
               - __MIN_OF_ConjunctsOutputRows: 1.246M (1245716)
             - ConjunctsTime: 189.248ms
               - __MAX_OF_ConjunctsTime: 255.063ms
               - __MIN_OF_ConjunctsTime: 128.422ms
             - OperatorTotalTime: 1s281ms
               - __MAX_OF_OperatorTotalTime: 1s547ms
               - __MIN_OF_OperatorTotalTime: 949.815ms
             - OutputChunkBytes: 31.902 GB
               - __MAX_OF_OutputChunkBytes: 630.176 MB
               - __MIN_OF_OutputChunkBytes: 626.080 MB
             - PullChunkNum: 16.809K (16809)
               - __MAX_OF_PullChunkNum: 326
               - __MIN_OF_PullChunkNum: 320
             - PullRowNum: 65.000M (65000006)
               - __MAX_OF_PullRowNum: 1.254M (1253867)
               - __MIN_OF_PullRowNum: 1.246M (1245716)
             - PullTotalTime: 1s280ms
               - __MAX_OF_PullTotalTime: 1s547ms
               - __MIN_OF_PullTotalTime: 949.529ms
             - PushChunkNum: 7.344K (7344)
               - __MAX_OF_PushChunkNum: 142
               - __MIN_OF_PushChunkNum: 141
             - PushRowNum: 30.000M (30000003)
               - __MAX_OF_PushRowNum: 578.523K (578523)
               - __MIN_OF_PushRowNum: 574.730K (574730)
             - PushTotalTime: 246.062us
               - __MAX_OF_PushTotalTime: 455.859us
               - __MIN_OF_PushTotalTime: 198.348us
             - RuntimeFilterNum: 0
             - RuntimeInFilterNum: 0
          UniqueMetrics:
             - JoinType: INNER_JOIN
             - JoinConjuncts: 1: event_day > 11: bound_day
             - PermuteRows: 0
        AGGREGATE_DISTINCT_BLOCKING_SOURCE (plan_node_id=3):
          CommonMetrics:
             - JoinRuntimeFilterEvaluate: 0
             - JoinRuntimeFilterHashTime: 0ns
             - JoinRuntimeFilterInputRows: 0
             - JoinRuntimeFilterOutputRows: 0
             - JoinRuntimeFilterTime: 0ns
             - OperatorTotalTime: 780.036ms
               - __MAX_OF_OperatorTotalTime: 979.616ms
               - __MIN_OF_OperatorTotalTime: 641.924ms
             - OutputChunkBytes: 14.585 GB
               - __MAX_OF_OutputChunkBytes: 287.999 MB
               - __MIN_OF_OutputChunkBytes: 286.111 MB
             - PullChunkNum: 7.344K (7344)
               - __MAX_OF_PullChunkNum: 142
               - __MIN_OF_PullChunkNum: 141
             - PullRowNum: 30.000M (30000003)
               - __MAX_OF_PullRowNum: 578.523K (578523)
               - __MIN_OF_PullRowNum: 574.730K (574730)
             - PullTotalTime: 779.121ms
               - __MAX_OF_PullTotalTime: 974.039ms
               - __MIN_OF_PullTotalTime: 641.151ms
             - PushChunkNum: 0
             - PushRowNum: 0
             - PushTotalTime: 0ns
             - RuntimeFilterNum: 0
             - RuntimeInFilterNum: 0
          UniqueMetrics:
      Pipeline (id=1):
         - LocalRfWaitingSet: 1
         - IsGroupExecution: false
         - ActiveTime: 1s383ms
           - __MAX_OF_ActiveTime: 1s656ms
           - __MIN_OF_ActiveTime: 1s92ms
         - BlockByInputEmpty: 1.329K (1329)
           - __MAX_OF_BlockByInputEmpty: 46
           - __MIN_OF_BlockByInputEmpty: 13
         - BlockByOutputFull: 0
         - BlockByPrecondition: 0
         - DegreeOfParallelism: 52
         - DriverTotalTime: 3s268ms
           - __MAX_OF_DriverTotalTime: 3s272ms
           - __MIN_OF_DriverTotalTime: 3s263ms
         - OverheadTime: 10.736ms
           - __MAX_OF_OverheadTime: 17.710ms
           - __MIN_OF_OverheadTime: 8.335ms
         - PeakDriverQueueSize: 2.064K (2064)
           - __MAX_OF_PeakDriverQueueSize: 60
           - __MIN_OF_PeakDriverQueueSize: 19
         - PendingTime: 0ns
           - InputEmptyTime: 1s858ms
             - __MAX_OF_InputEmptyTime: 2s165ms
             - __MIN_OF_InputEmptyTime: 1s580ms
             - FirstInputEmptyTime: 1s37ms
               - __MAX_OF_FirstInputEmptyTime: 1s271ms
               - __MIN_OF_FirstInputEmptyTime: 933.487ms
             - FollowupInputEmptyTime: 820.864ms
               - __MAX_OF_FollowupInputEmptyTime: 1s79ms
               - __MIN_OF_FollowupInputEmptyTime: 542.643ms
           - PreconditionBlockTime: 3.060ms
             - __MAX_OF_PreconditionBlockTime: 3.276ms
             - __MIN_OF_PreconditionBlockTime: 2.833ms
         - ScheduleCount: 1.941K (1941)
           - __MAX_OF_ScheduleCount: 56
           - __MIN_OF_ScheduleCount: 26
         - ScheduleTime: 1s885ms
           - __MAX_OF_ScheduleTime: 2s175ms
           - __MIN_OF_ScheduleTime: 1s611ms
         - TotalDegreeOfParallelism: 52
         - YieldByLocalWait: 0
         - YieldByPreempt: 0
         - YieldByTimeLimit: 732
           - __MAX_OF_YieldByTimeLimit: 18
           - __MIN_OF_YieldByTimeLimit: 10
        AGGREGATE_DISTINCT_BLOCKING_SINK (plan_node_id=3):
          CommonMetrics:
             - OperatorTotalTime: 1s272ms
               - __MAX_OF_OperatorTotalTime: 1s534ms
               - __MIN_OF_OperatorTotalTime: 1s8ms
             - OutputChunkBytes: 0.000 B
             - PullChunkNum: 0
             - PullRowNum: 0
             - PullTotalTime: 0ns
             - PushChunkNum: 8.112K (8112)
               - __MAX_OF_PushChunkNum: 156
               - __MIN_OF_PushChunkNum: 156
             - PushRowNum: 30.000M (30000003)
               - __MAX_OF_PushRowNum: 578.523K (578523)
               - __MIN_OF_PushRowNum: 574.730K (574730)
             - PushTotalTime: 1s272ms
               - __MAX_OF_PushTotalTime: 1s534ms
               - __MIN_OF_PushTotalTime: 1s8ms
             - RuntimeFilterNum: 0
             - RuntimeInFilterNum: 0
          UniqueMetrics:
             - GroupingKeys: 1: event_day, 3: payload
             - AggComputeTime: 1s272ms
               - __MAX_OF_AggComputeTime: 1s534ms
               - __MIN_OF_AggComputeTime: 1s7ms
             - AggFuncComputeTime: 0ns
             - ChunkBufferPeakMem: 0.000 B
             - ChunkBufferPeakSize: 0
             - ExprComputeTime: 906.446us
               - __MAX_OF_ExprComputeTime: 1.041ms
               - __MIN_OF_ExprComputeTime: 694.237us
             - ExprReleaseTime: 6.205ms
               - __MAX_OF_ExprReleaseTime: 12.782ms
               - __MIN_OF_ExprReleaseTime: 4.907ms
             - GetResultsTime: 778.296ms
               - __MAX_OF_GetResultsTime: 973.091ms
               - __MIN_OF_GetResultsTime: 640.449ms
             - HashTableMemoryUsage: 16.049 GB
               - __MAX_OF_HashTableMemoryUsage: 316.996 MB
               - __MIN_OF_HashTableMemoryUsage: 314.996 MB
             - HashTableSize: 30.000M (30000003)
               - __MAX_OF_HashTableSize: 578.523K (578523)
               - __MIN_OF_HashTableSize: 574.730K (574730)
             - InputRowCount: 30.000M (30000003)
               - __MAX_OF_InputRowCount: 578.523K (578523)
               - __MIN_OF_InputRowCount: 574.730K (574730)
             - PassThroughRowCount: 0
             - ResultAggAppendTime: 0ns
             - ResultGroupByAppendTime: 762.406ms
               - __MAX_OF_ResultGroupByAppendTime: 951.614ms
               - __MIN_OF_ResultGroupByAppendTime: 627.916ms
             - ResultIteratorTime: 0ns
             - RowsReturned: 0
             - StateAllocate: 0ns
             - StateDestroy: 0ns
             - StreamingTime: 0ns
             - UdafCacheHitCount: 0
             - UdafCachePopulateCount: 0
             - UdafLoadTime: 0ns
        CHUNK_ACCUMULATE (plan_node_id=2):
          CommonMetrics:
             - IsSubordinate
             - OperatorTotalTime: 390.677us
               - __MAX_OF_OperatorTotalTime: 488.531us
               - __MIN_OF_OperatorTotalTime: 279.062us
             - OutputChunkBytes: 14.585 GB
               - __MAX_OF_OutputChunkBytes: 287.999 MB
               - __MIN_OF_OutputChunkBytes: 286.111 MB
             - PullChunkNum: 8.112K (8112)
               - __MAX_OF_PullChunkNum: 156
               - __MIN_OF_PullChunkNum: 156
             - PullRowNum: 30.000M (30000003)
               - __MAX_OF_PullRowNum: 578.523K (578523)
               - __MIN_OF_PullRowNum: 574.730K (574730)
             - PullTotalTime: 88.013us
               - __MAX_OF_PullTotalTime: 110.724us
               - __MIN_OF_PullTotalTime: 67.167us
             - PushChunkNum: 8.112K (8112)
               - __MAX_OF_PushChunkNum: 156
               - __MIN_OF_PushChunkNum: 156
             - PushRowNum: 30.000M (30000003)
               - __MAX_OF_PushRowNum: 578.523K (578523)
               - __MIN_OF_PushRowNum: 574.730K (574730)
             - PushTotalTime: 301.412us
               - __MAX_OF_PushTotalTime: 389.997us
               - __MIN_OF_PushTotalTime: 203.971us
          UniqueMetrics:
        EXCHANGE_SOURCE (plan_node_id=2):
          CommonMetrics:
             - RuntimeFilterDesc: <0: RuntimeEmptyFilter(has_null=0, join_mode=, num_elements=1)RuntimeMinMax(type=50, has_null=0, _min=2024-03-10, _max=9999-12-31, left_close_interval=1, right_close_interval=1)> 
             - ConjunctsInputRows: 31.000M (31000003)
               - __MAX_OF_ConjunctsInputRows: 597.874K (597874)
               - __MIN_OF_ConjunctsInputRows: 593.982K (593982)
             - ConjunctsOutputRows: 30.000M (30000003)
               - __MAX_OF_ConjunctsOutputRows: 578.523K (578523)
               - __MIN_OF_ConjunctsOutputRows: 574.730K (574730)
             - ConjunctsTime: 89.176ms
               - __MAX_OF_ConjunctsTime: 106.938ms
               - __MIN_OF_ConjunctsTime: 68.224ms
             - JoinRuntimeFilterEvaluate: 260
               - __MAX_OF_JoinRuntimeFilterEvaluate: 5
               - __MIN_OF_JoinRuntimeFilterEvaluate: 5
             - JoinRuntimeFilterHashTime: 4.412us
               - __MAX_OF_JoinRuntimeFilterHashTime: 8.308us
               - __MIN_OF_JoinRuntimeFilterHashTime: 2.048us
             - JoinRuntimeFilterInputRows: 30.000M (30000003)
               - __MAX_OF_JoinRuntimeFilterInputRows: 578.523K (578523)
               - __MIN_OF_JoinRuntimeFilterInputRows: 574.730K (574730)
             - JoinRuntimeFilterOutputRows: 30.000M (30000003)
               - __MAX_OF_JoinRuntimeFilterOutputRows: 578.523K (578523)
               - __MIN_OF_JoinRuntimeFilterOutputRows: 574.730K (574730)
             - JoinRuntimeFilterTime: 526.626us
               - __MAX_OF_JoinRuntimeFilterTime: 6.457ms
               - __MIN_OF_JoinRuntimeFilterTime: 236.276us
             - OperatorTotalTime: 99.926ms
               - __MAX_OF_OperatorTotalTime: 130.916ms
               - __MIN_OF_OperatorTotalTime: 75.854ms
             - OutputChunkBytes: 14.585 GB
               - __MAX_OF_OutputChunkBytes: 287.999 MB
               - __MIN_OF_OutputChunkBytes: 286.111 MB
             - PullChunkNum: 8.112K (8112)
               - __MAX_OF_PullChunkNum: 156
               - __MIN_OF_PullChunkNum: 156
             - PullRowNum: 30.000M (30000003)
               - __MAX_OF_PullRowNum: 578.523K (578523)
               - __MIN_OF_PullRowNum: 574.730K (574730)
             - PullTotalTime: 99.854ms
               - __MAX_OF_PullTotalTime: 130.809ms
               - __MIN_OF_PullTotalTime: 75.812ms
             - PushChunkNum: 0
             - PushRowNum: 0
             - PushTotalTime: 0ns
             - RuntimeFilterNum: 1
             - RuntimeInFilterNum: 1
          UniqueMetrics:
             - BufferUnplugCount: 85
               - __MAX_OF_BufferUnplugCount: 3
               - __MIN_OF_BufferUnplugCount: 1
             - BytesPassThrough: 15.071 GB
               - __MAX_OF_BytesPassThrough: 772.062 MB
               - __MIN_OF_BytesPassThrough: 137.757 MB
             - BytesReceived: 0.000 B
             - ClosureBlockCount: 2.131K (2131)
               - __MAX_OF_ClosureBlockCount: 50
               - __MIN_OF_ClosureBlockCount: 30
             - ClosureBlockTime: 1s472ms
               - __MAX_OF_ClosureBlockTime: 4s740ms
               - __MIN_OF_ClosureBlockTime: 296.668ms
             - DecompressChunkTime: 0ns
             - DeserializeChunkTime: 0ns
             - PeakBufferMemoryBytes: 4.733 GB
               - __MAX_OF_PeakBufferMemoryBytes: 527.257 MB
               - __MIN_OF_PeakBufferMemoryBytes: 2.008 MB
             - ReceiverProcessTotalTime: 4.879ms
               - __MAX_OF_ReceiverProcessTotalTime: 10.353ms
               - __MIN_OF_ReceiverProcessTotalTime: 2.311ms
             - RequestReceived: 8.113K (8113)
               - __MAX_OF_RequestReceived: 157
               - __MIN_OF_RequestReceived: 156
             - WaitLockTime: 0ns
      Pipeline (id=0):
         - IsGroupExecution: false
         - ActiveTime: 62.926us
           - __MAX_OF_ActiveTime: 1.023ms
           - __MIN_OF_ActiveTime: 20.374us
         - BlockByInputEmpty: 0
         - BlockByOutputFull: 0
         - BlockByPrecondition: 0
         - DegreeOfParallelism: 52
         - DriverTotalTime: 2.696ms
           - __MAX_OF_DriverTotalTime: 3.717ms
           - __MIN_OF_DriverTotalTime: 2.475ms
         - PeakDriverQueueSize: 1.092K (1092)
           - __MAX_OF_PeakDriverQueueSize: 46
           - __MIN_OF_PeakDriverQueueSize: 0
         - PendingTime: 0ns
           - InputEmptyTime: 2.304ms
             - __MAX_OF_InputEmptyTime: 2.309ms
             - __MIN_OF_InputEmptyTime: 2.275ms
             - FirstInputEmptyTime: 2.304ms
               - __MAX_OF_FirstInputEmptyTime: 2.309ms
               - __MIN_OF_FirstInputEmptyTime: 2.275ms
         - ScheduleCount: 52
           - __MAX_OF_ScheduleCount: 1
           - __MIN_OF_ScheduleCount: 1
         - ScheduleTime: 2.633ms
           - __MAX_OF_ScheduleTime: 2.799ms
           - __MIN_OF_ScheduleTime: 2.440ms
         - TotalDegreeOfParallelism: 52
         - YieldByLocalWait: 0
         - YieldByPreempt: 0
         - YieldByTimeLimit: 0
        NESTLOOP_JOIN_BUILD (plan_node_id=6):
          CommonMetrics:
             - OperatorTotalTime: 50.366us
               - __MAX_OF_OperatorTotalTime: 984.267us
               - __MIN_OF_OperatorTotalTime: 17.143us
             - OutputChunkBytes: 0.000 B
             - PullChunkNum: 0
             - PullRowNum: 0
             - PullTotalTime: 0ns
             - PushChunkNum: 1
               - __MAX_OF_PushChunkNum: 1
               - __MIN_OF_PushChunkNum: 0
             - PushRowNum: 3
               - __MAX_OF_PushRowNum: 3
               - __MIN_OF_PushRowNum: 0
             - PushTotalTime: 124ns
               - __MAX_OF_PushTotalTime: 6.468us
               - __MIN_OF_PushTotalTime: 0ns
             - RuntimeFilterNum: 0
             - RuntimeInFilterNum: 0
          UniqueMetrics:
             - NumBuilders: 52
             - BuildChunks: 17
               - __MAX_OF_BuildChunks: 1
               - __MIN_OF_BuildChunks: 0
             - BuildRows: 54
               - __MAX_OF_BuildRows: 3
               - __MIN_OF_BuildRows: 0
        EXCHANGE_SOURCE (plan_node_id=5):
          CommonMetrics:
             - ConjunctsInputRows: 3
             - ConjunctsOutputRows: 3
             - ConjunctsTime: 797ns
             - JoinRuntimeFilterEvaluate: 0
             - JoinRuntimeFilterHashTime: 0ns
             - JoinRuntimeFilterInputRows: 0
             - JoinRuntimeFilterOutputRows: 0
             - JoinRuntimeFilterTime: 0ns
             - OperatorTotalTime: 22.453us
               - __MAX_OF_OperatorTotalTime: 49.771us
               - __MIN_OF_OperatorTotalTime: 10.746us
             - OutputChunkBytes: 15.000 B
               - __MAX_OF_OutputChunkBytes: 15.000 B
               - __MIN_OF_OutputChunkBytes: 0.000 B
             - PullChunkNum: 1
               - __MAX_OF_PullChunkNum: 1
               - __MIN_OF_PullChunkNum: 0
             - PullRowNum: 3
               - __MAX_OF_PullRowNum: 3
               - __MIN_OF_PullRowNum: 0
             - PullTotalTime: 628ns
               - __MAX_OF_PullTotalTime: 28.018us
               - __MIN_OF_PullTotalTime: 0ns
             - PushChunkNum: 0
             - PushRowNum: 0
             - PushTotalTime: 0ns
             - RuntimeFilterNum: 0
             - RuntimeInFilterNum: 0
          UniqueMetrics:
             - BufferUnplugCount: 0
             - BytesPassThrough: 31.000 B
               - __MAX_OF_BytesPassThrough: 31.000 B
               - __MIN_OF_BytesPassThrough: 0.000 B
             - BytesReceived: 0.000 B
             - ClosureBlockCount: 0
             - ClosureBlockTime: 0ns
             - DecompressChunkTime: 0ns
             - DeserializeChunkTime: 0ns
             - PeakBufferMemoryBytes: 31.000 B
               - __MAX_OF_PeakBufferMemoryBytes: 31.000 B
               - __MIN_OF_PeakBufferMemoryBytes: 0.000 B
             - ReceiverProcessTotalTime: 229ns
               - __MAX_OF_ReceiverProcessTotalTime: 6.482us
               - __MIN_OF_ReceiverProcessTotalTime: 0ns
             - RequestReceived: 2
               - __MAX_OF_RequestReceived: 1
               - __MIN_OF_RequestReceived: 0
             - WaitLockTime: 0ns
    Fragment 2:
       - BackendAddresses: 172.20.0.2:9060
       - InstanceIds: 019f690c-b658-7e47-9ce8-a82395a47085
       - EnableEventScheduler: true
       - BackendNum: 1
       - BackendProfileMergeTime: 374.301us
       - InitialProcessDriverCount: 262
       - InitialProcessMem: 9.328 GB
       - InstanceAllocatedMemoryUsage: 234.969 KB
       - InstanceDeallocatedMemoryUsage: 89.211 KB
       - InstanceNum: 1
       - InstancePeakMemoryUsage: 167.563 KB
       - QueryMemoryLimit: -1.000 B
      Pipeline (id=1):
         - IsGroupExecution: false
         - ActiveTime: 328.939us
         - BlockByInputEmpty: 1
         - BlockByOutputFull: 0
         - BlockByPrecondition: 0
         - DegreeOfParallelism: 1
         - DriverTotalTime: 1.239ms
         - PeakDriverQueueSize: 40
         - ScheduleCount: 2
         - TotalDegreeOfParallelism: 1
         - YieldByLocalWait: 0
         - YieldByPreempt: 0
         - YieldByTimeLimit: 0
        EXCHANGE_SINK (plan_node_id=5):
          CommonMetrics:
             - OperatorTotalTime: 67.787us
             - OutputChunkBytes: 0.000 B
             - PullChunkNum: 0
             - PullRowNum: 0
             - PullTotalTime: 0ns
             - PushChunkNum: 1
             - PushRowNum: 3
             - PushTotalTime: 10.801us
          UniqueMetrics:
             - DestID: 5
             - DestFragments: 019f690cb6587e47-9ce8a82395a47084
             - PartType: UNPARTITIONED
             - ChannelNum: 1
             - BytesPassThrough: 31.000 B
             - BytesSent: 0.000 B
             - BytesUnsent: 0.000 B
             - CompressTime: 0ns
             - CompressedBytes: 0.000 B
             - NetworkBandwidth: 0.000 B/sec
             - NetworkTime: 224.340us
             - OverallThroughput: 0.000 B/sec
             - OverallTime: 450.026us
             - PassThroughBufferPeakMemoryUsage: 688.000 B
             - RawInputBytes: 0.000 B
             - RequestSent: 0
             - RequestUnsent: 0
             - RpcAvgTime: 112.170us
             - RpcCount: 2
             - SerializeChunkTime: 0ns
             - SerializedBytes: 0.000 B
             - ShuffleChunkAppendCounter: 0
             - ShuffleChunkAppendTime: 0ns
             - ShuffleHashTime: 0ns
             - WaitTime: 966.968us
        OLAP_SCAN (plan_node_id=4):
          CommonMetrics:
             - JoinRuntimeFilterEvaluate: 0
             - JoinRuntimeFilterHashTime: 0ns
             - JoinRuntimeFilterInputRows: 0
             - JoinRuntimeFilterOutputRows: 0
             - JoinRuntimeFilterTime: 0ns
             - OperatorTotalTime: 484.028us
             - OutputChunkBytes: 15.000 B
             - PullChunkNum: 1
             - PullRowNum: 3
             - PullTotalTime: 209.240us
             - PushChunkNum: 0
             - PushRowNum: 0
             - PushTotalTime: 0ns
             - RuntimeFilterNum: 0
             - RuntimeInFilterNum: 0
          UniqueMetrics:
             - MorselQueueType: fixed_morsel_queue
             - NumHeavyExprs: 0
             - SharedScan: False
             - Table: build_range_multi_indexed
             - Database
             - Rollup: build_range_multi_indexed
             - BytesRead: 15.000 B
             - CachedPagesNum: 0
             - ChunkBufferCapacity: 64
             - CompressedBytesRead: 78.000 B
             - DefaultChunkBufferCapacity: 64
             - IOTaskExecTime: 206.774us
               - CreateSegmentIter: 32.382us
               - GetDelVec: 0ns
               - GetDeltaColumnGroup: 5.886us
               - GetRowsets: 4.187us
               - IOTime: 8.016us
               - ReadPKIndex: 0ns
               - SegmentInit: 86.334us
                 - BitmapIndexFilter: 0ns
                 - BitmapIndexFilterRows: 0
                 - BitmapIndexIteratorInit: 4.048us
                 - BloomFilterFilter: 0ns
                 - BloomFilterFilterRows: 0
                 - ColumnIteratorInit: 42.208us
                 - GetVectorRowRangesTime: 0ns
                 - GinFilter: 0ns
                   - GinDictFilter: 0ns
                   - GinDictNum: 0
                   - GinFilterRows: 0
                   - GinNGramDictNum: 0
                   - GinNGramFilteredDictNum: 0
                   - GinNgramDictFilter: 0ns
                   - GinPredicateFilteredDictNum: 0
                   - GinPrefixFilter: 0ns
                 - ProcessVectorDistanceAndIdTime: 0ns
                 - RemainingRowsAfterShortKeyFilter: 3
                 - SegmentMetadataFilterRows: 0
                 - SegmentRuntimeZoneMapFilterRows: 0
                 - SegmentZoneMapFilterRows: 0
                 - SegmentsMetadataFiltered: 0
                 - ShortKeyFilter: 1.564us
                 - ShortKeyFilterRows: 0
                 - ShortKeyRangeNumber: 0
                 - VectorIndexFilterRows: 0
                 - VectorSearchTime: 0ns
                 - ZoneMapIndexFilter: 2.594us
                 - ZoneMapIndexFilterRows: 0
               - SegmentRead: 34.624us
                 - BlockFetch: 4.916us
                 - BlockFetchCount: 1
                 - BlockSeek: 26.005us
                 - BlockSeekCount: 1
                 - ChunkCopy: 0ns
                 - DecompressT: 0ns
                 - DelVecFilterRows: 0
                 - PredFilter: 0ns
                 - PredFilterRows: 0
                 - RowsetsReadCount: 2
                 - SegmentsReadCount: 1
                 - TotalColumnsDataPageCount: 1
             - IOTaskWaitTime: 36.919us
             - MorselsCount: 1
             - PeakChunkBufferMemoryUsage: 20.017 KB
             - PeakChunkBufferSize: 2
             - PeakIOTasks: 1
             - PeakScanTaskQueueSize: 0
             - PrepareChunkSourceTime: 172.595us
             - PushdownAccessPaths: 0
             - PushdownPredicates: 0
             - RawRowsRead: 3
             - ReadPagesNum: 1
             - RowsRead: 3
             - RuntimeFilterEvalTime: 0ns
             - RuntimeFilterInputRows: 0
             - RuntimeFilterOutputRows: 0
             - ScanTime: 243.693us
             - SubmitTaskCount: 1
             - SubmitTaskTime: 6.757us
             - TabletCount: 1
             - UncompressedBytesRead: 74.000 B
      Pipeline (id=0):
         - IsGroupExecution: false
         - ActiveTime: 38.074us
         - BlockByInputEmpty: 0
         - BlockByOutputFull: 0
         - BlockByPrecondition: 0
         - DegreeOfParallelism: 1
         - DriverTotalTime: 57.500us
         - PeakDriverQueueSize: 0
         - ScheduleCount: 1
         - TotalDegreeOfParallelism: 1
         - YieldByLocalWait: 0
         - YieldByPreempt: 0
         - YieldByTimeLimit: 0
        NOOP_SINK (plan_node_id=4):
          CommonMetrics:
             - IsSubordinate
             - OperatorTotalTime: 352ns
             - OutputChunkBytes: 0.000 B
             - PullChunkNum: 0
             - PullRowNum: 0
             - PullTotalTime: 0ns
             - PushChunkNum: 0
             - PushRowNum: 0
             - PushTotalTime: 0ns
          UniqueMetrics:
        OLAP_SCAN_PREPARE (plan_node_id=4):
          CommonMetrics:
             - IsSubordinate
             - OperatorTotalTime: 37.612us
             - OutputChunkBytes: 0.000 B
             - PullChunkNum: 0
             - PullRowNum: 0
             - PullTotalTime: 31.057us
             - PushChunkNum: 0
             - PushRowNum: 0
             - PushTotalTime: 0ns
             - RuntimeFilterNum: 0
             - RuntimeInFilterNum: 0
          UniqueMetrics:
             - CaptureTabletRowsetsTime: 7.764us
    Fragment 3:
       - BackendAddresses: 172.20.0.2:9060
       - InstanceIds: 019f690c-b658-7e47-9ce8-a82395a47086
       - EnableEventScheduler: true
       - BackendNum: 1
       - BackendProfileMergeTime: 45.922ms
       - FragmentInstancePrepareTime: 4.634ms
       - InitialProcessDriverCount: 264
       - InitialProcessMem: 9.328 GB
       - InstanceAllocatedMemoryUsage: 90.853 GB
       - InstanceDeallocatedMemoryUsage: 82.398 GB
       - InstanceNum: 1
       - InstancePeakMemoryUsage: 20.665 GB
       - QueryMemoryLimit: -1.000 B
      Pipeline (id=3):
         - IsGroupExecution: false
         - ActiveTime: 2s440ms
           - __MAX_OF_ActiveTime: 2s555ms
           - __MIN_OF_ActiveTime: 2s190ms
         - BlockByInputEmpty: 6
           - __MAX_OF_BlockByInputEmpty: 1
           - __MIN_OF_BlockByInputEmpty: 0
         - BlockByOutputFull: 1.068K (1068)
           - __MAX_OF_BlockByOutputFull: 47
           - __MIN_OF_BlockByOutputFull: 9
         - BlockByPrecondition: 0
         - DegreeOfParallelism: 52
         - DriverTotalTime: 3s42ms
           - __MAX_OF_DriverTotalTime: 3s238ms
           - __MIN_OF_DriverTotalTime: 2s917ms
         - OverheadTime: 7.635ms
           - __MAX_OF_OverheadTime: 16.475ms
           - __MIN_OF_OverheadTime: 5.471ms
         - PeakDriverQueueSize: 4.050K (4050)
           - __MAX_OF_PeakDriverQueueSize: 104
           - __MIN_OF_PeakDriverQueueSize: 40
         - PendingTime: 0ns
           - InputEmptyTime: 39.976ms
             - __MAX_OF_InputEmptyTime: 75.033ms
             - __MIN_OF_InputEmptyTime: 21.343ms
             - FirstInputEmptyTime: 37.324ms
               - __MAX_OF_FirstInputEmptyTime: 73.176ms
               - __MIN_OF_FirstInputEmptyTime: 9.804ms
             - FollowupInputEmptyTime: 2.651ms
               - __MAX_OF_FollowupInputEmptyTime: 55.210ms
               - __MIN_OF_FollowupInputEmptyTime: 0ns
           - OutputFullTime: 398.944ms
             - __MAX_OF_OutputFullTime: 769.481ms
             - __MIN_OF_OutputFullTime: 256.702ms
           - PendingFinishTime: 4.637ms
             - __MAX_OF_PendingFinishTime: 241.129ms
             - __MIN_OF_PendingFinishTime: 0ns
         - ScheduleCount: 1.981K (1981)
           - __MAX_OF_ScheduleCount: 65
           - __MIN_OF_ScheduleCount: 26
         - ScheduleTime: 601.291ms
           - __MAX_OF_ScheduleTime: 1s46ms
           - __MIN_OF_ScheduleTime: 458.284ms
         - TotalDegreeOfParallelism: 52
         - YieldByLocalWait: 0
         - YieldByPreempt: 0
         - YieldByTimeLimit: 1.154K (1154)
           - __MAX_OF_YieldByTimeLimit: 26
           - __MIN_OF_YieldByTimeLimit: 17
        EXCHANGE_SINK (plan_node_id=2):
          CommonMetrics:
             - OperatorTotalTime: 2s421ms
               - __MAX_OF_OperatorTotalTime: 2s538ms
               - __MIN_OF_OperatorTotalTime: 2s178ms
             - OutputChunkBytes: 0.000 B
             - PullChunkNum: 0
             - PullRowNum: 0
             - PullTotalTime: 0ns
             - PushChunkNum: 7.608K (7608)
               - __MAX_OF_PushChunkNum: 147
               - __MIN_OF_PushChunkNum: 145
             - PushRowNum: 31.000M (31000003)
               - __MAX_OF_PushRowNum: 602.112K (602112)
               - __MIN_OF_PushRowNum: 587.819K (587819)
             - PushTotalTime: 2s313ms
               - __MAX_OF_PushTotalTime: 2s410ms
               - __MIN_OF_PushTotalTime: 2s151ms
             - SetFinishingTime: 107.761ms
               - __MAX_OF_SetFinishingTime: 181.325ms
               - __MIN_OF_SetFinishingTime: 27.564ms
          UniqueMetrics:
             - ChannelNum: 1
             - DestID: 2
             - DestFragments: 019f690cb6587e47-9ce8a82395a47084
             - PartType: HASH_PARTITIONED
             - ShuffleNumPerChannel: 52
             - PipelineLevelShuffle: Yes
             - TotalShuffleNum: 52
             - HashFunction: xxh3
             - BytesPassThrough: 15.071 GB
               - __MAX_OF_BytesPassThrough: 299.747 MB
               - __MIN_OF_BytesPassThrough: 292.632 MB
             - BytesSent: 0.000 B
             - BytesUnsent: 0.000 B
             - CompressTime: 0ns
             - CompressedBytes: 0.000 B
             - NetworkBandwidth: 0.000 B/sec
             - NetworkTime: 402.076ms
             - OverallThroughput: 0.000 B/sec
             - OverallTime: 2s360ms
             - PassThroughBufferPeakMemoryUsage: 627.860 MB
             - RawInputBytes: 0.000 B
             - RequestSent: 0
             - RequestUnsent: 0
             - RpcAvgTime: 2.294ms
             - RpcCount: 8.113K (8113)
             - SerializeChunkTime: 0ns
             - SerializedBytes: 0.000 B
             - ShuffleChunkAppendCounter: 395.607K (395607)
               - __MAX_OF_ShuffleChunkAppendCounter: 7.644K (7644)
               - __MIN_OF_ShuffleChunkAppendCounter: 7.540K (7540)
             - ShuffleChunkAppendTime: 1s322ms
               - __MAX_OF_ShuffleChunkAppendTime: 1s558ms
               - __MIN_OF_ShuffleChunkAppendTime: 1s78ms
             - ShuffleHashTime: 353.002ms
               - __MAX_OF_ShuffleHashTime: 390.898ms
               - __MIN_OF_ShuffleHashTime: 308.238ms
             - WaitTime: 996.522ms
        AGGREGATE_DISTINCT_STREAMING_SOURCE (plan_node_id=1):
          CommonMetrics:
             - JoinRuntimeFilterEvaluate: 0
             - JoinRuntimeFilterHashTime: 0ns
             - JoinRuntimeFilterInputRows: 0
             - JoinRuntimeFilterOutputRows: 0
             - JoinRuntimeFilterTime: 0ns
             - OperatorTotalTime: 11.381ms
               - __MAX_OF_OperatorTotalTime: 18.029ms
               - __MIN_OF_OperatorTotalTime: 4.445ms
             - OutputChunkBytes: 15.071 GB
               - __MAX_OF_OutputChunkBytes: 299.742 MB
               - __MIN_OF_OutputChunkBytes: 292.627 MB
             - PullChunkNum: 7.608K (7608)
               - __MAX_OF_PullChunkNum: 147
               - __MIN_OF_PullChunkNum: 145
             - PullRowNum: 31.000M (31000003)
               - __MAX_OF_PullRowNum: 602.112K (602112)
               - __MIN_OF_PullRowNum: 587.819K (587819)
             - PullTotalTime: 11.249ms
               - __MAX_OF_PullTotalTime: 17.842ms
               - __MIN_OF_PullTotalTime: 4.389ms
             - PushChunkNum: 0
             - PushRowNum: 0
             - PushTotalTime: 0ns
             - RuntimeFilterNum: 0
             - RuntimeInFilterNum: 0
          UniqueMetrics:
      Pipeline (id=2):
         - IsGroupExecution: false
         - ActiveTime: 1s400ms
           - __MAX_OF_ActiveTime: 1s571ms
           - __MIN_OF_ActiveTime: 1s211ms
         - BlockByInputEmpty: 44
           - __MAX_OF_BlockByInputEmpty: 7
           - __MIN_OF_BlockByInputEmpty: 0
         - BlockByOutputFull: 516
           - __MAX_OF_BlockByOutputFull: 29
           - __MIN_OF_BlockByOutputFull: 3
         - BlockByPrecondition: 0
         - DegreeOfParallelism: 52
         - DriverTotalTime: 2s133ms
           - __MAX_OF_DriverTotalTime: 2s221ms
           - __MIN_OF_DriverTotalTime: 2s73ms
         - OverheadTime: 10.677ms
           - __MAX_OF_OverheadTime: 24.641ms
           - __MIN_OF_OverheadTime: 6.220ms
         - PeakDriverQueueSize: 4.744K (4744)
           - __MAX_OF_PeakDriverQueueSize: 103
           - __MIN_OF_PeakDriverQueueSize: 56
         - PendingTime: 0ns
           - InputEmptyTime: 6.828ms
             - __MAX_OF_InputEmptyTime: 74.815ms
             - __MIN_OF_InputEmptyTime: 0ns
             - FirstInputEmptyTime: 3.125ms
               - __MAX_OF_FirstInputEmptyTime: 33.404ms
               - __MIN_OF_FirstInputEmptyTime: 0ns
             - FollowupInputEmptyTime: 3.703ms
               - __MAX_OF_FollowupInputEmptyTime: 51.542ms
               - __MIN_OF_FollowupInputEmptyTime: 0ns
           - OutputFullTime: 534.384ms
             - __MAX_OF_OutputFullTime: 745.487ms
             - __MIN_OF_OutputFullTime: 372.602ms
         - ScheduleCount: 1.222K (1222)
           - __MAX_OF_ScheduleCount: 41
           - __MIN_OF_ScheduleCount: 17
         - ScheduleTime: 733.341ms
           - __MAX_OF_ScheduleTime: 905.593ms
           - __MIN_OF_ScheduleTime: 553.270ms
         - TotalDegreeOfParallelism: 52
         - YieldByLocalWait: 0
         - YieldByPreempt: 0
         - YieldByTimeLimit: 734
           - __MAX_OF_YieldByTimeLimit: 25
           - __MIN_OF_YieldByTimeLimit: 10
        AGGREGATE_DISTINCT_STREAMING_SINK (plan_node_id=1):
          CommonMetrics:
             - OperatorTotalTime: 1s388ms
               - __MAX_OF_OperatorTotalTime: 1s545ms
               - __MIN_OF_OperatorTotalTime: 1s203ms
             - OutputChunkBytes: 0.000 B
             - PullChunkNum: 0
             - PullRowNum: 0
             - PullTotalTime: 0ns
             - PushChunkNum: 7.612K (7612)
               - __MAX_OF_PushChunkNum: 147
               - __MIN_OF_PushChunkNum: 146
             - PushRowNum: 31.000M (31000003)
               - __MAX_OF_PushRowNum: 602.112K (602112)
               - __MIN_OF_PushRowNum: 587.819K (587819)
             - PushTotalTime: 1s388ms
               - __MAX_OF_PushTotalTime: 1s545ms
               - __MIN_OF_PushTotalTime: 1s203ms
             - RuntimeFilterNum: 0
             - RuntimeInFilterNum: 0
          UniqueMetrics:
             - GroupingKeys: 1: event_day, 3: payload
             - AggComputeTime: 1s371ms
               - __MAX_OF_AggComputeTime: 1s531ms
               - __MIN_OF_AggComputeTime: 1s187ms
             - AggFuncComputeTime: 0ns
             - ChunkBufferPeakMem: 129.681 MB
               - __MAX_OF_ChunkBufferPeakMem: 130.129 MB
               - __MIN_OF_ChunkBufferPeakMem: 128.093 MB
             - ChunkBufferPeakSize: 61
               - __MAX_OF_ChunkBufferPeakSize: 62
               - __MIN_OF_ChunkBufferPeakSize: 61
             - ExprComputeTime: 1.715ms
               - __MAX_OF_ExprComputeTime: 8.385ms
               - __MIN_OF_ExprComputeTime: 1.171ms
             - ExprReleaseTime: 398.061us
               - __MAX_OF_ExprReleaseTime: 6.231ms
               - __MIN_OF_ExprReleaseTime: 132.784us
             - GetResultsTime: 9.132ms
               - __MAX_OF_GetResultsTime: 15.774ms
               - __MIN_OF_GetResultsTime: 2.248ms
             - HashTableMemoryUsage: 168.954 MB
               - __MAX_OF_HashTableMemoryUsage: 4.191 MB
               - __MIN_OF_HashTableMemoryUsage: 2.691 MB
             - HashTableSize: 283.185K (283185)
               - __MAX_OF_HashTableSize: 7.165K (7165)
               - __MIN_OF_HashTableSize: 4.096K (4096)
             - InputRowCount: 31.000M (31000003)
               - __MAX_OF_InputRowCount: 602.112K (602112)
               - __MIN_OF_InputRowCount: 587.819K (587819)
             - PassThroughRowCount: 30.717M (30716818)
               - __MAX_OF_PassThroughRowCount: 598.016K (598016)
               - __MIN_OF_PassThroughRowCount: 581.510K (581510)
             - ResultAggAppendTime: 0ns
             - ResultGroupByAppendTime: 8.865ms
               - __MAX_OF_ResultGroupByAppendTime: 15.477ms
               - __MIN_OF_ResultGroupByAppendTime: 2.170ms
             - ResultIteratorTime: 0ns
             - RowsReturned: 0
             - StateAllocate: 0ns
             - StateDestroy: 0ns
             - StreamingTime: 11.300ms
               - __MAX_OF_StreamingTime: 28.859ms
               - __MIN_OF_StreamingTime: 6.752ms
             - UdafCacheHitCount: 0
             - UdafCachePopulateCount: 0
             - UdafLoadTime: 0ns
        LOCAL_EXCHANGE_SOURCE (plan_node_id=1):
          CommonMetrics:
             - IsSubordinate
             - OperatorTotalTime: 1.265ms
               - __MAX_OF_OperatorTotalTime: 10.007ms
               - __MIN_OF_OperatorTotalTime: 823.750us
             - OutputChunkBytes: 15.071 GB
               - __MAX_OF_OutputChunkBytes: 299.742 MB
               - __MIN_OF_OutputChunkBytes: 292.627 MB
             - PullChunkNum: 7.612K (7612)
               - __MAX_OF_PullChunkNum: 147
               - __MIN_OF_PullChunkNum: 146
             - PullRowNum: 31.000M (31000003)
               - __MAX_OF_PullRowNum: 602.112K (602112)
               - __MIN_OF_PullRowNum: 587.819K (587819)
             - PullTotalTime: 1.240ms
               - __MAX_OF_PullTotalTime: 9.977ms
               - __MIN_OF_PullTotalTime: 811.213us
             - PushChunkNum: 0
             - PushRowNum: 0
             - PushTotalTime: 0ns
          UniqueMetrics:
      Pipeline (id=1):
         - IsGroupExecution: false
         - ActiveTime: 21.793ms
           - __MAX_OF_ActiveTime: 48.794ms
           - __MIN_OF_ActiveTime: 5.004ms
         - BlockByInputEmpty: 5.347K (5347)
           - __MAX_OF_BlockByInputEmpty: 126
           - __MIN_OF_BlockByInputEmpty: 59
         - BlockByOutputFull: 0
         - BlockByPrecondition: 0
         - DegreeOfParallelism: 52
         - DriverTotalTime: 1s433ms
           - __MAX_OF_DriverTotalTime: 1s534ms
           - __MIN_OF_DriverTotalTime: 1s127ms
         - OverheadTime: 4.873ms
           - __MAX_OF_OverheadTime: 17.938ms
           - __MIN_OF_OverheadTime: 805.137us
         - PeakDriverQueueSize: 4.888K (4888)
           - __MAX_OF_PeakDriverQueueSize: 103
           - __MIN_OF_PeakDriverQueueSize: 87
         - PendingTime: 0ns
           - InputEmptyTime: 970.757ms
             - __MAX_OF_InputEmptyTime: 1s107ms
             - __MIN_OF_InputEmptyTime: 713.992ms
             - FirstInputEmptyTime: 10.229ms
               - __MAX_OF_FirstInputEmptyTime: 23.180ms
               - __MIN_OF_FirstInputEmptyTime: 4.447ms
             - FollowupInputEmptyTime: 960.527ms
               - __MAX_OF_FollowupInputEmptyTime: 1s96ms
               - __MIN_OF_FollowupInputEmptyTime: 702.567ms
         - ScheduleCount: 5.400K (5400)
           - __MAX_OF_ScheduleCount: 127
           - __MIN_OF_ScheduleCount: 60
         - ScheduleTime: 1s411ms
           - __MAX_OF_ScheduleTime: 1s503ms
           - __MIN_OF_ScheduleTime: 1s120ms
         - TotalDegreeOfParallelism: 52
         - YieldByLocalWait: 0
         - YieldByPreempt: 0
         - YieldByTimeLimit: 0
        LOCAL_EXCHANGE_SINK (plan_node_id=1):
          CommonMetrics:
             - IsSubordinate
             - OperatorTotalTime: 1.712ms
               - __MAX_OF_OperatorTotalTime: 17.911ms
               - __MIN_OF_OperatorTotalTime: 457.590us
             - OutputChunkBytes: 0.000 B
             - PullChunkNum: 0
             - PullRowNum: 0
             - PullTotalTime: 0ns
             - PushChunkNum: 7.612K (7612)
               - __MAX_OF_PushChunkNum: 160
               - __MIN_OF_PushChunkNum: 79
             - PushRowNum: 31.000M (31000003)
               - __MAX_OF_PushRowNum: 646.801K (646801)
               - __MIN_OF_PushRowNum: 322.922K (322922)
             - PushTotalTime: 1.703ms
               - __MAX_OF_PushTotalTime: 17.909ms
               - __MIN_OF_PushTotalTime: 455.487us
          UniqueMetrics:
             - Type: Passthrough
             - ShuffleNum: 52
             - LocalExchangePeakMemoryUsage: 3.143 GB
             - LocalExchangePeakNumRows: 6.247M (6247288)
        CHUNK_ACCUMULATE (plan_node_id=0):
          CommonMetrics:
             - IsSubordinate
             - OperatorTotalTime: 579.775us
               - __MAX_OF_OperatorTotalTime: 11.290ms
               - __MIN_OF_OperatorTotalTime: 122.072us
             - OutputChunkBytes: 15.071 GB
               - __MAX_OF_OutputChunkBytes: 321.989 MB
               - __MIN_OF_OutputChunkBytes: 160.756 MB
             - PullChunkNum: 7.612K (7612)
               - __MAX_OF_PullChunkNum: 160
               - __MIN_OF_PullChunkNum: 79
             - PullRowNum: 31.000M (31000003)
               - __MAX_OF_PullRowNum: 646.801K (646801)
               - __MIN_OF_PullRowNum: 322.922K (322922)
             - PullTotalTime: 49.794us
               - __MAX_OF_PullTotalTime: 153.948us
               - __MIN_OF_PullTotalTime: 24.204us
             - PushChunkNum: 7.612K (7612)
               - __MAX_OF_PushChunkNum: 160
               - __MIN_OF_PushChunkNum: 79
             - PushRowNum: 31.000M (31000003)
               - __MAX_OF_PushRowNum: 646.801K (646801)
               - __MIN_OF_PushRowNum: 322.922K (322922)
             - PushTotalTime: 526.783us
               - __MAX_OF_PushTotalTime: 11.238ms
               - __MIN_OF_PushTotalTime: 91.333us
          UniqueMetrics:
        OLAP_SCAN (plan_node_id=0):
          CommonMetrics:
             - RuntimeFilterDesc: <0: RuntimeEmptyFilter(has_null=0, join_mode=, num_elements=1)RuntimeMinMax(type=50, has_null=0, _min=2024-03-10, _max=9999-12-31, left_close_interval=1, right_close_interval=1)> 
             - CloseTime: 1.036ms
               - __MAX_OF_CloseTime: 2.094ms
               - __MIN_OF_CloseTime: 295.347us
             - JoinRuntimeFilterEvaluate: 0
             - JoinRuntimeFilterHashTime: 0ns
             - JoinRuntimeFilterInputRows: 31.000M (31000003)
               - __MAX_OF_JoinRuntimeFilterInputRows: 646.801K (646801)
               - __MIN_OF_JoinRuntimeFilterInputRows: 322.922K (322922)
             - JoinRuntimeFilterOutputRows: 31.000M (31000003)
               - __MAX_OF_JoinRuntimeFilterOutputRows: 646.801K (646801)
               - __MIN_OF_JoinRuntimeFilterOutputRows: 322.922K (322922)
             - JoinRuntimeFilterTime: 295.388us
               - __MAX_OF_JoinRuntimeFilterTime: 3.236ms
               - __MIN_OF_JoinRuntimeFilterTime: 96.111us
             - OperatorTotalTime: 14.627ms
               - __MAX_OF_OperatorTotalTime: 33.799ms
               - __MIN_OF_OperatorTotalTime: 3.519ms
             - OutputChunkBytes: 15.071 GB
               - __MAX_OF_OutputChunkBytes: 321.989 MB
               - __MIN_OF_OutputChunkBytes: 160.756 MB
             - PullChunkNum: 7.612K (7612)
               - __MAX_OF_PullChunkNum: 160
               - __MIN_OF_PullChunkNum: 79
             - PullRowNum: 31.000M (31000003)
               - __MAX_OF_PullRowNum: 646.801K (646801)
               - __MIN_OF_PullRowNum: 322.922K (322922)
             - PullTotalTime: 13.585ms
               - __MAX_OF_PullTotalTime: 33.293ms
               - __MIN_OF_PullTotalTime: 2.208ms
             - PushChunkNum: 0
             - PushRowNum: 0
             - PushTotalTime: 0ns
             - RuntimeFilterNum: 1
             - RuntimeInFilterNum: 0
          UniqueMetrics:
             - MorselQueueType: fixed_morsel_queue
             - NumHeavyExprs: 0
             - SharedScan: False
             - Table: fact_probe_indexed
             - Database
             - Rollup: fact_probe_indexed
             - BytesRead: 15.077 GB
               - __MAX_OF_BytesRead: 161.210 MB
               - __MIN_OF_BytesRead: 160.443 MB
             - CachedPagesNum: 0
             - ChunkBufferCapacity: 3.328K (3328)
             - CompressedBytesRead: 359.055 MB
               - __MAX_OF_CompressedBytesRead: 3.749 MB
               - __MIN_OF_CompressedBytesRead: 3.731 MB
             - DefaultChunkBufferCapacity: 3.328K (3328)
             - IOTaskExecTime: 1s325ms
               - __MAX_OF_IOTaskExecTime: 1s478ms
               - __MIN_OF_IOTaskExecTime: 875.062ms
               - CreateSegmentIter: 96.737us
                 - __MAX_OF_CreateSegmentIter: 3.352ms
                 - __MIN_OF_CreateSegmentIter: 20.150us
               - GetDelVec: 0ns
               - GetDeltaColumnGroup: 37.170us
                 - __MAX_OF_GetDeltaColumnGroup: 3.291ms
                 - __MIN_OF_GetDeltaColumnGroup: 1.189us
               - GetRowsets: 4.156us
                 - __MAX_OF_GetRowsets: 18.954us
                 - __MIN_OF_GetRowsets: 1.385us
               - IOTime: 38.667ms
                 - __MAX_OF_IOTime: 74.117ms
                 - __MIN_OF_IOTime: 16.620ms
               - LateMaterialize: 16.654ms
                 - __MAX_OF_LateMaterialize: 99.657ms
                 - __MIN_OF_LateMaterialize: 3.750ms
               - LateMaterializeRows: 532.480K (532480)
                 - __MAX_OF_LateMaterializeRows: 16.384K (16384)
                 - __MIN_OF_LateMaterializeRows: 4.096K (4096)
               - ReadPKIndex: 0ns
               - SegmentInit: 678.821us
                 - __MAX_OF_SegmentInit: 8.411ms
                 - __MIN_OF_SegmentInit: 127.374us
                 - BitmapIndexFilter: 161.601us
                   - __MAX_OF_BitmapIndexFilter: 7.666ms
                   - __MIN_OF_BitmapIndexFilter: 13.301us
                 - BitmapIndexFilterRows: 0
                 - BitmapIndexIteratorInit: 56.621us
                   - __MAX_OF_BitmapIndexIteratorInit: 3.129ms
                   - __MIN_OF_BitmapIndexIteratorInit: 7.221us
                 - BloomFilterFilter: 2.179us
                   - __MAX_OF_BloomFilterFilter: 16.231us
                   - __MIN_OF_BloomFilterFilter: 384ns
                 - BloomFilterFilterRows: 0
                 - ColumnIteratorInit: 168.891us
                   - __MAX_OF_ColumnIteratorInit: 3.421ms
                   - __MIN_OF_ColumnIteratorInit: 10.793us
                 - GetVectorRowRangesTime: 0ns
                 - GinFilter: 0ns
                   - GinDictFilter: 0ns
                   - GinDictNum: 0
                   - GinFilterRows: 0
                   - GinNGramDictNum: 0
                   - GinNGramFilteredDictNum: 0
                   - GinNgramDictFilter: 0ns
                   - GinPredicateFilteredDictNum: 0
                   - GinPrefixFilter: 0ns
                 - ProcessVectorDistanceAndIdTime: 0ns
                 - RemainingRowsAfterShortKeyFilter: 100.000M (100000010)
                   - __MAX_OF_RemainingRowsAfterShortKeyFilter: 1.043M (1042707)
                   - __MIN_OF_RemainingRowsAfterShortKeyFilter: 1.040M (1040379)
                 - SegmentMetadataFilterRows: 0
                 - SegmentRuntimeZoneMapFilterRows: 0
                 - SegmentZoneMapFilterRows: 0
                 - SegmentsMetadataFiltered: 0
                 - ShortKeyFilter: 5.881us
                   - __MAX_OF_ShortKeyFilter: 86.564us
                   - __MIN_OF_ShortKeyFilter: 715ns
                 - ShortKeyFilterRows: 0
                 - ShortKeyRangeNumber: 0
                 - VectorIndexFilterRows: 0
                 - VectorSearchTime: 0ns
                 - ZoneMapIndexFilter: 48.229us
                   - __MAX_OF_ZoneMapIndexFilter: 210.794us
                   - __MIN_OF_ZoneMapIndexFilter: 18.532us
                 - ZoneMapIndexFilterRows: 67.633M (67633152)
                   - __MAX_OF_ZoneMapIndexFilterRows: 704.512K (704512)
                   - __MIN_OF_ZoneMapIndexFilterRows: 704.512K (704512)
               - SegmentRead: 1s280ms
                 - __MAX_OF_SegmentRead: 1s415ms
                 - __MIN_OF_SegmentRead: 853.751ms
                 - BlockFetch: 1s272ms
                   - __MAX_OF_BlockFetch: 1s409ms
                   - __MIN_OF_BlockFetch: 850.415ms
                 - BlockFetchCount: 7.996K (7996)
                   - __MAX_OF_BlockFetchCount: 85
                   - __MIN_OF_BlockFetchCount: 83
                 - BlockSeek: 259.237us
                   - __MAX_OF_BlockSeek: 6.720ms
                   - __MIN_OF_BlockSeek: 64.724us
                 - BlockSeekCount: 240.047K (240047)
                   - __MAX_OF_BlockSeekCount: 2.518K (2518)
                   - __MIN_OF_BlockSeekCount: 2.412K (2412)
                 - ChunkCopy: 65.068us
                   - __MAX_OF_ChunkCopy: 247.847us
                   - __MIN_OF_ChunkCopy: 49.840us
                 - DecompressT: 380.462ms
                   - __MAX_OF_DecompressT: 468.348ms
                   - __MIN_OF_DecompressT: 244.340ms
                 - DelVecFilterRows: 0
                 - PredFilter: 3.836ms
                   - __MAX_OF_PredFilter: 17.766ms
                   - __MIN_OF_PredFilter: 1.333ms
                 - PredFilterRows: 1.367M (1366855)
                   - __MAX_OF_PredFilterRows: 15.257K (15257)
                   - __MIN_OF_PredFilterRows: 13.404K (13404)
                 - RowsetsReadCount: 130
                   - __MAX_OF_RowsetsReadCount: 4
                   - __MIN_OF_RowsetsReadCount: 1
                 - SegmentsReadCount: 130
                   - __MAX_OF_SegmentsReadCount: 4
                   - __MIN_OF_SegmentsReadCount: 1
                 - TotalColumnsDataPageCount: 787.480K (787480)
                   - __MAX_OF_TotalColumnsDataPageCount: 8.211K (8211)
                   - __MIN_OF_TotalColumnsDataPageCount: 8.192K (8192)
             - IOTaskWaitTime: 9.925ms
               - __MAX_OF_IOTaskWaitTime: 23.779ms
               - __MIN_OF_IOTaskWaitTime: 2.418ms
             - MorselsCount: 96
               - __MAX_OF_MorselsCount: 2
               - __MIN_OF_MorselsCount: 1
             - PeakChunkBufferMemoryUsage: 428.410 MB
             - PeakChunkBufferSize: 18
             - PeakIOTasks: 1
               - __MAX_OF_PeakIOTasks: 2
               - __MIN_OF_PeakIOTasks: 1
             - PeakScanTaskQueueSize: 874
               - __MAX_OF_PeakScanTaskQueueSize: 30
               - __MIN_OF_PeakScanTaskQueueSize: 2
             - PrepareChunkSourceTime: 721.689us
               - __MAX_OF_PrepareChunkSourceTime: 4.304ms
               - __MIN_OF_PrepareChunkSourceTime: 197.899us
             - PushdownAccessPaths: 0
             - PushdownPredicates: 2
             - RawRowsRead: 32.367M (32366858)
               - __MAX_OF_RawRowsRead: 338.195K (338195)
               - __MIN_OF_RawRowsRead: 335.867K (335867)
             - ReadPagesNum: 244.467K (244467)
               - __MAX_OF_ReadPagesNum: 2.552K (2552)
               - __MIN_OF_ReadPagesNum: 2.539K (2539)
             - RowsRead: 31.000M (31000003)
               - __MAX_OF_RowsRead: 323.702K (323702)
               - __MIN_OF_RowsRead: 322.161K (322161)
             - RuntimeFilterEvalTime: 974.569us
               - __MAX_OF_RuntimeFilterEvalTime: 18.672ms
               - __MIN_OF_RuntimeFilterEvalTime: 326.309us
             - RuntimeFilterInputRows: 31.974M (31973642)
               - __MAX_OF_RuntimeFilterInputRows: 334.099K (334099)
               - __MIN_OF_RuntimeFilterInputRows: 331.771K (331771)
             - RuntimeFilterOutputRows: 31.000M (31000003)
               - __MAX_OF_RuntimeFilterOutputRows: 323.702K (323702)
               - __MIN_OF_RuntimeFilterOutputRows: 322.161K (322161)
             - ScanTime: 1s335ms
               - __MAX_OF_ScanTime: 1s495ms
               - __MIN_OF_ScanTime: 885.084ms
             - SubmitTaskCount: 1.194K (1194)
               - __MAX_OF_SubmitTaskCount: 27
               - __MIN_OF_SubmitTaskCount: 11
             - SubmitTaskTime: 8.425ms
               - __MAX_OF_SubmitTaskTime: 23.892ms
               - __MIN_OF_SubmitTaskTime: 402.233us
             - TabletCount: 96
             - UncompressedBytesRead: 14.909 GB
               - __MAX_OF_UncompressedBytesRead: 159.436 MB
               - __MIN_OF_UncompressedBytesRead: 158.634 MB
      Pipeline (id=0):
         - IsGroupExecution: false
         - ActiveTime: 22.844us
           - __MAX_OF_ActiveTime: 42.026us
           - __MIN_OF_ActiveTime: 10.803us
         - BlockByInputEmpty: 0
         - BlockByOutputFull: 0
         - BlockByPrecondition: 0
         - DegreeOfParallelism: 52
         - DriverTotalTime: 64.844us
           - __MAX_OF_DriverTotalTime: 178.742us
           - __MIN_OF_DriverTotalTime: 31.853us
         - PeakDriverQueueSize: 51
           - __MAX_OF_PeakDriverQueueSize: 3
           - __MIN_OF_PeakDriverQueueSize: 0
         - ScheduleCount: 52
           - __MAX_OF_ScheduleCount: 1
           - __MIN_OF_ScheduleCount: 1
         - TotalDegreeOfParallelism: 52
         - YieldByLocalWait: 0
         - YieldByPreempt: 0
         - YieldByTimeLimit: 0
        NOOP_SINK (plan_node_id=0):
          CommonMetrics:
             - IsSubordinate
             - OperatorTotalTime: 368ns
               - __MAX_OF_OperatorTotalTime: 632ns
               - __MIN_OF_OperatorTotalTime: 155ns
             - OutputChunkBytes: 0.000 B
             - PullChunkNum: 0
             - PullRowNum: 0
             - PullTotalTime: 0ns
             - PushChunkNum: 0
             - PushRowNum: 0
             - PushTotalTime: 0ns
          UniqueMetrics:
        OLAP_SCAN_PREPARE (plan_node_id=0):
          CommonMetrics:
             - RuntimeFilterDesc: <0: RuntimeEmptyFilter(has_null=0, join_mode=, num_elements=1)RuntimeMinMax(type=50, has_null=0, _min=2024-03-10, _max=9999-12-31, left_close_interval=1, right_close_interval=1)> 
             - IsSubordinate
             - OperatorTotalTime: 31.967us
               - __MAX_OF_OperatorTotalTime: 71.299us
               - __MIN_OF_OperatorTotalTime: 15.580us
             - OutputChunkBytes: 0.000 B
             - PullChunkNum: 0
             - PullRowNum: 0
             - PullTotalTime: 17.977us
               - __MAX_OF_PullTotalTime: 34.638us
               - __MIN_OF_PullTotalTime: 7.673us
             - PushChunkNum: 0
             - PushRowNum: 0
             - PushTotalTime: 0ns
             - RuntimeFilterNum: 1
             - RuntimeInFilterNum: 0
          UniqueMetrics:
             - CaptureTabletRowsetsTime: 5.846us
               - __MAX_OF_CaptureTabletRowsetsTime: 10.522us
               - __MIN_OF_CaptureTabletRowsetsTime: 2.308us
    PerTableScanStats:
       - TableNum: 2
       - RawScanRows: 32.367M (32366861)
       - ScanBytes: 15.077 GB
       - ScanRows: 31.000M (31000006)
      Table: build_range_multi_indexed:
         - HostNum: 1
         - RawScanRows: 3
         - ScanBytes: 15.000 B
         - ScanRows: 3
        Host: 172.20.0.2:9060:
           - RawScanRows: 3
           - ScanBytes: 15.000 B
           - ScanRows: 3
      Table: fact_probe_indexed:
         - HostNum: 1
         - RawScanRows: 32.367M (32366858)
         - ScanBytes: 15.077 GB
         - ScanRows: 31.000M (31000003)
        Host: 172.20.0.2:9060:
           - RawScanRows: 32.367M (32366858)
           - ScanBytes: 15.077 GB
           - ScanRows: 31.000M (31000003)
```

</details>
