# Materialized View Task Run Details

This document explains the internal details of materialized view (MV) refresh task runs, which can be helpful for understanding MV refresh behavior, troubleshooting issues, and monitoring MV performance.

## Overview

When StarRocks refreshes a materialized view, it creates a task run that contains detailed information about the refresh operation. This information is stored in the `MVTaskRunExtraMessage` object and can be queried through system views.

## Task Run Extra Message Fields

#### forceRefresh
- **Type**: Boolean
- **Description**: Indicates whether this is a forced refresh that bypasses normal refresh conditions
- **Usage**: Set to `true` when manually triggering a full refresh using `REFRESH MATERIALIZED VIEW ... FORCE`
- **Default**: `false`

#### partitionStart
- **Type**: String
- **Description**: The starting partition boundary for this refresh operation
- **Usage**: Defines the lower bound of the partition range to refresh
- **Format**: Partition key value (e.g., `"2024-01-01"` for date partitions)
- **Example**: When refreshing data from January 2024 onwards, this would be `"2024-01-01"`

#### partitionEnd
- **Type**: String
- **Description**: The ending partition boundary for this refresh operation
- **Usage**: Defines the upper bound of the partition range to refresh
- **Format**: Partition key value (e.g., `"2024-01-31"` for date partitions)
- **Example**: When refreshing data up to January 2024, this would be `"2024-01-31"`

#### mvPartitionsToRefresh
- **Type**: Set of Strings
- **Description**: The set of materialized view partitions that are scheduled to be refreshed in this task run
- **Usage**: Tracks which MV partitions will be updated
- **Size Limit**: Automatically truncated to `max_mv_task_run_meta_message_values_length` (default: 100) to prevent excessive metadata storage
- **Example**: `["p20240101", "p20240102", "p20240103"]`
- **Note**: This represents the MV's own partitions, not the base table partitions

#### refBasePartitionsToRefreshMap
- **Type**: Map of String to Set of Strings
- **Description**: Maps reference base table names to the set of their partitions that should be refreshed
- **Usage**: Set during the MV plan scheduler stage; tracks which base table partitions need to be scanned
- **Format**: `{tableName -> Set<partitionName>}`
- **Size Limit**: Automatically truncated to `max_mv_task_run_meta_message_values_length`
- **Example**: 
  ```json
  {
    "orders": ["p20240101", "p20240102"],
    "customers": ["p202401"]
  }
  ```
- **Note**: This is the **planned** set of partitions before optimization

#### basePartitionsToRefreshMap
- **Type**: Map of String to Set of Strings
- **Description**: Maps all base table names to the actual set of their partitions that were refreshed during execution
- **Usage**: Set after the MV version map is committed; reflects the real partitions used by the optimizer
- **Format**: `{tableName -> Set<partitionName>}`
- **Size Limit**: Automatically truncated to `max_mv_task_run_meta_message_values_length`
- **Example**: 
  ```json
  {
    "orders": ["p20240101", "p20240102"],
    "line_items": ["p20240101_batch1", "p20240101_batch2"]
  }
  ```
- **Note**: This is the **actual** set of partitions after query optimization and execution

**Difference between refBasePartitionsToRefreshMap and basePartitionsToRefreshMap:**
- `refBasePartitionsToRefreshMap`: Planned partitions before optimization (usually for the main reference table)
- `basePartitionsToRefreshMap`: Actual partitions after optimization (includes all tables and optimized partition sets)

#### nextPartitionStart
- **Type**: String
- **Description**: The starting partition boundary for the next incremental refresh
- **Usage**: Used when a refresh operation is split across multiple task runs due to resource limits or large data volumes
- **Example**: If current run refreshes up to `"2024-01-15"`, this might be `"2024-01-16"`

#### nextPartitionEnd
- **Type**: String
- **Description**: The ending partition boundary for the next incremental refresh
- **Usage**: Paired with `nextPartitionStart` to define the range for the next refresh iteration
- **Example**: `"2024-01-31"` for the next batch of partitions to process

#### nextPartitionValues
- **Type**: String
- **Description**: Serialized partition values for the next refresh (used for list partitioning or complex partition schemes)
- **Usage**: Stores specific partition values when simple start/end ranges are insufficient
- **Example**: `"('US', 'ACTIVE'), ('UK', 'ACTIVE')"` for multi-column list partitions

#### processStartTime
- **Type**: Long (timestamp in milliseconds)
- **Description**: The timestamp when the task run actually started processing (excluding pending time)
- **Usage**: Used to calculate actual processing time: `finishTime - processStartTime`
- **Formula**: `Processing Time = Finish Time - Process Start Time` (excludes queue waiting time)
- **Example**: `1704067200000` (2024-01-01 00:00:00 UTC)

#### executeOption
- **Type**: ExecuteOption object
- **Description**: Configuration options for task execution
- **Default**: Priority = LOWEST, isMergeRedundant = false
- **Fields**:
  - `priority`: Task execution priority (values from `Constants.TaskRunPriority`)
    - `HIGHEST`: 0
    - `HIGH`: 32
    - `NORMAL`: 64
    - `LOW`: 96
    - `LOWEST`: 127
  - `isMergeRedundant`: Whether to merge redundant refresh operations
  - `properties`: Additional execution properties (Map<String, String>)

#### planBuilderMessage
- **Type**: Map of String to String
- **Description**: Diagnostic messages and metadata from the query plan builder
- **Usage**: Contains information about query planning, optimization decisions, and potential issues
- **Size Limit**: Automatically truncated to `max_mv_task_run_meta_message_values_length`

#### refreshMode
- **Type**: String
- **Description**: The refresh mode used for this task run
- **Usage**: Indicates how the MV was refreshed
- **Possible Values**:
  - `"COMPLETE"`: Full refresh of all partitions
  - `"PARTIAL"`: Incremental refresh of specific partitions
  - `"FORCE"`: Forced refresh bypassing staleness checks
  - `""` (empty): Default or unspecified mode

#### adaptivePartitionRefreshNumber
- **Type**: Integer
- **Description**: The number of partitions that should be refreshed in each iteration when using adaptive partition refresh
- **Usage**: Automatically determined based on system resources and data volume to optimize refresh performance
- **Default**: -1 (not set or not using adaptive refresh)
- **Example**: `10` means refresh 10 partitions at a time

## Querying Task Run Details

You can query materialized view task run information through the system views:

### Using information_schema.task_runs

```sql
SELECT 
    TASK_NAME,
    CREATE_TIME,
    FINISH_TIME,
    STATE,
    EXTRA_MESSAGE
FROM information_schema.task_runs
WHERE TASK_NAME LIKE 'mv-%'
ORDER BY CREATE_TIME DESC
LIMIT 10;
```

### Parsing Extra Message JSON

The `EXTRA_MESSAGE` column contains the JSON representation of `MVTaskRunExtraMessage`:

```sql
SELECT 
    TASK_NAME,
    CREATE_TIME,
    get_json_string(EXTRA_MESSAGE, '$.refreshMode') AS refresh_mode,
    get_json_string(EXTRA_MESSAGE, '$.forceRefresh') AS force_refresh,
    get_json_string(EXTRA_MESSAGE, '$.mvPartitionsToRefresh') AS mv_partitions,
    get_json_int(EXTRA_MESSAGE, '$.processStartTime') AS process_start_ms,
    get_json_int(EXTRA_MESSAGE, '$.adaptivePartitionRefreshNumber') AS adaptive_batch_size
FROM information_schema.task_runs
WHERE TASK_NAME = 'mv-12345'
ORDER BY CREATE_TIME DESC;
```

## Understanding Refresh Performance

### Calculating Processing Time

```sql
SELECT 
    TASK_NAME,
    FINISH_TIME,
    get_json_bigint(EXTRA_MESSAGE, '$.processStartTime') AS process_start_time,
    (unix_timestamp(FINISH_TIME) * 1000 - 
     get_json_bigint(EXTRA_MESSAGE, '$.processStartTime')) / 1000 AS processing_seconds
FROM information_schema.task_runs
WHERE TASK_NAME LIKE 'mv-%' AND STATE = 'SUCCESS';
```

### Analyzing Partition Refresh Patterns

```sql
SELECT 
    TASK_NAME,
    CREATE_TIME,
    get_json_string(EXTRA_MESSAGE, '$.partitionStart') AS start_partition,
    get_json_string(EXTRA_MESSAGE, '$.partitionEnd') AS end_partition,
    get_json_string(EXTRA_MESSAGE, '$.nextPartitionStart') AS next_start,
    get_json_string(EXTRA_MESSAGE, '$.nextPartitionEnd') AS next_end
FROM information_schema.task_runs
WHERE TASK_NAME = 'mv-12345'
ORDER BY CREATE_TIME DESC;
```

## Configuration Parameters

### max_mv_task_run_meta_message_values_length

- **Type**: Integer
- **Default**: 100
- **Description**: Maximum number of items to store in set/map fields to prevent excessive metadata growth
- **Scope**: FE configuration parameter
- **Usage**: Limits the size of:
  - `mvPartitionsToRefresh`
  - `refBasePartitionsToRefreshMap`
  - `basePartitionsToRefreshMap`
  - `planBuilderMessage`

## Best Practices

### 1. Monitor Refresh Performance
- Track `processStartTime` vs actual finish time to identify queueing issues
- Use `adaptivePartitionRefreshNumber` to optimize batch sizes

### 2. Debug Failed Refreshes
- Check `planBuilderMessage` for optimizer issues
- Review `refBasePartitionsToRefreshMap` vs `basePartitionsToRefreshMap` for partition pruning problems

### 3. Optimize Incremental Refreshes
- Monitor `nextPartitionStart` and `nextPartitionEnd` to understand multi-iteration refresh patterns
- Adjust partition granularity if refreshes frequently span multiple runs

### 4. Understand Partition Coverage
- Compare `mvPartitionsToRefresh` with `basePartitionsToRefreshMap` to see MV-to-base-table partition mapping
- Verify that base table partitions align with expected refresh ranges

## Troubleshooting

### Problem: Refresh Takes Too Long

**Check:**
1. `processStartTime` - High difference from create time indicates queueing
2. `basePartitionsToRefreshMap` - Too many partitions being scanned
3. `adaptivePartitionRefreshNumber` - May need tuning for your workload

### Problem: Unexpected Partitions Refreshed

**Check:**
1. `forceRefresh` - May be set to true causing full refresh
2. `refBasePartitionsToRefreshMap` - Shows planned partitions
3. `basePartitionsToRefreshMap` - Shows actual partitions after optimization
4. Compare the two maps to see if optimizer changed the plan

### Problem: Refresh Stuck in Multiple Iterations

**Check:**
1. `nextPartitionStart` and `nextPartitionEnd` - Shows incomplete refresh state
2. `adaptivePartitionRefreshNumber` - May be too small for data volume
3. Consider increasing batch size or reducing partition granularity

## See Also

- [CREATE MATERIALIZED VIEW](../sql-reference/sql-statements/materialized_view/CREATE_MATERIALIZED_VIEW.md)
- [REFRESH MATERIALIZED VIEW](../sql-reference/sql-statements/materialized_view/REFRESH_MATERIALIZED_VIEW.md)
- [Information Schema: task_runs](../sql-reference/information_schema/task_runs.md)
- [Materialized View Management](../using_starrocks/Materialized_view.md)

