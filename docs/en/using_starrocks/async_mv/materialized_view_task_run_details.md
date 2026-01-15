# Understand Materialized View Task Runs

The information of materialized view refresh task runs can help understand materialized view refresh behavior, troubleshoot issues, and monitor performance.

## Overview

When the system refreshes a materialized view, it creates a task run that contains detailed information about the refresh operation. This information is stored in the `MVTaskRunExtraMessage` object and can be obtained by querying the `EXTRA_MESSAGE` field in the system view [`information_schema.task_runs`](../../sql-reference/information_schema/task_runs.md).

## Extra message of task runs

This section describes the fields provided in `MVTaskRunExtraMessage`.

#### `forceRefresh`

- **Type**: Boolean
- **Description**: Indicates whether this is a forced refresh that bypasses normal refresh conditions. `true` is returned when a forced full refresh is manually triggered by running `REFRESH MATERIALIZED VIEW ... FORCE`.

#### `partitionStart`

- **Type**: String
- **Description**: The starting partition boundary for this refresh operation. It defines the lower bound of the partition range to refresh.
- **Format**: Partition key value (for example, `"2024-01-01"` for date-based partitions)
- **Example**: When the data from January 2024 onwards is refreshed, this field would be `"2024-01-01"`.

#### `partitionEnd`

- **Type**: String
- **Description**: The ending partition boundary for this refresh operation. It defines the upper bound of the partition range to refresh.
- **Format**: Partition key value (for example, `"2024-01-31"` for date-based partitions)
- **Example**: When the data up to January 2024 is refresh, this field would be `"2024-01-31"`.

#### `mvPartitionsToRefresh`

- **Type**: Set of Strings
- **Description**: The list of materialized view partitions that are scheduled to be refreshed in the task run. This item helps you track which materialized view partitions will be updated.
- **Size Limit**: The system will automatically truncated the number of partitions to `max_mv_task_run_meta_message_values_length` (Default: 100) to prevent excessive metadata storage if the actual number exceeds the value.
- **Example**: `["p20240101", "p20240102", "p20240103"]`
- **Note**: This represents the materialized view's own partitions, not the base table partitions.

#### `refBasePartitionsToRefreshMap`

- **Type**: Map of String to Set of Strings
- **Description**: The mapping between reference base tables to the set of their partitions that should be refreshed.
- **Usage**: The value is set during the materialized view plan scheduler stage. It can be used to track which base table partitions need to be scanned.
- **Format**: `{tableName -> Set<partitionName>}`
- **Size Limit**: The system will automatically truncated the number of partitions to `max_mv_task_run_meta_message_values_length` (Default: 100) to prevent excessive metadata storage if the actual number exceeds the value.
- **Example**:

  ```json
  {
    "orders": ["p20240101", "p20240102"],
    "customers": ["p202401"]
  }
  ```

- **Note**: This is the **planned** set of partitions before optimization.

#### `basePartitionsToRefreshMap`

- **Type**: Map of String to Set of Strings
- **Description**: The mapping between reference base tables to the set of their partitions that were refreshed during execution.
- **Usage**: The value is set after the materialized view version map is committed. It reflects the real partitions used by the optimizer.
- **Format**: `{tableName -> Set<partitionName>}`
- **Size Limit**: The system will automatically truncated the number of partitions to `max_mv_task_run_meta_message_values_length` (Default: 100) to prevent excessive metadata storage if the actual number exceeds the value.
- **Example**: 

  ```json
  {
    "orders": ["p20240101", "p20240102"],
    "line_items": ["p20240101_batch1", "p20240101_batch2"]
  }
  ```

- **Note**: This is the **actual** set of partitions after query optimization and execution.

:::note

**Difference between refBasePartitionsToRefreshMap and basePartitionsToRefreshMap:**
- `refBasePartitionsToRefreshMap`: Planned partitions before optimization (usually for the main reference table)
- `basePartitionsToRefreshMap`: Actual partitions after optimization (includes all tables and optimized partition sets)

:::

#### `nextPartitionStart`

- **Type**: String
- **Description**: The starting partition boundary for the next incremental refresh. This value defines the lower bound of the partition range to be refreshed in the next task run when a refresh operation is split across multiple task runs due to resource limits or large data volumes.
- **Example**: If the current task run refreshes the data up to `"2024-01-15"`, the value of this field might be `"2024-01-16"`.

#### `nextPartitionEnd`

- **Type**: String
- **Description**: The ending partition boundary for the next incremental refresh. This value defines the upper bound of the partition range to be refreshed in the next task run when a refresh operation is split across multiple task runs due to resource limits or large data volumes.
- **Example**: `"2024-01-31"` for the next batch of partitions to process.

#### `nextPartitionValues`

- **Type**: String
- **Description**: Serialized partition values for the next refresh (used for list partitioning or complex partition schemes). It stores specific partition values when simple start/end ranges are insufficient.
- **Example**: `"('US', 'ACTIVE'), ('UK', 'ACTIVE')"` for multi-column list partitions.

#### `processStartTime`

- **Type**: Integer (timestamp in milliseconds)
- **Description**: The timestamp when the task run actually started processing (pending time is excluded). It can be used to calculate the actual processing time using the formula `Processing Time = Finish Time - Process Start Time` (queue waiting time is excluded).
- **Example**: `1704067200000` (2024-01-01 00:00:00 UTC)

#### `executeOption`

- **Type**: ExecuteOption object
- **Description**: Configuration options for the task execution.
- **Default**: `Priority` = `LOWEST`, `isMergeRedundant` = `false`
- **Fields**:
  - `priority`: Task execution priority (values from `Constants.TaskRunPriority`)
    - `HIGHEST`: 0
    - `HIGH`: 32
    - `NORMAL`: 64
    - `LOW`: 96
    - `LOWEST`: 127
  - `isMergeRedundant`: Whether to merge redundant refresh operations.
  - `properties`: Additional execution properties in the format `Map<String, String>`.

#### `planBuilderMessage`

- **Type**: Map of String to String
- **Description**: Diagnostic messages and metadata from the query plan builder. It contains information about query planning, optimization decisions, and potential issues.
- **Size Limit**: The system will automatically truncated the number of partitions to `max_mv_task_run_meta_message_values_length` (Default: 100) to prevent excessive metadata storage if the actual number exceeds the value.

#### `refreshMode`

- **Type**: String
- **Description**: The refresh mode of this task run. It indicates how the materialized view was refreshed.
- **Valid Values**:
  - `"COMPLETE"`: Full refresh of all partitions
  - `"PARTIAL"`: Incremental refresh of specific partitions
  - `"FORCE"`: Forced refresh bypassing staleness checks
  - `""` (empty): Default or unspecified

#### `adaptivePartitionRefreshNumber`

- **Type**: Integer
- **Description**: The number of partitions that should be refreshed in each iteration when adaptive partition refresh is used. This value is automatically determined based on system resources and data volume to optimize refresh performance.
- **Default**: `-1` (indicates that adaptive refresh is not set or used)
- **Example**: `10` (indicates to refresh 10 partitions at a time)

## Query Task Run Details

You can query materialized view task run information through the system view `information_schema.task_runs`.

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

The `EXTRA_MESSAGE` column contains the JSON representation of `MVTaskRunExtraMessage`.

You can further parse the JSON string `EXTRA_MESSAGE` for better readability.

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

## Understand Refresh Performance

### Calculate Processing Time

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

### Analyze Partition Refresh Patterns

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

## Configuration Items

### max_mv_task_run_meta_message_values_length

- **Type**: Integer
- **Default**: 100
- **Scope**: FE configuration
- **Description**: Maximum number of items to store in the set or the MAP fields to prevent excessive metadata growth. It limits the size of `mvPartitionsToRefresh`, `refBasePartitionsToRefreshMap`, `basePartitionsToRefreshMap`, and `planBuilderMessage`.

## Best Practices

### Monitor Refresh Performance

- Compare `processStartTime` with the actual finish time to identify queueing issues.
- Use `adaptivePartitionRefreshNumber` to optimize batch sizes.

### Debug Failed Refreshes

- Check `planBuilderMessage` for optimizer issues.
- Compare `refBasePartitionsToRefreshMap` with `basePartitionsToRefreshMap` for partition pruning problems.

### Optimize Incremental Refreshes

- Monitor `nextPartitionStart` and `nextPartitionEnd` to understand multi-iteration refresh patterns.
- Adjust partition granularity if refreshes frequently span multiple runs.

### Understand Partition Coverage

- Compare `mvPartitionsToRefresh` with `basePartitionsToRefreshMap` to check the materialized view-to-base-table partition mapping.
- Verify whether base table partitions align with expected refresh ranges.

## Troubleshooting

### Issue: Refresh Takes Too Long

**Check:**

1. `processStartTime` - A significant difference from the create time indicates the task run were in the queue.
2. `basePartitionsToRefreshMap` - A significant value indicates too many partitions being scanned.
3. `adaptivePartitionRefreshNumber` - You may need to tune the workload.

### Issue: Unexpected Partitions Refreshed

**Check:**

1. `forceRefresh` - If `true` is returned, it indicates that a forced full refresh is performed.
2. `refBasePartitionsToRefreshMap` - This field shows the planned partitions.
3. `basePartitionsToRefreshMap` - This field shows the actual partitions after optimization.
4. Compare the above two maps to see if optimizer changed the plan.

### Issue: Refresh Stuck in Multiple Iterations

**Check:**

1. `nextPartitionStart` and `nextPartitionEnd` - This field shows the incomplete refresh state.
2. `adaptivePartitionRefreshNumber` - You may need to tune the workload.
3. Consider to increase batch size or reducing partition granularity.

## See Also

- [CREATE MATERIALIZED VIEW](../../sql-reference/sql-statements/materialized_view/CREATE_MATERIALIZED_VIEW.md)
- [REFRESH MATERIALIZED VIEW](../../sql-reference/sql-statements/materialized_view/REFRESH_MATERIALIZED_VIEW.md)
- [Information Schema.task_runs](../../sql-reference/information_schema/task_runs.md)
- [Materialized View Management](./Materialized_view.md)
