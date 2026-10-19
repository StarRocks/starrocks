---
displayed_sidebar: docs
sidebar_position: 20
description: "From v2.5, StarRocks supports query queues."
---

# Query queues

This topic describes how to manage query queues in StarRocks.

From v2.5, StarRocks supports query queues. With query queues enabled, StarRocks automatically queues the incoming queries when the concurrency threshold or resource limit is reached, thereby avoiding the overload deteriorating. Pending queries wait in a queue until there is enough compute resources available to begin execution.

The Query Queue feature has two versions:

- [**Query Queue v1**](#query-queue-v1): Triggers queuing based on query concurrency, BE memory usage, and BE CPU usage. The original query queue configurations and behaviors in this topic belong to v1. From v3.1.4 onwards, v1 supports setting query queues on the resource group level.
- [**Query Queue v2**](#query-queue-v2): Supported from v3.3 onwards. v2 estimates the BE resources consumed by each query, represents BE resources as logical slots, and queues and schedules queries based on the number of slots each query needs.

## Query Queue v1

Query Queue v1 supports setting thresholds on CPU usage, memory usage, and query concurrency to trigger query queues.

**Roadmap**:

| Version | Global query queue | Resource group-level query queue | Collective concurrency management | Dynamic concurrency adjustment  |
| ------  | ------------------ | -------------------------------- | --------------------------------- | ------------------------------- |
| v2.5    | ✅                 | ❌                                | ❌                                | ❌                              |
| v3.1.4  | ✅                 | ✅                                | ✅                                | ✅                              |

### Enable Query Queue v1

Query queues are disabled by default. You can enable global or resource group-level query queues for INSERT loading, SELECT queries, and statistics queries by setting corresponding global session variables.

#### Enable global query queues

- Enable query queues for loading tasks:

```SQL
SET GLOBAL enable_query_queue_load = true;
```

- Enable query queues for SELECT queries:

```SQL
SET GLOBAL enable_query_queue_select = true;
```

- Enable query queues for statistics queries:

```SQL
SET GLOBAL enable_query_queue_statistic = true;
```

#### Enable resource group-level query queues

From v3.1.4 onwards, StarRocks supports setting query queues on the resource group level.

To enable the resource group-level query queues, you also need to set `enable_group_level_query_queue` in addition to the global session variables mentioned above.

```SQL
SET GLOBAL enable_group_level_query_queue = true;
```

### Specify resource thresholds

#### Specify resource thresholds for global query queues

You can set the thresholds that trigger query queues via the following global session variables:

| **Variable**                        | **Default** | **Description**                                              |
| ----------------------------------- | ----------- | ------------------------------------------------------------ |
| query_queue_concurrency_limit       | 0           | The upper limit of concurrent queries on a BE. It takes effect only after being set greater than `0`. Setting it to `0` indicates no limit is imposed. |
| query_queue_mem_used_pct_limit      | 0           | The upper limit of memory usage percentage on a BE. It takes effect only after being set greater than `0`. Setting it to `0` indicates no limit is imposed. Range: [0, 1] |
| query_queue_cpu_used_permille_limit | 0           | The upper limit of CPU usage permille (CPU usage * 1000) on a BE. It takes effect only after being set greater than `0`. Setting it to `0` indicates no limit is imposed. Range: [0, 1000] |

:::note

- After Query Queue v2 is enabled, `query_queue_cpu_used_permille_limit` is no longer supported for queue triggering, and `query_queue_mem_used_pct_limit` still applies. See [Memory admission](#memory-admission).
- By default, BE reports resource usage to FE at one-second intervals. You can change this interval by setting the BE configuration item `report_resource_usage_interval_ms`.

:::

#### Specify resource thresholds for resource group-level query queues

From v3.1.4 onwards, you can set individual concurrency limits (`concurrency_limit`) and CPU core limits (`max_cpu_cores`) when creating a resource group. You can also set a memory usage threshold (`mem_used_pct_limit`) for the resource group. When a query is initiated, if any of the resource consumptions exceed the resource threshold at either the global or resource group level, the query will be placed in queue until all resource consumptions are within the threshold.

| **Variable**       | **Default** | **Description**                                                                                                                                                                                                                                   |
|--------------------|-------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| concurrency_limit  | 0           | The concurrency limit for the resource group on a single BE node. It takes effect only when it is set to greater than `0`.                                                                                                                        |
| max_cpu_cores      | 0           | The CPU core limit for this resource group on a single BE node. It takes effect only when it is set to greater than `0`. Range: [0, `avg_be_cpu_cores`], where `avg_be_cpu_cores` represents the average number of CPU cores across all BE nodes. |
| mem_used_pct_limit | 0           | The memory usage percentage limit for this resource group on a single BE node. It takes effect only when it is set to greater than `0`. Range: [0, 1]                                                                                             |

`mem_used_pct_limit` applies only to Query Queue v1. After Query Queue v2 is enabled (`enable_query_queue_v2` is set to `true`), this parameter no longer takes effect.

You can use SHOW USAGE RESOURCE GROUPS to view the resource usage information for each resource group on each BE node, as described in [View Resource Group Usage Information](./resource_group.md#view-resource-group-usage-information).

#### Manage query concurrency

When the number of running queries (`num_running_queries`) exceeds the global or resource group's `concurrency_limit`, incoming queries are placed in the queue. The way to obtain `num_running_queries` differs between versions `<` v3.1.4 and `>=` v3.1.4.

- In versions `<` v3.1.4, `num_running_queries` is reported by BEs at the interval specified in `report_resource_usage_interval_ms`. Therefore, there might be some delay in the identification of changes in `num_running_queries`. For example, if the `num_running_queries` reported by BEs at the moment does not exceed the global or resource group's `concurrency_limit`, but incoming queries arrive and exceed the `concurrency_limit` before the next report, these incoming queries will be executed without waiting in the queue.

- In versions `>=` v3.1.4, all running queries are collectively managed by the Leader FE. Each Follower FE notifies the Leader FE when initiating or finishing a query, allowing the StarRocks to handle scenarios where there is a sudden increase in queries exceeding the `concurrency_limit`.

### Configure Query Queue v1

You can set the capacity of a query queue and the maximum timeout of queries in queues via the following global session variables:

| **Variable**                       | **Default** | **Description**                                              |
| ---------------------------------- | ----------- | ------------------------------------------------------------ |
| query_queue_max_queued_queries     | 1024        | The upper limit of queries in a queue. When this threshold is reached, incoming queries are rejected. It takes effect only after being set greater than `0`. |
| query_queue_pending_timeout_second | 300         | The maximum timeout of a pending query in a queue. When this threshold is reached, the corresponding query is rejected. Unit: second. |

### Configure dynamic adjustment of query concurrency

Starting from version v3.1.4, for queries managed by the query queue and run by the Pipeline Engine, StarRocks can dynamically adjust the query concurrency `pipeline_dop` for incoming queries based on the current number of running queries `num_running_queries`, the number of fragments `num_fragments`, and the query concurrency `pipeline_dop`. This allows you to dynamically control query concurrency while minimizing scheduling overhead, ensuring optimal BE resource utilization. For more information about fragments and query concurrency `pipeline_dop`, see [Query Management - Adjusting Query Concurrency](./Query_management.md).

For each query under a query queue, StarRocks maintains a concept of drivers, which represent the concurrent fragments of a query on a single BE. Its logical value `num_drivers`, which represents the total concurrency of all fragments of that query on a single BE, is equal to `num_fragments * pipeline_dop`. When a new query arrives, StarRocks adjusts the query concurrency `pipeline_dop` based on the following rules:

- The more the number of running drivers `num_drivers` exceeds the low water limit of concurrent drivers `query_queue_driver_low_water`, the lower the query concurrency `pipeline_dop` is adjusted to.
- StarRocks restrains the number of running drivers `num_drivers` below the high water limit of concurrent drivers for queries `query_queue_driver_high_water`.

You can configure the dynamic adjustment of query concurrency `pipeline_dop` using the following global session variables:

| **Variable**                  | **Default** | **Description**                                             |
| ----------------------------- | ----------- | ----------------------------------------------------------- |
| query_queue_driver_high_water | -1          | The high water limit of concurrent drivers for a query. It takes effect only when it is set to a non-negative value. When set to `0`, it is equivalent to `avg_be_cpu_cores * 16`, where `avg_be_cpu_cores` represents the average number of CPU cores across all BE nodes. When set to a value greater than `0`, that value is used directly. |
| query_queue_driver_low_water  | -1          | The lower limit of concurrent drivers for queries. It takes effect only when it is set to a non-negative value. When set to `0`, it is equivalent to `avg_be_cpu_cores * 8`. When set to a value greater than `0`, that value is used directly. |

## Query Queue v2

From v3.3 onwards, StarRocks supports Query Queue v2. In Query Queue v2, query queues are no longer triggered based on fixed thresholds for query concurrency, BE memory usage, or BE CPU usage. Instead, it estimates the BE resources required by each query and queues and schedules queries based on logical slots. If there are not enough available slots, the query waits in the queue until enough slots are released.

### Configure Query Queue v2

Query Queue v2 is enabled and tuned through FE configuration items. Changes to `enable_query_queue_v2` require restarting FE nodes to take effect.

| Configuration item | Default | Description |
| ------------------ | ------- | ----------- |
| `enable_query_queue_v2` | `false` (v3.3 to v4.0)<br />`true` (from v4.1 onwards) | Whether to enable Query Queue v2. When this item is set to `true`, StarRocks uses the v2 slot-based query scheduling mechanism. |
| `query_queue_v2_concurrency_level` | `4` | The logical concurrency level used by Query Queue v2 to calculate the total number of cluster slots. A larger value allows the system to admit more queries. This is a relative tuning parameter. |
| `query_queue_slots_estimator_strategy` | `PBE` | The slot estimation strategy used for queue-based queries. Valid values: `PBE` (parallelism-based, the default), `MBE` (memory-cost-based), and `CBE` (CPU-cost-based). PBE estimates a query's slots from scan parallelism, capped by the worker count: for OLAP tables it uses the number of scan ranges left after pruning, so only very small queries fall below the worker count; a connector/external scan is treated as a full-parallelism scan (the worker count) rather than a single-slot query. MBE estimates slots from the query's memory cost divided by `query_queue_v2_mem_bytes_per_slot`. CBE estimates slots from the plan CPU cost divided by `query_queue_v2_cpu_costs_per_slot`. MBE and CBE per-query slots are additionally capped by `number_of_workers * max(1, pipeline_dop / 2)`. The legacy values `MAX` and `MIN` are still accepted for forward compatibility and are treated as the default estimator; any other value is rejected by configuration validation. |
| `query_queue_v2_schedule_strategy` | `SWRR` | The scheduling policy used by Query Queue V2 to order pending queries. Supported values (case-insensitive) are `SWRR` (Smooth Weighted Round Robin) — the default, suitable for mixed/hybrid workloads that need fair weighted sharing — and `SJF` (Short Job First + Aging) — prioritizes short jobs while using aging to avoid starvation. The value is parsed with case-insensitive enum lookup; an unrecognized value is logged as an error and the default policy is used. This configuration only affects behavior when Query Queue V2 is enabled and interacts with V2 sizing settings such as `query_queue_v2_concurrency_level`. |
| `query_queue_v2_mem_bytes_per_slot` | `0` | Per-slot memory target used by the memory-cost-based estimator (MBE). When `query_queue_slots_estimator_strategy` is `MBE`, the total slots are derived from the warehouse memory budget, and a query's slots are estimated from its total memory cost divided by this value, capped by `number_of_workers * max(1, pipeline_dop / 2)`. If it is non-positive, Query Queue V2 uses the average worker memory per core. |
| `query_queue_v2_cpu_costs_per_slot` | `1000000000` | Per-slot CPU cost threshold used by the CPU-cost-based estimator (CBE) to estimate how many slots a query needs from its plan CPU cost. The scheduler computes slots as `ceil(plan_cpu_costs / query_queue_v2_cpu_costs_per_slot)` and clamps the result to the range `[1, min(totalSlots, number_of_workers * max(1, pipeline_dop / 2))]`. A non-positive value is normalized to `1`. Increasing this value reduces slots allocated per query (favoring fewer, larger-slot queries); decreasing it increases slots per query. |
| `query_queue_mem_used_pct_limit` | `0` | A global session variable. The memory usage limit of a node, as a fraction of the memory limit of the node. While the process memory usage of any alive node reaches this value, new queries wait in the queue. Range: [0, 1]. `0` means no memory gating. See [Memory admission](#memory-admission). |
| query_queue_concurrency_limit       | 0           | The upper limit of concurrent queries on a BE. It takes effect only after being set greater than `0`. Setting it to `0` indicates no limit is imposed. |

:::note

`query_queue_cpu_used_permille_limit` applies only to Query Queue v1. For how Query Queue v2 applies `query_queue_mem_used_pct_limit`, see [Memory admission](#memory-admission).

:::

### Resource slots

Query Queue v2 represents BE resources as logical slots:

- **Total cluster slots**: StarRocks sets a logical total number of slots for the entire cluster. This total is positively correlated with the number of BEs and BE CPU cores, and is also affected by `query_queue_v2_concurrency_level`.
- **Slots required by a query**: StarRocks estimates the number of slots required by each query. The estimation is based on factors such as statistics, query complexity, the number of fragments, estimated input and output data volumes of complex operators, and DOP.

### Queuing logic

When the number of slots required by a query exceeds the current number of remaining slots, the query waits in the queue. Query Queue v2 preferentially satisfies queries that require fewer slots, allowing small queries to obtain resources first and avoiding head-of-line blocking where a large query at the head of the queue blocks later small queries.

The entire queuing logic is completed on FE, including setting the total number of cluster slots, estimating the number of slots required by a query, and deciding which query's slot requirement to satisfy first. Slot scheduling does not depend on the actual resource usage of BEs. The only input taken from actual usage is the optional memory usage limit `query_queue_mem_used_pct_limit`, which makes new queries wait while a node uses too much memory. See [Memory admission](#memory-admission).

### Choose an estimation strategy

#### PBE

Parallel-based estimation (PBE) strategy is best for:

- Normal reporting queries
- Mixed point lookups and large queries
- Users who do not want to understand cost model details
- DBAs who want stable, simple, and explainable queueing behavior first

Expected behaviors with PBE include:

- Point lookups or queries that scan little data after pruning use fewer slots
- Queries that scan larger ranges use more slots
- Small queries are more likely to get execution resources during peak hours

The following example sets PBE as the strategy:

```SQL
ADMIN SET FRONTEND CONFIG ("query_queue_slots_estimator_strategy" = "PBE");
```

#### MBE

Memory cost-based estimation (MBE) strategy is suitable for dealing with memory pressure, such as large joins, large aggregations, or high-cardinality aggregations.

The following example sets MBE as the strategy, and allocates 2 GB of memory to each slot:

```SQL
ADMIN SET FRONTEND CONFIG ("query_queue_slots_estimator_strategy" = "MBE");
ADMIN SET FRONTEND CONFIG ("query_queue_v2_mem_bytes_per_slot" = "2147483648");
```

MBE divides the query’s total memory cost by this value to get query slots, and divides the warehouse memory budget by this value to get total slots.

Tune the MBE strategy in the following directions:

**Symptom: Memory still gets saturated easily**

- **Adjustment**: Decrease `query_queue_v2_concurrency_level`
- **Effect**: Directly lowers the MBE total memory budget

**Symptom: Queue is too long but BE memory still has room**

- **Adjustment**: Increase `query_queue_v2_concurrency_level`
- **Effect**: Directly raises the MBE total memory budget

**Symptom: `max_slots` is very small and integer rounding is visible**

- **Adjustment**: Decrease `query_queue_v2_mem_bytes_per_slot`
- **Effect**: Uses a finer memory slot granularity and reduces coarse rounding error

#### CBE

CPU cost-based estimation (CBE) strategy is suitable for dealing with memory pressure, such as compute-heavy SQL, complex expressions, or heavy CPU work after scanning.

The following example sets CBE as the strategy, and set the CPU cost threshold to `1000000000` for each slot:

```SQL
ADMIN SET FRONTEND CONFIG ("query_queue_slots_estimator_strategy" = "CBE");
ADMIN SET FRONTEND CONFIG ("query_queue_v2_cpu_costs_per_slot" = "1000000000");
```

**Symptom: CPU is often saturated**

- **Adjustment**: Decrease `query_queue_v2_cpu_costs_per_slot`
- **Effect**: The same CPU cost for more slots, making concurrency more conservative

**Symptom: Queries queue noticeably but CPU still has room**

- **Adjustment**: Increase `query_queue_v2_cpu_costs_per_slot`
- **Effect**: The same CPU cost for fewer slots, making concurrency looser

### Tune concurrency capacity

If you only want to increase or decrease overall concurrency, do not switch between PBE, MBE, and CBE first. Tune total slot capacity first:

```SQL
ADMIN SET FRONTEND CONFIG ("query_queue_v2_concurrency_level" = "<value>");
```

Recommended process:

1. Start with default value `4`.
2. Observe `remain_slots`, `max_slots`, `query_pending_length`, CPU, memory, and query latency.
3. If there is resource headroom but queries queue noticeably, gradually increase `query_queue_v2_concurrency_level`.
4. If resources are often saturated or queries interfere with each other heavily, gradually decrease `query_queue_v2_concurrency_level`.
5. Make small changes each time, such as 10% to 25%, and observe one business peak period before making another change.

**Tuning Priority**: Use `query_queue_v2_concurrency_level` to tune overall capacity first. Consider switching to MBE or PBE only after that. Do not change multiple parameters at the same time at the beginning, because it becomes hard to tell which parameter caused the effect.

#### Fallback Concurrency Cap

`query_queue_concurrency_limit` is a fallback concurrency cap and applies to PBE, MBE, and CBE. Query Queue V2 first estimates the slots required by a query with the current estimator and checks whether total slots are available. After that, it checks whether the current number of running queries has reached `query_queue_concurrency_limit`.

The default value `0` means unlimited. Set it only when you need an absolute cap on the number of concurrently running queries:

```SQL
ALTER WAREHOUSE default_warehouse SET ("query_queue_concurrency_limit" = "8");
```

Use `query_queue_v2_concurrency_level` to tune resource capacity first. Use `query_queue_concurrency_limit` only when you need to explicitly limit the number of queries run at the same time.

### Memory admission

Slots do not measure the memory that queries use. Memory admission controls memory with two settings:

- `query_queue_mem_used_pct_limit`: the memory usage limit of a node. While the process memory usage of any alive node reaches this fraction of the node's memory limit, new queries wait in the queue. It is the same global session variable that Query Queue v1 uses.
- `query_mem_limit`: the existing session variable that sets the memory limit of a query on each BE node.

`query_queue_mem_used_pct_limit` defaults to `0`, which means that no query waits because of memory. Set it with `SET GLOBAL`.

#### Memory usage limit

`query_queue_mem_used_pct_limit` is a fraction in [0, 1], such as `0.8` for 80%. It is compared with the memory usage of each node: the process memory that the node uses, divided by the memory limit of the node. The process memory includes not only queries but also loads, compaction, metadata, and the in-memory data cache. A query fails when either its query pool or the BE process runs out of memory, so comparing the process memory keeps a buffer against both limits, and new queries wait before a node runs out of memory.

The memory usage limit works as follows:

- Each BE and CN node reports its memory usage every second, as set by the BE configuration item `report_resource_usage_interval_ms`. Only the leader FE receives these reports, and the leader FE makes the admission decisions.
- While the memory usage of any alive node is at or above the limit, new queries wait in the queue. Running queries are not affected. Once every node is below the limit, the queued queries are admitted again. The decision is binary: there is no gradual throttling between the two states.
- The memory usage limit is checked together with the slot pool and `query_queue_concurrency_limit`. A query is admitted only when all three allow it.
- When no query that was admitted by the queue is running, one query is still admitted even if a node is over the limit. The memory may be held by loads, caches, or queries that bypass the queue, and keeping queued queries waiting cannot release it.
- A node whose latest report is older than the global variable `query_queue_fresh_resource_usage_interval_ms` (`5000` milliseconds by default) does not take part in the decision, so that a node that stops reporting cannot keep new queries waiting. A node that is not alive does not take part either.
- The memory usage limit does not read or depend on resource groups.

Memory admission is part of the admission decision of Query Queue v2, so it applies only to queries that go through the query queue. The nodes are all BE and CN nodes of the cluster in a shared-nothing cluster, and all CN nodes of the cluster in a shared-data cluster.

#### Choose a limit

A large part of the process memory is usually the in-memory data cache, which holds the decompressed pages that queries read from segment files. Its size is limited by the BE configuration item `datacache_mem_size` (`20%` of the process memory limit by default), and it grows as queries read data. BE gives this memory back when the process memory is high: above the BE configuration item `memory_high_level` (`75` percent of the process memory limit by default), BE evicts the cache gradually, and above `memory_urgent_level` (`85` by default), BE shrinks the cache immediately.

We recommend a limit between `0.75` and `0.85`:

- Within this range, when queries need more memory, the cache gives memory back as the process memory usage rises above 75%, and new queries wait only until the cache has shrunk.
- Below `0.75`, BE does not release the cache, so a node with a large cache can keep new queries waiting. The cluster then admits one query at a time, when nothing is running.
- A lower limit leaves more room for the queries that are already running to grow. On a node that holds little besides queries, the room left before the query pool limit (90% of the process memory limit by default, as set by the BE configuration item `query_max_memory_limit_percent`) is 0.9 minus the limit.

#### Per-query memory limit on each node

The session variable `query_mem_limit` sets the memory limit of a query on each BE node, in bytes. The default value `0` means no limit. To set it for the current session, run `SET query_mem_limit = <bytes>;`. To set it for all future sessions, run `SET GLOBAL query_mem_limit = <bytes>;`. For more information, see [query_mem_limit](../../../sql-reference/System_variable.md#query_mem_limit).

On each node, BE applies the smallest of `query_mem_limit`, the resource group property `big_query_mem_limit`, and the limits of the parent memory trackers. When a query exceeds `query_mem_limit`, the error message contains `Mem usage has exceed the limit of single query, You can change the limit by set session variable query_mem_limit.`

#### Configure memory admission

The following example sets the memory usage limit:

```SQL
SET GLOBAL query_queue_mem_used_pct_limit = 0.8;
```

The following example limits each query to 8 GB of memory on each BE node in all future sessions:

```SQL
SET GLOBAL query_mem_limit = 8589934592;
```

Whenever you set `query_queue_mem_used_pct_limit`, also set `query_mem_limit`. See [Limitations](#limitations) for the reason.

#### Monitor memory admission

To see the memory usage that is compared with the limit, run [SHOW BACKENDS](../../../sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_BACKENDS.md) in a shared-nothing cluster or [SHOW COMPUTE NODES](../../../sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_COMPUTE_NODES.md) in a shared-data cluster. The `MemUsedPct` column is the memory usage of each node, as a percentage of its memory limit. Only the leader FE receives the memory usage that the nodes report, so run the statement on the leader FE.

#### Limitations

- Admission reserves no memory. A newly admitted query adds to the usage only after it allocates memory and the next report arrives, so the queries admitted in one batch can together overshoot the limit. The slot pool and `query_queue_concurrency_limit` bound how many queries a batch admits, but only `query_mem_limit` bounds how much memory each of them can use on a node. This is why the FE log records a warning, once for each limit value, when the memory usage limit is set but the global default of `query_mem_limit` is `0`.
- One node at or above the limit makes all new queries wait, even if the other nodes have free memory, because queries usually run on all nodes.

## Monitor query queues

You can view information related to query queues using the following methods.

### SHOW PROC

You can check the number of running queries, and memory and CPU usages in BE nodes using [SHOW PROC](../../../sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_PROC.md):

```Plain
mysql> SHOW PROC '/backends'\G
*************************** 1. row ***************************
...
    NumRunningQueries: 0
           MemUsedPct: 0.79 %
           CpuUsedPct: 0.0 %
```

### SHOW PROCESSLIST

You can check if a query is in a queue (when `IsPending` is `true`) using [SHOW PROCESSLIST](../../../sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_PROCESSLIST.md):

```Plain
mysql> SHOW PROCESSLIST;
+------+------+---------------------+-------+---------+---------------------+------+-------+-------------------+-----------+
| Id   | User | Host                | Db    | Command | ConnectionStartTime | Time | State | Info              | IsPending |
+------+------+---------------------+-------+---------+---------------------+------+-------+-------------------+-----------+
|    2 | root | xxx.xx.xxx.xx:xxxxx |       | Query   | 2022-11-24 18:08:29 |    0 | OK    | SHOW PROCESSLIST  | false     |
+------+------+---------------------+-------+---------+---------------------+------+-------+-------------------+-----------+
```

### FE audit log

You can check the FE audit log file **fe.audit.log**. The field `PendingTimeMs` indicates the time a query spent waiting in a queue, and its unit is milliseconds.

### Monitoring metrics

You can obtain metrics of query queues in StarRocks using the [Monitor and Alert](../monitoring/monitoring.md) feature. The following FE metrics are derived from the statistical data of each FE node.

| Metric                                          | Unit | Type    | Description                                                    |
| ----------------------------------------------- | ---- | ------- | -------------------------------------------------------------- |
| starrocks_fe_query_queue_pending                | Count | Instantaneous | The current number of queries in the queue.                  |
| starrocks_fe_query_queue_total                  | Count | Instantaneous | The total number of queries historically queued (including those currently running). |
| starrocks_fe_query_queue_timeout                | Count | Instantaneous | The total number of queries that have timed out while in the queue. |
| starrocks_fe_resource_group_query_queue_total   | Count | Instantaneous | The total number of queries historically queued in this resource group (including those currently running). The `name` label indicates the name of the resource group. This metric is supported from v3.1.4 onwards. |
| starrocks_fe_resource_group_query_queue_pending | Count | Instantaneous | The number of queries currently in the queue for this resource group. The `name` label indicates the name of the resource group. This metric is supported from v3.1.4 onwards. |
| starrocks_fe_resource_group_query_queue_timeout | Count | Instantaneous | The number of queries that have timed out while in the queue for this resource group. The `name` label indicates the name of the resource group. This metric is supported from v3.1.4 onwards. |

### SHOW RUNNING QUERIES

From v3.1.4 onwards, StarRocks supports the SQL statement `SHOW RUNNING QUERIES`, which is used to display queue information for each query. The meanings of each field are as follows:

- `QueryId`: The ID of the query.
- `ResourceGroupId`: The ID of the resource group that the query hit. When there is no hit on a user-defined resource group, it will be displayed as "-".
- `StartTime`: The start time of the query.
- `PendingTimeout`: The time when the PENDING query will time out in the queue.
- `QueryTimeout`: The time when the query times out.
- `State`: The queue state of the query, where "PENDING" indicates it is in the queue, and "RUNNING" indicates it is currently executing.
- `Slots`: The logical resource quantity requested by the query. In Query Queue v1, this value is usually `1`. In Query Queue v2, this value is the estimated number of slots for the query.
- `Frontend`: The FE node that initiated the query.
- `FeStartTime`: The start time of the FE node that initiated the query.

Example:

```Plain
MySQL [(none)]> SHOW RUNNING QUERIES;
+--------------------------------------+-----------------+---------------------+---------------------+---------------------+-----------+-------+---------------------------------+---------------------+
| QueryId                              | ResourceGroupId | StartTime           | PendingTimeout      | QueryTimeout        |   State   | Slots | Frontend                        | FeStartTime         |
+--------------------------------------+-----------------+---------------------+---------------------+---------------------+-----------+-------+---------------------------------+---------------------+
| a46f68c6-3b49-11ee-8b43-00163e10863a | -               | 2023-08-15 16:56:37 | 2023-08-15 17:01:37 | 2023-08-15 17:01:37 |  RUNNING  | 1     | 127.00.00.01_9010_1692069711535 | 2023-08-15 16:37:03 |
| a6935989-3b49-11ee-935a-00163e13bca3 | 12003           | 2023-08-15 16:56:40 | 2023-08-15 17:01:40 | 2023-08-15 17:01:40 |  RUNNING  | 1     | 127.00.00.02_9010_1692069658426 | 2023-08-15 16:37:03 |
| a7b5e137-3b49-11ee-8b43-00163e10863a | 12003           | 2023-08-15 16:56:42 | 2023-08-15 17:01:42 | 2023-08-15 17:01:42 |  PENDING  | 1     | 127.00.00.03_9010_1692069711535 | 2023-08-15 16:37:03 |
+--------------------------------------+-----------------+---------------------+---------------------+---------------------+-----------+-------+---------------------------------+---------------------+
```
