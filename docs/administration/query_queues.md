# Query queues

This topic describes how to manage query queues in StarRocks.

From v2.5, StarRocks supports query queues. With query queues enabled, StarRocks automatically queues the incoming queries when the concurrency threshold or resource limit is reached, thereby avoiding the overload deteriorating. Pending queries wait in a queue until there is enough compute resources available to begin execution. From v3.1.4 onwards, StarRocks supports setting query queues on the resource group level.

You can set thresholds on CPU usage, memory usage, and query concurrency to trigger query queues.

**Roadmap**:

| Version | Global query queue | Resource group-level query queue | Collective concurrency management | Dynamic concurrency adjustment  |
| ------  | ------------------ | -------------------------------- | --------------------------------- | ------------------------------- |
| v2.5    | ✅                 | ❌                                | ❌                                | ❌                              |
| v3.1.4  | ✅                 | ✅                                | ✅                                | ✅                              |

## Enable query queues

Query queues are disabled by default. You can enable global or resource group-level query queues for INSERT loading, SELECT queries, and statistics queries by setting corresponding global session variables.

### Enable global query queues

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

### Enable resource group-level query queues

From v3.1.4 onwards, StarRocks supports setting query queues on the resource group level.

To enable the resource group-level query queues, you also need to set `enable_group_level_query_queue` in addition to the global session variables mentioned above.

```SQL
SET GLOBAL enable_group_level_query_queue = true;
```

## Specify resource thresholds

### Specify resource thresholds for global query queues

You can set the thresholds that trigger query queues via the following global session variables:

| **Variable**                        | **Default** | **Description**                                              |
| ----------------------------------- | ----------- | ------------------------------------------------------------ |
| query_queue_concurrency_limit       | 0           | The upper limit of concurrent queries on a BE. It takes effect only after being set greater than `0`. |
| query_queue_mem_used_pct_limit      | 0           | The upper limit of memory usage percentage on a BE. It takes effect only after being set greater than `0`. Range: [0, 1] |
| query_queue_cpu_used_permille_limit | 0           | The upper limit of CPU usage permille (CPU usage * 1000) on a BE. It takes effect only after being set greater than `0`. Range: [0, 1000] |

> **NOTE**
>
> By default, BE reports resource usage to FE at one-second intervals. You can change this interval by setting the BE configuration item `report_resource_usage_interval_ms`.

### Specify resource thresholds for resource group-level query queues

From v3.1.4 onwards, you can set individual concurrency limits (`concurrency_limit`) and CPU core limits (`max_cpu_cores`) for each resource group. When a query is initiated, if any of the resource consumptions exceed the resource threshold at either the global or resource group level, the query will be placed in queue until all resource consumptions are within the threshold.

| **Variable**        | **Default** | **Description**                                              |
| ------------------- | ----------- | ------------------------------------------------------------ |
| concurrency_limit   | 0           | The concurrency limit for the resource group on a single BE node. It takes effect only when it is set to greater than `0`. |
| max_cpu_cores       | 0           | The CPU core limit for this resource group on a single BE node. It takes effect only when it is set to greater than `0`. Range: [0, `avg_be_cpu_cores`], where `avg_be_cpu_cores` represents the average number of CPU cores across all BE nodes. |

You can use SHOW USAGE RESOURCE GROUPS to view the resource usage information for each resource group on each BE node, as described in [View Resource Group Usage Information](./resource_group.md#view-resource-group-usage-information).

### Manage query concurrency

When the number of running queries (`num_running_queries`) exceeds the global or resource group's `concurrency_limit`, incoming queries are plcaced in the queue. The way to obtain `num_running_queries` differs between versions < v3.1.4 and ≥ v3.1.4.

- In versions < v3.1.4, `num_running_queries` is reported by BEs at the interval specified in `report_resource_usage_interval_ms`. Therefore, there might be some delay in the identifiction of changes in `num_running_queries`. For example, if the `num_running_queries` reported by BEs at the moment does not exceed the global or resource group's `concurrency_limit`, but incoming queries arrive and exceed the `concurrency_limit`, these incoming queries will be executed without waiting in the queue.

- In versions ≥ v3.1.4, all running queries are collectively managed by the Leader FE. Each Follower FE notifies the Leader FE when initiating or finishing a query, allowing the StarRocks to handle scenarios where there is a sudden increase in queries exceeding the `concurrency_limit`.

## Configure query queues

You can set the capacity of a query queue and the maximum timeout of queries in queues via the following global session variables:

| **Variable**                       | **Default** | **Description**                                              |
| ---------------------------------- | ----------- | ------------------------------------------------------------ |
| query_queue_max_queued_queries     | 1024        | The upper limit of queries in a queue. When this threshold is reached, incoming queries are rejected. It takes effect only after being set greater than `0`. |
| query_queue_pending_timeout_second | 300         | The maximum timeout of a pending query in a queue. When this threshold is reached, the corresponding query is rejected. Unit: second. |

## Configure dynamic adjustment of query concurrency

Starting from version v3.1.4, for queries managed by the query queue and run by the Pipeline Engine, StarRocks can dynamically adjust the query concurrency `pipeline_dop` for new incoming queries based on the current number of running queries `num_running_queries`, the number of fragments `num_fragments`, and the query concurrency `pipeline_dop`. This allows you to dynamically control query concurrency while minimizing scheduling overhead, ensuring optimal BE resource utilization. For more information about fragments and query concurrency `pipeline_dop`, see [Query Management - Adjusting Query Concurrency](./Query_management.md).

For each query under a query queue, StarRocks maintains a concept of drivers, which represent the concurrent fragments of a query on a single BE. Its logical value `num_drivers`, which represents the total concurrency of all fragments of that query on a single BE, is equal to `num_fragments * pipeline_dop`. When a new query arrives, StarRocks adjusts the query concurrency `pipeline_dop` based on the following rules:

- The more the number of running drivers `num_drivers` exceeds the low water limit of concurrent drivers `query_queue_driver_low_water`, the lower the query concurrency `pipeline_dop` is adjusted to.
- StarRocks restrains the number of running drivers `num_drivers` below the high water limit of concurrent drivers for queries `query_queue_driver_high_water` as far as possible.

You can configure the dynamic adjustment of query concurrency `pipeline_dop` using the following global session variables:

| **Variable**                  | **Default** | **Description**                                             |
| ----------------------------- | ----------- | ----------------------------------------------------------- |
| query_queue_driver_high_water | -1          | The high water limit of concurrent drivers for a query. It takes effect only when it is set to a non-negative value. When set to `0`, it is equivalent to `avg_be_cpu_cores * 16`, where `avg_be_cpu_cores` represents the average number of CPU cores across all BE nodes. When set to a value greater than `0`, that value is used directly. |
| query_queue_driver_low_water  | -1          | The lower limit of concurrent drivers for queries. It takes effect only when it is set to a non-negative value. When set to `0`, it is equivalent to `avg_be_cpu_cores * 8`. When set to a value greater than `0`, that value is used directly. |

## Monitor query queues

You can view information related to query queues using the following methods.

### SHOW PROC

You can check the number of running queries, and memory and CPU usages in BE nodes using [SHOW PROC](../sql-reference/sql-statements/Administration/SHOW_PROC.md):

```Plain
mysql> SHOW PROC '/backends'\G
*************************** 1. row ***************************
...
    NumRunningQueries: 0
           MemUsedPct: 0.79 %
           CpuUsedPct: 0.0 %
```

### SHOW PROCESSLIST

You can check if a query is in a queue (when `IsPending` is `true`) using [SHOW PROCESSLIST](../sql-reference/sql-statements/Administration/SHOW_PROCESSLIST.md):

```Plain
mysql> SHOW PROCESSLIST;
+------+------+---------------------+-------+---------+---------------------+------+-------+-------------------+-----------+
| Id   | User | Host                | Db    | Command | ConnectionStartTime | Time | State | Info              | IsPending |
+------+------+---------------------+-------+---------+---------------------+------+-------+-------------------+-----------+
|    2 | root | xxx.xx.xxx.xx:xxxxx |       | Query   | 2022-11-24 18:08:29 |    0 | OK    | SHOW PROCESSLIST  | false     |
+------+------+---------------------+-------+---------+---------------------+------+-------+-------------------+-----------+
```

### FE audit log

You can check the FE audit log file **fe.audit.log**. The field `PendingTimeMs` indicates the time that a query is in a queue, and its unit is milliseconds.

### FE metrics

The following FE metrics are derived from the statistical data of each FE node.

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
- `PendingTimeout`: The time in the future when the query will time out in the queue.
- `QueryTimeout`: The time when the query has timed out.
- `State`: The queue state of the query, where "PENDING" indicates it is in the queue, and "RUNNING" indicates it is currently executing.
- `Slots`: The logical resource quantity requested by the query, currently fixed at `1`.
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
