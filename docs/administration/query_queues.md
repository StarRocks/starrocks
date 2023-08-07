# Query queues

This topic describes how to manage query queues in StarRocks.

From v2.5, StarRocks supports query queues. With query queues enabled, StarRocks automatically queues the incoming queries when the concurrency threshold or resource limit is reached, thereby avoiding the overload deteriorating. Pending queries wait in a queue until there is enough compute resources available to begin execution.

You can set thresholds on CPU usage, memory usage, and query concurrency to trigger query queues.

## Enable query queues

Query queues are disabled by default. You can enable query queues for INSERT loading, SELECT queries, and statistics queries by setting corresponding global session variables.

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

## Specify resource thresholds

You can set the thresholds that trigger query queues via the following global session variables:

| **Variable**                        | **Default** | **Description**                                              |
| ----------------------------------- | ----------- | ------------------------------------------------------------ |
| query_queue_concurrency_limit       | 0           | The upper limit of concurrent queries on a BE. It takes effect only after being set greater than `0`. |
| query_queue_mem_used_pct_limit      | 0           | The upper limit of memory usage percentage on a BE. It takes effect only after being set greater than `0`. Range: [0, 1] |
| query_queue_cpu_used_permille_limit | 0           | The upper limit of CPU usage permille (CPU usage * 1000) on a BE. It takes effect only after being set greater than `0`. Range: [0, 1000] |

> **NOTE**
>
> By default, BE reports resource usage to FE at one-second intervals. You can change this interval by setting the BE configuration item `report_resource_usage_interval_ms`.

## Configure query queues

You can set the capacity of a query queue and the maximum timeout of queries in queues via the following global session variables:

| **Variable**                       | **Default** | **Description**                                              |
| ---------------------------------- | ----------- | ------------------------------------------------------------ |
| query_queue_max_queued_queries     | 0           | The upper limit of queries in a queue. When this threshold is reached, incoming queries are rejected. It takes effect only after being set greater than `0`. |
| query_queue_pending_timeout_second | 300         | The maximum timeout of a pending query in a queue. When this threshold is reached, the corresponding query is rejected. Unit: second. |

## View query queue statistics

You can view the statistics of query queues via the following ways:

- Check the number of running queries, and memory and CPU usages in BE nodes using [SHOW PROC](../sql-reference/sql-statements/Administration/SHOW%20PROC.md):

```Plain
mysql> SHOW PROC '/backends'\G
*************************** 1. row ***************************
...
    NumRunningQueries: 0
           MemUsedPct: 0.79 %
           CpuUsedPct: 0.0 %
```

- Check if a query is in a queue (when `IsPending` is `true`) using [SHOW PROCESSLIST](../sql-reference/sql-statements/Administration/SHOW%20PROCESSLIST.md):

```Plain
mysql> SHOW PROCESSLIST;
+------+------+---------------------+-------+---------+---------------------+------+-------+-------------------+-----------+
| Id   | User | Host                | Db    | Command | ConnectionStartTime | Time | State | Info              | IsPending |
+------+------+---------------------+-------+---------+---------------------+------+-------+-------------------+-----------+
|    2 | root | xxx.xx.xxx.xx:xxxxx |       | Query   | 2022-11-24 18:08:29 |    0 | OK    | SHOW PROCESSLIST  | false     |
+------+------+---------------------+-------+---------+---------------------+------+-------+-------------------+-----------+
```

- Check the FE audit log file **fe.audit.log**. The field `PendingTimeMs` indicates the time that a query is in a queue, and its unit is milliseconds.

- Check the following FE metrics:

| **Metrics**                      | **Description**                                |
| -------------------------------- | ---------------------------------------------- |
| starrocks_fe_query_queue_pending | Number of pending queries in the queue.        |
| starrocks_fe_query_queue_total   | Total number of queries that have been queued. |
| starrocks_fe_query_queue_timeout | Number of timeout queries in the queue.        |
