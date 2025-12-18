---
displayed_sidebar: docs
---

# Troubleshooting Resource Isolation

This topic provides answers to some frequently asked questions about resource isolation.

## Resource Group

### What resources must be configured in a resource group?

The CPU resource limit must be configured. You must set either `cpu_weight` or `exclusive_cpu_core`, and the values must be greater than 0.

### Does StarRocks support hard resource limits?

Yes. StarRocks supports hard limits for memory. From v3.3.5 onwards, StarRocks supports hard limits for CPU via `exclusive_cpu_cores`.

### How is CPU allocated among resource groups?

When multiple resource groups run queries simultaneously, CPU usage is proportional to each group’s `cpu_core_limit`. If the normal group exceeds the `BE vCPU cores - short_query.cpu_core_limit` within a scheduling cycle, it will not be scheduled further in that cycle.

### How are resources calculated for the `short_query` resource group?

When the `short_query` resource group has running queries, the CPU limit of all normal groups becomes `BE vCPU cores − short_query.cpu_core_limit`. If the `short_query` resource group is idle, its resources can be used by normal groups.

### How are queries without a matched resource group handled?

They use the default resource group `default_wg`, which has the following resource limits and properties:

- `cpu_core_limit` = vCPU cores
- `mem_limit` = 100%
- `type` = `normal`

### If the resource group `rg3` has no queries and all resources are allocated to resource groups `rg1` and `rg2`, will those resources be reallocated when `rg3` receives a large query?

Yes. Reclamation happens gradually and stabilizes within tens of milliseconds to seconds.

### What is the role of classifiers? What if no classifier matches, or two classifiers overlap?

If no classifier matches, the query falls back to the default resource group `default_wg`. Classifiers have weights; if a query matches multiple classifiers, the one with the highest weight is selected.

### Why do resource groups require `mem_limit` if BE already uses 90% memory by default?

`mem_limit` restricts memory at the resource-group level. It applies only to queries match that resource group.

### If the resource group's `query_type` is set to `insert`, does INSERT INTO SELECT only limit INSERT or also SELECT?

Only the SELECT part is limited by the resource group. The INSERT operation is not limited.

### If the resource groups `mem_limit` is set to `20%`, is the usable memory calculated as `BE_memory * 90% * 20%`? What if total `mem_limit` exceeds 100% when there are multiple resource groups?

Total `mem_limit` across groups can exceed 100%. But if a query exceeds its resource group limit, it will fail.

### How can I verify whether a resource group is applied to a query?

Check `fe.audit.log` or run `EXPLAIN VERBOSE <SQL>` to view the matched resource group.

### Are resource groups defined per cluster or per BE node?

Resources are divided per BE node, and the resource group configuration applies to all BE nodes in the cluster.

### How to inspect resource group usage or monitoring metrics?

Use FE/BE metrics endpoints to view specific resource-group-related metrics.

- For FE, collect the following metrics from `fe_host:8030/metrics?type=json`:
  - `starrocks_fe_query_resource_group`: The number of queries historically run in this resource group (including those currently running).
  - `starrocks_fe_query_resource_group_latency`: The query latency percentile for this resource group. The label type indicates specific percentiles, including `mean`, `75_quantile`, `95_quantile`, `98_quantile`, `99_quantile`, `999_quantile`.
  - `starrocks_fe_query_resource_group_err`: The number of queries in this resource group that encountered an error.

- For BE, collect the following metrics from `be_host:8040/metrics?type=json`:
  - `starrocks_be_resource_group_cpu_limit_ratio`: The ratio of this resource group's `cpu_core_limit` to the total `cpu_core_limit` across all resource groups.
  - `starrocks_be_resource_group_mem_limit_bytes`: The memory limit for this resource group.

### What is the difference between `short_query` and normal resource groups? Can I create multiple `short_query` resource groups?

Only one `short_query` resource group is allowed. When there are queries running in `short_query` resource group, it uses actual BE cores, while normal groups share remaining resources proportionally.

### Does StarRocks provide query priority or large-query-based prioritization?

No priority system exists. A query becomes a “large query” when exceeding any of the configured resource thresholds.

### Does a resource group belong to a specific BE node or the BE running the query?

Resource groups apply uniformly across all BEs in the cluster.

### How should I configure `concurrency_limit`?

It depends on query complexity, cluster size, and workload patterns.

### How does classifier-based matching work? Is it tied to user and/or database?

Matching depends on classifier attributes such as IP, user, db, role, or `query_type`.

### How is resource group specified through session variables?

You can set it as a variable:

```SQL
SET resource_group = '<resource_group_name>';
```

Or specify it in queries via hint:

```SQL
SELECT /*+ SET_VAR(SET resource_group = '<resource_group_name>') */ * FROM tbl;
```

### Does concurrency control take effect globally, per user, or per BE?

`concurrency_limit` restricts concurrency per resource group, while `pipeline_dop` controls parallelism of a single pipeline instance.

### Does memory limit take effect globally, per user, or per BE?

`mem_limit` applies to the resource group per BE. Per-instance memory is controlled by `exec_mem_limit`.

### Are concurrency and memory limits only effective when resource groups are enabled?

Concurrency is controlled only by resource groups. Query parallelism is controlled by session variables such as `pipeline_dop`.

### Will CTAS tasks match resource groups if the `query_type` is set to `INSERT`?

Yes. Resource group will restrict the resources for the SELECT part of CTAS tasks. The big query limits will also take effect if the SELECT part exceeds the threshold.

### Why can't the queries in the `short_query` resource group consume all CPU?

The `short_query` resource group must leave at least 1 CPU core for normal groups.

### Without query queue and resource groups, are concurrent queries limited?

No. Overload results in query timeouts or memory limit errors.

### With resource groups enabled but query queue disabled, is concurrency limited by resource groups?

Yes. New queries exceeding the resource group `concurrency_limit` will fail.

### When is a query recognized as a "big query"?

A query is considered as a big query when it exceeds any of:

- `big_query_cpu_second_limit`
- `big_query_scan_rows_limit`
- `big_query_mem_limit`

### Can `default_wg` resource limits be changed?

No. As a workaround, create a general resource group that can match all queries.

Example:

```SQL
CREATE RESOURCE GROUP general_group TO (
    query_type IN ('select', 'insert') 
)
WITH (
    'cpu_core_limit' = '6', 
    'mem_limit' = '0.0000000000001'
);
```

### Why does the “query_resource_group” metric not show newly created groups?

The metric is lazy-initialized; it appears only after a query hits that group.

### If many queries run on a BE, and one hits CPU limit, do all fail?

Only the query reaching the limit fails.

### If BE nodes have different memory/CPU sizes, does `mem_limit` or `cpu_core_limit` affect results?

Memory limit is hard and may cause failures on BEs with less memory size first. CPU is soft and does not cause errors.

### Are `big_query_` parameters applied per node or globally?

They apply per BE node.

### How to configure resource groups for Broker Load?

Example classifier: `query_type="insert", user="alice"`.

### `exec_mem_limit`-related questions

Q: How many instances will be generated for a query?

A: It is unpredictable because different queries can generate different number of fragments.

Q: How to check the number of instance?

A: You cannot check the number of instance.

Q: If a query consumes 128 GB memory in total and generates 60 instances, while `query_mem_limit=0` and `exec_mem_limit=2G`. Will the query fail?

A: The query fails as long as any instance consumes more than 2 GB (`exec_mem_limit`) memory.

### How to disable global query queue for one resource group only?

1. Enable resource group-level query queue:

   ```SQL
   SET GLOBAL enable_show_all_variables = true;
   SET enable_group_level_query_queue = true;
   ```

2. Disable Query Queue for the current session or at user level:

   ```SQL
   -- Disable Query Queue for the current session
   SET enable_query_queue = false;
   -- Disable Query Queue at user level
   ALTER USER 'xxx' SET PROPERTIES ("session.enable_query_queue" = "false");
   ```

### Can queries that doesn't match any resource group be forced to fail?

No. They will always fall back to the default resource group `default_wg`.

## Query Queue

### How is the query queue memory trigger calculated?

Query Queue trigger = BE available memory size * `query_queue_mem_used_pct_limit`.

### Which takes priority: resource-group concurrency or query-queue concurrency?

If `enable_group_level_query_queue` is set to:

- `false`: global or group limit may trigger first.
- `true`: both apply; the smaller limit triggers queueing.

### If queue size or timeout is reached, will queries fail immediately?

- When `query_queue_max_queued_queries` is reached, the query immediately fails.
- When `query_queue_concurrency_limit` is reached, the query waits in queue.

### What is the difference between resource group limits and query queue limits?

Resource groups restrict resource usage per group per BE node.

Query queue uses BE-level limits for all queries.

`concurrency_limit` and `max_cpu_cores` both apply when resource group-level query queue is enabled.

### What is the difference between `pipeline_dop`, `exec_mem_limit`, and resource group concurrency limits?

`pipeline_dop` controls in-query parallelism.

Resource groups/query queue control cluster-wide concurrent queries.

`query_mem_limit` controls per-query-per-BE memory.
