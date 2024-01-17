---
displayed_sidebar: "English"
---

# Resource group

This topic describes the resource group feature of StarRocks.

![resource group](../assets/resource_group.png)

With this feature, you could simultaneously run several workloads in a single cluster, including short query, ad-hoc query, ETL jobs, to save extra cost of deploying multiple clusters. From technical perspective, the execution engine would schedule concurrent workloads according to users' specification and isolate the interference among them.

The roadmap of Resource Group:

- Since v2.2, StarRocks supports limiting resource consumption for queries and implementing isolation and efficient use of resources among tenants in the same cluster.
- In StarRocks v2.3, you can further restrict the resource consumption for big queries, and prevent the cluster resources from getting exhausted by oversized query requests, to guarantee the system stability.
- StarRocks v2.5 supports limiting computation resource consumption for data loading (INSERT).

|  | Internal Table | External Table | Big Query Restriction | Short Query | INSERT INTO, Broker Load  | Routine Load, Stream Load, Schema Change |
|---|---|---|---|---|---|---|
| 2.2 | √ | × | × | × | × | × |
| 2.3 | √ | √ | √ | √ | × | × |
| 2.4 | √ | √ | √ | √ | × | × |
| 2.5 and later | √ | √ | √ | √ | √ | × |

## Terms

This section describes the terms that you must understand before you use the resource group feature.

### resource group

Each resource group is a set of computing resources from a specific BE. You can divide each BE of your cluster into multiple resource groups. When a query is assigned to a resource group, StarRocks allocates CPU and memory resources to the resource group based on the resource quotas that you specified for the resource group.

You can specify CPU and memory resource quotas for a resource group on a BE by using the following parameters:

- `cpu_core_limit`

  This parameter specifies the soft limit for the number of CPU cores that can be allocated to the resource group on the BE. Valid values: any non-zero positive integer. Range: (1, `avg_be_cpu_cores`], where `avg_be_cpu_cores` represents the average number of CPU cores across all BEs.

  In actual business scenarios, CPU cores that are allocated to the resource group proportionally scale based on the availability of CPU cores on the BE.

  > **NOTE**
  >
  > For example, you configure three resource groups on a BE that provides 16 CPU cores: rg1, rg2, and rg3. The values of `cpu_core_limit` for the three resource groups are `2`, `6`, and `8`, respectively.
  >
  > If all CPU cores of the BE are occupied, the number of CPU cores that can be allocated to each of the three resource groups are 2, 6, and 8, respectively, based on the following calculations:
  >
  > - Number of CPU cores for rg1 = Total number of CPU cores on the BE × (2/16) = 2
  > - Number of CPU cores for rg2 = Total number of CPU cores on the BE × (6/16) = 6
  > - Number of CPU cores for rg3 = Total number of CPU cores on the BE × (8/16) = 8
  >
  > If not all CPU cores of the BE are occupied, as when rg1 and rg2 are loaded but rg3 is not, the number of CPU cores that can be allocated to rg1 and rg2 are 4 and 12, respectively, based on the following calculations:
  >
  > - Number of CPU cores for rg1 = Total number of CPU cores on the BE × (2/8) = 4
  > - Number of CPU cores for rg2 = Total number of CPU cores on the BE × (6/8) = 12

- `mem_limit`

  This parameter specifies the percentage of memory that can be used for queries in the total memory that is provided by the BE. Valid values: (0, 1).

  > **NOTE**
  >
  > The amount of memory that can be used for queries is indicated by the `query_pool` parameter. For more information about the parameter, see [Memory management](Memory_management.md).

- `concurrency_limit`

  This parameter specifies the upper limit of concurrent queries in a resource group. It is used to avoid system overload caused by too many concurrent queries. This parameter takes effect only when it is set greater than 0. Default: 0.

- `max_cpu_cores`

  The CPU core limit for this resource group on a single BE node. It takes effect only when it is set to greater than `0`. Range: [0, `avg_be_cpu_cores`], where `avg_be_cpu_cores` represents the average number of CPU cores across all BE nodes. Default: 0.

- `spill_mem_limit_threshold`

  The memory usage threshold (percentage) at which a resource group triggers the spilling of intermediate results. The valid range is (0, 1). The default value is 1, indicating the threshold does not take effect. This parameter was introduced in v3.1.7.
  - If automatic spilling is enabled (that is, the system variable `spill_mode` is set to `auto`) but the resource group feature is disabled, the system will trigger spilling when the memory usage of a query exceeds 80% of `query_mem_limit`. Here, `query_mem_limit` is the maximum memory that a single query can use, controlled by the system variable `query_mem_limit`, with a default value of 0, indicating no limit.
  - If automatic spilling is enabled, and the query hits a resource group (including all system built-in resource groups), spilling will be triggered if the query meets any of the following conditions:
    - The memory used by all queries in the current resource group exceeds `current BE node memory limit * mem_limit * spill_mem_limit_threshold`.
    - The current query consumes more than 80% of `query_mem_limit`.

On the basis of the above resource consumption restrictions, you can further restrict the resource consumption for big queries with the following parameters:

- `big_query_cpu_second_limit`: This parameter specifies the CPU upper time limit for a big query on a single BE. Concurrent queries add up the time. The unit is second. This parameter takes effect only when it is set greater than 0. Default: 0.
- `big_query_scan_rows_limit`: This parameter specifies the scan row count upper limit for a big query on a single BE. This parameter takes effect only when it is set greater than 0. Default: 0.
- `big_query_mem_limit`: This parameter specifies the memory usage upper limit for a big query on a single BE. The unit is byte. This parameter takes effect only when it is set greater than 0. Default: 0.

> **NOTE**
>
> When a query running in a resource group exceeds the above big query limit, the query will be terminated with an error. You can also view error messages in the `ErrorCode` column of the FE node **fe.audit.log**.

You can set the resource group `type` to `short_query`, or `normal`.

- The default value is `normal`. You do not need specify `normal` in the parameter `type`.
- When queries hit a `short_query` resource group, the BE node reserves the CPU resource specified in `short_query.cpu_core_limit`. The CPU resource reserved for queries that hit `normal` resource group is limited to `BE core - short_query.cpu_core_limit`.
- When no query hits the `short_query` resource group, no limit is imposed to the resource of `normal` resource group.

> **CAUTION**
>
> - You can create at most ONE short query resource group in a StarRocks Cluster.
> - StarRocks does not set set a hard upper limit of CPU resource for `short_query` resource group.

### classifier

Each classifier holds one or more conditions that can be matched to the properties of queries. StarRocks identifies the classifier that best matches each query based on the match conditions and assigns resources for running the query.

Classifiers support the following conditions:

- `user`: the name of the user.
- `role`: the role of the user.
- `query_type`: the type of the query. `SELECT` and `INSERT` (from v2.5) are supported. When INSERT INTO or BROKER LOAD tasks hit a resource group with `query_type` as `insert`, the BE node reserves the specified CPU resources for the tasks.
- `source_ip`: the CIDR block from which the query is initiated.
- `db`: the database which the query accesses. It can be specified by strings separated by commas `,`.
- `plan_cpu_cost_range`: The estimated CPU cost range of the query. The format is `(DOUBLE, DOUBLE]`. The default value is NULL, indicating no such restriction. The `PlanCpuCost` column in `fe.audit.log` represents the system's estimate of the CPU cost for the query. This parameter is supported from v3.1.4 onwards.
- `plan_mem_cost_range`: The system-estimated memory cost range of a query. The format is `(DOUBLE, DOUBLE]`. The default value is NULL, indicating no such restriction. The `PlanMemCost` column in `fe.audit.log` represents the system's estimate of the memory cost for the query. This parameter is supported from v3.1.4 onwards.

A classifier matches a query only when one or all conditions of the classifier match the information about the query. If multiple classifiers match a query, StarRocks calculates the degree of matching between the query and each classifier and identifies the classifier with the highest degree of matching.

> **NOTE**
>
> You can view the resource group to which a query belongs in the `ResourceGroup` column of the FE node **fe.audit.log** or by running `EXPLAIN VERBOSE <query>`, as described in [View the resource group of a query](#view-the-resource-group-of-a-query).

StarRocks calculates the degree of matching between a query and a classifier by using the following rules:

- If the classifier has the same value of `user` as the query, the degree of matching of the classifier increases by 1.
- If the classifier has the same value of `role` as the query, the degree of matching of the classifier increases by 1.
- If the classifier has the same value of `query_type` as the query, the degree of matching of the classifier increases by 1 plus the number obtained from the following calculation: 1/Number of `query_type` fields in the classifier.
- If the classifier has the same value of `source_ip` as the query, the degree of matching of the classifier increases by 1 plus the number obtained from the following calculation: (32 - `cidr_prefix`)/64.
- If the classifier has the same value of `db` as the query, the degree of matching of the classifier increases by 10.
- If the query's CPU cost falls within the `plan_cpu_cost_range`, the degree of matching of the classifier increases by 1.
- If the query's memory cost falls within the `plan_mem_cost_range`, the degree of matching of the classifier increases by 1.

If multiple classifiers match a query, the classifier with a larger number of conditions has a higher degree of matching.

```Plain
-- Classifier B has more conditions than Classifier A. Therefore, Classifier B has a higher degree of matching than Classifier A.


classifier A (user='Alice')


classifier B (user='Alice', source_ip = '192.168.1.0/24')
```

If multiple matching classifiers have the same number of conditions, the classifier whose conditions are described more accurately has a higher degree of matching.

```Plain
-- The CIDR block that is specified in Classifier B is smaller in range than Classifier A. Therefore, Classifier B has a higher degree of matching than Classifier A.
classifier A (user='Alice', source_ip = '192.168.1.0/16')
classifier B (user='Alice', source_ip = '192.168.1.0/24')

-- Classifier C has fewer query types specified in it than Classifier D. Therefore, Classifier C has a higher degree of matching than Classifier D.
classifier C (user='Alice', query_type in ('select'))
classifier D (user='Alice', query_type in ('insert','select'))
```

If multiple classifiers have the same degree of matching, one of the classifiers will be randomly selected.

```Plain
-- If a query simultaneously queries both db1 and db2 and the classifiers E and F have the 
-- highest degree of matching among the hit classifiers, one of E and F will be randomly selected.
classifier E (db='db1')
classifier F (db='db2')
```

## Isolate computing resources

You can isolate computing resources among queries by configuring resource groups and classifiers.

### Enable resource groups

To use resource group, you must enable Pipeline Engine for your StarRcosk cluster:

```SQL
-- Enable Pipeline Engine in the current session.
SET enable_pipeline_engine = true;
-- Enable Pipeline Engine globally.
SET GLOBAL enable_pipeline_engine = true;
```

For loading tasks, you also need to set the FE configuration item `enable_pipeline_load` to enable the Pipeline engine for loading tasks. This item is supported from v2.5.0 onwards.

```sql
ADMIN SET FRONTEND CONFIG ("enable_pipeline_load" = "true");
```

> **NOTE**
>
> From v3.1.0 onwards, Resource Group is enabled by default, and the session variable `enable_resource_group` is deprecated.

### Create resource groups and classifiers

Execute the following statement to create a resource group, associate the resource group with a classifier, and allocate computing resources to the resource group:

```SQL
CREATE RESOURCE GROUP <group_name> 
TO (
    user='string', 
    role='string', 
    query_type in ('select'), 
    source_ip='cidr'
) --Create a classifier. If you create more than one classifier, separate the classifiers with commas (`,`).
WITH (
    "cpu_core_limit" = "INT",
    "mem_limit" = "m%",
    "concurrency_limit" = "INT",
    "type" = "str" --The type of the resource group. Set the value to normal.
);
```

Example:

```SQL
CREATE RESOURCE GROUP rg1
TO 
    (user='rg1_user1', role='rg1_role1', query_type in ('select'), source_ip='192.168.x.x/24'),
    (user='rg1_user2', query_type in ('select'), source_ip='192.168.x.x/24'),
    (user='rg1_user3', source_ip='192.168.x.x/24'),
    (user='rg1_user4'),
    (db='db1')
WITH (
    'cpu_core_limit' = '10',
    'mem_limit' = '20%',
    'big_query_cpu_second_limit' = '100',
    'big_query_scan_rows_limit' = '100000',
    'big_query_mem_limit' = '1073741824'
);
```

### Specify resource group (Optional)

You can specify resource group for the current session directly.

```SQL
SET resource_group = 'group_name';
```

### View resource groups and classifiers

Execute the following statement to query all resource groups and classifiers:

```SQL
SHOW RESOURCE GROUPS ALL;
```

Execute the following statement to query the resource groups and classifiers of the logged-in user:

```SQL
SHOW RESOURCE GROUPS;
```

Execute the following statement to query a specified resource group and its classifiers:

```SQL
SHOW RESOURCE GROUP group_name;
```

Example:

```plain
mysql> SHOW RESOURCE GROUPS ALL;
+------+--------+--------------+----------+------------------+--------+------------------------------------------------------------------------------------------------------------------------+
| Name | Id     | CPUCoreLimit | MemLimit | ConcurrencyLimit | Type   | Classifiers                                                                                                            |
+------+--------+--------------+----------+------------------+--------+------------------------------------------------------------------------------------------------------------------------+
| rg1  | 300039 | 10           | 20.0%    | 11               | NORMAL | (id=300040, weight=4.409375, user=rg1_user1, role=rg1_role1, query_type in (SELECT), source_ip=192.168.2.1/24)         |
| rg1  | 300039 | 10           | 20.0%    | 11               | NORMAL | (id=300041, weight=3.459375, user=rg1_user2, query_type in (SELECT), source_ip=192.168.3.1/24)                         |
| rg1  | 300039 | 10           | 20.0%    | 11               | NORMAL | (id=300042, weight=2.359375, user=rg1_user3, source_ip=192.168.4.1/24)                                                 |
| rg1  | 300039 | 10           | 20.0%    | 11               | NORMAL | (id=300043, weight=1.0, user=rg1_user4)                                                                                |
+------+--------+--------------+----------+------------------+--------+------------------------------------------------------------------------------------------------------------------------+
```

> **NOTE**
>
> In the preceding example, `weight` indicates the degree of matching.

### Manage resource groups and classifiers

You can modify the resource quotas for each resource group. You can also add or delete classifiers from resource groups.

Execute the following statement to modify the resource quotas for an existing resource group:

```SQL
ALTER RESOURCE GROUP group_name WITH (
    'cpu_core_limit' = 'INT',
    'mem_limit' = 'm%'
);
```

Execute the following statement to delete a resource group:

```SQL
DROP RESOURCE GROUP group_name;
```

Execute the following statement to add a classifier to a resource group:

```SQL
ALTER RESOURCE GROUP <group_name> ADD (user='string', role='string', query_type in ('select'), source_ip='cidr');
```

Execute the following statement to delete a classifier from a resource group:

```SQL
ALTER RESOURCE GROUP <group_name> DROP (CLASSIFIER_ID_1, CLASSIFIER_ID_2, ...);
```

Execute the following statement to delete all classifiers of a resource group:

```SQL
ALTER RESOURCE GROUP <group_name> DROP ALL;
```

## Observe resource groups

### View the resource group of a query

You can view the resource group that a query hits from the `ResourceGroup` column in the **fe.audit.log**, or from the `RESOURCE GROUP` column returned after executing `EXPLAIN VERBOSE <query>`. They indicate the resource group that a specific query task matches.

- If the query is not under the management of resource groups, the column value is an empty string `""`.
- If the query is under the management of resource groups but doesn't match any classifier, the column value is an empty string `""`. But this query is assigned to the default resource group `default_wg`.

The resource limits of `default_wg` are as follows:

- `cpu_core_limit`: 1 (for v2.3.7 or earlier) or the number of CPU cores of the BE (for versions later than v2.3.7).
- `mem_limit`: 100%.
- `concurrency_limit`: 0.
- `big_query_cpu_second_limit`: 0.
- `big_query_scan_rows_limit`: 0.
- `big_query_mem_limit`: 0.
- `spill_mem_limit_threshold`: 1.

### Monitoring resource groups

You can set [monitor and alert](Monitor_and_Alert.md) for your resource groups.

Resource group-related FE and BE metrics are as follows. All the metrics below have a `name` label indicating their corresponding resource group.

### FE metrics

The following FE metrics only provide statistics within the current FE node:

| Metric                                          | Unit | Type          | Description                                                        |
| ----------------------------------------------- | ---- | ------------- | ------------------------------------------------------------------ |
| starrocks_fe_query_resource_group               | Count | Instantaneous | The number of queries historically run in this resource group (including those currently running). |
| starrocks_fe_query_resource_group_latency       | ms    | Instantaneous | The query latency percentile for this resource group. The label `type` indicates specific percentiles, including `mean`, `75_quantile`, `95_quantile`, `98_quantile`, `99_quantile`, `999_quantile`. |
| starrocks_fe_query_resource_group_err           | Count | Instantaneous | The number of queries in this resource group that encountered an error. |
| starrocks_fe_resource_group_query_queue_total   | Count | Instantaneous | The total number of queries historically queued in this resource group (including those currently running). This metric is supported from v3.1.4 onwards. It is vailid only when query queues are enabled, see [Query Queues](query_queues.md) for details. |
| starrocks_fe_resource_group_query_queue_pending | Count | Instantaneous | The number of queries currently in the queue of this resource group. This metric is supported from v3.1.4 onwards. It is valid only when query queues are enabled, see [Query Queues](query_queues.md) for details. |
| starrocks_fe_resource_group_query_queue_timeout | Count | Instantaneous | The number of queries in this resource group that have timed out while in the queue. This metric is supported from v3.1.4 onwards. It is valid only when query queues are enabled, see [Query Queues](query_queues.md) for details. |

### BE metrics

| Metric                                      | Unit     | Type          | Description                                                        |
| ----------------------------------------- | -------- | ------------- | ------------------------------------------------------------------ |
| resource_group_running_queries            | Count    | Instantaneous | The number of queries currently running in this resource group.   |
| resource_group_total_queries              | Count    | Instantaneous | The number of queries historically run in this resource group (including those currently running). |
| resource_group_bigquery_count             | Count    | Instantaneous | The number of queries in this resource group that triggered the big query limit. |
| resource_group_concurrency_overflow_count | Count    | Instantaneous | The number of queries in this resource group that triggered the `concurrency_limit` limit. |
| resource_group_mem_limit_bytes            | Bytes    | Instantaneous | The memory limit for this resource group.                         |
| resource_group_mem_inuse_bytes            | Bytes    | Instantaneous | The memory currently in use by this resource group.               |
| resource_group_cpu_limit_ratio            | Percentage | Instantaneous | The ratio of this resource group's `cpu_core_limit` to the total `cpu_core_limit` across all resource groups. |
| resource_group_inuse_cpu_cores            | Count     | Average     | The estimated number of CPU cores in use by this resource group. This value is an approximate estimate. It represents the average value calculated based on the statistics from two consecutive metric collections. This metric is supported from v3.1.4 onwards. |
| resource_group_cpu_use_ratio              | Percentage | Average     | **Deprecated** The ratio of the Pipeline thread time slices used by this resource group to the total Pipeline thread time slices used by all resource groups. It represents the average value calculated based on the statistics from two consecutive metric collections. |
| resource_group_connector_scan_use_ratio   | Percentage | Average     | **Deprecated** The ratio of the external table Scan thread time slices used by this resource group to the total Pipeline thread time slices used by all resource groups. It represents the average value calculated based on the statistics from two consecutive metric collections. |
| resource_group_scan_use_ratio             | Percentage | Average     | **Deprecated** The ratio of the internal table Scan thread time slices used by this resource group to the total Pipeline thread time slices used by all resource groups. It represents the average value calculated based on the statistics from two consecutive metric collections. |

### View resource group usage information

From v3.1.4 onwards, StarRocks supports the SQL statement [SHOW USAGE RESOURCE GROUPS](../sql-reference/sql-statements/Administration/SHOW_USAGE_RESOURCE_GROUPS.md), which is used to display usage information for each resource group across BEs. The descriptions of each field are as follows:

- `Name`: The name of the resource group.
- `Id`: The ID of the resource group.
- `Backend`: The BE's IP or FQDN.
- `BEInUseCpuCores`: The number of CPU cores currently in use by this resource group on this BE. This value is an approximate estimate.
- `BEInUseMemBytes`: The number of memory bytes currently in use by this resource group on this BE.
- `BERunningQueries`: The number of queries from this resource group that are still running on this BE.

Please note:

- BEs periodically report this resource usage information to the Leader FE at the interval specified in `report_resource_usage_interval_ms`, which is by default set to 1 second.
- The results will only show rows where at least one of `BEInUseCpuCores`/`BEInUseMemBytes`/`BERunningQueries` is a positive number. In other words, the information is displayed only when a resource group is actively using some resources on a BE.

Example:

```Plain
MySQL [(none)]> SHOW USAGE RESOURCE GROUPS;
+------------+----+-----------+-----------------+-----------------+------------------+
| Name       | Id | Backend   | BEInUseCpuCores | BEInUseMemBytes | BERunningQueries |
+------------+----+-----------+-----------------+-----------------+------------------+
| default_wg | 0  | 127.0.0.1 | 0.100           | 1               | 5                |
+------------+----+-----------+-----------------+-----------------+------------------+
| default_wg | 0  | 127.0.0.2 | 0.200           | 2               | 6                |
+------------+----+-----------+-----------------+-----------------+------------------+
| wg1        | 0  | 127.0.0.1 | 0.300           | 3               | 7                |
+------------+----+-----------+-----------------+-----------------+------------------+
| wg2        | 0  | 127.0.0.1 | 0.400           | 4               | 8                |
+------------+----+-----------+-----------------+-----------------+------------------+
```

## What to do next

After you configure resource groups, you can manage memory resources and queries. For more information, see the following topics:

- [Memory management](../administration/Memory_management.md)

- [Query management](../administration/Query_management.md)
