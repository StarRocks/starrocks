---
sidebar_position: 120
---

# Audit Log-based Resource Group Configuration

In StarRocks, **Resource Groups** provide an effective mechanism for resource isolation by allocating CPU, memory, and concurrency limits based on classifiers such as user identity and query type. This feature is essential for achieving efficient resource utilization in multi-tenant environments.

Traditional resource group configuration often relies on empirical judgment. By analyzing historical query data from the audit log table
`starrocks_audit_db__.starrocks_audit_tbl__`, administrators can instead adopt a **data-driven approach** to tuning resource groups. Key metrics such as CPU time, memory consumption, and query concurrency offer objective insights into actual workload characteristics.

This approach helps:

- Prevent query latency caused by resource contention
- Protect the cluster from resource exhaustion
- Improve overall stability and predictability

This topic provides step-by-step tutorial on how to derive appropriate resource group parameters based on workload patterns observed from audit logs.

:::note
This tutorial is based on the analysis using AuditLoader plugin, which allows you to query audit logs using SQL statements directly within your cluster. For detailed instructions to install the plugin, see [AuditLoader](../administration/management/audit_loader.md).
:::

## CPU Resource Allocation

### Objective

Determine per-user CPU consumption and allocate CPU resources proportionally using `cpu_weight` or `exclusive_cpu_cores`.

### Analysis

The following SQL aggregates total CPU time per user (`cpuCostNs`) over the last 30 days, converts it to seconds, and calculates the percentage of total CPU usage.

```SQL
SELECT 
    user,
    SUM(cpuCostNs) / 1e9 AS total_cpu_seconds,                  -- Query the total CPU time.
    (
        SUM(cpuCostNs) /
        (
            SELECT SUM(cpuCostNs)
            FROM starrocks_audit_db__.starrocks_audit_tbl__
            WHERE state IN ('EOF','OK')
              AND timestamp >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        )
    ) * 100 AS cpu_usage_percentage                             -- Calculate the percentage of total CPU usage per user.
FROM starrocks_audit_db__.starrocks_audit_tbl__
WHERE state IN ('EOF','OK')                                     -- Include queries that are finished only.
  AND timestamp >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)    -- Query the data of the last 30 days.
GROUP BY user
ORDER BY total_cpu_seconds DESC
LIMIT 20;                                                       -- List the top 20 users with the most CPU resource consumption.
```

### Best Practices

Assume a fixed number of CPU cores per BE (for example, 64 cores). If a user accounts for 16% (`cpu_usage_percentage`) of total CPU time, allocating approximately `64 × 16% ≈ 11 cores` is reasonable.

You can configure the CPU limits for the resource group as follows:

- `exclusive_cpu_cores`:

  - Its value must not exceed the total number of cores on a single BE.
  - The sum of `exclusive_cpu_cores` of all resource groups must not exceed the total number of cores on a single BE.

- `cpu_weight`:

  - Applies only to **soft-isolation** resource groups.
  - Determines relative CPU share among competing queries on remaining cores.
  - Does **not** map directly to a fixed number of CPU cores.

## Memory Management

### Objective

Identify memory-intensive users and define appropriate memory limits and circuit breakers.

### Analysis

The following SQL computes the maximum memory usage per user (`memCostBytes`) for a single query over the last 30 days.

```SQL
SELECT 
    user,
    MAX(memCostBytes) / (1024 * 1024) AS max_mem_mb            -- Max memory usage (in MB) per query.
FROM starrocks_audit_db__.starrocks_audit_tbl__
WHERE state IN ('EOF','OK')                                    -- Include queries that are finished only.
  AND timestamp >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)   -- Query the data of the last 30 days.
GROUP BY user
ORDER BY max_mem_mb DESC
LIMIT 20;                                                      -- List the top 20 users with the most memory resource consumption.
```

### Best Practices

`max_mem_mb` represents **total memory usage across all BEs**. You can calculate the approximate per-BE memory usage as: `max_mem_mb / number_of_BEs`.

You can configure the memory limits for the resource group as follows:

- `big_query_mem_limit`:

  - Protects the cluster from anomalously large queries.
  - You can set it to a relatively high threshold to avoid false-positive query termination.

- `mem_limit`:

  - In most cases, set it to a high value (for example, `0.9`).

## Concurrency Control

### Objective

Identify peak query concurrency per user and define appropriate `concurrency_limit` values.

### Analysis

The following SQL analyzes per-minute query concurrency over the last 30 days and extracts the maximum observed concurrency per user.

```SQL
WITH UserConcurrency AS (
    SELECT 
        user,
        DATE_FORMAT(timestamp, '%Y-%m-%d %H:%i') AS minute_bucket,
        COUNT(*) AS query_concurrency
    FROM starrocks_audit_db__.starrocks_audit_tbl__
    WHERE state IN ('EOF', 'OK')                              -- Include queries that are finished only.
      AND timestamp >= DATE_SUB(NOW(), INTERVAL 30 DAY)       -- Query the data of the last 30 days.
      AND LOWER(stmt) LIKE '%select%'                         -- Include SELECT statements only.
    GROUP BY user, minute_bucket
    HAVING query_concurrency > 1                              -- Exclude scenarios where concurrency is less than one query per minute.
)
SELECT 
    user,
    minute_bucket,
    query_concurrency / 60.0 AS query_concurrency_per_second  -- Query the per-second concurrency.
FROM (
    SELECT 
        user,
        minute_bucket,
        query_concurrency,
        ROW_NUMBER() OVER (
            PARTITION BY user
            ORDER BY query_concurrency DESC
        ) AS rn
    FROM UserConcurrency
) ranked
WHERE rn = 1                                                  -- Keep the highest record for each user.
ORDER BY query_concurrency_per_second DESC
LIMIT 50;                                                     -- List the top 50 users with the highest concurrency.
```

### Best Practices

The above analysis is performed at **minute granularity**. Actual per-second concurrency may be higher.

You can configure the concurrency limits for the resource group as follows:

- `concurrency_limit`

  - Set it to approximately **1.5× the observed peak** to provide headroom.
  - For users with extreme concurrency spikes, you can further enable **Query Queues** to smooth peak load and protect cluster stability.

## Resource Isolation for Asynchronous Materialized Views

### Objective

Prevent asynchronous materialized view refresh operations from impacting interactive queries.

### Analysis

The following SQL identifies memory-intensive materialized view refresh operations, typically characterized by `INSERT OVERWRITE` statements.

```SQL
SELECT 
    user,
    MAX(memCostBytes) / (1024 * 1024) AS max_mem_mb             -- Max memory usage (in MB) per query.
FROM starrocks_audit_db__.starrocks_audit_tbl__
WHERE state IN ('EOF','OK')                                     -- Include queries that are finished only.
  AND timestamp >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)    -- Query the data of the last 30 days.
  AND LOWER(stmt) LIKE '%insert overwrite%'                     -- Include materialized view refresh operations only.
GROUP BY user
ORDER BY max_mem_mb DESC
LIMIT 20;                                                       -- List the top 20 users with the most memory resource consumption.
```

### Best Practices

StarRocks provides a system-defined resource group (`default_mv_wg`) for materialized view refresh tasks by default. However, customizing a dedicated resource group for materialized view refresh tasks is strongly recommended to enforce strict isolation and prevent materialized view refresh operations from degrading foreground query performance.

For instructions on configuring resource group limits, see [Best Practice for CPU Resource Allocation](#best-practices) and [Best Practice for Memory Management](#best-practices-1).

The following example only provides guidance on creating and assign a dedicated resource group to materialized view refresh tasks.

1. Create a dedicated resource group for materialized view refresh:

    ```SQL
    CREATE RESOURCE GROUP rg_mv
    TO (
        user = 'mv_user',
        query_type IN ('insert', 'select')
    )
    WITH (
        'cpu_weight' = '32',
        'mem_limit' = '0.9',
        'concurrency_limit' = '10',
        'spill_mem_limit_threshold' = '0.5'
    );
    ```

2. Assign the resource group to a materialized view.

    - Assign while creating a materialized view:

    ```SQL
    CREATE MATERIALIZED VIEW mv_example
    REFRESH ASYNC
    PROPERTIES (
        'resource_group' = 'rg_mv'
    )
    AS
    SELECT * FROM example_table;
    ```

    - Assign to an existing materialized view:

    ```SQL
    ALTER MATERIALIZED VIEW mv_example SET ("resource_group" = "rg_mv");
    ```

## See Also

- [AuditLoader](../administration/management/audit_loader.md)
- [Resource Group](../administration/management/resource_management/resource_group.md)
- [Query Queues](../administration/management/resource_management/query_queues.md)
