---
sidebar_position: 120
---

# 基于审计日志的资源组配置

在 StarRocks 中，**资源组** 提供了一种有效的资源隔离机制，通过基于用户身份和查询类型等分类器分配 CPU、内存和并发限制。这一特性对于在多租户环境中实现高效的资源利用至关重要。

传统的资源组配置通常依赖于经验判断。通过分析审计日志表 `starrocks_audit_db__.starrocks_audit_tbl__` 中的历史查询数据，管理员可以采用一种**数据驱动的方法**来调整资源组。关键指标如 CPU 时间、内存消耗和查询并发性提供了关于实际工作负载特征的客观见解。

这种方法有助于：

- 防止因资源争用导致的查询延迟
- 保护集群免于资源耗尽
- 提高整体稳定性和可预测性

本主题提供了基于从审计日志中观察到的工作负载模式推导出适当的资源组参数的分步教程。

:::note
本教程基于使用 AuditLoader 插件的分析，该插件允许您在集群内直接使用 SQL 语句查询审计日志。有关安装插件的详细说明，请参见 [AuditLoader](../administration/management/audit_loader.md)。
:::

## CPU 资源分配

### 目标

确定每个用户的 CPU 消耗，并使用 `cpu_weight` 或 `exclusive_cpu_cores` 按比例分配 CPU 资源。

### 分析

以下 SQL 聚合了过去 30 天每个用户的总 CPU 时间 (`cpuCostNs`)，将其转换为秒，并计算总 CPU 使用率的百分比。

```SQL
SELECT 
    user,
    SUM(cpuCostNs) / 1e9 AS total_cpu_seconds,                  -- 查询总 CPU 时间。
    (
        SUM(cpuCostNs) /
        (
            SELECT SUM(cpuCostNs)
            FROM starrocks_audit_db__.starrocks_audit_tbl__
            WHERE state IN ('EOF','OK')
              AND timestamp >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        )
    ) * 100 AS cpu_usage_percentage                             -- 计算每个用户的总 CPU 使用率百分比。
FROM starrocks_audit_db__.starrocks_audit_tbl__
WHERE state IN ('EOF','OK')                                     -- 仅包含已完成的查询。
  AND timestamp >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)    -- 查询过去 30 天的数据。
GROUP BY user
ORDER BY total_cpu_seconds DESC
LIMIT 20;                                                       -- 列出 CPU 资源消耗最多的前 20 个用户。
```

### 最佳实践

假设每个 BE 固定有一定数量的 CPU 核心（例如，64 核）。如果某个用户占用了总 CPU 时间的 16% (`cpu_usage_percentage`)，那么分配大约 `64 × 16% ≈ 11 核` 是合理的。

您可以按如下方式配置资源组的 CPU 限制：

- `exclusive_cpu_cores`:

  - 其值不得超过单个 BE 上的总核心数。
  - 所有资源组的 `exclusive_cpu_cores` 总和不得超过单个 BE 上的总核心数。

- `cpu_weight`:

  - 仅适用于**软隔离**资源组。
  - 确定在剩余核心上竞争查询之间的相对 CPU 份额。
  - **并不直接映射**到固定数量的 CPU 核心。

## 内存管理

### 目标

识别内存密集型用户，并定义适当的内存限制和熔断器。

### 分析

以下 SQL 计算了过去 30 天内每个用户单个查询的最大内存使用量 (`memCostBytes`)。

```SQL
SELECT 
    user,
    MAX(memCostBytes) / (1024 * 1024) AS max_mem_mb            -- 每个查询的最大内存使用量（以 MB 为单位）。
FROM starrocks_audit_db__.starrocks_audit_tbl__
WHERE state IN ('EOF','OK')                                    -- 仅包含已完成的查询。
  AND timestamp >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)   -- 查询过去 30 天的数据。
GROUP BY user
ORDER BY max_mem_mb DESC
LIMIT 20;                                                      -- 列出内存资源消耗最多的前 20 个用户。
```

### 最佳实践

`max_mem_mb` 表示**所有 BEs 的总内存使用量**。您可以通过以下公式计算每个 BE 上的近似内存使用量：`max_mem_mb / number_of_BEs`。

您可以按如下方式配置资源组的内存限制：

- `big_query_mem_limit`:

  - 用于保护集群免受异常大查询的影响。
  - 您可以将其设置为相对较高的阈值，以避免误报查询终止。

- `mem_limit`:

  - 在大多数情况下，将其设置为较高的值（例如，`0.9`）。

## 并发控制

### 目标

识别每个用户的峰值查询并发性，并定义适当的 `concurrency_limit` 值。

### 分析

以下 SQL 分析了过去 30 天内每分钟的查询并发性，并提取了每个用户观察到的最大并发性。

```SQL
WITH UserConcurrency AS (
    SELECT 
        user,
        DATE_FORMAT(timestamp, '%Y-%m-%d %H:%i') AS minute_bucket,
        COUNT(*) AS query_concurrency
    FROM starrocks_audit_db__.starrocks_audit_tbl__
    WHERE state IN ('EOF', 'OK')                              -- 仅包含已完成的查询。
      AND timestamp >= DATE_SUB(NOW(), INTERVAL 30 DAY)       -- 查询过去 30 天的数据。
      AND LOWER(stmt) LIKE '%select%'                         -- 仅包含 SELECT 语句。
    GROUP BY user, minute_bucket
    HAVING query_concurrency > 1                              -- 排除每分钟并发性小于一个查询的情况。
)
SELECT 
    user,
    minute_bucket,
    query_concurrency / 60.0 AS query_concurrency_per_second  -- 查询每秒并发性。
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
WHERE rn = 1                                                  -- 保留每个用户的最高记录。
ORDER BY query_concurrency_per_second DESC
LIMIT 50;                                                     -- 列出并发性最高的前 50 个用户。
```

### 最佳实践

上述分析是在**分钟粒度**上进行的。实际的每秒并发性可能更高。

您可以按如下方式配置资源组的并发限制：

- `concurrency_limit`

  - 将其设置为观察到的峰值的**1.5 倍**以提供余量。
  - 对于并发峰值极端的用户，您可以进一步启用**查询队列**以平滑峰值负载并保护集群稳定性。

## 异步物化视图的资源隔离

### 目标

防止异步物化视图刷新操作影响交互式查询。

### 分析

以下 SQL 识别内存密集型物化视图刷新操作，通常以 `INSERT OVERWRITE` 语句为特征。

```SQL
SELECT 
    user,
    MAX(memCostBytes) / (1024 * 1024) AS max_mem_mb             -- 每个查询的最大内存使用量（以 MB 为单位）。
FROM starrocks_audit_db__.starrocks_audit_tbl__
WHERE state IN ('EOF','OK')                                     -- 仅包含已完成的查询。
  AND timestamp >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)    -- 查询过去 30 天的数据。
  AND LOWER(stmt) LIKE '%insert overwrite%'                     -- 仅包含物化视图刷新操作。
GROUP BY user
ORDER BY max_mem_mb DESC
LIMIT 20;                                                       -- 列出内存资源消耗最多的前 20 个用户。
```

### 最佳实践

StarRocks 默认提供了一个系统定义的资源组 (`default_mv_wg`) 用于物化视图刷新任务。然而，强烈建议为物化视图刷新任务自定义一个专用的资源组，以强制执行严格的隔离，防止物化视图刷新操作降低前台查询性能。

有关配置资源组限制的说明，请参见 [CPU 资源分配的最佳实践](#最佳实践) 和 [内存管理的最佳实践](#最佳实践-1)。

以下示例仅提供了为物化视图刷新任务创建和分配专用资源组的指导。

1. 为物化视图刷新创建专用资源组：

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

2. 将资源组分配给物化视图。

    - 在创建物化视图时分配：

    ```SQL
    CREATE MATERIALIZED VIEW mv_example
    REFRESH ASYNC
    PROPERTIES (
        'resource_group' = 'rg_mv'
    )
    AS
    SELECT * FROM example_table;
    ```

    - 分配给现有的物化视图：

    ```SQL
    ALTER MATERIALIZED VIEW mv_example SET ("resource_group" = "rg_mv");
    ```

## 另请参阅

- [AuditLoader](../administration/management/audit_loader.md)
- [Resource Group](../administration/management/resource_management/resource_group.md)
- [Query Queues](../administration/management/resource_management/query_queues.md)