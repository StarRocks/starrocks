---
displayed_sidebar: docs
description: "Asynchronous materialized view monitoring metrics support from v3.1 onwards."
sidebar_position: 20
---

# 异步物化视图监控指标

从 v3.1 版本开始，StarRocks 支持异步物化视图相关监控项。

Prometheus 需要相关权限以访问物化视图相关元数据。因此您需要在 Prometheus 配置文件 **prometheus/prometheus.yml** 中添加以下配置：

```YAML
global:
....
scrape_configs:

  - job_name: 'dev' 
    metrics_path: '/metrics'    
    # 添加以下配置项。
    basic_auth:
      username: 'root'
      password: ''
    params:
      'with_materialized_view_metrics' : ['all']   
....
```

- `username`：用于登录到您的 StarRocks 集群的用户名。除非使用root账户，否则用户必须同时拥有`user_admin` 和 `db_admin`角色。
- `password`：用于登录到您的 StarRocks 集群的密码。
- `'with_materialized_view_metrics'`：要收集的指标的范围。有效值包括：
- `'all'`：收集所有与物化视图相关的指标。
- `'minified'`：不收集 Gauge 类指标和值为 `0` 的指标。

## 监控项说明

### mv_refresh_jobs

- 类型：Counter
- 描述：物化视图触发的刷新作业总数。一个刷新作业对应一次用户发起或调度的刷新；单个作业内部可能执行多个 Task Run。每个作业在达到终止状态时计数一次。MERGED 状态的 Task Run（被合并到后续批次的子任务）不计入。

### mv_refresh_total_success_jobs

- 类型：Counter
- 描述：执行成功的物化视图刷新作业的数量。每个作业成功时计数一次。

### mv_refresh_total_failed_jobs

- 类型：Counter
- 描述：执行失败的物化视图刷新作业的数量。每个作业失败时计数一次。

### mv_refresh_total_empty_jobs

- 类型：Counter
- 描述：因刷新数据为空而取消的物化视图刷新作业的数量。

### mv_refresh_total_retry_meta_count

- 类型：Counter
- 描述：物化视图刷新作业检查基表是否变更的次数。

### mv_query_total_count

- 类型：Counter
- 描述：物化视图在查询的预处理中被使用的次数。

### mv_query_total_hit_count

- 类型：Counter
- 描述：物化视图被查询计划认为能够改写当前查询的次数。由于最终的查询计划可能由于成本过高而跳过改写，因此观察到的值会相比实际较高。

### mv_query_total_considered_count

- 类型：Counter
- 描述：物化视图改写查询的次数（不包括直接针对物化视图的查询）。

### mv_query_total_matched_count

- 类型：Counter
- 描述：物化视图参与最终查询计划的次数（包括直接针对物化视图的查询）。

### mv_refresh_pending_jobs

- 类型：Gauge
- 描述：物化视图当前等待执行的刷新作业数量。

### mv_refresh_running_jobs

- 类型：Gauge
- 描述：物化视图当前正在执行的刷新作业数量。

### mv_row_count

- 类型：Gauge
- 描述：物化视图的行数。

### mv_storage_size

- 类型：Gauge
- 描述：物化视图的大小。单位：Byte。

### mv_inactive_state

- 类型：Gauge
- 描述：物化视图的状态。有效值：`0`（active）和 `1`（inactive）。

### mv_partition_count

- 类型：Gauge
- 描述：物化视图中的分区数。如果物化视图未分区，则该值为 `0`。

### mv_refresh_duration

- 类型：Histogram
- 描述：刷新作业的挂钟时长，单位为毫秒。对于多批次作业，从第一个 Task Run 开始到最后一个 Task Run 完成为止计算。

### mv_global_count

- 类型：Gauge
- 描述：集群中异步物化视图的当前数量，包含标签 `refresh_mode`（物化视图的刷新模式）和 `status`（`ACTIVE` 或 `INACTIVE`）。该指标始终输出，不受单个物化视图指标权限的限制。

### mv_global_query_rewrite_queries_total

- 类型：Counter
- 描述：按物化视图改写结果分组的查询数量，标签 `state` 取值：`HIT`（查询被改写为使用物化视图）、`NO_HIT`（已启用改写但未使用任何物化视图）或 `DISABLED`（会话变量或 FE 配置关闭了物化视图改写）。每个查询计一次。

### mv_global_query_mv_usage_total

- 类型：Counter
- 描述：物化视图被查询使用的次数，包含标签 `usage_type`（`REWRITE` 表示查询被改写为使用物化视图，`DIRECT` 表示直接查询物化视图）和 `refresh_mode`。
