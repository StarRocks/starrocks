---
displayed_sidebar: docs
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

- `username`：用于登录到您的 StarRocks 集群的用户名。此用户必须拥有 `user_admin` 角色。
- `password`：用于登录到您的 StarRocks 集群的密码。
- `'with_materialized_view_metrics'`：要收集的指标的范围。有效值包括：
- `'all'`：收集所有与物化视图相关的指标。
- `'minified'`：不收集 Gauge 类指标和值为 `0` 的指标。

## 监控项说明

### mv_refresh_jobs

- 类型：Counter
- 描述：物化视图刷新作业的总数。

### mv_refresh_total_success_jobs

- 类型：Counter
- 描述：执行成功的物化视图刷新作业的数量。

### mv_refresh_total_failed_jobs

- 类型：Counter
- 描述：执行失败的物化视图刷新作业的数量。

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
- 描述：执行成功的物化视图刷新作业的持续时间。
