---
displayed_sidebar: docs
sidebar_position: 20
description: "From v3.1 onwards, StarRocks supports metrics for asynchronous materialized views."
---

# Monitoring Metrics for Asynchronous Materialized Views

From v3.1 onwards, StarRocks supports metrics for asynchronous materialized views.

To allow Prometheus to access the materialized view metadata in your cluster, you must add the following configurations in the Prometheus configuration file **prometheus/prometheus.yml**:

```YAML
global:
....
scrape_configs:

  - job_name: 'dev' 
    metrics_path: '/metrics'    
    # Add the following configurations.
    basic_auth:
      username: 'root'
      password: ''
    params:
      'with_materialized_view_metrics' : ['all']   
....
```

- `username`: The username used to log into your StarRocks cluster. Unless using the root account, the user must be granted both the `user_admin` and `db_admin` roles.
- `password`: The password used to log into your StarRocks cluster.
- `'with_materialized_view_metrics'`: The scope of the metrics to collect. Valid values:
  - `'all'`: All metrics relevant to materialized views are collected.
  - `'minified'`: Gauge metrics and metrics whose values are `0` will not be collected.

## Metric items

### mv_refresh_jobs

- Type: Counter
- Description: Total number of refresh jobs triggered for the materialized view. A refresh job corresponds to one user-initiated or scheduled refresh; a single job may execute multiple task runs internally. Each job is counted once when it reaches a terminal state. MERGED task runs (sub-tasks merged into a later batch) are not counted.

### mv_refresh_total_success_jobs

- Type: Counter
- Description: Number of refresh jobs that completed successfully. Counted once per job.

### mv_refresh_total_failed_jobs

- Type: Counter
- Description: Number of refresh jobs that failed. Counted once per job.

### mv_refresh_total_empty_jobs

- Type: Counter
- Description: Number of canceled refresh jobs of the materialized view because the data to refresh is empty.

### mv_refresh_total_retry_meta_count

- Type: Counter
- Description: Number of times when the materialized view refresh job checks whether the base table is updated.

### mv_query_total_count

- Type: Counter
- Description: Number of times when the materialized view is used in the pre-processing of a query.

### mv_query_total_hit_count

- Type: Counter
- Description: Number of times when the materialized view is considered able to rewrite a query in the query plan. This value may appear higher because the final query plan may skip rewriting due to a high cost.

### mv_query_total_considered_count

- Type: Counter
- Description: Number of times when the materialized view rewrites a query (excluding direct queries against the materialized view).

### mv_query_total_matched_count

- Type: Counter
- Description: Number of times when the materialized view is involved in the final plan of a query (including direct queries against the materialized view).

### mv_refresh_pending_jobs

- Type: Gauge
- Description:| Number of currently pending refresh jobs of the materialized view.

### mv_refresh_running_jobs

- Type: Gauge
- Description:| Number of currently running refresh jobs of the materialized view.

### mv_row_count

- Type: Gauge
- Description:| Row count of the materialized view.

### mv_storage_size

- Type: Gauge
- Description:| Size of the materialized view. Unit: Byte.

### mv_inactive_state

- Type: Gauge
- Description:| Status of the materialized view. Valid values: `0`(active) and `1`(inactive).

### mv_partition_count

- Type: Gauge
- Description:| Number of partitions in the materialized view. The value is `0` if the materialized view is not partitioned.

### mv_refresh_duration

- Type: Histogram
- Description: Wall-clock duration of refresh jobs, in milliseconds. For multi-batch jobs, measured from the first task run start to the final task run completion.

### mv_global_count

- Type: Gauge
- Description: Current number of asynchronous materialized views in the cluster, with labels `refresh_mode` (the materialized view's refresh mode) and `status` (`ACTIVE` or `INACTIVE`). This metric is always emitted, regardless of the per-materialized-view metrics privilege.

### mv_global_query_rewrite_queries_total

- Type: Counter
- Description: Number of queries grouped by materialized view rewrite outcome, with label `state`: `HIT` (the query was rewritten to use a materialized view), `NO_HIT` (rewrite was enabled but no materialized view was used), or `DISABLED` (materialized view rewrite was disabled by the session variable or the FE configuration). Counted once per query.

### mv_global_query_mv_usage_total

- Type: Counter
- Description: Number of times materialized views are used by queries, with labels `usage_type` (`REWRITE` if a query was rewritten to use the materialized view, or `DIRECT` if a query reads the materialized view directly) and `refresh_mode`.
### mv_global_refresh_jobs_total

- Type: Counter
- Description: Total number of materialized view refresh jobs across all materialized views, with label `warehouse_name`. This is the fleet-level aggregate of `mv_refresh_jobs`: counted once per job on its terminal task run, excluding `MERGED` runs. Always emitted, regardless of the per-materialized-view metrics privilege.

### mv_global_refresh_success_jobs_total

- Type: Counter
- Description: Total number of successful materialized view refresh jobs across all materialized views, by `warehouse_name`.

### mv_global_refresh_failed_jobs_total

- Type: Counter
- Description: Total number of failed materialized view refresh jobs across all materialized views, by `warehouse_name`.

### mv_global_refresh_duration

- Type: Histogram
- Description: Per-job wall-clock duration of materialized view refresh jobs, in milliseconds, by `warehouse_name`.

### mv_global_refresh_pending_jobs

- Type: Gauge
- Description: Current number of pending materialized view refresh jobs across all materialized views, aggregated by `warehouse_name`.

### mv_global_refresh_running_jobs

- Type: Gauge
- Description: Current number of running materialized view refresh jobs across all materialized views, aggregated by `warehouse_name`.
