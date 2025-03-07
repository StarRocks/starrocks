---
displayed_sidebar: docs
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

- `username`: The username used to log into your StarRocks cluster. This user must be granted the `user_admin` role.
- `password`: The password used to log into your StarRocks cluster.
- `'with_materialized_view_metrics'`: The scope of the metrics to collect. Valid values:
  - `'all'`: All metrics relevant to materialized views are collected.
  - `'minified'`: Gauge metrics and metrics whose values are `0` will not be collected.

## Metric items

### mv_refresh_jobs

- Type: Counter
- Description: Total number of refresh jobs of the materialized view.

### mv_refresh_total_success_jobs

- Type: Counter
- Description: Number of successful refresh jobs of the materialized view.

### mv_refresh_total_failed_jobs

- Type: Counter
- Description: Number of failed refresh jobs of the materialized view.

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
- Description: Duration of the successful materialized view refresh jobs.
