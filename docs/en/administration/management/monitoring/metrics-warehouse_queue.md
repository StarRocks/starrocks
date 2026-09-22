---
displayed_sidebar: docs
description: "Metrics for managing warehouses and monitoring the query queue of each warehouse."
---

# Monitoring Metrics for Warehouses

From v3.5 onwards, StarRocks provides a variety of metrics for managing warehouses and monitoring the query queue when the Query Queue feature is enabled for warehouses.

You can enable the Query Queue feature for the warehouse by executing the following SQL command:

```SQL
ALTER WAREHOUSE <warehouse_name> SET("enable_query_queue" = "true");
```

## Metric items

### warehouse_query_queue

- Type: Gauge
- Description: Warehouse query queue metrics with different field labels to monitor various aspects of the warehouse query processing.

#### Field labels

##### query_pending_length

- Type: Gauge
- Description: Number of queries currently pending in the warehouse's query queue.

##### query_running_length

- Type: Gauge
- Description: Number of queries currently running in the warehouse.

##### max_query_queue_length

- Type: Gauge
- Description: Maximum length of the query queue for the warehouse.

##### earliest_query_wait_time

- Type: Gauge
- Description: Wait time of the earliest query in the queue (in seconds). `0.0` is returned if not set.

##### max_query_pending_time_second

- Type: Gauge
- Description: Maximum time (in seconds) that a query has been in the pending state in the warehouse's query queue.

##### max_required_slots

- Type: Gauge
- Description: Maximum number of slots required by any query that has not been allocated with one yet.

##### sum_required_slots

- Type: Gauge
- Description: Sum of all required slots for queries that have not been allocated with one yet.

##### remain_slots

- Type: Gauge
- Description: Number of available slots remained in the warehouse.

##### max_slots

- Type: Gauge
- Description: Maximum number of slots available in the warehouse.

### Per-warehouse query metrics

The following metrics are the per-warehouse counterparts of the cluster-wide FE query metrics (`request_total`, `query_total`, `query_err`, `query_timeout`, `query_analysis_err`, `query_internal_err`, `slow_query`, `qps`, `rps`, `query_latency`, and `query_latency_ms`). They are maintained by each FE node for the requests it handles, carry the `warehouse_id` and `warehouse_name` labels, and are controlled by the FE configuration item `enable_per_warehouse_query_metrics`. All metrics of a warehouse are created (with value 0) as soon as the warehouse handles its first request, and removed when the warehouse is dropped. A query that switches warehouse with a `SET_VAR(warehouse='...')` hint is attributed to the warehouse that executed it, while `warehouse_request_total` is always attributed to the session's warehouse.

#### warehouse_request_total

- Type: Counter
- Description: Total number of requests handled for the warehouse on this FE node.

#### warehouse_query_total

- Type: Counter
- Description: Total number of queries (both successful and failed) that finished in the warehouse on this FE node.

#### warehouse_query_err

- Type: Counter
- Description: Total number of failed queries in the warehouse on this FE node.

#### warehouse_query_timeout

- Type: Counter
- Description: Total number of queries in the warehouse on this FE node that failed because of a timeout.

#### warehouse_query_analysis_err

- Type: Counter
- Description: Total number of queries in the warehouse on this FE node that failed during analysis (for example, syntax errors or references to unknown objects).

#### warehouse_query_internal_err

- Type: Counter
- Description: Total number of queries in the warehouse on this FE node that failed with an internal error (any failure that is neither an analysis error nor a timeout).

#### warehouse_slow_query

- Type: Counter
- Description: Total number of successful queries in the warehouse on this FE node whose latency exceeded the FE configuration item `qe_slow_log_ms`.

#### warehouse_qps

- Type: Gauge
- Description: Queries per second of the warehouse on this FE node, computed from `warehouse_query_total` over the last calculation interval (15 seconds).

#### warehouse_rps

- Type: Gauge
- Description: Requests per second of the warehouse on this FE node, computed from `warehouse_request_total` over the last calculation interval (15 seconds).

#### warehouse_query_latency

- Type: Gauge
- Description: Latency (in milliseconds) of the successful queries that finished in the warehouse on this FE node during the last calculation interval (15 seconds). Uses the same `type` label values as the cluster-wide `query_latency` metric:
  - `mean`: Mean latency.
  - `50_quantile`: Median latency.
  - `75_quantile`: 75th percentile latency.
  - `90_quantile`: 90th percentile latency.
  - `95_quantile`: 95th percentile latency.
  - `99_quantile`: 99th percentile latency.
  - `999_quantile`: 99.9th percentile latency.

#### warehouse_query_latency_ms

- Type: Summary
- Description: Latency (in milliseconds) of all successful queries of the warehouse on this FE node since the FE started. Exposes the `quantile` label (`0.75`, `0.95`, `0.98`, `0.99`, `0.999`) plus the `_sum` and `_count` series, which can be used with `rate()` to compute average latency over any window.

## Usage examples

### Monitor the status of the warehouse's query queue

You can use these metrics to monitor the health and performance of your warehouses.

On the Prometheus endpoint of the FE HTTP port, the metric is exposed as `starrocks_fe_warehouse_query_queue`. The bare name `warehouse_query_queue` is the internal metric name and appears only in the JSON output shown under [Metrics examples](#metrics-examples).

```promql
# Check pending queries in all warehouses
starrocks_fe_warehouse_query_queue{field="query_pending_length"}

# Check running queries in all warehouses
starrocks_fe_warehouse_query_queue{field="query_running_length"}

# Monitor slot utilization
starrocks_fe_warehouse_query_queue{field="remain_slots"} / starrocks_fe_warehouse_query_queue{field="max_slots"}
```

### Monitor the query throughput and latency of warehouses

On the Prometheus endpoint the per-warehouse query metrics are exposed with the `starrocks_fe_` prefix, for example `starrocks_fe_warehouse_query_total`. The counters are per FE node, so sum them across FE nodes to get cluster-wide values for a warehouse.

```promql
# QPS of each warehouse, from the counter (summed across all FE nodes)
sum by (warehouse_name) (rate(starrocks_fe_warehouse_query_total[1m]))

# QPS of each warehouse, from the pre-computed gauge (one series per FE node)
starrocks_fe_warehouse_qps

# Query error rate of each warehouse
sum by (warehouse_name) (rate(starrocks_fe_warehouse_query_err[1m]))
  / sum by (warehouse_name) (rate(starrocks_fe_warehouse_query_total[1m]))

# P95 query latency of each warehouse over the last calculation interval
starrocks_fe_warehouse_query_latency{type="95_quantile"}

# Average query latency of each warehouse over the last 5 minutes, from the summary
sum by (warehouse_name) (rate(starrocks_fe_warehouse_query_latency_ms_sum[5m]))
  / sum by (warehouse_name) (rate(starrocks_fe_warehouse_query_latency_ms_count[5m]))
```

## Collection scope

The metrics on this page are collected in two different ways, so do not `sum()` them together:

- `warehouse_query_queue` and [`warehouse_cngroup`](./metrics-warehouse_cngroup.md) are collected **only on the leader FE** and are controlled by `enable_collect_warehouse_metrics`. They come from the query scheduler and describe the whole warehouse, so they must not be summed across FE nodes.
- The per-warehouse query metrics (`warehouse_request_total`, `warehouse_query_*`, `warehouse_slow_query`, `warehouse_qps`, `warehouse_rps`) are collected **on every FE node** for the statements that node executed and are controlled by `enable_per_warehouse_query_metrics`. Sum them across FE nodes to get the warehouse total.

The query counts and latencies of `warehouse_cngroup` (`success_queries_count`, `failed_queries_count`, `query_avg_latency_ms`, ...) and the per-warehouse query metrics measure different things and are not expected to match. `warehouse_cngroup` is recorded by the scheduler when a query that was assigned to a CN group finishes, and is broken down by `cngroup_name`. The per-warehouse query metrics are recorded at the FE statement layer for every statement, including those that never reach a CN group (analysis errors, metadata-only queries, SHOW statements, reported with an empty `cngroup_name`). Use `warehouse_cngroup` for the scheduler's view of a CN group (node counts, running queries, CPU), and the per-warehouse query metrics for the throughput, error rate and latency that clients observe, per warehouse or per CN group.

## Metric labels

All warehouse metrics include the following labels:

- `warehouse_id`: The unique identifier of the warehouse
- `warehouse_name`: The name of the warehouse

The per-warehouse query metrics (`warehouse_query_*`, `warehouse_slow_query`, `warehouse_qps`) additionally have the `cngroup_name` label, and `warehouse_query_queue` has the `field` label, `warehouse_query_latency` the `type` label, and `warehouse_query_latency_ms` the `quantile` label:

- `cngroup_name`: The CN group the statement was executed in. Empty when the statement ran without a CN group (for example, metadata-only statements, or a warehouse without compute nodes). `warehouse_request_total` and `warehouse_rps` do not carry this label because requests are counted before the statement is parsed; sum the query metrics over `cngroup_name` to get warehouse-level values.
- `field`: The specific field being measured (as listed above)
- `type`: The latency statistic being reported (as listed above)
- `quantile`: The percentile of the summary (`0.75`, `0.95`, `0.98`, `0.99`, `0.999`)

These labels allow you to filter and group metrics by specific warehouses and monitor their individual performance characteristics. 

## Metrics examples

Example 1: Demo for warehouse query queue metrics:

```Plain
{"tags":{"metric":"warehouse_query_queue","field":"query_pending_length","warehouse_id":"0","warehouse_name":"default_warehouse"},"unit":"nounit","value":0},
{"tags":{"metric":"warehouse_query_queue","field":"query_running_length","warehouse_id":"0","warehouse_name":"default_warehouse"},"unit":"nounit","value":0},
{"tags":{"metric":"warehouse_query_queue","field":"max_query_queue_length","warehouse_id":"0","warehouse_name":"default_warehouse"},"unit":"nounit","value":1024},
{"tags":{"metric":"warehouse_query_queue","field":"earliest_query_wait_time","warehouse_id":"0","warehouse_name":"default_warehouse"},"unit":"nounit","value":0.0},
{"tags":{"metric":"warehouse_query_queue","field":"max_query_pending_time_second","warehouse_id":"0","warehouse_name":"default_warehouse"},"unit":"nounit","value":600},
{"tags":{"metric":"warehouse_query_queue","field":"max_required_slots","warehouse_id":"0","warehouse_name":"default_warehouse"},"unit":"nounit","value":0},
{"tags":{"metric":"warehouse_query_queue","field":"sum_required_slots","warehouse_id":"0","warehouse_name":"default_warehouse"},"unit":"nounit","value":0},
{"tags":{"metric":"warehouse_query_queue","field":"remain_slots","warehouse_id":"0","warehouse_name":"default_warehouse"},"unit":"nounit","value":208},
{"tags":{"metric":"warehouse_query_queue","field":"max_slots","warehouse_id":"0","warehouse_name":"default_warehouse"},"unit":"nounit","value":208},
```

Example 2: Demo for per-warehouse query metrics:

```Plain
{"tags":{"metric":"warehouse_request_total","warehouse_id":"0","warehouse_name":"default_warehouse"},"unit":"requests","value":1610},
{"tags":{"metric":"warehouse_query_total","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"requests","value":1532},
{"tags":{"metric":"warehouse_query_err","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"requests","value":7},
{"tags":{"metric":"warehouse_query_timeout","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"requests","value":1},
{"tags":{"metric":"warehouse_query_analysis_err","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"requests","value":5},
{"tags":{"metric":"warehouse_query_internal_err","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"requests","value":1},
{"tags":{"metric":"warehouse_slow_query","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"requests","value":3},
{"tags":{"metric":"warehouse_qps","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":12.6},
{"tags":{"metric":"warehouse_rps","warehouse_id":"0","warehouse_name":"default_warehouse"},"unit":"nounit","value":13.2},
{"tags":{"metric":"warehouse_query_latency","type":"mean","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"milliseconds","value":42.5},
{"tags":{"metric":"warehouse_query_latency","type":"50_quantile","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"milliseconds","value":31.0},
{"tags":{"metric":"warehouse_query_latency","type":"75_quantile","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"milliseconds","value":58.0},
{"tags":{"metric":"warehouse_query_latency","type":"90_quantile","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"milliseconds","value":95.0},
{"tags":{"metric":"warehouse_query_latency","type":"95_quantile","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"milliseconds","value":120.0},
{"tags":{"metric":"warehouse_query_latency","type":"99_quantile","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"milliseconds","value":250.0},
{"tags":{"metric":"warehouse_query_latency","type":"999_quantile","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"milliseconds","value":900.0},
{"tags":{"metric":"warehouse_query_latency_ms","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_","quantile":"0.75"},"unit":"milliseconds","value":58.0},
{"tags":{"metric":"warehouse_query_latency_ms","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_","quantile":"0.95"},"unit":"milliseconds","value":120.0},
{"tags":{"metric":"warehouse_query_latency_ms","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_","quantile":"0.98"},"unit":"milliseconds","value":180.0},
{"tags":{"metric":"warehouse_query_latency_ms","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_","quantile":"0.99"},"unit":"milliseconds","value":250.0},
{"tags":{"metric":"warehouse_query_latency_ms","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_","quantile":"0.999"},"unit":"milliseconds","value":900.0},
{"tags":{"metric":"warehouse_query_latency_ms_sum","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"milliseconds","value":64812.5},
{"tags":{"metric":"warehouse_query_latency_ms_count","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":1525},
```
