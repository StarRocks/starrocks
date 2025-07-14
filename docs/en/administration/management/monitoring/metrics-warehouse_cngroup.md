---
displayed_sidebar: docs
---

# Monitoring Metrics for Warehouse CN Groups

From v4.0 onwards, StarRocks provides a variety of metrics for monitoring and managing Compute Node Groups (CN Groups) in warehouses.

## Metric items

### warehouse_cngroup

- Type: Gauge/Counter
- Description: Warehouse Compute Node Group metrics with different field labels to monitor various aspects of CN Group performance and health.

#### Field labels

##### cngroup_nodes_count

- Type: Gauge
- Description: Total number of compute nodes in the CN Group.

##### cngroup_alive_nodes_count

- Type: Gauge
- Description: Number of compute nodes alive in the CN Group.

##### running_queries_count

- Type: Gauge
- Description: Number of queries currently running in the CN Group on the current Frontend (FE).

##### cngroup_status

- Type: Gauge
- Description: Status of the CN Group. Valid values: `0` (disabled) and `1` (enabled).

##### scheduled_queries_count

- Type: Counter
- Description: Total number of queries scheduled to the CN Group.

##### success_queries_count

- Type: Counter
- Description: Total number of successfully executed queries in the CN Group.

##### failed_queries_count

- Type: Counter
- Description: Total number of failed queries in the CN Group.

##### query_max_latency_ms

- Type: Gauge
- Description: Maximum query latency (in milliseconds) for the CN Group.

##### query_avg_latency_ms

- Type: Gauge
- Description: Average query latency (in milliseconds) for the CN Group.

##### avg_cpu_used_permille

- Type: Gauge
- Description: Average CPU usage in permille (per thousand) across all compute nodes in the CN Group. `-1.0` is returned if the value is invalid or unavailable.

##### max_compute_node_running_queries_count

- Type: Gauge
- Description: Maximum number of running queries across all compute nodes in the CN Group. `-1` is returned if the value is invalid or unavailable.

## Usage examples

### Monitor CN Group health and performance

You can use these metrics to monitor the health and performance of your warehouse CN Groups:

```promql
# Check the availability of nodes in the CN Group 
warehouse_cngroup{field="cngroup_alive_nodes_count"} / warehouse_cngroup{field="cngroup_nodes_count"}

# Monitor CN Group status
warehouse_cngroup{field="cngroup_status"}

# Check query success rate
warehouse_cngroup{field="success_queries_count"} / warehouse_cngroup{field="scheduled_queries_count"}

# Monitor query latency
warehouse_cngroup{field="query_avg_latency_ms"}

# Check CPU utilization
warehouse_cngroup{field="avg_cpu_used_permille"} / 10
```

## Metric labels

All warehouse CN Group metrics include the following labels:

- `warehouse_id`: The unique identifier of the warehouse
- `warehouse_name`: The name of the warehouse
- `cngroup_name`: The name of the CN Group
- `field`: The specific field being measured (as listed above)

These labels allow you to filter and group metrics by specific warehouses and CN Groups to monitor their individual performance characteristics.

## Performance considerations

- The CN Group resource usage metrics are cached for 1 second to avoid excessive computation
- CPU usage metrics return `-1.0` when the value is invalid, null, or NaN
- Maximum running queries count returns `-1` when the value is invalid or unavailable
- Query latency metrics are updated atomically to ensure thread safety 

## Metric examples

Example 1: Warehouse CN Group metrics demo:

```Plain
{"tags":{"metric":"warehouse_cngroup","field":"cngroup_nodes_count","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":2},
{"tags":{"metric":"warehouse_cngroup","field":"cngroup_alive_nodes_count","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":2},
{"tags":{"metric":"warehouse_cngroup","field":"running_queries_count","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":0},
{"tags":{"metric":"warehouse_cngroup","field":"cngroup_status","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":1},
{"tags":{"metric":"warehouse_cngroup","field":"scheduled_queries_count","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":98},
{"tags":{"metric":"warehouse_cngroup","field":"success_queries_count","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":83},
{"tags":{"metric":"warehouse_cngroup","field":"failed_queries_count","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":15},
{"tags":{"metric":"warehouse_cngroup","field":"query_max_latency_ms","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":1485.0},
{"tags":{"metric":"warehouse_cngroup","field":"query_avg_latency_ms","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":54.255102040816325},
{"tags":{"metric":"warehouse_cngroup","field":"avg_cpu_used_permille","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":54.255102040816325},
{"tags":{"metric":"warehouse_cngroup","field":"max_compute_node_running_queries_count","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":0},
```
