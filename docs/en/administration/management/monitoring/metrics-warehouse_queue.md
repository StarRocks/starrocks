---
displayed_sidebar: docs
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

## Usage examples

### Monitor the status of the warehouse's query queue

You can use these metrics to monitor the health and performance of your warehouses:

```promql
# Check pending queries in all warehouses
warehouse_query_queue{field="query_pending_length"}

# Check running queries in all warehouses
warehouse_query_queue{field="query_running_length"}

# Monitor slot utilization
warehouse_query_queue{field="remain_slots"} / warehouse_query_queue{field="max_slots"}
```

## Metric labels

All warehouse metrics include the following labels:

- `warehouse_id`: The unique identifier of the warehouse
- `warehouse_name`: The name of the warehouse
- `field`: The specific field being measured (as listed above)

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
