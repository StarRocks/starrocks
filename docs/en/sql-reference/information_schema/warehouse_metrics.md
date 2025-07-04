---
displayed_sidebar: docs
---

# warehouse_metrics

`warehouse_metrics` provides information about the metrics of each warehouse.

The following fields are provided in `warehouse_metrics`:

| **Field**                 | **Description**                                              |
| ------------------------- | ------------------------------------------------------------ |
| WAREHOUSE_ID              | ID of the warehouse.                                         |
| WAREHOUSE_NAME            | Name of the warehouse.                                       |
| QUEUE_PENDING_LENGTH      | Number of pending queries in the queue.                      |
| QUEUE_RUNNING_LENGTH      | Number of running queries in the queue.                      |
| MAX_PENDING_LENGTH        | Maximum number of pending queries allowed in the queue.      |
| MAX_PENDING_TIME_SECOND   | Maximum pending time in seconds for queries in the queue.    |
| EARLIEST_QUERY_WAIT_TIME  | Earliest query wait time.                                    |
| MAX_REQUIRED_SLOTS        | Maximum required slots.                                      |
| SUM_REQUIRED_SLOTS        | Sum of required slots.                                       |
| REMAIN_SLOTS              | Remaining slots.                                             |
| MAX_SLOTS                 | Maximum slots.                                               |
| EXTRA_MESSAGE             | Extra message.                                               |
