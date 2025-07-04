---
displayed_sidebar: docs
---

# warehouse_queries

`warehouse_queries` provides information about queries running on each warehouse.

The following fields are provided in `warehouse_queries`:

| **Field**             | **Description**                                              |
| --------------------- | ------------------------------------------------------------ |
| WAREHOUSE_ID          | ID of the warehouse.                                         |
| WAREHOUSE_NAME        | Name of the warehouse.                                       |
| QUERY_ID              | ID of the query.                                             |
| STATE                 | State of the query (e.g., `PENDING`, `RUNNING`, `FINISHED`). |
| EST_COSTS_SLOTS       | Estimated cost slots for the query.                          |
| ALLOCATE_SLOTS        | Allocated slots for the query.                               |
| QUEUED_WAIT_SECONDS   | Time in seconds the query waited in the queue.               |
| QUERY                 | The SQL query string.                                        |
| QUERY_START_TIME      | Start time of the query.                                     |
| QUERY_END_TIME        | End time of the query.                                       |
| QUERY_DURATION        | Duration of the query.                                       |
| EXTRA_MESSAGE         | Extra message.                                               |
