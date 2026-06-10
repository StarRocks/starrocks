---
displayed_sidebar: docs
hide_table_of_contents: true
description: "Alphabetical t - z"
---

# Metrics t through z

## `tablet_base_max_compaction_score`

- Unit: -
- Description: Highest base compaction score of tablets in this BE.

## `tablet_cumulative_max_compaction_score`

- Unit: -
- Description: Highest cumulative compaction score of tablets in this BE.

## `tablet_metadata_mem_bytes`

- Unit: Bytes
- Description: Memory used by tablet metadata.

## `tablet_schema_mem_bytes`

- Unit: Bytes
- Description: Memory used by tablet schema.

## `tablet_update_max_compaction_score`

- Unit: -
- Description: Highest compaction score of tablets in Primary Key tables in the current BE.

## `thrift_connections_total`

- Unit: Count
- Description: Total number of thrift connections (including finished connections).

## `thrift_current_connections (Deprecated)`

## `thrift_opened_clients`

- Unit: Count
- Description: Number of currently opened thrift clients.

## `thrift_used_clients`

- Unit: Count
- Description: Number of thrift clients in use currently.

## `total_column_pool_bytes (Deprecated)`

## `transaction_streaming_load_bytes`

- Unit: Bytes
- Description: Total loading bytes of transaction load.

## `transaction_streaming_load_current_processing`

- Unit: Count
- Description: Number of currently running transactional Stream Load tasks.

## `transaction_streaming_load_duration_ms`

- Unit: ms
- Description: Total time spent on Stream Load transaction Interface.

## `transaction_streaming_load_requests_total`

- Unit: Count
- Description: Total number of transaction load requests.

## `txn_request`

- Unit: -
- Description: Transaction requests of BEGIN, COMMIT, ROLLBACK, and EXEC.

## `uint8_column_pool_bytes`

- Unit: Bytes
- Description: Bytes used by the UINT8 column pool.

## `unused_rowsets_count`

- Unit: Count
- Description: Total number of unused rowsets. Please note that these rowsets will be reclaimed later.

## `update_apply_queue_count`

- Unit: Count
- Description: Queued task count in the Primary Key table transaction APPLY thread pool.

## `update_compaction_duration_us`

- Unit: us
- Description: Total time spent on Primary Key table compactions.

## `update_compaction_outputs_bytes_total`

- Unit: Bytes
- Description: Total bytes written by Primary Key table compactions.

## `update_compaction_outputs_total`

- Unit: Count
- Description: Total number of Primary Key table compactions.

## `update_compaction_task_byte_per_second`

- Unit: Bytes/s
- Description: Estimated rate of Primary Key table compactions.

## `update_compaction_task_cost_time_ns`

- Unit: ns
- Description: Total time spent on the Primary Key table compactions.

## `update_del_vector_bytes_total`

- Unit: Bytes
- Description: Total memory used for caching DELETE vectors in Primary Key tables.

## `update_del_vector_deletes_new`

- Unit: Count
- Description: Total number of newly generated DELETE vectors used in Primary Key tables.

## `update_del_vector_deletes_total (Deprecated)`

## `update_del_vector_dels_num (Deprecated)`

## `update_del_vector_num`

- Unit: Count
- Description: Number of the DELETE vector cache items in Primary Key tables.

## `update_mem_bytes`

- Unit: Bytes
- Description: Memory used by Primary Key table APPLY tasks and Primary Key index.

## `update_primary_index_bytes_total`

- Unit: Bytes
- Description: Total memory cost of the Primary Key index.

## `update_primary_index_num`

- Unit: Count
- Description: Number of Primary Key indexes cached in memory.

## `update_rowset_commit_apply_duration_us`

- Unit: us
- Description: Total time spent on Primary Key table APPLY tasks.

## `update_rowset_commit_apply_total`

- Unit: Count
- Description: Total number of COMMIT and APPLY for Primary Key tables.

## `update_rowset_commit_request_failed`

- Unit: Count
- Description: Total number of failed rowset COMMIT requests in Primary Key tables.

## `update_rowset_commit_request_total`

- Unit: Count
- Description: Total number of rowset COMMIT requests in Primary Key tables.

## `wait_base_compaction_task_num`

- Unit: Count
- Description: Number of base compaction tasks waiting for execution.

## `wait_cumulative_compaction_task_num`

- Unit: Count
- Description: Number of cumulative compaction tasks waiting for execution.

## `writable_blocks_total (Deprecated)`
