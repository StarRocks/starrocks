---
displayed_sidebar: docs
hide_table_of_contents: true
description: "Alphabetical d - h"
---

# Metrics d through h

:::note

Metrics for materialized views and shared-data clusters are detailed in the corresponding sections:

- [Metrics for asynchronous materialized view metrics](../metrics-materialized_view.md)
- [Metrics for Shared-data Dashboard metrics, and Starlet Dashboard metrics](../metrics-shared-data.md)

For more information on how to build a monitoring service for your StarRocks cluster, see [Monitor and Alert](../Monitor_and_Alert.md).

:::

## `data_stream_receiver_count`

- Unit: Count
- Description: Cumulative number of instances serving as Exchange receivers in BE.

## `datacache_disk_quota_bytes`

- Unit: Bytes
- Type: Gauge
- Description: The configured disk quota for datacache.

## `datacache_disk_used_bytes`

- Unit: Bytes
- Type: Gauge
- Description: The current disk usage of datacache.

## `datacache_mem_quota_bytes`

- Unit: Bytes
- Type: Gauge
- Description: The configured memory quota for datacache.

## `datacache_mem_used_bytes`

- Unit: Bytes
- Type: Gauge
- Description: The current memory usage of datacache.

## `datacache_meta_used_bytes`

- Unit: Bytes
- Type: Gauge
- Description: The memory usage for datacache metadata.

## `date_column_pool_bytes`

- Unit: Bytes
- Description: Memory used by the DATE column pool.

## `datetime_column_pool_bytes`

- Unit: Bytes
- Description: Memory used by the DATETIME column pool.

## `decimal_column_pool_bytes`

- Unit: Bytes
- Description: Memory used by the DECIMAL column pool.

## `delta_column_group_get_hit_cache`

- Unit: Count
- Description: Total number of delta column group cache hits (for Primary Key tables only).

## `delta_column_group_get_non_pk_hit_cache`

- Unit: Count
- Description: Total number of hits in the delta column group cache (for non-Primary Key tables).

## `delta_column_group_get_non_pk_total`

- Unit: Count
- Description: Total number of times to get delta column group (for non-Primary Key tables only).

## `delta_column_group_get_total`

- Unit: Count
- Description: Total number of times to get delta column groups (for Primary Key tables).

## `disk_bytes_read`

- Unit: Bytes
- Description: Total bytes read from the disk(sectors read * 512).

## `disk_bytes_written`

- Unit: Bytes
- Description: Total bytes written to disk.

## `disk_free`

- Unit: Bytes
- Type: Average
- Description: Free disk capacity.

## `disk_io_svctm`

- Unit: ms
- Type: Average
- Description: Disk IO service time.

## `disk_io_time_ms`

- Unit: ms
- Description: Time spent on I/Os.

## `disk_io_time_weigthed`

- Unit: ms
- Description: Weighted time spent on I/Os.

## `disk_io_util`

- Unit: -
- Type: Average
- Description: Disk usage.

## `disk_read_time_ms`

- Unit: ms
- Description: Time spent on reading from disk. Unit: ms.

## `disk_reads_completed`

- Unit: Count
- Description: Number of successfully completed disk reads.

## `disk_sync_total (Deprecated)`

## `disk_used`

- Unit: Bytes
- Type: Average
- Description: Used disk capacity.

## `disk_write_time_ms`

- Unit: ms
- Description: Time spent on disk writing. Unit: ms.

## `disk_writes_completed`

- Unit: Count
- Description: Total number of successfully completed disk writes.

## `disks_avail_capacity`

- Description: Available capacity of a specific disk.

## `disks_data_used_capacity`

- Description: Used capacity of each disk (represented by a storage path).

## `disks_state`

- Unit: -
- Description: State of each disk. `1` indicates that the disk is in use, and `0` indicates that it is not in use.

## `disks_total_capacity`

- Description: Total capacity of the disk.

## `double_column_pool_bytes`

- Unit: Bytes
- Description: Memory used by the DOUBLE column pool.

## `encryption_keys_created`

- Unit: Count
- Type: Cumulative
- Description: number of file encryption keys created for file encryption

## `encryption_keys_in_cache`

- Unit: Count
- Type: Instantaneous
- Description: number of encryption keys currently in key cache

## `encryption_keys_unwrapped`

- Unit: Count
- Type: Cumulative
- Description: number of encryption meta unwrapped for file decryption

## `engine_requests_total`

- Unit: Count
- Description: Total count of all types of requests between BE and FE, including CREATE TABLE, Publish Version, and tablet clone.

## `fd_num_limit`

- Unit: Count
- Description: Maximum number of file descriptors.

## `fd_num_used`

- Unit: Count
- Description: Number of file descriptors currently in use.

## `fe_cancelled_broker_load_job`

- Unit: Count
- Type: Average
- Description: Number of cancelled broker jobs.

## `fe_cancelled_delete_load_job`

- Unit: Count
- Type: Average
- Description: Number of cancelled delete jobs.

## `fe_cancelled_hadoop_load_job`

- Unit: Count
- Type: Average
- Description: Number of cancelled hadoop jobs.

## `fe_cancelled_insert_load_job`

- Unit: Count
- Type: Average
- Description: Number of cancelled insert jobs.

## `fe_checkpoint_push_per_second`

- Unit: Count/s
- Type: Average
- Description: Number of FE checkpoints.

## `fe_committed_broker_load_job`

- Unit: Count
- Type: Average
- Description: Number of committed broker jobs.

## `fe_committed_delete_load_job`

- Unit: Count
- Type: Average
- Description: Number of committed delete jobs.

## `fe_committed_hadoop_load_job`

- Unit: Count
- Type: Average
- Description: Number of committed hadoop jobs.

## `fe_committed_insert_load_job`

- Unit: Count
- Type: Average
- Description: Number of committed insert jobs.

## `fe_connection_total`

- Unit: Count
- Type: Cumulative
- Description: Total number of FE connections.

## `fe_connections_per_second`

- Unit: Count/s
- Type: Average
- Description: New connection rate of FE.

## `fe_edit_log_read`

- Unit: Count/s
- Type: Average
- Description: Read speed of FE edit log.

## `fe_edit_log_size_bytes`

- Unit: Bytes/s
- Type: Average
- Description: Size of FE edit log.

## `fe_edit_log_write`

- Unit: Bytes/s
- Type: Average
- Description: Write speed of FE edit log.

## `fe_finished_broker_load_job`

- Unit: Count
- Type: Average
- Description: Number of finished broker jobs.

## `fe_finished_delete_load_job`

- Unit: Count
- Type: Average
- Description: Number of finished delete jobs.

## `fe_finished_hadoop_load_job`

- Unit: Count
- Type: Average
- Description: Number of completedhadoop jobs.

## `fe_finished_insert_load_job`

- Unit: Count
- Type: Average
- Description: Number of completed insert jobs.

## `fe_loading_broker_load_job`

- Unit: Count
- Type: Average
- Description: Number of loading broker jobs.

## `fe_loading_delete_load_job`

- Unit: Count
- Type: Average
- Description: Number of loading delete jobs.

## `fe_loading_hadoop_load_job`

- Unit: Count
- Type: Average
- Description: Number of loading hadoop jobs.

## `fe_loading_insert_load_job`

- Unit: Count
- Type: Average
- Description: Number of loading insert jobs.

## `fe_pending_broker_load_job`

- Unit: Count
- Type: Average
- Description: Number of pending broker jobs.

## `fe_pending_delete_load_job`

- Unit: Count
- Type: Average
- Description: Number of pending delete jobs.

## `fe_pending_hadoop_load_job`

- Unit: Count
- Type: Average
- Description: Number of pending hadoop jobs.

## `fe_pending_insert_load_job`

- Unit: Count
- Type: Average
- Description: Number of pending insert jobs.

## `fe_rollup_running_alter_job`

- Unit: Count
- Type: Average
- Description: Number of jobs created in rollup.

## `fe_schema_change_running_job`

- Unit: Count
- Type: Average
- Description: Number of jobs in schema change.

## `float_column_pool_bytes`

- Unit: Bytes
- Description: Memory used by the FLOAT column pool.

## `fragment_endpoint_count`

- Unit: Count
- Description: Cumulative number of instances serving as Exchange senders in BE.

## `fragment_request_duration_us`

- Unit: us
- Description: Cumulative execution time of fragment instances (for non-pipeline engine).

## `fragment_requests_total`

- Unit: Count
- Description: Total fragment instances executing on a BE (for non-pipeline engine).

## `hive_write_bytes`

- Unit: Bytes
- Type: Cumulative
- Labels: `write_type` (`insert` or `overwrite`)
- Description: Total written bytes from Hive write tasks (`INSERT`, `INSERT OVERWRITE`). This represents the total size of data files written to the Hive table. `write_type` distinguishes between the operation types.

## `hive_write_duration_ms_total`

- Unit: Millisecond
- Type: Cumulative
- Labels: `write_type` (`insert` or `overwrite`)
- Description: Total execution time of Hive write tasks (`INSERT`, `INSERT OVERWRITE`) in milliseconds. The duration of each task is added after it ends. `write_type` distinguishes between the operation types.

## `hive_write_files`

- Unit: Count
- Type: Cumulative
- Labels: `write_type` (`insert` or `overwrite`)
- Description: Total number of data files written to Hive from write tasks (`INSERT`, `INSERT OVERWRITE`). This represents the count of data files written to the Hive table. `write_type` distinguishes between the operation types.

## `hive_write_rows`

- Unit: Rows
- Type: Cumulative
- Labels: `write_type` (`insert` or `overwrite`)
- Description: Total written rows from Hive write tasks (`INSERT`, `INSERT OVERWRITE`). This represents the number of rows written to the Hive table. `write_type` distinguishes between the operation types.

## `hive_write_total`

- Unit: Count
- Type: Cumulative
- Labels:
  - `status` (`success` or `failed`)
  - `reason` (`none`, `timeout`, `oom`, `access_denied`, `unknown`)
  - `write_type` (`insert` or `overwrite`)
- Description: Total number of `INSERT` or `INSERT OVERWRITE` tasks that target Hive tables. The metric is incremented by 1 after each task ends, regardless of success or failure. `write_type` distinguishes between the operation types.

## `http_request_send_bytes (Deprecated)`

## `http_requests_total (Deprecated)`

