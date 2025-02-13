---
displayed_sidebar: docs
---

import BEConfigMethod from '../../_assets/commonMarkdown/BE_config_method.md'

import CNConfigMethod from '../../_assets/commonMarkdown/CN_config_method.md'

import PostBEConfig from '../../_assets/commonMarkdown/BE_dynamic_note.md'

import StaticBEConfigNote from '../../_assets/commonMarkdown/StaticBE_config_note.md'

# BE Configuration

<BEConfigMethod />

<CNConfigMethod />


## View BE configuration items

You can view the BE configuration items using the following command:

```shell
curl http://<BE_IP>:<BE_HTTP_PORT>/varz
```

## Configure BE parameters

<PostBEConfig />

<StaticBEConfigNote />

## Understand BE Parameters

### Server

<!--
##### cluster_id

- Default: -1
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

##### priority_networks

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: Declares a selection strategy for servers that have multiple IP addresses. Note that at most one IP address must match the list specified by this parameter. The value of this parameter is a list that consists of entries, which are separated with semicolons (;) in CIDR notation, such as `10.10.10.0/24`. If no IP address matches the entries in this list, an available IP address of the server will be randomly selected. From v3.3.0, StarRocks supports deployment based on IPv6. If the server has both IPv4 and IPv6 addresses, and this parameter is not specified, the system uses an IPv4 address by default. You can change this behavior by setting `net_use_ipv6_when_priority_networks_empty` to `true`.
- Introduced in: -

##### net_use_ipv6_when_priority_networks_empty

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: A boolean value to control whether to use IPv6 addresses preferentially when `priority_networks` is not specified. `true` indicates to allow the system to use an IPv6 address preferentially when the server that hosts the node has both IPv4 and IPv6 addresses and `priority_networks` is not specified.
- Introduced in: v3.3.0

##### mem_limit

- Default: 90%
- Type: String
- Unit: -
- Is mutable: No
- Description: BE process memory upper limit. You can set it as a percentage ("80%") or a physical limit ("100G"). The default hard limit is 90% of the server's memory size, and the soft limit is 80%. You need to configure this parameter if you want to deploy StarRocks with other memory-intensive services on a same server.
- Introduced in: -

##### num_threads_per_core

- Default: 3
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of threads started on each CPU core.
- Introduced in: -

##### be_http_port

- Default: 8040
- Type: Int
- Unit: -
- Is mutable: No
- Description: The BE HTTP server port.
- Introduced in: -

##### be_http_num_workers

- Default: 48
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of threads used by the HTTP server.
- Introduced in: -

##### be_exit_after_disk_write_hang_second

- Default: 60
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: The length of time that the BE waits to exit after the disk hangs.
- Introduced in: -

##### compress_rowbatches

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: A boolean value to control whether to compress the row batches in RPCs between BEs. `true` indicates compressing the row batches, and `false` indicates not compressing them.
- Introduced in: -

<!--
##### rpc_compress_ratio_threshold

- Default: 1.1
- Type: Double
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### serialize_batch

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: A boolean value to control whether to serialize the row batches in RPCs between BEs. `true` indicates serializing the row batches, and `false` indicates not serializing them.
- Introduced in: -

#### Thrift

##### be_port

- Default: 9060
- Type: Int
- Unit: -
- Is mutable: No
- Description: The BE thrift server port, which is used to receive requests from FEs.
- Introduced in: -

<!--
##### thrift_port

- Default: 0
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### thrift_connect_timeout_seconds

- Default: 3
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description:
- Introduced in: -
-->

##### thrift_client_retry_interval_ms

- Default: 100
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: The time interval at which a thrift client retries.
- Introduced in: -

##### thrift_rpc_timeout_ms

- Default: 5000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: The timeout for a thrift RPC.
- Introduced in: -

<!--
##### thrift_rpc_strict_mode

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### thrift_rpc_max_body_size

- Default: 0
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

#### bRPC

##### brpc_port

- Default: 8060
- Type: Int
- Unit: -
- Is mutable: No
- Description: The BE bRPC port, which is used to view the network statistics of bRPCs.
- Introduced in: -

##### brpc_num_threads

- Default: -1
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of bthreads of a bRPC. The value `-1` indicates the same number with the CPU threads.
- Introduced in: -

<!--
##### brpc_max_connections_per_server

- Default: 1
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

##### brpc_max_body_size

- Default: 2147483648
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: The maximum body size of a bRPC.
- Introduced in: -

<!--
##### brpc_socket_max_unwritten_bytes

- Default: 1073741824
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### enable_auto_adjust_pagecache

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### memory_urgent_level

- Default: 85
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### memory_high_level

- Default: 75
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### pagecache_adjust_period

- Default: 20
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### auto_adjust_pagecache_interval_seconds

- Default: 10
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

#### Heartbeat

##### heartbeat_service_port

- Default: 9050
- Type: Int
- Unit: -
- Is mutable: No
- Description: The BE heartbeat service port, which is used to receive heartbeats from FEs.
- Introduced in: -

##### heartbeat_service_thread_count

- Default: 1
- Type: Int
- Unit: -
- Is mutable: No
- Description: The thread count of the BE heartbeat service.
- Introduced in: -

### Logging

##### sys_log_dir

- Default: `${STARROCKS_HOME}/log`
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory that stores system logs (including INFO, WARNING, ERROR, and FATAL).
- Introduced in: -

##### sys_log_level

- Default: INFO
- Type: String
- Unit: -
- Is mutable: Yes (from v3.3.0, v3.2.7, and v3.1.12)
- Description: The severity levels into which system log entries are classified. Valid values: INFO, WARN, ERROR, and FATAL. This item was changed to a dynamic configuration from v3.3.0, v3.2.7, and v3.1.12 onwards.
- Introduced in: -

##### sys_log_roll_mode

- Default: SIZE-MB-1024
- Type: String
- Unit: -
- Is mutable: No
- Description: The mode in which system logs are segmented into log rolls. Valid values include `TIME-DAY`, `TIME-HOUR`, and `SIZE-MB-`size. The default value indicates that logs are segmented into rolls, each of which is 1 GB.
- Introduced in: -

##### sys_log_roll_num

- Default: 10
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of log rolls to reserve.
- Introduced in: -

##### sys_log_verbose_modules

- Default: 
- Type: Strings
- Unit: -
- Is mutable: No
- Description: The module of the logs to be printed. For example, if you set this configuration item to OLAP, StarRocks only prints the logs of the OLAP module. Valid values are namespaces in BE, including `starrocks`, `starrocks::debug`, `starrocks::fs`, `starrocks::io`, `starrocks::lake`, `starrocks::pipeline`, `starrocks::query_cache`, `starrocks::stream`, and `starrocks::workgroup`.
- Introduced in: -

##### sys_log_verbose_level

- Default: 10
- Type: Int
- Unit: -
- Is mutable: No
- Description: The level of the logs to be printed. This configuration item is used to control the output of logs initiated with VLOG in codes.
- Introduced in: -

##### log_buffer_level

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The strategy for flushing logs. The default value indicates that logs are buffered in memory. Valid values are `-1` and `0`. `-1` indicates that logs are not buffered in memory.
- Introduced in: -

### Statistic report

##### report_task_interval_seconds

- Default: 10
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which to report the state of a task. A task can be creating a table, dropping a table, loading data, or changing a table schema.
- Introduced in: -

##### report_disk_state_interval_seconds

- Default: 60
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which to report the storage volume state, which includes the size of data within the volume.
- Introduced in: -

##### report_tablet_interval_seconds

- Default: 60
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which to report the most updated version of all tablets.
- Introduced in: -

##### report_workgroup_interval_seconds

- Default: 5
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which to report the most updated version of all workgroups.
- Introduced in: -

##### status_report_interval

- Default: 5
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which a query reports its profile, which can be used for query statistics collection by FE.
- Introduced in: -

<!--
##### report_resource_usage_interval_ms

- Default: 1000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### sleep_one_second

- Default: 1
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### sleep_five_seconds

- Default: 5
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description:
- Introduced in: -
-->

##### periodic_counter_update_period_ms

- Default: 500
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: The time interval at which to collect the Counter statistics.
- Introduced in: -

### Storage

<!--
##### create_tablet_worker_count

- Default: 3
- Type: Int
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: The number of threads used to create a tablet. This configuration is changed to dynamic from v3.1.7 onwards.
-->

##### primary_key_limit_size

- Default: 128
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: The maximum size of a key column in Primary Key tables.
- Introduced in: v2.5

##### drop_tablet_worker_count

- Default: 3
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The number of threads used to drop a tablet.
- Introduced in: -

##### alter_tablet_worker_count

- Default: 3
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The number of threads used for Schema Change.
- Introduced in: -

<!--
##### delete_worker_count_normal_priority

- Default: 2
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### delete_worker_count_high_priority

- Default: 1
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### parallel_clone_task_per_path

- Default: 8
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### clone_worker_count

- Default: 3
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of threads used for clone.
- Introduced in: -

##### storage_medium_migrate_count

- Default: 3
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of threads used for storage medium migration (from SATA to SSD).
- Introduced in: -

##### check_consistency_worker_count

- Default: 1
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of threads used for checking the consistency of tablets.
- Introduced in: -

<!--
##### update_schema_worker_count

- Default: 3
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

##### upload_worker_count

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of threads for the upload tasks of backup jobs on a BE node. `0` indicates setting the value to the number of CPU cores on the machine where the BE resides.
- Introduced in: -

##### download_worker_count

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of threads for the download tasks of restore jobs on a BE node. `0` indicates setting the value to the number of CPU cores on the machine where the BE resides.
- Introduced in: -

##### make_snapshot_worker_count

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of threads for the make snapshot tasks on a BE node.
- Introduced in: -

##### release_snapshot_worker_count

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of threads for the release snapshot tasks on a BE node.
- Introduced in: -

##### max_download_speed_kbps

- Default: 50000
- Type: Int
- Unit: KB/Second
- Is mutable: Yes
- Description: The maximum download speed of each HTTP request. This value affects the performance of data replica synchronization across BE nodes.
- Introduced in: -

##### download_low_speed_limit_kbps

- Default: 50
- Type: Int
- Unit: KB/Second
- Is mutable: Yes
- Description: The download speed lower limit of each HTTP request. An HTTP request aborts when it constantly runs with a lower speed than this value within the time span specified in the configuration item `download_low_speed_time`.
- Introduced in: -

##### download_low_speed_time

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The maximum time that an HTTP request can run with a download speed lower than the limit. An HTTP request aborts when it constantly runs with a lower speed than the value of `download_low_speed_limit_kbps` within the time span specified in this configuration item.
- Introduced in: -

##### memory_limitation_per_thread_for_schema_change

- Default: 2
- Type: Int
- Unit: GB
- Is mutable: Yes
- Description: The maximum memory size allowed for each schema change task.
- Introduced in: -

<!--
##### memory_ratio_for_sorting_schema_change

- Default: 0.8
- Type: Double
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### update_cache_expire_sec

- Default: 360
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The expiration time of Update Cache.
- Introduced in: -

##### file_descriptor_cache_clean_interval

- Default: 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which to clean file descriptors that have not been used for a certain period of time.
- Introduced in: -

##### disk_stat_monitor_interval

- Default: 5
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which to monitor health status of disks.
- Introduced in: -

##### replication_threads

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of threads used for replication. `0` indicates setting the thread number to four times the BE CPU core count.
- Introduced in: v3.3.5

##### replication_max_speed_limit_kbps

- Default: 50000
- Type: Int
- Unit: KB/s
- Is mutable: Yes
- Description: The maximum speed of each replication thread.
- Introduced in: v3.3.5

##### replication_min_speed_limit_kbps

- Default: 50
- Type: Int
- Unit: KB/s
- Is mutable: Yes
- Description: The minimum speed of each replication thread.
- Introduced in: v3.3.5
##### replication_min_speed_time_seconds

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time duration allowed for a replication thread to be under the minimum speed. Replication will fail if the time when the actual speed is lower than `replication_min_speed_limit_kbps` exceeds this value.
- Introduced in: v3.3.5

##### clear_expired_replication_snapshots_interval_seconds

- Default: 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which the system clears the expired snapshots left by abnormal replications.
- Introduced in: v3.3.5

##### unused_rowset_monitor_interval

- Default: 30
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which to clean the expired rowsets.
- Introduced in: -

##### storage_root_path

- Default: `${STARROCKS_HOME}/storage`
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory and medium of the storage volume. Example: `/data1,medium:hdd;/data2,medium:ssd`.
  - Multiple volumes are separated by semicolons (`;`).
  - If the storage medium is SSD, add `,medium:ssd` at the end of the directory.
  - If the storage medium is HDD, add `,medium:hdd` at the end of the directory.
- Introduced in: -

##### max_percentage_of_error_disk

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum percentage of error that is tolerable in a storage volume before the corresponding BE node quits.
- Introduced in: -

##### default_num_rows_per_column_file_block

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of rows that can be stored in each row block.
- Introduced in: -

##### max_tablet_num_per_shard

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of tablets in each shard. This configuration item is used to restrict the number of tablet child directories under each storage directory.
- Introduced in: -

##### pending_data_expire_time_sec

- Default: 1800
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The expiration time of the pending data in the storage engine.
- Introduced in: -

##### inc_rowset_expired_sec

- Default: 1800
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The expiration time of the incoming data. This configuration item is used in incremental clone.
- Introduced in: -

##### tablet_rowset_stale_sweep_time_sec

- Default: 1800
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which to sweep the stale rowsets in tablets.
- Introduced in: -

##### max_garbage_sweep_interval

- Default: 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The maximum time interval for garbage collection on storage volumes. This configuration is changed to dynamic from v3.0 onwards.
- Introduced in: -

##### min_garbage_sweep_interval

- Default: 180
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The minimum time interval for garbage collection on storage volumes. This configuration is changed to dynamic from v3.0 onwards.
- Introduced in: -

##### snapshot_expire_time_sec

- Default: 172800
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The expiration time of snapshot files.
- Introduced in: -

##### trash_file_expire_time_sec

- Default: 86400
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which to clean trash files. The default value has been changed from 259,200 to 86,400 since v2.5.17, v3.0.9, and v3.1.6.
- Introduced in: -

##### compact_threads

- Default: 4
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of threads used for concurrent compaction tasks. This configuration is changed to dynamic from v3.1.7 and v3.2.2 onwards.
- Introduced in: v3.0.0

<!--
##### compact_thread_pool_queue_size

- Default: 100
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

##### base_compaction_check_interval_seconds

- Default: 60
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval of thread polling for a Base Compaction.
- Introduced in: -

##### min_base_compaction_num_singleton_deltas

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The minimum number of segments that trigger a Base Compaction.
- Introduced in: -

##### max_base_compaction_num_singleton_deltas

- Default: 100
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of segments that can be compacted in each Base Compaction.
- Introduced in: -

##### base_compaction_num_threads_per_disk

- Default: 1
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of threads used for Base Compaction on each storage volume.
- Introduced in: -

##### base_cumulative_delta_ratio

- Default: 0.3
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: The ratio of cumulative file size to base file size. The ratio reaching this value is one of the conditions that trigger the Base Compaction.
- Introduced in: -

##### base_compaction_interval_seconds_since_last_operation

- Default: 86400
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval since the last Base Compaction. This configuration item is one of the conditions that trigger a Base Compaction.
- Introduced in: -

##### cumulative_compaction_check_interval_seconds

- Default: 1
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval of thread polling for a Cumulative Compaction.
- Introduced in: -

##### min_cumulative_compaction_num_singleton_deltas

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The minimum number of segments to trigger Cumulative Compaction.
- Introduced in: -

##### max_cumulative_compaction_num_singleton_deltas

- Default: 1000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of segments that can be merged in a single Cumulative Compaction. You can reduce this value if OOM occurs during compaction.
- Introduced in: -

##### cumulative_compaction_num_threads_per_disk

- Default: 1
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of Cumulative Compaction threads per disk.
- Introduced in: -

##### max_compaction_candidate_num

- Default: 40960
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of candidate tablets for compaction. If the value is too large, it will cause high memory usage and high CPU load.
- Introduced in: -

<!--
##### enable_lazy_delta_column_compaction

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### update_compaction_check_interval_seconds

- Default: 10
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which to check compaction for Primary Key tables.
- Introduced in: -

##### update_compaction_num_threads_per_disk

- Default: 1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The number of Compaction threads per disk for Primary Key tables.
- Introduced in: -

##### update_compaction_per_tablet_min_interval_seconds

- Default: 120
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The minimum time interval at which compaction is triggered for each tablet in a Primary Key table.
- Introduced in: -

<!--
##### update_compaction_chunk_size_for_row_store

- Default: 0
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### max_update_compaction_num_singleton_deltas

- Default: 1000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of rowsets that can be merged in a single Compaction for Primary Key tables.
- Introduced in: -

##### update_compaction_size_threshold

- Default: 268435456
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The Compaction Score of Primary Key tables is calculated based on the file size, which is different from other table types. This parameter can be used to make the Compaction Score of Primary Key tables similar to that of other table types, making it easier for users to understand.
- Introduced in: -

##### update_compaction_result_bytes

- Default: 1073741824
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: The maximum result size of a single compaction for Primary Key tables.
- Introduced in: -

##### update_compaction_delvec_file_io_amp_ratio

- Default: 2
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Used to control the priority of compaction for rowsets that contain Delvec files in Primary Key tables. The larger the value, the higher the priority.
- Introduced in: -

##### update_compaction_ratio_threshold

- Default: 0.5
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: The maximum proportion of data that a compaction can merge for a Primary Key table in a shared-data cluster. It is recommended to shrink this value if a single tablet becomes excessively large.
- Introduced in: v3.1.5

##### repair_compaction_interval_seconds

- Default: 600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval to poll Repair Compaction threads.
- Introduced in: -

##### manual_compaction_threads

- Default: 4
- Type: Int
- Unit: -
- Is mutable: No
- Description: Number of threads for Manual Compaction.
- Introduced in: -

##### min_compaction_failure_interval_sec

- Default: 120
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The minimum time interval at which a tablet compaction can be scheduled since the previous compaction failure.
- Introduced in: -

##### min_cumulative_compaction_failure_interval_sec

- Default: 30
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The minimum time interval at which Cumulative Compaction retries upon failures.
- Introduced in: -

##### max_compaction_concurrency

- Default: -1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum concurrency of compactions (including both Base Compaction and Cumulative Compaction). The value `-1` indicates that no limit is imposed on the concurrency. `0` indicates disabling compaction. This parameter is mutable when the Event-based Compaction Framework is enabled.
- Introduced in: -

##### compaction_trace_threshold

- Default: 60
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time threshold for each compaction. If a compaction takes more time than the time threshold, StarRocks prints the corresponding trace.
- Introduced in: -

<!--
##### compaction_max_memory_limit

- Default: -1
- Type: Int
- Unit: 
- Is mutable: No
- Description: 
- Introduced in: -
-->

<!--
##### compaction_max_memory_limit_percent

- Default: 100
- Type: Int
- Unit: -
- Is mutable: No
- Description: 
- Introduced in: -
-->

##### compaction_memory_limit_per_worker

- Default: 2147483648
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: The maximum memory size allowed for each Compaction thread.
- Introduced in: -

##### enable_rowset_verify

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to verify the correctness of generated rowsets. When enabled, the correctness of the generated rowsets will be checked after Compaction and Schema Change.
- Introduced in: -

##### vertical_compaction_max_columns_per_group

- Default: 5
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of columns per group of Vertical Compactions.
- Introduced in: -

##### enable_event_based_compaction_framework

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to enable the Event-based Compaction Framework. `true` indicates Event-based Compaction Framework is enabled, and `false` indicates it is disabled. Enabling Event-based Compaction Framework can greatly reduce the overhead of compaction in scenarios where there are many tablets or a single tablet has a large amount of data.
- Introduced in: -

##### enable_size_tiered_compaction_strategy

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to enable the Size-tiered Compaction policy (excluding Primary Key tables). `true` indicates the Size-tiered Compaction strategy is enabled, and `false` indicates it is disabled.
- Introduced in: -

##### enable_pk_size_tiered_compaction_strategy

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to enable the Size-tiered Compaction policy for Primary Key tables. `true` indicates the Size-tiered Compaction strategy is enabled, and `false` indicates it is disabled.
- Introduced in: This item takes effect for shared-data clusters from v3.2.4 and v3.1.10 onwards, and for shared-nothing clusters from v3.2.5 and v3.1.10 onwards.

##### size_tiered_min_level_size

- Default: 131072
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: The data size of the minimum level in the Size-tiered Compaction policy. Rowsets smaller than this value immediately trigger the data compaction.
- Introduced in: -

##### size_tiered_level_multiple

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The multiple of data size between two contiguous levels in the Size-tiered Compaction policy.
- Introduced in: -

##### size_tiered_level_multiple_dupkey

- Default: 10
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: In the Size-tiered Compaction policy, the multiple of the data amount difference between two adjacent levels for Duplicate Key tables.
- Introduced in: -

##### size_tiered_level_num

- Default: 7
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The number of levels for the Size-tiered Compaction policy. At most one rowset is reserved for each level. Therefore, under a stable condition, there are, at most, as many rowsets as the level number specified in this configuration item.
- Introduced in: -

##### enable_check_string_lengths

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to check the data length during loading to solve compaction failures caused by out-of-bound VARCHAR data.
- Introduced in: -

##### max_row_source_mask_memory_bytes

- Default: 209715200
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: The maximum memory size of the row source mask buffer. When the buffer is larger than this value, data will be persisted to a temporary file on the disk. This value should be set lower than the value of `compaction_mem_limit`.
- Introduced in: -

##### memory_maintenance_sleep_time_s

- Default: 10
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which ColumnPool GC is triggered. StarRocks executes GC periodically and returns the released memory to the operating system.
- Introduced in: -

##### load_process_max_memory_limit_bytes

- Default: 107374182400
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: The maximum size limit of memory resources that can be taken up by all load processes on a BE node.
- Introduced in: -

##### load_process_max_memory_limit_percent

- Default: 30
- Type: Int
- Unit: -
- Is mutable: No
- Description: The soft limit (in percentage) of memory resources that can be taken up by all load processes on a BE node.
- Introduced in: -


##### load_process_max_memory_hard_limit_ratio

- Default: 2
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The hard limit (ratio) of memory resources that can be taken up by all load processes on a BE node. When `enable_new_load_on_memory_limit_exceeded` is set to `false`, and the memory consumption of all loading processes exceeds `load_process_max_memory_limit_percent * load_process_max_memory_hard_limit_ratio`, new loading processes will be rejected.
- Introduced in: v3.3.2

##### enable_new_load_on_memory_limit_exceeded

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to allow new loading processes when the hard memory resource limit is reached. `true` indicates new loading processes will be allowed, and `false` indicates they will be rejected.
- Introduced in: v3.3.2

##### sync_tablet_meta

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: A boolean value to control whether to enable the synchronization of the tablet metadata. `true` indicates enabling synchronization, and `false` indicates disabling it.
- Introduced in: -

##### storage_flood_stage_usage_percent

- Default: 95
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Hard limit of the storage usage percentage in all BE directories. If the storage usage (in percentage) of the BE storage directory exceeds this value and the remaining storage space is less than `storage_flood_stage_left_capacity_bytes`, Load and Restore jobs are rejected. You need to set this item together with the FE configuration item `storage_usage_hard_limit_percent` to allow the configurations to take effect.
- Introduced in: -

##### storage_flood_stage_left_capacity_bytes

- Default: 107374182400
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: Hard limit of the remaining storage space in all BE directories. If the remaining storage space of the BE storage directory is less than this value and the storage usage (in percentage) exceeds `storage_flood_stage_usage_percent`, Load and Restore jobs are rejected. You need to set this item together with the FE configuration item `storage_usage_hard_limit_reserve_bytes` to allow the configurations to take effect.
- Introduced in: -

<!--
##### storage_high_usage_disk_protect_ratio

- Default: 0.1
- Type: Double
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### tablet_meta_checkpoint_min_new_rowsets_num

- Default: 10
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The minimum number of rowsets to create since the last TabletMeta Checkpoint.
- Introduced in: -

##### tablet_meta_checkpoint_min_interval_secs

- Default: 600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval of thread polling for a TabletMeta Checkpoint.
- Introduced in: -

##### tablet_map_shard_size

- Default: 32
- Type: Int
- Unit: -
- Is mutable: No
- Description: The tablet map shard size. The value must be a power of two.
- Introduced in: -

##### tablet_max_versions

- Default: 1000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of versions allowed on a tablet. If the number of versions exceeds this value, new write requests will fail.
- Introduced in: -

##### tablet_max_pending_versions

- Default: 1000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of pending versions that are tolerable on a Primary Key tablet. Pending versions refer to versions that are committed but not applied yet.
- Introduced in: -

##### tablet_stat_cache_update_interval_second

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: 是
- Description: The time interval at which Tablet Stat Cache updates.
- Introduced in: -

##### enable_bitmap_union_disk_format_with_set

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: A boolean value to control whether to enable the new storage format of the BITMAP type, which can improve the performance of bitmap_union. `true` indicates enabling the new storage format, and `false` indicates disabling it.
- Introduced in: -

<!--
##### l0_l1_merge_ratio

- Default: 10
- Type: Int
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### l0_max_file_size

- Default: 209715200
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### l0_min_mem_usage

- Default: 2097152
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### l0_max_mem_usage

- Default: 104857600
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### l0_snapshot_size

- Default: 16777216
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### max_tmp_l1_num

- Default: 10
- Type: Int
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### enable_parallel_get_and_bf

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### enable_pindex_minor_compaction

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### max_allow_pindex_l2_num

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### pindex_major_compaction_num_threads

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### pindex_major_compaction_limit_per_disk

- Default: 1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum concurrency of compaction on a disk. This addresses the issue of uneven I/O across disks due to compaction. This issue can cause excessively high I/O for certain disks.
- Introduced in: v3.0.9

<!--
##### pindex_major_compaction_schedule_interval_seconds

- Default: 15
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### pindex_shared_data_gc_evict_interval_seconds

- Default: 18000
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### enable_pindex_filter

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### enable_pindex_compression

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### max_bf_read_bytes_percent

- Default: 10
- Type: Int
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### enable_pindex_rebuild_in_compaction

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

### Loading

##### push_worker_count_normal_priority

- Default: 3
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of threads used to handle a load task with NORMAL priority.
- Introduced in: -

##### push_worker_count_high_priority

- Default: 3
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of threads used to handle a load task with HIGH priority.
- Introduced in: -

##### transaction_publish_version_worker_count

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of threads used to publish a version. When this value is set to less than or equal to `0`, the system uses the CPU core count as the value, so as to avoid insufficient thread resources when import concurrency is high but only a fixed number of threads are used. From v2.5, the default value has been changed from `8` to `0`.
- Introduced in: -

<!--
##### transaction_apply_worker_count

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### get_pindex_worker_count

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### clear_transaction_task_worker_count

- Default: 1
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of threads used for clearing transaction.
- Introduced in: -

##### load_data_reserve_hours

- Default: 4
- Type: Int
- Unit: Hours
- Is mutable: No
- Description: The reservation time for the files produced by small-scale loadings.
- Introduced in: -

##### load_error_log_reserve_hours

- Default: 48
- Type: Int
- Unit: Hours
- Is mutable: Yes
- Description: The time for which data loading logs are reserved.
- Introduced in: -

##### number_tablet_writer_threads

- Default: 16
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The number of threads used for Stream Load. This configuration is changed to dynamic from v3.1.7 onwards.
- Introduced in: -

<!--
##### max_queueing_memtable_per_tablet

- Default: 2
- Type: Int
- Unit:
- Is mutable: Yes
- Description: The maximum number of queuing memtables on each tablet. This parameter is used to control the memory usage of loading tasks.
- Introduced in: v3.1
-->

<!--
##### stale_memtable_flush_time_sec

- Default: 0
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: 0 means prohibited. Other memtables whose last update time is greater than stale_memtable_flush_time_sec will be persisted when memory is insufficient.
- Introduced in: -
-->

<!--
##### dictionary_encoding_ratio

- Default: 0.7
- Type: Double
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### dictionary_page_size

- Default: 1048576
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### dictionary_encoding_ratio_for_non_string_column

- Default: 0
- Type: Double
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### dictionary_speculate_min_chunk_size

- Default: 10000
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### enable_streaming_load_thread_pool

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### streaming_load_thread_pool_num_min

- Default: 0
- Type: Int
- Unit: Minutes -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### streaming_load_thread_pool_idle_time_ms

- Default: 2000
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description:
- Introduced in: -
-->

##### streaming_load_max_mb

- Default: 102400
- Type: Int
- Unit: MB
- Is mutable: Yes
- Description: The maximum size of a file that can be streamed into StarRocks. From v3.0, the default value has been changed from `10240` to `102400`.
- Introduced in: -

##### streaming_load_max_batch_size_mb

- Default: 100
- Type: Int
- Unit: MB
- Is mutable: Yes
- Description: The maximum size of a JSON file that can be streamed into StarRocks.
- Introduced in: -

##### streaming_load_rpc_max_alive_time_sec

- Default: 1200
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: The RPC timeout for Stream Load.
- Introduced in: -

##### write_buffer_size

- Default: 104857600
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: The buffer size of MemTable in the memory. This configuration item is the threshold to trigger a flush.
- Introduced in: -

##### load_process_max_memory_limit_bytes

- Default: 107374182400
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: The maximum size limit of memory resources that can be taken up by all load processes on a BE node.
- Introduced in: -

##### txn_commit_rpc_timeout_ms (Deprecated)

- Default: 60000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: The timeout for a transaction commit RPC. Since v3.2.0, this parameter is deprecated.
- Introduced in: -

##### max_consumer_num_per_group

- Default: 3
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of consumers in a consumer group of Routine Load.
- Introduced in: -

<!--
##### max_pulsar_consumer_num_per_group

- Default: 10
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### routine_load_kafka_timeout_second

- Default: 10
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### routine_load_pulsar_timeout_second

- Default: 10
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description:
- Introduced in: -
-->

##### flush_thread_num_per_store

- Default: 2
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Number of threads that are used for flushing MemTable in each store.
- Introduced in: -

##### lake_flush_thread_num_per_store

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Number of threads that are used for flushing MemTable in each store in shared-data mode. 
When this value is set to `0`, the system uses twice of the CPU core count as the value.
When this value is set to less than `0`, the system uses the product of its absolute value and the CPU core count as the value.
- Introduced in: 3.1.12, 3.2.7

##### max_runnings_transactions_per_txn_map

- Default: 100
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of transactions that can run concurrently in each partition.
- Introduced in: -

##### enable_stream_load_verbose_log

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Specifies whether to log the HTTP requests and responses for Stream Load jobs.
- Introduced in: v2.5.17, v3.0.9, v3.1.6, v3.2.1

### Query engine

##### scanner_thread_pool_thread_num

- Default: 48
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The number of threads which the storage engine used for concurrent storage volume scanning. All threads are managed in the thread pool.
- Introduced in: -

##### scanner_thread_pool_queue_size

- Default: 102400
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of scan tasks supported by the storage engine.
- Introduced in: -

<!--
##### udf_thread_pool_size

- Default: 1
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### port

- Default: 20001
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### broker_write_timeout_seconds

- Default: 30
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description:
- Introduced in: -
-->

##### scanner_row_num

- Default: 16384
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum row count returned by each scan thread in a scan.
- Introduced in: -

<!--
##### max_hdfs_scanner_num

- Default: 50
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

##### max_scan_key_num

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of scan keys segmented by each query.
- Introduced in: -

##### max_pushdown_conditions_per_column

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of conditions that allow pushdown in each column. If the number of conditions exceeds this limit, the predicates are not pushed down to the storage layer.
- Introduced in: -

##### exchg_node_buffer_size_bytes

- Default: 10485760
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: The maximum buffer size on the receiver end of an exchange node for each query. This configuration item is a soft limit. A backpressure is triggered when data is sent to the receiver end with an excessive speed.
- Introduced in: -

<!--
##### sorter_block_size

- Default: 8388608
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### column_dictionary_key_ratio_threshold

- Default: 0
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### column_dictionary_key_size_threshold

- Default: 0
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### profile_report_interval

- Default: 30
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### file_descriptor_cache_capacity

- Default: 16384
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of file descriptors that can be cached.
- Introduced in: -

##### min_file_descriptor_number

- Default: 60000
- Type: Int
- Unit: -
- Is mutable: No
- Description: The minimum number of file descriptors in the BE process.
- Introduced in: -

##### index_stream_cache_capacity

- Default: 10737418240
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: The cache capacity for the statistical information of BloomFilter, Min, and Max.
- Introduced in: -

##### storage_page_cache_limit

- Default: 20%
- Type: String
- Unit: -
- Is mutable: Yes
- Description: The PageCache size. It can be specified as size, for example, `20G`, `20,480M`, `20,971,520K`, or `21,474,836,480B`. It can also be specified as the ratio (percentage) to the memory size, for example, `20%`. It takes effect only when `disable_storage_page_cache` is set to `false`.
- Introduced in: -

##### disable_storage_page_cache

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: A boolean value to control whether to disable PageCache.
  - When PageCache is enabled, StarRocks caches the recently scanned data.
  - PageCache can significantly improve the query performance when similar queries are repeated frequently.
  - `true` indicates disabling PageCache.
  - The default value of this item has been changed from `true` to `false` since StarRocks v2.4.
- Introduced in: -

<!--
##### enable_bitmap_index_memory_page_cache

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable memory cache for Bitmap index. Memory cache is recommended if you want to use Bitmap indexes to accelerate point queries.
- Introduced in: v3.1
-->

<!--
##### enable_zonemap_index_memory_page_cache

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### enable_ordinal_index_memory_page_cache

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### disable_column_pool

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

##### fragment_pool_thread_num_min

- Default: 64
- Type: Int
- Unit: Minutes -
- Is mutable: No
- Description: The minimum number of threads used for query.
- Introduced in: -

##### fragment_pool_thread_num_max

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of threads used for query.
- Introduced in: -

##### fragment_pool_queue_size

- Default: 2048
- Type: Int
- Unit: -
- Is mutable: No
- Description: The upper limit of the query number that can be processed on each BE node.
- Introduced in: -

<!--
##### query_scratch_dirs

- Default: `${STARROCKS_HOME}`
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### max_free_io_buffers

- Default: 128
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### disable_mem_pools

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### use_mmap_allocate_chunk

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### chunk_reserved_bytes_limit

- Default: 2147483648
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### pprof_profile_dir

- Default: `${STARROCKS_HOME}/log`
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

##### enable_prefetch

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: A boolean value to control whether to enable the pre-fetch of the query. `true` indicates enabling pre-fetch, and `false` indicates disabling it.
- Introduced in: -

<!--
##### query_max_memory_limit_percent

- Default: 90
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

##### query_pool_spill_mem_limit_threshold

- Default: 1.0
- Type: Double
- Unit: -
- Is mutable: No
- Description: If automatic spilling is enabled, when the memory usage of all queries exceeds `query_pool memory limit * query_pool_spill_mem_limit_threshold`, intermediate result spilling will be triggered.
- Introduced in: v3.2.7

##### result_buffer_cancelled_interval_time

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The wait time before BufferControlBlock releases data.
- Introduced in: -

##### max_memory_sink_batch_count

- Default: 20
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of Scan Cache batches.
- Introduced in: -

##### scan_context_gc_interval_min

- Default: 5
- Type: Int
- Unit: Minutes
- Is mutable: Yes
- Description: The time interval at which to clean the Scan Context.
- Introduced in: -

##### path_gc_check_step

- Default: 1000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of files that can be scanned continuously each time.
- Introduced in: -

##### path_gc_check_step_interval_ms

- Default: 10
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: The time interval between file scans.
- Introduced in: -

##### path_scan_interval_second

- Default: 86400
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time interval at which GC cleans expired data.
- Introduced in: -

<!--
##### pipeline_scan_thread_pool_thread_num

- Default: 0
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

##### pipeline_connector_scan_thread_num_per_cpu

- Default: 8
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: The number of scan threads assigned to Pipeline Connector per CPU core in the BE node. This configuration is changed to dynamic from v3.1.7 onwards.
- Introduced in: -

<!--
##### pipeline_scan_thread_pool_queue_size

- Default: 102400
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### pipeline_exec_thread_pool_thread_num

- Default: 0
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### pipeline_prepare_thread_pool_thread_num

- Default: 0
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### pipeline_prepare_thread_pool_queue_size

- Default: 102400
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### pipeline_sink_io_thread_pool_thread_num

- Default: 0
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### pipeline_sink_io_thread_pool_queue_size

- Default: 102400
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### pipeline_sink_buffer_size

- Default: 64
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### pipeline_sink_brpc_dop

- Default: 64
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### pipeline_max_num_drivers_per_exec_thread

- Default: 10240
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### pipeline_print_profile

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### pipeline_driver_queue_level_time_slice_base_ns

- Default: 200000000
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### pipeline_driver_queue_ratio_of_adjacent_queue

- Default: 1.2
- Type: Double
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### pipeline_scan_queue_mode

- Default: 0
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### pipeline_scan_queue_level_time_slice_base_ns

- Default: 100000000
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### pipeline_scan_queue_ratio_of_adjacent_queue

- Default: 1.5
- Type: Double
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### pipeline_analytic_max_buffer_size

- Default: 128
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### pipeline_analytic_removable_chunk_num

- Default: 128
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### pipeline_analytic_enable_streaming_process

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### pipeline_analytic_enable_removable_cumulative_process

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### tablet_internal_parallel_min_splitted_scan_rows

- Default: 16384
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### tablet_internal_parallel_max_splitted_scan_rows

- Default: 1048576
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### tablet_internal_parallel_max_splitted_scan_bytes

- Default: 536870912
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### tablet_internal_parallel_min_scan_dop

- Default: 4
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### bitmap_serialize_version

- Default: 1
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

##### max_hdfs_file_handle

- Default: 1000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of HDFS file descriptors that can be opened.
- Introduced in: -

<!--
##### max_segment_file_size

- Default: 1073741824
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### rewrite_partial_segment

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### object_storage_access_key_id

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### object_storage_secret_access_key

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### object_storage_endpoint

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### object_storage_bucket

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### object_storage_region

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### object_storage_max_connection

- Default: 102400
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### object_storage_endpoint_use_https

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### object_storage_endpoint_path_style_access

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

##### object_storage_connect_timeout_ms

- Default: -1
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: Timeout duration to establish socket connections with object storage. `-1` indicates to use the default timeout duration of the SDK configurations.
- Introduced in: v3.0.9

##### object_storage_request_timeout_ms

- Default: -1
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: Timeout duration to establish HTTP connections with object storage. `-1` indicates to use the default timeout duration of the SDK configurations.
- Introduced in: v3.0.9

<!--
##### text_io_range_size

- Default: 16777216
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### enable_orc_late_materialization

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### enable_orc_libdeflate_decompression

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### orc_natural_read_size

- Default: 8388608
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### orc_coalesce_read_enable

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### orc_tiny_stripe_threshold_size

- Default: 8388608
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### parquet_coalesce_read_enable

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### parquet_late_materialization_enable

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: A boolean value to control whether to enable the late materialization of Parquet reader to improve performance. `true` indicates enabling late materialization, and `false` indicates disabling it.
- Introduced in: -

##### parquet_late_materialization_v2_enable

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: A boolean value to control whether to enable the late materialization v2 of Parquet reader to improve performance. `true` indicates enabling late materialization v2, and `false` indicates disabling it. In v3.3, only `parquet_late_materialization_enable` is used, and this variable is deprecated.
- Introduced in: v3.2

##### parquet_page_index_enable

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: A boolean value to control whether to enable the pageindex of Parquet file to improve performance. `true` indicates enabling pageindex, and `false` indicates disabling it.
- Introduced in: v3.3

<!--
##### io_coalesce_read_max_buffer_size

- Default: 8388608
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### io_coalesce_read_max_distance_size

- Default: 1048576
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

##### io_coalesce_adaptive_lazy_active

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Based on the selectivity of predicates, adaptively determines whether to combine the I/O of predicate columns and non-predicate columns.
- Introduced in: v3.2

<!--
##### io_tasks_per_scan_operator

- Default: 4
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### connector_io_tasks_per_scan_operator

- Default: 16
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### connector_io_tasks_min_size

- Default: 2
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### connector_io_tasks_adjust_interval_ms

- Default: 50
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### connector_io_tasks_adjust_step

- Default: 1
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### connector_io_tasks_adjust_smooth

- Default: 4
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### connector_io_tasks_slow_io_latency_ms

- Default: 50
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### scan_use_query_mem_ratio

- Default: 0.25
- Type: Double
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### connector_scan_use_query_mem_ratio

- Default: 0.3
- Type: Double
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

##### hdfs_client_enable_hedged_read

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Specifies whether to enable the hedged read feature.
- Introduced in: v3.0

##### hdfs_client_hedged_read_threadpool_size

- Default: 128
- Type: Int
- Unit: -
- Is mutable: No
- Description: Specifies the size of the Hedged Read thread pool on your HDFS client. The thread pool size limits the number of threads to dedicate to the running of hedged reads in your HDFS client. It is equivalent to the `dfs.client.hedged.read.threadpool.size` parameter in the **hdfs-site.xml** file of your HDFS cluster.
- Introduced in: v3.0

##### hdfs_client_hedged_read_threshold_millis

- Default: 2500
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: Specifies the number of milliseconds to wait before starting up a hedged read. For example, you have set this parameter to `30`. In this situation, if a read from a block has not returned within 30 milliseconds, your HDFS client immediately starts up a new read against a different block replica. It is equivalent to the `dfs.client.hedged.read.threshold.millis` parameter in the **hdfs-site.xml** file of your HDFS cluster.
- Introduced in: v3.0

<!--
##### hdfs_client_max_cache_size

- Default: 64
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### hdfs_client_io_read_retry

- Default: 0
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### aws_sdk_logging_trace_enabled

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### aws_sdk_logging_trace_level

- Default: trace
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### aws_sdk_enable_compliant_rfc3986_encoding

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### experimental_s3_max_single_part_size

- Default: 16777216
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### experimental_s3_min_upload_part_size

- Default: 16777216
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### max_load_dop

- Default: 16
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### enable_load_colocate_mv

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### meta_threshold_to_manual_compact

- Default: 10737418240
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### manual_compact_before_data_dir_load

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### deliver_broadcast_rf_passthrough_bytes_limit

- Default: 131072
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### deliver_broadcast_rf_passthrough_inflight_num

- Default: 10
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### send_rpc_runtime_filter_timeout_ms

- Default: 1000
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### send_runtime_filter_via_http_rpc_min_size

- Default: 67108864
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### rpc_connect_timeout_ms

- Default: 30000
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### max_batch_publish_latency_ms

- Default: 100
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### jaeger_endpoint

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### query_debug_trace_dir

- Default: `${STARROCKS_HOME}/query_debug_trace`
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

##### query_cache_capacity

- Default: 536870912
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: The size of the query cache in the BE. The default size is 512 MB. The size cannot be less than 4 MB. If the memory capacity of the BE is insufficient to provision your expected query cache size, you can increase the memory capacity of the BE.
- Introduced in: -

##### enable_json_flat

- Default: false
- Type: Boolean
- Unit:
- Is mutable: Yes
- Description: Whether to enable the Flat JSON feature. After this feature is enabled, newly loaded JSON data will be automatically flattened, improving JSON query performance.
- Introduced in: v3.3.0

##### json_flat_null_factor

- Default: 0.3
- Type: Double
- Unit:
- Is mutable: Yes
- Description: The proportion of NULL values in the column to extract for Flat JSON. A column will not be extracted if its proportion of NULL value is higher than this threshold. This parameter takes effect only when `enable_json_flat` is set to `true`.
- Introduced in: v3.3.0

##### json_flat_sparsity_factor

- Default: 0.9
- Type: Double
- Unit:
- Is mutable: Yes
- Description: The proportion of columns with the same name for Flat JSON. Extraction is not performed if the proportion of columns with the same name is lower than this value. This parameter takes effect only when `enable_json_flat` is set to `true`.
- Introduced in: v3.3.0

##### json_flat_column_max

- Default: 100
- Type: Int
- Unit:
- Is mutable: Yes
- Description: The maximum number of sub-fields that can be extracted by Flat JSON. This parameter takes effect only when `enable_json_flat` is set to `true`.
- Introduced in: v3.3.0

##### enable_compaction_flat_json

- Default: True
- Type: Boolean
- Unit:
- Is mutable: Yes
- Description: Whether to enable compaction for Flat JSON data.
- Introduced in: v3.3.3

##### enable_lazy_dynamic_flat_json

- Default: True
- Type: Boolean
- Unit:
- Is mutable: Yes
- Description: Whether to enable Lazy Dyamic Flat JSON when a query misses Flat JSON schema in read process. When this item is set to `true`, StarRocks will postpone the Flat JSON operation to calculation process instead of read process.
- Introduced in: v3.3.3

##### jit_lru_cache_size

- Default: 0
- Type: Int
- Unit: GB
- Is mutable: Yes
- Description: The LRU cache size for JIT compilation. It represents the actual size of the cache if it is set to greater than 0. If it is set to less than or equal to 0, the system will adaptively set the cache using the formula `jit_lru_cache_size = min(mem_limit*0.01, 1GB)` (while `mem_limit` of the node must be greater or equal to 16 GB).
- Introduced in: -

### Shared-data

##### starlet_port

- Default: 9070
- Type: Int
- Unit: -
- Is mutable: No
- Description: An extra agent service port for BE and CN.
- Introduced in: -

<!--
##### starlet_cache_thread_num

- Default: 16
- Type: Int
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### starlet_cache_dir

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### starlet_cache_check_interval

- Default: 900
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### starlet_cache_evict_interval

- Default: 60
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The interval at which the system performs cache eviction in a shared-data cluster with file data cache enabled.
- Introduced in: v3.0
-->

<!--
##### starlet_cache_evict_low_water

- Default: 0.1
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: The low water at which cache eviction is triggered. In a shared-data cluster with file data cache enabled, if the percentage of available disk space is lower than this value, cache eviction will be triggered.
- Introduced in: v3.0
-->

<!--
##### starlet_cache_evict_high_water

- Default: 0.2
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: The high water at which cache eviction is stopped. In a shared-data cluster with file data cache enabled, if the percentage of available disk space is higher than this value, cache eviction will be stopped.
- Introduced in: v3.0
-->

<!--
##### starlet_cache_dir_allocate_policy

- Default: 0
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### starlet_cache_evict_percent

- Default: 0.1
- Type: Double
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### starlet_cache_evict_throughput_mb

- Default: 200
- Type: Int
- Unit: MB
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### starlet_fs_stream_buffer_size_bytes

- Default: 1048576
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### starlet_use_star_cache

- Default: false in v3.1 and true from v3.2.3
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable Data Cache in a shared-data cluster. `true` indicates enabling this feature and `false` indicates disabling it. The default value is set from `false` to `true` from v3.2.3 onwards.
- Introduced in: v3.1

<!--
##### starlet_star_cache_mem_size_percent

- Default: 0
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

##### starlet_star_cache_disk_size_percent

- Default: 80
- Type: Int
- Unit: -
- Is mutable: No
- Description: The percentage of disk capacity that Data Cache can use at most in a shared-data cluster.
- Introduced in: v3.1

<!--
##### starlet_star_cache_disk_size_bytes

- Default: 0
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### starlet_star_cache_block_size_bytes

- Default: 1048576
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### starlet_s3_virtual_address_domainlist

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### starlet_s3_client_max_cache_capacity

- Default: 8
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### starlet_s3_client_num_instances_per_cache

- Default: 1
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### starlet_fs_read_prefetch_enable

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### starlet_fs_read_prefetch_threadpool_size

- Default: 128
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### starlet_fslib_s3client_nonread_max_retries

- Default: 5
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### starlet_fslib_s3client_nonread_retry_scale_factor

- Default: 200
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### starlet_fslib_s3client_connect_timeout_ms

- Default: 1000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### lake_metadata_cache_limit

- Default: 2147483648
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### lake_print_delete_log

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### starlet_fslib_s3client_request_timeout_ms

- Default: -1
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: An alias of `object_storage_request_timeout_ms`. Refer to [object_storage_request_timeout_ms](#object_storage_request_timeout_ms) for details.
- Introduced in: v3.3.9

##### lake_compaction_stream_buffer_size_bytes

- Default: 1048576
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: The reader's remote I/O buffer size for cloud-native table compaction in a shared-data cluster. The default value is 1MB. You can increase this value to accelerate compaction process.
- Introduced in: v3.2.3

<!--
##### experimental_lake_ignore_lost_segment

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### experimental_lake_wait_per_put_ms

- Default: 0
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### experimental_lake_wait_per_get_ms

- Default: 0
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### experimental_lake_wait_per_delete_ms

- Default: 0
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### experimental_lake_ignore_pk_consistency_check

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### lake_publish_version_slow_log_ms

- Default: 1000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### lake_enable_publish_version_trace_log

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### lake_vacuum_retry_pattern

- Default: *request rate*
- Type: String
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### lake_vacuum_retry_max_attempts

- Default: 5
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### lake_vacuum_retry_min_delay_ms

- Default: 100
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### lake_max_garbage_version_distance

- Default: 100
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### enable_primary_key_recover

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### lake_enable_compaction_async_write

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### lake_pk_compaction_max_input_rowsets

- Default: 500
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of input rowsets allowed in a Primary Key table compaction task in a shared-data cluster. The default value of this parameter is changed from `5` to `1000` since v3.2.4 and v3.1.10, and to `500` since v3.3.1 and v3.2.9. After the Sized-tiered Compaction policy is enabled for Primary Key tables (by setting `enable_pk_size_tiered_compaction_strategy` to `true`), StarRocks does not need to limit the number of rowsets for each compaction to reduce write amplification. Therefore, the default value of this parameter is increased.
- Introduced in: v3.1.8, v3.2.3

<!--
##### lake_pk_preload_memory_limit_percent

- Default: 30
- Type: Int
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### dependency_librdkafka_debug_enable

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### dependency_librdkafka_debug

- Default: all
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### loop_count_wait_fragments_finish

- Default: 0
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

### Data Lake

##### jdbc_connection_pool_size

- Default: 8
- Type: Int
- Unit: -
- Is mutable: No
- Description: The JDBC connection pool size. On each BE node, queries that access the external table with the same `jdbc_url` share the same connection pool.
- Introduced in: -

##### jdbc_minimum_idle_connections

- Default: 1
- Type: Int
- Unit: -
- Is mutable: No
- Description: The minimum number of idle connections in the JDBC connection pool.
- Introduced in: -

##### jdbc_connection_idle_timeout_ms

- Default: 600000
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: The length of time after which an idle connection in the JDBC connection pool expires. If the connection idle time in the JDBC connection pool exceeds this value, the connection pool closes idle connections beyond the number specified in the configuration item `jdbc_minimum_idle_connections`.
- Introduced in: -

<!--
##### spill_local_storage_dir

- Default: `${STARROCKS_HOME}/spill`
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### experimental_spill_skip_sync

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### spill_init_partition

- Default: 16
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### spill_max_partition_level

- Default: 7
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### spill_max_partition_size

- Default: 1024
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### spill_max_log_block_container_bytes

- Default: 10737418240
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### spill_max_dir_bytes_ratio

- Default: 0.8
- Type: Double
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### internal_service_query_rpc_thread_num

- Default: -1
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### cardinality_of_inject

- Default: 10
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### directory_of_inject

- Default: /src/exec/pipeline/hashjoin,/src/exec/pipeline/scan,/src/exec/pipeline/aggregate,/src/exec/pipeline/crossjoin,/src/exec/pipeline/sort,/src/exec/pipeline/exchange,/src/exec/pipeline/analysis
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

##### datacache_enable

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to enable Data Cache. `true` indicates Data Cache is enabled, and `false` indicates Data Cache is disabled. The default value is changed to `true` from v3.3.
- Introduced in: -

##### datacache_mem_size

- Default: 10%
- Type: String
- Unit: -
- Is mutable: No
- Description: The maximum amount of data that can be cached in memory. You can set it as a percentage (for example, `10%`) or a physical limit (for example, `10G`, `21474836480`). It is recommended to set the value of this parameter to at least 10 GB.
- Introduced in: -

##### datacache_disk_size

- Default: 0
- Type: String
- Unit: -
- Is mutable: No
- Description: The maximum amount of data that can be cached on a single disk. You can set it as a percentage (for example, `80%`) or a physical limit (for example, `2T`, `500G`). For example, if you use two disks and set the value of the `datacache_disk_size` parameter as `21474836480` (20 GB), a maximum of 40 GB data can be cached on these two disks. The default value is `0`, which indicates that only memory is used to cache data.
- Introduced in: -

##### datacache_auto_adjust_enable

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable Automatic Scaling for Data Cache disk capacity. When it is enabled, the system dynamically adjusts the cache capacity based on the current disk usage rate.
- Introduced in: v3.3.0

##### datacache_disk_high_level

- Default: 90
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The upper limit of disk usage (in percentage) that triggers the automatic scaling up of the cache capacity. When the disk usage exceeds this value, the system automatically evicts cache data from the Data Cache. From v3.4.0 onwards, the default value is changed from `80` to `90`.
- Introduced in: v3.3.0

##### datacache_disk_safe_level

- Default: 80
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The safe level of disk usage (in percentage) for Data Cache. When Data Cache performs automatic scaling, the system adjusts the cache capacity with the goal of maintaining disk usage as close to this value as possible. From v3.4.0 onwards, the default value is changed from `70` to `80`.
- Introduced in: v3.3.0

##### datacache_disk_low_level

- Default: 60
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The lower limit of disk usage (in percentage) that triggers the automatic scaling down of the cache capacity. When the disk usage remains below this value for the period specified in `datacache_disk_idle_seconds_for_expansion`, and the space allocated for Data Cache is fully utilized, the system will automatically expand the cache capacity by increasing the upper limit.
- Introduced in: v3.3.0

##### datacache_disk_adjust_interval_seconds

- Default: 10
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The interval of Data Cache automatic capacity scaling. At regular intervals, the system checks the cache disk usage, and triggers Automatic Scaling when necessary.
- Introduced in: v3.3.0

##### datacache_disk_idle_seconds_for_expansion

- Default: 7200
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The minimum wait time for Data Cache automatic expansion. Automatic scaling up is triggered only if the disk usage remains below `datacache_disk_low_level` for longer than this duration.
- Introduced in: v3.3.0

##### datacache_min_disk_quota_for_adjustment

- Default: 107374182400
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: The minimum effective capacity for Data Cache Automatic Scaling. If the system tries to adjust the cache capacity to less than this value, the cache capacity will be directly set to `0` to prevent suboptimal performance caused by frequent cache fills and evictions due to insufficient cache capacity.
- Introduced in: v3.3.0

##### datacache_block_buffer_enable

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to enable Block Buffer to optimize Data Cache efficiency. When Block Buffer is enabled, the system reads the Block data from the Data Cache and caches it in a temporary buffer, thus reducing the extra overhead caused by frequent cache reads.
- Introduced in: v3.2.0

##### datacache_tiered_cache_enable

- Default: false 
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to enable tiered cache mode for Data Cache. When tiered cache mode is enabled, Data Cache is configured with two layers of caching, memory and disk. When disk data becomes hot data, it is automatically loaded into the memory cache, and when the data in the memory cache becomes cold, it is automatically flushed to disk. When tiered cache mode is not enabled, the memory and disk configured for Data Cache form two separate cache spaces and cache different types of data, with no data flow between them.
- Introduced in: v3.2.5

##### datacache_eviction_policy

- Default: slru
- Type: String
- Unit: -
- Is mutable: No
- Description: The eviction policy of Data Cache. Valid values: `lru` (least recently used) and `slru` (Segmented LRU).
- Introduced in: v3.4.0

##### datacache_inline_item_count_limit

- Default: 130172
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of inline cache items in Data Cache. For some particularly small cache blocks, Data Cache stores them in `inline` mode, which caches the block data and metadata together in memory.
- Introduced in: v3.4.0

<!--
##### datacache_unified_instance_enable

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to use a unified Data Cache instance for queries against external catalogs and cloud-native tables (in shared-data clusters).
- Introduced in: v3.4.0
-->

##### query_max_memory_limit_percent

- Default: 90
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum memory that the Query Pool can use. It is expressed as a percentage of the Process memory limit.
- Introduced in: v3.1.0

<!--
##### datacache_block_size

- Default: 262144
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### datacache_checksum_enable

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### datacache_direct_io_enable

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### datacache_max_concurrent_inserts

- Default: 1500000
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### datacache_max_flying_memory_mb

- Default: 256
- Type: Int
- Unit: MB
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### datacache_adaptor_enable

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### datacache_skip_read_factor

- Default: 1
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### datacache_block_buffer_enable

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### datacache_engine

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### report_datacache_metrics_interval_ms

- Default: 60000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### block_cache_enable

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### block_cache_disk_size

- Default: 0
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### block_cache_disk_path

- Default: `${STARROCKS_HOME}/block_cache/`
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### block_cache_meta_path

- Default: `${STARROCKS_HOME}/block_cache/`
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### block_cache_block_size

- Default: 262144
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### block_cache_mem_size

- Default: 2147483648
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### block_cache_max_concurrent_inserts

- Default: 1500000
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### block_cache_checksum_enable

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### block_cache_direct_io_enable

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### block_cache_engine

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### query_cache_num_lanes_per_driver

- Default: 4
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### send_channel_buffer_limit

- Default: 67108864
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### exception_stack_level

- Default: 1
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### exception_stack_white_list

- Default: std::
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### exception_stack_black_list

- Default: apache::thrift::,ue2::,arangodb::
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### rocksdb_cf_options_string

- Default: block_based_table_factory={block_cache={capacity=256M;num_shard_bits=0}}
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### local_exchange_buffer_mem_limit_per_driver

- Default: 134217728
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### streaming_agg_limited_memory_size

- Default: 134217728
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### streaming_agg_chunk_buffer_size

- Default: 1024
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### wait_apply_time

- Default: 6000
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### binlog_file_max_size

- Default: 536870912
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### binlog_page_max_size

- Default: 1048576
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### txn_info_history_size

- Default: 20000
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### file_write_history_size

- Default: 10000
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### update_cache_evict_internal_sec

- Default: 11
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### enable_auto_evict_update_cache

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### load_tablet_timeout_seconds

- Default: 60
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### enable_pk_value_column_zonemap

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### primary_key_batch_get_index_memory_limit

- Default: 104857600
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### enable_short_key_for_one_column_filter

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### enable_http_stream_load_limit

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### finish_publish_version_internal

- Default: 100
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### get_txn_status_internal_sec

- Default: 10
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### dump_metrics_with_bvar

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### enable_drop_tablet_if_unfinished_txn

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### lake_service_max_concurrency

- Default: 0
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum concurrency of RPC requests in a shared-data cluster. Incoming requests will be rejected when this threshold is reached. When this item is set to `0`, no limit is imposed on the concurrency.
- Introduced in: -

<!--
##### lake_vacuum_min_batch_delete_size

- Default: 100
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### desc_hint_split_range

- Default: 10
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### lake_local_pk_index_unused_threshold_seconds

- Default: 86400
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### lake_enable_vertical_compaction_fill_data_cache

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to allow compaction tasks to cache data on local disks in a shared-data cluster.
- Introduced in: v3.1.7, v3.2.3

<!--
##### dictionary_cache_refresh_timeout_ms

- Default: 60000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### dictionary_cache_refresh_threadpool_size

- Default: 8
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### pk_dump_interval_seconds

- Default: 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### enable_profile_for_external_plan

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### olap_string_max_length

- Default: 1048576
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

### Other

##### user_function_dir

- Default: `${STARROCKS_HOME}/lib/udf`
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory used to store User-defined Functions (UDFs).
- Introduced in: -

##### default_mv_resource_group_memory_limit

- Default: 0.8
- Type: Double
- Unit:
- Is mutable: Yes
- Description: The maximum memory proportion (per BE node) that can be used by the materialized view refresh tasks in the resource group `default_mv_wg`. The default value indicates 80% of the memory.
- Introduced in: v3.1

##### default_mv_resource_group_cpu_limit

- Default: 1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of CPU cores (per BE node) that can be used by the materialized view refresh tasks in the resource group `default_mv_wg`.
- Introduced in: v3.1

##### default_mv_resource_group_concurrency_limit

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum concurrency (per BE node) of the materialized view refresh tasks in the resource group `default_mv_wg`. The default value `0` indicates no limits.
- Introduced in: v3.1

##### default_mv_resource_group_spill_mem_limit_threshold

- Default: 0.8
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: The memory usage threshold before a materialized view refresh task in the resource group `default_mv_wg` triggers intermediate result spilling. The default value indicates 80% of the memory.
- Introduced in: v3.1

<!--
##### pull_load_task_dir

- Default: `${STARROCKS_HOME}/var/pull_load`
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### web_log_bytes

- Default: 1048576
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### be_service_threads

- Default: 64
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### default_query_options

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### local_library_dir

- Default: `${UDF_RUNTIME_DIR}`
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### tablet_writer_open_rpc_timeout_sec

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### make_snapshot_rpc_timeout_ms

- Default: 20000
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### olap_table_sink_send_interval_ms

- Default: 10
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### enable_token_check

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: A boolean value to control whether to enable the token check. `true` indicates enabling the token check, and `false` indicates disabling it.
- Introduced in: -

<!--
##### enable_system_metrics

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### num_cores

- Default: 0
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### ignore_broken_disk

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### scratch_dirs

- Default: /tmp
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### madvise_huge_pages

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### mmap_buffers

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### memory_max_alignment

- Default: 16
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### priority_queue_remaining_tasks_increased_frequency

- Default: 512
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### enable_metric_calculator

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### es_scroll_keepalive

- Default: 5m
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### es_http_timeout_ms

- Default: 5000
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### es_index_max_result_window

- Default: 10000
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### max_client_cache_size_per_host

- Default: 10
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

##### small_file_dir

- Default: `${STARROCKS_HOME}/lib/small_file/`
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory used to store the files downloaded by the file manager.
- Introduced in: -

<!--
##### path_gc_check

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### path_gc_check_interval_second

- Default: 86400
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### pk_index_map_shard_size

- Default: 4096
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### plugin_path

- Default: `${STARROCKS_HOME}/plugin`
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### txn_map_shard_size

- Default: 128
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### txn_shard_size

- Default: 1024
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### ignore_load_tablet_failure

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### ignore_rowset_stale_unconsistent_delete

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### vector_chunk_size

- Default: 4096
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### late_materialization_ratio

- Default: 10
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### metric_late_materialization_ratio

- Default: 1000
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### max_transmit_batched_bytes

- Default: 262144
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### bitmap_max_filter_items

- Default: 30
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### bitmap_max_filter_ratio

- Default: 1
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### bitmap_filter_enable_not_equal

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### storage_format_version

- Default: 2
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### null_encoding

- Default: 0
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### pre_aggregate_factor

- Default: 80
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### sys_minidump_enable

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### sys_minidump_dir

- Default: `${STARROCKS_HOME}`
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### sys_minidump_max_files

- Default: 16
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### sys_minidump_limit

- Default: 20480
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### sys_minidump_interval

- Default: 600
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### dump_trace_info

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

##### max_length_for_to_base64

- Default: 200000
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: The maximum length of input values for the to_base64() function.
- Introduced in: -

##### max_length_for_bitmap_function

- Default: 1000000
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: The maximum length of input values for bitmap functions.
- Introduced in: -
