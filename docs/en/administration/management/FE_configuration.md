---
displayed_sidebar: docs
---

import FEConfigMethod from '../../_assets/commonMarkdown/FE_config_method.md'

import AdminSetFrontendNote from '../../_assets/commonMarkdown/FE_config_note.md'

import StaticFEConfigNote from '../../_assets/commonMarkdown/StaticFE_config_note.md'

# FE Configuration

<FEConfigMethod />

## View FE configuration items

After your FE is started, you can run the ADMIN SHOW FRONTEND CONFIG command on your MySQL client to check the parameter configurations. If you want to query the configuration of a specific parameter, run the following command:

```SQL
ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"];
```

For detailed description of the returned fields, see [ADMIN SHOW CONFIG](../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SHOW_CONFIG.md).

:::note
You must have administrator privileges to run cluster administration-related commands.
:::

## Configure FE parameters

### Configure FE dynamic parameters

You can configure or modify the settings of FE dynamic parameters using [ADMIN SET FRONTEND CONFIG](../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SET_CONFIG.md).

```SQL
ADMIN SET FRONTEND CONFIG ("key" = "value");
```

<AdminSetFrontendNote />

### Configure FE static parameters

<StaticFEConfigNote />

## Understand FE parameters

### Logging

##### log_roll_size_mb

- Default: 1024
- Type: Int
- Unit: MB
- Is mutable: No
- Description: The maximum size of a system log file or an audit log file.
- Introduced in: -

##### sys_log_dir

- Default: StarRocksFE.STARROCKS_HOME_DIR + "/log"
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory that stores system log files.
- Introduced in: -

##### sys_log_level

- Default: INFO
- Type: String
- Unit: -
- Is mutable: No
- Description: The severity levels into which system log entries are classified. Valid values: `INFO`, `WARN`, `ERROR`, and `FATAL`.
- Introduced in: -

##### sys_log_roll_num

- Default: 10
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of system log files that can be retained within each retention period specified by the `sys_log_roll_interval` parameter.
- Introduced in: -

##### sys_log_verbose_modules

- Default: Empty string
- Type: String[]
- Unit: -
- Is mutable: No
- Description: The modules for which StarRocks generates system logs. If this parameter is set to `org.apache.starrocks.catalog`, StarRocks generates system logs only for the catalog module. Separate the module names with a comma (,) and a space.
- Introduced in: -

##### sys_log_roll_interval

- Default: DAY
- Type: String
- Unit: -
- Is mutable: No
- Description: The time interval at which StarRocks rotates system log entries. Valid values: `DAY` and `HOUR`.
  - If this parameter is set to `DAY`, a suffix in the `yyyyMMdd` format is added to the names of system log files.
  - If this parameter is set to `HOUR`, a suffix in the `yyyyMMddHH` format is added to the names of system log files.
- Introduced in: -

##### sys_log_delete_age

- Default: 7d
- Type: String
- Unit: -
- Is mutable: No
- Description: The retention period of system log files. The default value `7d` specifies that each system log file can be retained for 7 days. StarRocks checks each system log file and deletes those that were generated 7 days ago.
- Introduced in: -

<!--
##### sys_log_roll_mode (Deprecated)

- Default: SIZE-MB-1024
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### sys_log_to_console

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

##### audit_log_dir

- Default: StarRocksFE.STARROCKS_HOME_DIR + "/log"
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory that stores audit log files.
- Introduced in: -

##### audit_log_roll_num

- Default: 90
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of audit log files that can be retained within each retention period specified by the `audit_log_roll_interval` parameter.
- Introduced in: -

##### audit_log_modules

- Default: slow_query, query
- Type: String[]
- Unit: -
- Is mutable: No
- Description: The modules for which StarRocks generates audit log entries. By default, StarRocks generates audit logs for the `slow_query` module and the `query` module. The `connection` module is supported from v3.0. Separate the module names with a comma (,) and a space.
- Introduced in: -

##### qe_slow_log_ms

- Default: 5000
- Type: Long
- Unit: Milliseconds
- Is mutable: Yes
- Description: The threshold used to determine whether a query is a slow query. If the response time of a query exceeds this threshold, it is recorded as a slow query in **fe.audit.log**.
- Introduced in: -

##### audit_log_roll_interval

- Default: DAY
- Type: String
- Unit: -
- Is mutable: No
- Description: The time interval at which StarRocks rotates audit log entries. Valid values: `DAY` and `HOUR`.
  - If this parameter is set to `DAY`, a suffix in the `yyyyMMdd` format is added to the names of audit log files.
  - If this parameter is set to `HOUR`, a suffix in the `yyyyMMddHH` format is added to the names of audit log files.
- Introduced in: -

##### audit_log_delete_age

- Default: 30d
- Type: String
- Unit: -
- Is mutable: No
- Description: The retention period of audit log files. The default value `30d` specifies that each audit log file can be retained for 30 days. StarRocks checks each audit log file and deletes those that were generated 30 days ago.
- Introduced in: -

<!--
##### slow_lock_threshold_ms

- Default: 3000
- Type: Long
- Unit: Milliseconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### slow_lock_log_every_ms

- Default: 3000
- Type: Long
- Unit: Milliseconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### custom_config_dir

- Default: /conf
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### internal_log_dir

- Default: StarRocksFE.STARROCKS_HOME_DIR + "/log"
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### internal_log_roll_num

- Default: 90
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### internal_log_modules

- Default: {"base", "statistic"}
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### internal_log_roll_interval

- Default: DAY
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### internal_log_delete_age

- Default: 7d
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

##### dump_log_dir

- Default: StarRocksFE.STARROCKS_HOME_DIR + "/log"
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory that stores dump log files.
- Introduced in: -

##### dump_log_roll_num

- Default: 10
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of dump log files that can be retained within each retention period specified by the `dump_log_roll_interval` parameter.
- Introduced in: -

##### dump_log_modules

- Default: query
- Type: String[]
- Unit: -
- Is mutable: No
- Description: The modules for which StarRocks generates dump log entries. By default, StarRocks generates dump logs for the query module. Separate the module names with a comma (,) and a space.
- Introduced in: -

##### dump_log_roll_interval

- Default: DAY
- Type: String
- Unit: -
- Is mutable: No
- Description: The time interval at which StarRocks rotates dump log entries. Valid values: `DAY` and `HOUR`.
  - If this parameter is set to `DAY`, a suffix in the `yyyyMMdd` format is added to the names of dump log files.
  - If this parameter is set to `HOUR`, a suffix in the `yyyyMMddHH` format is added to the names of dump log files.
- Introduced in: -

##### dump_log_delete_age

- Default: 7d
- Type: String
- Unit: -
- Is mutable: No
- Description: The retention period of dump log files. The default value `7d` specifies that each dump log file can be retained for 7 days. StarRocks checks each dump log file and deletes those that were generated 7 days ago.
- Introduced in: -

<!--
##### big_query_log_dir

- Default: StarRocksFE.STARROCKS_HOME_DIR + "/log"
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### big_query_log_roll_num

- Default: 10
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### big_query_log_modules

- Default: query
- Type: String[]
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### big_query_log_roll_interval

- Default: DAY
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### big_query_log_delete_age

- Default: 7d
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### log_plan_cancelled_by_crash_be

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to log the COSTS plan if the query is cancelled due to a BE crash or RpcException. This parameter is effective only when `enable_collect_query_detail_info` is set to `false`. When `enable_collect_query_detail_info` is set to `true`, the plan will be recorded in the query detail.
- Introduced in: v3.1
-->

<!--
##### log_register_and_unregister_query_id

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

### Server

##### frontend_address

- Default: 0.0.0.0
- Type: String
- Unit: -
- Is mutable: No
- Description: The IP address of the FE node.
- Introduced in: -

##### priority_networks

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: Declares a selection strategy for servers that have multiple IP addresses. Note that at most one IP address must match the list specified by this parameter. The value of this parameter is a list that consists of entries, which are separated with semicolons (;) in CIDR notation, such as 10.10.10.0/24. If no IP address matches the entries in this list, an available IP address of the server will be randomly selected. From v3.3.0, StarRocks supports deployment based on IPv6. If the server has both IPv4 and IPv6 addresses, and this parameter is not specified, the system uses an IPv4 address by default. You can change this behavior by setting `net_use_ipv6_when_priority_networks_empty` to `true`.
- Introduced in: -

##### net_use_ipv6_when_priority_networks_empty

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: A boolean value to control whether to use IPv6 addresses preferentially when `priority_networks` is not specified. `true` indicates to allow the system to use an IPv6 address preferentially when the server that hosts the node has both IPv4 and IPv6 addresses and `priority_networks` is not specified.
- Introduced in: v3.3.0

##### http_port

- Default: 8030
- Type: Int
- Unit: -
- Is mutable: No
- Description: The port on which the HTTP server in the FE node listens.
- Introduced in: -

##### http_worker_threads_num

- Default: 0
- Type: Int
- Unit: -
- Is mutable: No
- Description: Number of worker threads for http server to deal with http requests. For a negative or 0 value, the number of threads will be twice the number of cpu cores.
- Introduced in: v2.5.18, v3.0.10, v3.1.7, v3.2.2

##### http_backlog_num

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: No
- Description: The length of the backlog queue held by the HTTP server in the FE node.
- Introduced in: -

<!--
##### http_max_initial_line_length

- Default: 4096
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### http_max_header_size

- Default: 32768
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### http_max_chunk_size

- Default: 8192
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### http_web_page_display_hardware

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### enable_http_detail_metrics

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### cluster_name

- Default: StarRocks Cluster
- Type: String
- Unit: -
- Is mutable: No
- Description: The name of the StarRocks cluster to which the FE belongs. The cluster name is displayed for `Title` on the web page.
- Introduced in: -

##### rpc_port

- Default: 9020
- Type: Int
- Unit: -
- Is mutable: No
- Description: The port on which the Thrift server in the FE node listens.
- Introduced in: -

##### thrift_server_max_worker_threads

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of worker threads that are supported by the Thrift server in the FE node.
- Introduced in: -

##### thrift_server_queue_size

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: No
- Description: The length of queue where requests are pending. If the number of threads that are being processed in the thrift server exceeds the value specified in `thrift_server_max_worker_threads`, new requests are added to the pending queue.
- Introduced in: -

##### thrift_client_timeout_ms

- Default: 5000
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: The length of time after which idle client connections time out.
- Introduced in: -

##### thrift_backlog_num

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: No
- Description: The length of the backlog queue held by the Thrift server in the FE node.
- Introduced in: -

<!--
##### thrift_rpc_timeout_ms

- Default: 10000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### thrift_rpc_retry_times

- Default: 3
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

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

- Default: -1
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### brpc_connection_pool_size

- Default: 16
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

##### brpc_idle_wait_max_time

- Default: 10000
- Type: Int
- Unit: ms
- Is mutable: No
- Description: The maximum length of time for which bRPC clients wait as in the idle state.
- Introduced in: -

##### query_port

- Default: 9030
- Type: Int
- Unit: -
- Is mutable: No
- Description: The port on which the MySQL server in the FE node listens.
- Introduced in: -

##### mysql_nio_backlog_num

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: No
- Description: The length of the backlog queue held by the MySQL server in the FE node.
- Introduced in: -

##### mysql_service_nio_enabled

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Specifies whether asynchronous I/O is enabled for the FE node.
- Introduced in: -

##### mysql_service_nio_enable_keep_alive

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Enable TCP Keep-Alive for MySQL connections. Useful for long-idled connections behind load balancers.
- Introduced in: -

##### mysql_service_io_threads_num

- Default: 4
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of threads that can be run by the MySQL server in the FE node to process I/O events.
- Introduced in: -

##### max_mysql_service_task_threads_num

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of threads that can be run by the MySQL server in the FE node to process tasks.
- Introduced in: -

<!--
##### max_http_sql_service_task_threads_num

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

##### mysql_server_version

- Default: 5.1.0
- Type: String
- Unit: -
- Is mutable: Yes
- Description: The MySQL server version returned to the client. Modifying this parameter will affect the version information in the following situations:
  1. `select version();`
  2. Handshake packet version
  3. Value of the global variable `version` (`show variables like 'version';`)
- Introduced in: -

##### qe_max_connection

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of connections that can be established by all users to the FE node. From v3.1.12 and v3.2.7 onwards, the default value has been changed from `1024` to `4096`.
- Introduced in: -

##### max_connection_scheduler_threads_num

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of threads that are supported by the connection scheduler.
- Introduced in: -

### Metadata and cluster management

##### cluster_id

- Default: -1
- Type: Int
- Unit: -
- Is mutable: No
- Description: The ID of the StarRocks cluster to which the FE belongs. FEs or BEs that have the same cluster ID belong to the same StarRocks cluster. Valid values: any positive integer. The default value `-1` specifies that StarRocks will generate a random cluster ID for the StarRocks cluster at the time when the leader FE of the cluster is started for the first time.
- Introduced in: -

##### meta_dir

- Default: StarRocksFE.STARROCKS_HOME_DIR + "/meta"
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory that stores metadata.
- Introduced in: -

##### edit_log_type

- Default: BDB
- Type: String
- Unit: -
- Is mutable: No
- Description: The type of edit log that can be generated. Set the value to `BDB`.
- Introduced in: -

##### edit_log_port

- Default: 9010
- Type: Int
- Unit: -
- Is mutable: No
- Description: The port that is used for communication among the Leader, Follower, and Observer FEs in the cluster.
- Introduced in: -

##### edit_log_roll_num

- Default: 50000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of metadata log entries that can be written before a log file is created for these log entries. This parameter is used to control the size of log files. The new log file is written to the BDBJE database.
- Introduced in: -

<!--
##### edit_log_write_slow_log_threshold_ms

- Default: 2000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### ignore_unknown_log_id

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to ignore an unknown log ID. When an FE is rolled back, the FEs of the earlier version may be unable to recognize some log IDs. If the value is `TRUE`, the FE ignores unknown log IDs. If the value is `FALSE`, the FE exits.
- Introduced in: -

<!--
##### hdfs_read_buffer_size_kb

- Default: 8192
- Type: Int
- Unit: KB
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### hdfs_write_buffer_size_kb

- Default: 1024
- Type: Int
- Unit: KB
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### hdfs_file_system_expire_seconds

- Default: 300
- Alias: hdfs_file_sytem_expire_seconds
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### meta_delay_toleration_second

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The maximum duration by which the metadata on the follower and observer FEs can lag behind that on the leader FE. Unit: seconds. If this duration is exceeded, the non-leader FEs stops providing services.
- Introduced in: -

##### master_sync_policy

- Default: SYNC
- Type: String
- Unit: -
- Is mutable: No
- Description: The policy based on which the leader FE flushes logs to disk. This parameter is valid only when the current FE is a leader FE. Valid values:
  - `SYNC`: When a transaction is committed, a log entry is generated and flushed to disk simultaneously.
  - `NO_SYNC`: The generation and flushing of a log entry do not occur at the same time when a transaction is committed.
  - `WRITE_NO_SYNC`: When a transaction is committed, a log entry is generated simultaneously but is not flushed to disk.

  If you have deployed only one follower FE, we recommend that you set this parameter to `SYNC`. If you have deployed three or more follower FEs, we recommend that you set this parameter and the `replica_sync_policy` both to `WRITE_NO_SYNC`.

- Introduced in: -

##### replica_sync_policy

- Default: SYNC
- Type: String
- Unit: -
- Is mutable: No
- Description: The policy based on which the follower FE flushes logs to disk. This parameter is valid only when the current FE is a follower FE. Valid values:
  - `SYNC`: When a transaction is committed, a log entry is generated and flushed to disk simultaneously.
  - `NO_SYNC`: The generation and flushing of a log entry do not occur at the same time when a transaction is committed.
  - `WRITE_NO_SYNC`: When a transaction is committed, a log entry is generated simultaneously but is not flushed to disk.
- Introduced in: -

##### replica_ack_policy

- Default: SIMPLE_MAJORITY
- Type: String
- Unit: -
- Is mutable: No
- Description: The policy based on which a log entry is considered valid. The default value `SIMPLE_MAJORITY` specifies that a log entry is considered valid if a majority of follower FEs return ACK messages.
- Introduced in: -

##### bdbje_heartbeat_timeout_second

- Default: 30
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: The amount of time after which the heartbeats among the leader, follower, and observer FEs in the StarRocks cluster time out.
- Introduced in: -

##### bdbje_replica_ack_timeout_second

- Default: 10
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: The maximum amount of time for which the leader FE can wait for ACK messages from a specified number of follower FEs when metadata is written from the leader FE to the follower FEs. Unit: second. If a large amount of metadata is being written, the follower FEs require a long time before they can return ACK messages to the leader FE, causing ACK timeout. In this situation, metadata writes fail, and the FE process exits. We recommend that you increase the value of this parameter to prevent this situation.
- Introduced in: -

##### bdbje_lock_timeout_second

- Default: 1
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: The amount of time after which a lock in the BDB JE-based FE times out.
- Introduced in: -

##### bdbje_reset_election_group

- Default: false
- Type: String
- Unit: -
- Is mutable: No
- Description: Whether to reset the BDBJE replication group. If this parameter is set to `TRUE`, the FE will reset the BDBJE replication group (that is, remove the information of all electable FE nodes) and start as the leader FE. After the reset, this FE will be the only member in the cluster, and other FEs can rejoin this cluster by using `ALTER SYSTEM ADD/DROP FOLLOWER/OBSERVER 'xxx'`. Use this setting only when no leader FE can be elected because the data of most follower FEs have been damaged. `reset_election_group` is used to replace `metadata_failure_recovery`.
- Introduced in: -

##### max_bdbje_clock_delta_ms

- Default: 5000
- Type: Long
- Unit: Milliseconds
- Is mutable: No
- Description: The maximum clock offset that is allowed between the leader FE and the follower or observer FEs in the StarRocks cluster.
- Introduced in: -

<!--
##### bdbje_log_level

- Default: INFO
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### bdbje_cleaner_threads

- Default: 1
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### bdbje_replay_cost_percent

- Default: 150
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### bdbje_reserved_disk_size

- Default: 512 * 1024 * 1024
- Type: Long
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

##### txn_rollback_limit

- Default: 100
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of transactions that can be rolled back.
- Introduced in: -

##### heartbeat_mgr_threads_num

- Default: 8
- Type: Int
- Unit: -
- Is mutable: No
- Description: The number of threads that can be run by the Heartbeat Manager to run heartbeat tasks.
- Introduced in: -

##### heartbeat_mgr_blocking_queue_size

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: No
- Description: The size of the blocking queue that stores heartbeat tasks run by the Heartbeat Manager.
- Introduced in: -

##### catalog_try_lock_timeout_ms

- Default: 5000
- Type: Long
- Unit: Milliseconds
- Is mutable: Yes
- Description: The timeout duration to obtain the global lock.
- Introduced in: -

##### ignore_materialized_view_error

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether FE ignores the metadata exception caused by materialized view errors. If FE fails to start due to the metadata exception caused by materialized view errors, you can set this parameter to `true` to allow FE to ignore the exception.
- Introduced in: v2.5.10

##### ignore_meta_check

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether non-Leader FEs ignore the metadata gap from the Leader FE. If the value is TRUE, non-Leader FEs ignore the metadata gap from the Leader FE and continue providing data reading services. This parameter ensures continuous data reading services even when you stop the Leader FE for a long period of time. If the value is FALSE, non-Leader FEs do not ignore the metadata gap from the Leader FE and stop providing data reading services.
- Introduced in: -

##### drop_backend_after_decommission

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to delete a BE after the BE is decommissioned. `TRUE` indicates that the BE is deleted immediately after it is decommissioned. `FALSE` indicates that the BE is not deleted after it is decommissioned.
- Introduced in: -

##### enable_collect_query_detail_info

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to collect the profile of a query. If this parameter is set to `TRUE`, the system collects the profile of the query. If this parameter is set to `FALSE`, the system does not collect the profile of the query.
- Introduced in: -

##### enable_background_refresh_connector_metadata

- Default: true in v3.0 and later and false in v2.5
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable the periodic Hive metadata cache refresh. After it is enabled, StarRocks polls the metastore (Hive Metastore or AWS Glue) of your Hive cluster, and refreshes the cached metadata of the frequently accessed Hive catalogs to perceive data changes. `true` indicates to enable the Hive metadata cache refresh, and `false` indicates to disable it.
- Introduced in: v2.5.5

<!--
##### enable_background_refresh_resource_table_metadata

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### background_refresh_file_metadata_concurrency

- Default: 4
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

##### background_refresh_metadata_interval_millis

- Default: 600000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: The interval between two consecutive Hive metadata cache refreshes.
- Introduced in: v2.5.5

##### background_refresh_metadata_time_secs_since_last_access_secs

- Default: 3600 * 24
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The expiration time of a Hive metadata cache refresh task. For the Hive catalog that has been accessed, if it has not been accessed for more than the specified time, StarRocks stops refreshing its cached metadata. For the Hive catalog that has not been accessed, StarRocks will not refresh its cached metadata.
- Introduced in: v2.5.5

##### enable_statistics_collect_profile

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to generate profiles for statistics queries. You can set this item to `true` to allow StarRocks to generate query profiles for queries on system statistics.
- Introduced in: v3.1.5

#### metadata_enable_recovery_mode

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to enable the metadata recovery mode. When this mode is enabled, if part of the cluster metadata is lost, it can be restored based on the information from BE. Currently, only the version information of partitions can be restored.
- Introduced in: v3.3.0

##### black_host_history_sec

- Default: 2 * 60
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time duration for retaining historical connection failures of BE nodes in the BE Blacklist. If a BE node is added to the BE Blacklist automatically, StarRocks will assess its connectivity and judge whether it can be removed from the BE Blacklist. Within `black_host_history_sec`, only if a blacklisted BE node has fewer connection failures than the threshold set in `black_host_connect_failures_within_time`, it can be removed from the BE Blacklist.
- Introduced in: v3.3.0

##### black_host_connect_failures_within_time

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The threshold of connection failures allowed for a blacklisted BE node. If a BE node is added to the BE Blacklist automatically, StarRocks will assess its connectivity and judge whether it can be removed from the BE Blacklist. Within `black_host_history_sec`, only if a blacklisted BE node has fewer connection failures than the threshold set in `black_host_connect_failures_within_time`, it can be removed from the BE Blacklist.
- Introduced in: v3.3.0

#### lock_manager_enabled

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to enable the lock manager. The lock manager performs central management for locks. For example, it can control whether to refine the granularity of metadata locks from the database level to the table level.
- Introduced in: v3.3.0

##### lock_manager_enable_using_fine_granularity_lock

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to refine the granularity of metadata locks from the database level to the table level. After metadata locks are refined to the table level, lock conflicts and contentions can be reduced, which improves load and query concurrency. This parameter only takes effect when `lock_manager_enabled` is enabled.
- Introduced in: v3.3.0

##### enable_legacy_compatibility_for_replication

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable the Legacy Compatibility for Replication. StarRocks may behave differently between the old and new versions, causing problems during cross-cluster data migration. Therefore, you must enable Legacy Compatibility for the target cluster before data migration and disable it after data migration is completed. `true` indicates enabling this mode.
- Introduced in: v3.1.10, v3.2.6

### User, role, and privilege

##### privilege_max_total_roles_per_user

- Default: 64
- Type: Int
- Unit:
- Is mutable: Yes
- Description: The maximum number of roles a user can have.
- Introduced in: v3.0.0

##### privilege_max_role_depth

- Default: 16
- Type: Int
- Unit:
- Is mutable: Yes
- Description: The maximum role depth (level of inheritance) of a role.
- Introduced in: v3.0.0

### Query engine

##### publish_version_interval_ms

- Default: 10
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: The time interval at which release validation tasks are issued.
- Introduced in: -

##### statistic_cache_columns

- Default: 100000
- Type: Long
- Unit: -
- Is mutable: No
- Description: The number of rows that can be cached for the statistics table.
- Introduced in: -

##### statistic_cache_thread_pool_size

- Default: 10
- Type: Int
- Unit: -
- Is mutable: No
- Description: The size of the thread-pool which will be used to refresh statistic caches.
- Introduced in: -

##### max_allowed_in_element_num_of_delete

- Default: 10000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of elements allowed for the IN predicate in a DELETE statement.
- Introduced in: -

##### enable_materialized_view

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable the creation of materialized views.
- Introduced in: -

##### enable_materialized_view_spill

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable Intermediate Result Spilling for materialized view refresh tasks.
- Introduced in: v3.1.1

##### enable_backup_materialized_view

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable the BACKUP and RESTORE of asynchronous materialized views when backing up or restoring a specific database. If this item is set to `false`, StarRocks will skip backing up asynchronous materialized views.
- Introduced in: v3.2.0

<!--
##### enable_show_materialized_views_include_all_task_runs

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### materialized_view_min_refresh_interval

- Default: 60
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### skip_whole_phase_lock_mv_limit

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### enable_experimental_mv

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable the asynchronous materialized view feature. TRUE indicates this feature is enabled. From v2.5.2 onwards, this feature is enabled by default. For versions earlier than v2.5.2, this feature is disabled by default.
- Introduced in: v2.4

##### enable_colocate_mv_index

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to support colocating the synchronous materialized view index with the base table when creating a synchronous materialized view. If this item is set to `true`, tablet sink will speed up the write performance of synchronous materialized views.
- Introduced in: v3.2.0

##### default_mv_refresh_immediate

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to refresh an asynchronous materialized view immediately after creation. When this item is set to `true`, newly created materialized view will be refreshed immediately.
- Introduced in: v3.2.3

##### enable_materialized_view_metrics_collect

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to collect monitoring metrics for asynchronous materialized views by default.
- Introduced in: v3.1.11, v3.2.5

##### enable_materialized_view_text_based_rewrite

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable text-based query rewrite by default. If this item is set to `true`, the system builds the abstract syntax tree while creating an asynchronous materialized view.
- Introduced in: v3.2.5

##### enable_mv_automatic_active_check

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable the system to automatically check and re-activate the asynchronous materialized views that are set inactive because their base tables (views) had undergone Schema Change or had been dropped and re-created. Please note that this feature will not re-activate the materialized views that are manually set inactive by users.
- Introduced in: v3.1.6

##### enable_active_materialized_view_schema_strict_check

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to strictly check the length consistency of data types when activating an inactive materialized view. When this item is set to `false`, the activation of the materialized view is not affected if the length of the data types has changed in the base table.
- Introduced in: v3.3.4

<!--
##### mv_active_checker_interval_seconds

- Default: 60
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### default_mv_partition_refresh_number

- Default: 1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### mv_auto_analyze_async

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### enable_udf

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to enable UDF.
- Introduced in: -

##### enable_decimal_v3

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to support the DECIMAL V3 data type.
- Introduced in: -

##### enable_sql_blacklist

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable blacklist check for SQL queries. When this feature is enabled, queries in the blacklist cannot be executed.
- Introduced in: -

##### dynamic_partition_enable

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable the dynamic partitioning feature. When this feature is enabled, StarRocks dynamically creates partitions for new data and automatically deletes expired partitions to ensure the freshness of data.
- Introduced in: -

##### dynamic_partition_check_interval_seconds

- Default: 600
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The interval at which new data is checked. If new data is detected, StarRocks automatically creates partitions for the data.
- Introduced in: -

<!--
##### max_dynamic_partition_num

- Default: 500
- Type: Int
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### memory_tracker_enable

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### memory_tracker_interval_seconds

- Default: 60
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### enable_create_partial_partition_in_batch

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### max_query_retry_time

- Default: 2
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of query retries on an FE.
- Introduced in: -

##### max_create_table_timeout_second

- Default: 600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The maximum timeout duration for creating a table.
- Introduced in: -

##### create_table_max_serial_replicas

- Default: 128
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of replicas to create serially. If actual replica count exceeds this value, replicas will be created concurrently. Try to reduce this value if table creation is taking a long time to complete.
- Introduced in: -

##### http_slow_request_threshold_ms

- Default: 5000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: If the response time for an HTTP request exceeds the value specified by this parameter, a log is generated to track this request.
- Introduced in: v2.5.15, v3.1.5

##### max_partitions_in_one_batch

- Default: 4096
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: The maximum number of partitions that can be created when you bulk create partitions.
- Introduced in: -

##### max_running_rollup_job_num_per_table

- Default: 1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of rollup jobs can run in parallel for a table.
- Introduced in: -

##### expr_children_limit

- Default: 10000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of child expressions allowed in an expression.
- Introduced in: -

##### max_planner_scalar_rewrite_num

- Default: 100000
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: The maximum number of times that the optimizer can rewrite a scalar operator.
- Introduced in: -

##### enable_statistic_collect

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to collect statistics for the CBO. This feature is enabled by default.
- Introduced in: -

##### enable_statistic_collect_on_first_load

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to automatically collect statistics when data is loaded into a table for the first time. If a table has multiple partitions, any data loading into an empty partition of this table will trigger automatic statistics collection on this partition. If new tables are frequently created and data is frequently loaded, the memory and CPU overhead will increase.
- Introduced in: v3.1

<!--
##### semi_sync_collect_statistic_await_seconds

- Default: 30
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### statistic_auto_analyze_start_time

- Default: 00:00:00
- Type: String
- Unit: -
- Is mutable: Yes
- Description: The start time of automatic collection. Value range: `00:00:00` - `23:59:59`.
- Introduced in: -

##### statistic_auto_analyze_end_time

- Default: 23:59:59
- Type: String
- Unit: -
- Is mutable: Yes
- Description: The end time of automatic collection. Value range: `00:00:00` - `23:59:59`.
- Introduced in: -

<!--
##### statistic_manager_sleep_time_sec

- Default: 60
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### statistic_analyze_status_keep_second

- Default: 3 * 24 * 3600
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The duration to retain the history of collection tasks. The default value is 3 days.
- Introduced in: -

<!--
##### statistic_check_expire_partition

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### statistic_collect_interval_sec

- Default: 5 * 60
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The interval for checking data updates during automatic collection.
- Introduced in: -

<!--
##### statistic_analyze_task_pool_size

- Default: 3
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### statistic_collect_query_timeout

- Default: 3600
- Type: Long
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### slot_manager_response_thread_pool_size

- Default: 16
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### statistic_dict_columns

- Default: 100000
- Type: Long
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

##### statistic_update_interval_sec

- Default: 24 * 60 * 60
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The interval at which the cache of statistical information is updated.
- Introduced in: -

<!--
##### statistic_collect_too_many_version_sleep

- Default: 600000
- Type: Long
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### enable_collect_full_statistic

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable automatic full statistics collection. This feature is enabled by default.
- Introduced in: -

##### statistic_auto_collect_ratio

- Default: 0.8
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: The threshold for determining whether the statistics for automatic collection are healthy. If statistics health is below this threshold, automatic collection is triggered.
- Introduced in: -

<!--
##### statistic_full_collect_buffer

- Default: 1024 * 1024 * 20
- Type: Long
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### statistic_auto_collect_sample_threshold

- Default: 0.3
- Type: Double
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### statistic_auto_collect_small_table_size

- Default: 5 * 1024 * 1024 * 1024
- Type: Long
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### statistic_auto_collect_small_table_rows

- Default: 10000000
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: Threshold to determine whether a table in an external data source (Hive, Iceberg, Hudi) is a small table during automatic collection. If the table has rows less than this value, the table is considered a small table.
- Introduced in: v3.2

<!--
##### statistic_auto_collect_small_table_interval

- Default: 0
- Type: Long
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### statistic_auto_collect_large_table_interval

- Default: 3600 * 12
- Type: Long
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### statistic_max_full_collect_data_size

- Default: 100 * 1024 * 1024 * 1024
- Type: Long
- Unit: bytes
- Is mutable: Yes
- Description: The data size threshold for the automatic collection of statistics. If the total size exceeds this value, then sampled collection is performed instead of full.
- Introduced in: -

##### statistic_collect_max_row_count_per_query

- Default: 5000000000
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: The maximum number of rows to query for a single analyze task. An analyze task will be split into multiple queries if this value is exceeded.
- Introduced in: -

##### statistic_sample_collect_rows

- Default: 200000
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: The minimum number of rows to collect for sampled collection. If the parameter value exceeds the actual number of rows in your table, full collection is performed.
- Introduced in: -

##### histogram_buckets_size

- Default: 64
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: The default bucket number for a histogram.
- Introduced in: -

##### histogram_mcv_size

- Default: 100
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: The number of most common values (MCV) for a histogram.
- Introduced in: -

##### histogram_sample_ratio

- Default: 0.1
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: The sampling ratio for a histogram.
- Introduced in: -

##### histogram_max_sample_row_count

- Default: 10000000
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: The maximum number of rows to collect for a histogram.
- Introduced in: -

##### connector_table_query_trigger_analyze_small_table_rows

- Default: 10000000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The threshold for determining whether a table is a small table for query-trigger ANALYZE tasks.
- Introduced in: v3.4.0

##### connector_table_query_trigger_analyze_small_table_interval

- Default: 2 * 3600
- Type: Int
- Unit: Second
- Is mutable: Yes
- Description: The interval for query-trigger ANALYZE tasks of small tables.
- Introduced in: v3.4.0

##### connector_table_query_trigger_analyze_large_table_interval

- Default: 12 * 3600
- Type: Int
- Unit: Second
- Is mutable: Yes
- Description: The interval for query-trigger ANALYZE tasks of large tables.
- Introduced in: v3.4.0

##### connector_table_query_trigger_analyze_max_pending_task_num

- Default: 100
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Maximum number of query-trigger ANALYZE tasks that are in Pending state on the FE.
- Introduced in: v3.4.0

##### connector_table_query_trigger_analyze_schedule_interval

- Default: 30
- Type: Int
- Unit: Second
- Is mutable: Yes
- Description: The interval at which the Scheduler thread schedules to query-trigger ANALYZE tasks.
- Introduced in: v3.4.0

##### connector_table_query_trigger_analyze_max_running_task_num

- Default: 2
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Maximum number of query-trigger ANALYZE tasks that are in Running state on the FE.
- Introduced in: v3.4.0

##### enable_local_replica_selection

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to select local replicas for queries. Local replicas reduce the network transmission cost. If this parameter is set to TRUE, the CBO preferentially selects tablet replicas on BEs that have the same IP address as the current FE. If this parameter is set to `FALSE`, both local replicas and non-local replicas can be selected.
- Introduced in: -

##### max_distribution_pruner_recursion_depth

- Default: 100
- Type: Int
- Unit: -
- Is mutable: Yes
- Description:: The maximum recursion depth allowed by the partition pruner. Increasing the recursion depth can prune more elements but also increases CPU consumption.
- Introduced in: -

##### slow_query_analyze_threshold

- Default: 5
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description:: The execution time threshold for queries to trigger the analysis of Query Feedback.
- Introduced in: v3.4.0

### Loading and unloading

##### load_straggler_wait_second

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The maximum loading lag that can be tolerated by a BE replica. If this value is exceeded, cloning is performed to clone data from other replicas.
- Introduced in: -

##### load_checker_interval_second

- Default: 5
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: The time interval at which load jobs are processed on a rolling basis.
- Introduced in: -

<!--
##### lock_checker_interval_second

- Default: 30
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### lock_checker_enable_deadlock_check

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### broker_load_default_timeout_second

- Default: 14400
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The timeout duration for a Broker Load job.
- Introduced in: -

<!--
##### spark_load_submit_timeout_second

- Default: 300
- Type: Long
- Unit: Seconds
- Is mutable: No
- Description:
- Introduced in: -
-->

##### min_bytes_per_broker_scanner

- Default: 67108864
- Type: Long
- Unit: Bytes
- Is mutable: Yes
- Description: The minimum allowed amount of data that can be processed by a Broker Load instance.
- Introduced in: -

##### insert_load_default_timeout_second

- Default: 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The timeout duration for the INSERT INTO statement that is used to load data.
- Introduced in: -

##### stream_load_default_timeout_second

- Default: 600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The default timeout duration for each Stream Load job.
- Introduced in: -

##### max_stream_load_timeout_second

- Default: 259200
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The maximum allowed timeout duration for a Stream Load job.
- Introduced in: -

<!--
##### max_stream_load_batch_size_mb

- Default: 100
- Type: Int
- Unit: MB
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### stream_load_max_txn_num_per_be

- Default: -1
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### prepared_transaction_default_timeout_second

- Default: 86400
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### max_load_timeout_second

- Default: 259200
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The maximum timeout duration allowed for a load job. The load job fails if this limit is exceeded. This limit applies to all types of load jobs.
- Introduced in: -

##### min_load_timeout_second

- Default: 1
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The minimum timeout duration allowed for a load job. This limit applies to all types of load jobs.
- Introduced in: -

##### spark_dpp_version

- Default: 1.0.0
- Type: String
- Unit: -
- Is mutable: No
- Description: The version of Spark Dynamic Partition Pruning (DPP) used.
- Introduced in: -

##### spark_load_default_timeout_second

- Default: 86400
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The timeout duration for each Spark Load job.
- Introduced in: -

##### spark_home_default_dir

- Default: StarRocksFE.STARROCKS_HOME_DIR + "/lib/spark2x"
- Type: String
- Unit: -
- Is mutable: No
- Description: The root directory of a Spark client.
- Introduced in: -

##### spark_resource_path

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The root directory of the Spark dependency package.
- Introduced in: -

##### spark_launcher_log_dir

- Default: sys_log_dir + "/spark_launcher_log"
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory that stores Spark log files.
- Introduced in: -

##### yarn_client_path

- Default: StarRocksFE.STARROCKS_HOME_DIR + "/lib/yarn-client/hadoop/bin/yarn"
- Type: String
- Unit: -
- Is mutable: No
- Description: The root directory of the Yarn client package.
- Introduced in: -

##### yarn_config_dir

- Default: StarRocksFE.STARROCKS_HOME_DIR + "/lib/yarn-config"
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory that stores the Yarn configuration file.
- Introduced in: -

##### desired_max_waiting_jobs

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of pending jobs in an FE. The number refers to all jobs, such as table creation, loading, and schema change jobs. If the number of pending jobs in an FE reaches this value, the FE will reject new load requests. This parameter takes effect only for asynchronous loading. From v2.5 onwards, the default value is changed from 100 to 1024.
- Introduced in: -

##### max_running_txn_num_per_db

- Default: 1000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of load transactions allowed to be running for each database within a StarRocks cluster. The default value is `1000`. From v3.1 onwards, the default value is changed to `1000` from `100`. When the actual number of load transactions running for a database exceeds the value of this parameter, new load requests will not be processed. New requests for synchronous load jobs will be denied, and new requests for asynchronous load jobs will be placed in queue. We do not recommend you increase the value of this parameter because this will increase system load.
- Introduced in: -

##### max_broker_load_job_concurrency

- Default: 5
- Alias: async_load_task_pool_size
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of concurrent Broker Load jobs allowed within the StarRocks cluster. This parameter is valid only for Broker Load. The value of this parameter must be less than the value of `max_running_txn_num_per_db`. From v2.5 onwards, the default value is changed from `10` to `5`.
- Introduced in: -

##### load_parallel_instance_num (Deprecated)

- Default: 1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of concurrent loading instances for each load job on a BE. This item is deprecated from v3.1 onwards.
- Introduced in: -

##### disable_load_job

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to disable loading when the cluster encounters an error. This prevents any loss caused by cluster errors. The default value is `FALSE`, indicating that loading is not disabled. `TRUE` indicates loading is disabled and the cluster is in read-only state.
- Introduced in: -

##### history_job_keep_max_second

- Default: 7 * 24 * 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The maximum duration a historical job can be retained, such as schema change jobs.
- Introduced in: -

##### label_keep_max_second

- Default: 3 * 24 * 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The maximum duration in seconds to keep the labels of load jobs that have been completed and are in the FINISHED or CANCELLED state. The default value is 3 days. After this duration expires, the labels will be deleted. This parameter applies to all types of load jobs. A value too large consumes a lot of memory.
- Introduced in: -

##### label_keep_max_num

- Default: 1000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of load jobs that can be retained within a period of time. If this number is exceeded, the information of historical jobs will be deleted.
- Introduced in: -

##### max_routine_load_task_concurrent_num

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of concurrent tasks for each Routine Load job.
- Introduced in: -

##### max_routine_load_task_num_per_be

- Default: 16
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of concurrent Routine Load tasks on each BE. Since v3.1.0, the default value for this parameter is increased to 16 from 5, and no longer needs to be less than or equal to the value of BE static parameter `routine_load_thread_pool_size` (deprecated).
- Introduced in: -

##### max_routine_load_batch_size

- Default: 4294967296
- Type: Long
- Unit: Bytes
- Is mutable: Yes
- Description: The maximum amount of data that can be loaded by a Routine Load task.
- Introduced in: -

##### routine_load_task_consume_second

- Default: 15
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The maximum time for each Routine Load task within the cluster to consume data. Since v3.1.0, Routine Load job supports a new parameter `task_consume_second` in [job_properties](../../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md#job_properties). This parameter applies to individual load tasks within a Routine Load job, which is more flexible.
- Introduced in: -

##### routine_load_task_timeout_second

- Default: 60
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The timeout duration for each Routine Load task within the cluster. Since v3.1.0, Routine Load job supports a new parameter `task_timeout_second` in [job_properties](../../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md#job_properties). This parameter applies to individual load tasks within a Routine Load job, which is more flexible.
- Introduced in: -

<!--
##### routine_load_kafka_timeout_second

- Default: 12
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### routine_load_pulsar_timeout_second

- Default: 12
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### routine_load_unstable_threshold_second

- Default: 3600
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: Routine Load job is set to the UNSTABLE state if any task within the Routine Load job lags. To be specific, the difference between the timestamp of the message being consumed and the current time exceeds this threshold, and unconsumed messages exist in the data source.
- Introduced in: -

##### max_tolerable_backend_down_num

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of faulty BE nodes allowed. If this number is exceeded, Routine Load jobs cannot be automatically recovered.
- Introduced in: -

##### period_of_auto_resume_min

- Default: 5
- Type: Int
- Unit: Minutes
- Is mutable: Yes
- Description: The interval at which Routine Load jobs are automatically recovered.
- Introduced in: -

##### export_task_default_timeout_second

- Default: 2 * 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The timeout duration for a data exporting task.
- Introduced in: -

##### export_max_bytes_per_be_per_task

- Default: 268435456
- Type: Long
- Unit: Bytes
- Is mutable: Yes
- Description: The maximum amount of data that can be exported from a single BE by a single data unload task.
- Introduced in: -

##### export_task_pool_size

- Default: 5
- Type: Int
- Unit: -
- Is mutable: No
- Description: The size of the unload task thread pool.
- Introduced in: -

##### export_checker_interval_second

- Default: 5
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: The time interval at which load jobs are scheduled.
- Introduced in: -

##### export_running_job_num_limit

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of data exporting tasks that can run in parallel.
- Introduced in: -

##### empty_load_as_error

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to return an error message "all partitions have no load data" if no data is loaded. Valid values:
  - `true`: If no data is loaded, the system displays a failure message and returns an error "all partitions have no load data".
  - `false`: If no data is loaded, the system displays a success message and returns OK, instead of an error.
- Introduced in: -

##### external_table_commit_timeout_ms

- Default: 10000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: The timeout duration for committing (publishing) a write transaction to a StarRocks external table. The default value `10000` indicates a 10-second timeout duration.
- Introduced in: -

##### enable_sync_publish

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to synchronously execute the apply task at the publish phase of a load transaction. This parameter is applicable only to Primary Key tables. Valid values:
  - `TRUE` (default): The apply task is synchronously executed at the publish phase of a load transaction. It means that the load transaction is reported as successful only after the apply task is completed, and the loaded data can truly be queried. When a task loads a large volume of data at a time or loads data frequently, setting this parameter to `true` can improve query performance and stability, but may increase load latency.
  - `FALSE`: The apply task is asynchronously executed at the publish phase of a load transaction. It means that the load transaction is reported as successful after the apply task is submitted, but the loaded data cannot be immediately queried. In this case, concurrent queries need to wait for the apply task to complete or time out before they can continue. When a task loads a large volume of data at a time or loads data frequently, setting this parameter to `false` may affect query performance and stability.
- Introduced in: v3.2.0

<!--
##### stream_load_task_keep_max_num

- Default: 1000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### stream_load_task_keep_max_second

- Default: 3 * 24 * 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### label_clean_interval_second

- Default: 4 * 3600
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: The time interval at which labels are cleaned up. Unit: second. We recommend that you specify a short time interval to ensure that historical labels can be cleaned up in a timely manner.
- Introduced in: -

<!--
##### task_check_interval_second

- Default: 3600
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### task_ttl_second

- Default: 24 * 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### task_runs_ttl_second

- Default: 24 * 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### task_runs_max_history_number

- Default: 10000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### transaction_clean_interval_second

- Default: 30
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: The time interval at which finished transactions are cleaned up. Unit: second. We recommend that you specify a short time interval to ensure that finished transactions can be cleaned up in a timely manner.
- Introduced in: -

### Storage

##### default_replication_num

- Default: 3
- Type: Short
- Unit: -
- Is mutable: Yes
- Description: Sets the default number of replicas for each data partition when creating a table in StarRocks. This setting can be overridden when creating a table by specifying `replication_num=x` in the CREATE TABLE DDL.
- Introduced in: -

##### enable_strict_storage_medium_check

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether the FE strictly checks the storage medium of BEs when users create tables. If this parameter is set to `TRUE`, the FE checks the storage medium of BEs when users create tables and returns an error if the storage medium of the BE is different from the `storage_medium` parameter specified in the CREATE TABLE statement. For example, the storage medium specified in the CREATE TABLE statement is SSD but the actual storage medium of BEs is HDD. As a result, the table creation fails. If this parameter is `FALSE`, the FE does not check the storage medium of BEs when users create a table.
- Introduced in: -

##### catalog_trash_expire_second

- Default: 86400
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The longest duration the metadata can be retained after a database, table, or partition is dropped. If this duration expires, the data will be deleted and cannot be recovered through the [RECOVER](../../sql-reference/sql-statements/backup_restore/RECOVER.md) command.
- Introduced in: -

##### enable_auto_tablet_distribution

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to automatically set the number of buckets.
  - If this parameter is set to `TRUE`, you don't need to specify the number of buckets when you create a table or add a partition. StarRocks automatically determines the number of buckets.
  - If this parameter is set to `FALSE`, you need to manually specify the number of buckets when you create a table or add a partition. If you do not specify the bucket count when adding a new partition to a table, the new partition inherits the bucket count set at the creation of the table. However, you can also manually specify the number of buckets for the new partition.
- Introduced in: v2.5.7

##### enable_experimental_rowstore

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable the [hybrid row-column storage](../../table_design/hybrid_table.md) feature.
- Introduced in: v3.2.3

#### enable_experimental_gin

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable the [full-text inverted index](../../table_design/indexes/inverted_index.md) feature.
- Introduced in: v3.3.0

##### storage_usage_soft_limit_percent

- Default: 90
- Alias: storage_high_watermark_usage_percent
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Soft limit of the storage usage percentage in a BE directory. If the storage usage (in percentage) of the BE storage directory exceeds this value and the remaining storage space is less than `storage_usage_soft_limit_reserve_bytes`, tablets cannot be cloned into this directory.
- Introduced in: -

##### storage_usage_soft_limit_reserve_bytes

- Default: 200 * 1024 * 1024 * 1024
- Alias: storage_min_left_capacity_bytes
- Type: Long
- Unit: Bytes
- Is mutable: Yes
- Description: Soft limit of the remaining storage space in a BE directory. If the remaining storage space in the BE storage directory is less than this value and the storage usage (in percentage) exceeds `storage_usage_soft_limit_percent`, tablets cannot be cloned into this directory.
- Introduced in: -

##### storage_usage_hard_limit_percent

- Default: 95
- Alias: storage_flood_stage_usage_percent
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Hard limit of the storage usage percentage in a BE directory. If the storage usage (in percentage) of the BE storage directory exceeds this value and the remaining storage space is less than `storage_usage_hard_limit_reserve_bytes`, Load and Restore jobs are rejected. You need to set this item together with the BE configuration item `storage_flood_stage_usage_percent` to allow the configurations to take effect.
- Introduced in: -

##### storage_usage_hard_limit_reserve_bytes

- Default: 100 * 1024 * 1024 * 1024
- Alias: storage_flood_stage_left_capacity_bytes
- Type: Long
- Unit: Bytes
- Is mutable: Yes
- Description: Hard limit of the remaining storage space in a BE directory. If the remaining storage space in the BE storage directory is less than this value and the storage usage (in percentage) exceeds `storage_usage_hard_limit_percent`, Load and Restore jobs are rejected. You need to set this item together with the BE configuration item `storage_flood_stage_left_capacity_bytes` to allow the configurations to take effect.
- Introduced in: -

##### alter_table_timeout_second

- Default: 86400
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The timeout duration for the schema change operation (ALTER TABLE).
- Introduced in: -

##### enable_fast_schema_evolution

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable fast schema evolution for all tables within the StarRocks cluster. Valid values are `TRUE` and `FALSE` (default). Enabling fast schema evolution can increase the speed of schema changes and reduce resource usage when columns are added or dropped.
- Introduced in: v3.2.0

> **NOTE**
>
> - StarRocks shared-data clusters supports this parameter from v3.3.0.
> - If you need to configure the fast schema evolution for a specific table, such as disabling fast schema evolution for a specific table, you can set the table property [`fast_schema_evolution`](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md#set-fast-schema-evolution) at table creation.

##### recover_with_empty_tablet

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to replace a lost or corrupted tablet replica with an empty one. If a tablet replica is lost or corrupted, data queries on this tablet or other healthy tablets may fail. Replacing the lost or corrupted tablet replica with an empty tablet ensures that the query can still be executed. However, the result may be incorrect because data is lost. The default value is `FALSE`, which means lost or corrupted tablet replicas are not replaced with empty ones, and the query fails.
- Introduced in: -

##### tablet_create_timeout_second

- Default: 10
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The timeout duration for creating a tablet. The default value is changed from 1 to 10 from v3.1 onwards.
- Introduced in: -

##### tablet_delete_timeout_second

- Default: 2
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The timeout duration for deleting a tablet.
- Introduced in: -

##### check_consistency_default_timeout_second

- Default: 600
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The timeout duration for a replica consistency check. You can set this parameter based on the size of your tablet.
- Introduced in: -

##### tablet_sched_slot_num_per_path

- Default: 8
- Alias: schedule_slot_num_per_path
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of tablet-related tasks that can run concurrently in a BE storage directory. From v2.5 onwards, the default value of this parameter is changed from `4` to `8`.
- Introduced in: -

##### tablet_sched_max_scheduling_tablets

- Default: 10000
- Alias: max_scheduling_tablets
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of tablets that can be scheduled at the same time. If the value is exceeded, tablet balancing and repair checks will be skipped.
- Introduced in: -

##### tablet_sched_disable_balance

- Default: false
- Alias: disable_balance
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to disable tablet balancing. `TRUE` indicates that tablet balancing is disabled. `FALSE` indicates that tablet balancing is enabled.
- Introduced in: -

##### tablet_sched_disable_colocate_balance

- Default: false
- Alias: disable_colocate_balance
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to disable replica balancing for Colocate Table. `TRUE` indicates replica balancing is disabled. `FALSE` indicates replica balancing is enabled.
- Introduced in: -

<!--
##### tablet_sched_disable_colocate_overall_balance

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### tablet_sched_colocate_balance_high_prio_backends

- Default: {}
- Type: Long[]
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### tablet_sched_always_force_decommission_replica

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### tablet_sched_be_down_tolerate_time_s

- Default: 900
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The maximum duration the scheduler allows for a BE node to remain inactive. After the time threshold is reached, tablets on that BE node will be migrated to other active BE nodes.
- Introduced in: v2.5.7

<!--
##### tablet_sched_colocate_be_down_tolerate_time_s

- Default: 12 * 3600
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### tablet_sched_max_balancing_tablets

- Default: 500
- Alias: max_balancing_tablets
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of tablets that can be balanced at the same time. If this value is exceeded, tablet re-balancing will be skipped.
- Introduced in: -

##### tablet_sched_storage_cooldown_second

- Default: -1
- Alias: storage_cooldown_second
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The latency of automatic cooling starting from the time of table creation. The default value `-1` specifies that automatic cooling is disabled. If you want to enable automatic cooling, set this parameter to a value greater than `-1`.
- Introduced in: -

##### tablet_sched_max_not_being_scheduled_interval_ms

- Default: 15 * 60 * 1000
- Type: Long
- Unit: Milliseconds
- Is mutable: Yes
- Description: When the tablet clone tasks are being scheduled, if a tablet has not been scheduled for the specified time in this parameter, StarRocks gives it a higher priority to schedule it as soon as possible.
- Introduced in: -

##### tablet_sched_balance_load_score_threshold

- Default: 0.1
- Alias: balance_load_score_threshold
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: The percentage threshold for determining whether the load of a BE is balanced. If a BE has a lower load than the average load of all BEs and the difference is greater than this value, this BE is in a low load state. On the contrary, if a BE has a higher load than the average load and the difference is greater than this value, this BE is in a high load state.
- Introduced in: -

##### tablet_sched_num_based_balance_threshold_ratio

- Default: 0.5
- Alias: -
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: Doing num based balance may break the disk size balance, but the maximum gap between disks cannot exceed tablet_sched_num_based_balance_threshold_ratio * tablet_sched_balance_load_score_threshold. If there are tablets in the cluster that are constantly balancing from A to B and B to A, reduce this value. If you want the tablet distribution to be more balanced, increase this value.
- Introduced in: - 3.1

##### tablet_sched_balance_load_disk_safe_threshold

- Default: 0.5
- Alias: balance_load_disk_safe_threshold
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: The percentage threshold for determining whether the disk usage of BEs is balanced. If the disk usage of all BEs is lower than this value, it is considered balanced. If the disk usage is greater than this value and the difference between the highest and lowest BE disk usage is greater than 10%, the disk usage is considered unbalanced and a tablet re-balancing is triggered.
- Introduced in: -

##### tablet_sched_repair_delay_factor_second

- Default: 60
- Alias: tablet_repair_delay_factor_second
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The interval at which replicas are repaired, in seconds.
- Introduced in: -

##### tablet_sched_min_clone_task_timeout_sec

- Default: 3 * 60
- Alias: min_clone_task_timeout_sec
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The minimum timeout duration for cloning a tablet.
- Introduced in: -

##### tablet_sched_max_clone_task_timeout_sec

- Default: 2 * 60 * 60
- Alias: max_clone_task_timeout_sec
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description:The maximum timeout duration for cloning a tablet.
- Introduced in: -

<!--
##### tablet_sched_checker_interval_seconds

- Default: 20
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### tablet_sched_max_migration_task_sent_once

- Default: 1000
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### tablet_sched_consecutive_full_clone_delay_sec

- Default: 180
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### tablet_report_drop_tablet_delay_sec

- Default: 120
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### tablet_checker_partition_batch_num

- Default: 500
- Type: Int
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### tablet_stat_update_interval_second

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: The time interval at which the FE retrieves tablet statistics from each BE.
- Introduced in: -

##### max_automatic_partition_number

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of automatically created partitions.
- Introduced in: v3.1

##### auto_partition_max_creation_number_per_load

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of partitions can be created in a table (with Expression Partitioning strategy) by a loading task.
- Introduced in: v3.3.2

##### max_partition_number_per_table

- Default: 100000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of partitions can be created in a table.
- Introduced in: v3.3.2

##### max_bucket_number_per_partition

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of buckets can be created in a partition.
- Introduced in: v3.3.2

##### max_column_number_per_table

- Default: 10000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of columns can be created in a table.
- Introduced in: v3.3.2

### Shared-data

##### run_mode

- Default: shared_nothing
- Type: String
- Unit: -
- Is mutable: No
- Description: The running mode of the StarRocks cluster. Valid values: `shared_data` and `shared_nothing` (Default).
  - `shared_data` indicates running StarRocks in shared-data mode.
  - `shared_nothing` indicates running StarRocks in shared-nothing mode.

  > **CAUTION**
  >
  > - You cannot adopt the `shared_data` and `shared_nothing` modes simultaneously for a StarRocks cluster. Mixed deployment is not supported.
  > - DO NOT change `run_mode` after the cluster is deployed. Otherwise, the cluster fails to restart. The transformation from a shared-nothing cluster to a shared-data cluster or vice versa is not supported.

- Introduced in: -

<!--
##### shard_group_clean_threshold_sec

- Default: 3600
- Type: Long
- Unit: Seconds
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### star_mgr_meta_sync_interval_sec

- Default: 600
- Type: Long
- Unit: Seconds
- Is mutable: No
- Description:
- Introduced in: -
-->

##### cloud_native_meta_port

- Default: 6090
- Type: Int
- Unit: -
- Is mutable: No
- Description: FE cloud-native metadata server RPC listen port.
- Introduced in: -


##### enable_load_volume_from_conf

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to allow StarRocks to create the built-in storage volume by using the object storage-related properties specified in the FE configuration file. The default value is changed from `true` to `false` from v3.4.1 onwards.
- Introduced in: v3.1.0


##### cloud_native_storage_type

- Default: S3
- Type: String
- Unit: -
- Is mutable: No
- Description: The type of object storage you use. In shared-data mode, StarRocks supports storing data in Azure Blob (supported from v3.1.1 onwards), and object storages that are compatible with the S3 protocol (such as AWS S3, Google GCP, and MinIO). Valid value: `S3` (Default) and `AZBLOB`. If you specify this parameter as `S3`, you must add the parameters prefixed by `aws_s3`. If you specify this parameter as `AZBLOB`, you must add the parameters prefixed by `azure_blob`.
- Introduced in: -

<!--
##### cloud_native_hdfs_url

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

##### aws_s3_path

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The S3 path used to store data. It consists of the name of your S3 bucket and the sub-path (if any) under it, for example, `testbucket/subpath`.
- Introduced in: v3.0

##### aws_s3_region

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The region in which your S3 bucket resides, for example, `us-west-2`.
- Introduced in: v3.0

##### aws_s3_endpoint

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The endpoint used to access your S3 bucket, for example, `https://s3.us-west-2.amazonaws.com`.
- Introduced in: v3.0

##### aws_s3_use_aws_sdk_default_behavior

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to use the default authentication credential of AWS SDK. Valid values: true and false (Default).
- Introduced in: v3.0

##### aws_s3_use_instance_profile

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to use Instance Profile and Assumed Role as credential methods for accessing S3. Valid values: true and false (Default).
  - If you use IAM user-based credential (Access Key and Secret Key) to access S3, you must specify this item as `false`, and specify `aws_s3_access_key` and `aws_s3_secret_key`.
  - If you use Instance Profile to access S3, you must specify this item as `true`.
  - If you use Assumed Role to access S3, you must specify this item as `true`, and specify `aws_s3_iam_role_arn`.
  - And if you use an external AWS account, you must also specify `aws_s3_external_id`.
- Introduced in: v3.0

##### aws_s3_access_key

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The Access Key ID used to access your S3 bucket.
- Introduced in: v3.0

##### aws_s3_secret_key

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The Secret Access Key used to access your S3 bucket.
- Introduced in: v3.0

##### aws_s3_iam_role_arn

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The ARN of the IAM role that has privileges on your S3 bucket in which your data files are stored.
- Introduced in: v3.0

##### aws_s3_external_id

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The external ID of the AWS account that is used for cross-account access to your S3 bucket.
- Introduced in: v3.0

##### azure_blob_endpoint

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The endpoint of your Azure Blob Storage Account, for example, `https://test.blob.core.windows.net`.
- Introduced in: v3.1

##### azure_blob_path

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The Azure Blob Storage path used to store data. It consists of the name of the container within your storage account and the sub-path (if any) under the container, for example, `testcontainer/subpath`.
- Introduced in: v3.1

##### azure_blob_shared_key

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The Shared Key used to authorize requests for your Azure Blob Storage.
- Introduced in: v3.1

##### azure_blob_sas_token

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The shared access signatures (SAS) used to authorize requests for your Azure Blob Storage.
- Introduced in: v3.1

<!--
##### starmgr_grpc_timeout_seconds

- Default: 5
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### lake_compaction_score_selector_min_score

- Default: 10.0
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: The Compaction Score threshold that triggers Compaction operations in a shared-data cluster. When the Compaction Score of a partition is greater than or equal to this value, the system performs Compaction on that partition.
- Introduced in: v3.1.0

##### lake_compaction_max_tasks

- Default: -1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of concurrent Compaction tasks allowed in a shared-data cluster. Setting this item to `-1` indicates to calculate the concurrent task number in an adaptive manner. Setting this value to `0` will disable compaction.
- Introduced in: v3.1.0

##### lake_compaction_history_size

- Default: 20
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The number of recent successful Compaction task records to keep in the memory of the Leader FE node in a shared-data cluster. You can view recent successful Compaction task records using the `SHOW PROC '/compactions'` command. Note that the Compaction history is stored in the FE process memory, and it will be lost if the FE process is restarted.
- Introduced in: v3.1.0

##### lake_publish_version_max_threads

- Default: 512
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of threads for Version Publish tasks in a shared-data cluster.
- Introduced in: v3.2.0

<!--
##### lake_publish_delete_txnlog_max_threads

- Default: 16
- Type: Int
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### lake_compaction_default_timeout_second

- Default: 86400
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### lake_autovacuum_max_previous_versions

- Default: 0
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### lake_autovacuum_parallel_partitions

- Default: 8
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of partitions that can undergo AutoVacuum simultaneously in a shared-data cluster. AutoVacuum is the Garbage Collection after Compactions.
- Introduced in: v3.1.0

##### lake_autovacuum_partition_naptime_seconds

- Default: 180
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The minimum interval between AutoVacuum operations on the same partition in a shared-data cluster.
- Introduced in: v3.1.0

##### lake_autovacuum_grace_period_minutes

- Default: 30
- Type: Long
- Unit: Minutes
- Is mutable: Yes
- Description: The time range for retaining historical data versions in a shared-data cluster. Historical data versions within this time range are not automatically cleaned via AutoVacuum after Compactions. You need to set this value greater than the maximum query time to avoid that the data accessed by running queries get deleted before the queries finish. The default value has been changed from `5` to `30` since v3.3.0, v3.2.5, and v3.1.10.
- Introduced in: v3.1.0

##### lake_autovacuum_stale_partition_threshold

- Default: 12
- Type: Long
- Unit: Hours
- Is mutable: Yes
- Description: If a partition has no updates (loading, DELETE, or Compactions) within this time range, the system will not perform AutoVacuum on this partition.
- Introduced in: v3.1.0

##### lake_enable_ingest_slowdown

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable Data Ingestion Slowdown in a shared-data cluster. When Data Ingestion Slowdown is enabled, if the Compaction Score of a partition exceeds `lake_ingest_slowdown_threshold`, loading tasks on that partition will be throttled down. This configuration only takes effect when `run_mode` is set to `shared_data`. From v3.3.6 onwards, the default value is chenged from `false` to `true`.
- Introduced in: v3.2.0

##### lake_ingest_slowdown_threshold

- Default: 100
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: The Compaction Score threshold that triggers Data Ingestion Slowdown in a shared-data cluster. This configuration only takes effect when `lake_enable_ingest_slowdown` is set to `true`.
- Introduced in: v3.2.0

##### lake_ingest_slowdown_ratio

- Default: 0.1
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: The ratio of the loading rate slowdown when Data Ingestion Slowdown is triggered.

  Data loading tasks consist of two phases: data writing and data committing (COMMIT). Data Ingestion Slowdown is achieved by delaying data committing. The delay ratio is calculated using the following formula: `(compaction_score - lake_ingest_slowdown_threshold) * lake_ingest_slowdown_ratio`. For example, if the data writing phase takes 5 minutes, `lake_ingest_slowdown_ratio` is 0.1, and the Compaction Score is 10 higher than `lake_ingest_slowdown_threshold`, the delay in data committing time is `5 * 10 * 0.1 = 5` minutes, which means the average loading speed is halved.

- Introduced in: v3.2.0

> **NOTE**
>
> - If a loading task writes to multiple partitions simultaneously, the maximum Compaction Score among all partitions is used to calculate the delay in committing time.
> - The delay in committing time is calculated during the first attempt to commit. Once set, it will not change. Once the delay time is up, as long as the Compaction Score is not above `lake_compaction_score_upper_bound`, the system will perform the data committing operation.
> - If the delay in committing time exceeds the timeout of the loading task, the task will fail directly.

##### lake_compaction_score_upper_bound

- Default: 2000
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: The upper limit of the Compaction Score for a partition in a shared-data cluster. `0` indicates no upper limit. This item only takes effect when `lake_enable_ingest_slowdown` is set to `true`. When the Compaction Score of a partition reaches or exceeds this upper limit, incoming loading tasks will be rejected. From v3.3.6 onwards, the default value is changed from `0` to `2000`.
- Introduced in: v3.2.0

##### lake_compaction_disable_tables

- Default: ""
- Type: String
- Unit: -
- Is mutable: Yes
- Description: The table list of which compaction is disabled in shared-data mode. The format is `tableId1;tableId2`, seperated by semicolon, for example, `12345;98765`.
- Introduced in: v3.1.11

##### lake_enable_balance_tablets_between_workers

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to balance the number of tablets among Compute Nodes during the tablet migration of cloud-native tables in a shared-data cluster. `true` indicates to balance the tablets among Compute Nodes, and `false` indicates to disabling this feature.
- Introduced in: v3.3.4

##### lake_balance_tablets_threshold

- Default: 0.15
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: The threshold the system used to judge the tablet balance among workers in a shared-data cluster, The imbalance factor is calculated as `f = (MAX(tablets) - MIN(tablets)) / AVERAGE(tablets)`. If the factor is greater than `lake_balance_tablets_threshold`, a tablet balance will be triggered. This item takes effect only when `lake_enable_balance_tablets_between_workers` is set to `true`.
- Introduced in: v3.3.4

### Other

##### tmp_dir

- Default: StarRocksFE.STARROCKS_HOME_DIR + "/temp_dir"
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory that stores temporary files such as files generated during backup and restore procedures. After these procedures finish, the generated temporary files are deleted.
- Introduced in: -

##### plugin_dir

- Default: System.getenv("STARROCKS_HOME") + "/plugins"
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory that stores plugin installation packages.
- Introduced in: -

##### plugin_enable

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether plugins can be installed on FEs. Plugins can be installed or uninstalled only on the Leader FE.
- Introduced in: -

<!--
##### profile_process_threads_num

- Default: 2
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### profile_process_blocking_queue_size

- Default: profile_process_threads_num * 128
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

##### max_agent_task_threads_num

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of threads that are allowed in the agent task thread pool.
- Introduced in: -

##### agent_task_resend_wait_time_ms

- Default: 5000
- Type: Long
- Unit: Milliseconds
- Is mutable: Yes
- Description: The duration the FE must wait before it can resend an agent task. An agent task can be resent only when the gap between the task creation time and the current time exceeds the value of this parameter. This parameter is used to prevent repetitive sending of agent tasks.
- Introduced in: -

<!--
##### start_with_incomplete_meta

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### max_backend_down_time_second

- Default: 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### lake_enable_batch_publish_version 

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### lake_batch_publish_max_version_num

- Default: 10
- Type: Int
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### lake_batch_publish_min_version_num

- Default: 1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### capacity_used_percent_high_water

- Default: 0.75
- Type: Double
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### alter_max_worker_threads

- Default: 4
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### alter_max_worker_queue_size

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### task_runs_queue_length

- Default: 500
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### task_runs_concurrency

- Default: 4
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### max_task_runs_threads_num

- Default: 512
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### consistency_check_start_time

- Default: 23
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### consistency_check_end_time

- Default: 4
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### consistency_tablet_meta_check_interval_ms

- Default: 2 * 3600 * 1000
- Type: Long
- Unit: Milliseconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### backup_plugin_path (Deprecated)

- Default: /tools/trans_file_tool/trans_files.sh
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

##### backup_job_default_timeout_ms

- Default: 86400 * 1000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: The timeout duration of a backup job. If this value is exceeded, the backup job fails.
- Introduced in: -

##### locale

- Default: zh_CN.UTF-8
- Type: String
- Unit: -
- Is mutable: No
- Description: The character set that is used by the FE.
- Introduced in: -

<!--
##### db_used_data_quota_update_interval_secs

- Default: 300
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### disable_hadoop_load

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### report_queue_size (Deprecated)

- Default: 100
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of jobs that can wait in a report queue. The report is about disk, task, and tablet information of BEs. If too many report jobs are piling up in a queue, OOM will occur.
- Introduced in: -

##### enable_metric_calculator

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Specifies whether to enable the feature that is used to periodically collect metrics. Valid values: `TRUE` and `FALSE`. `TRUE` specifies to enable this feature, and `FALSE` specifies to disable this feature.
- Introduced in: -

<!--
##### enable_replicated_storage_as_default_engine

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### enable_schedule_insert_query_by_row_count

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### max_small_file_number

- Default: 100
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of small files that can be stored on an FE directory.
- Introduced in: -

##### max_small_file_size_bytes

- Default: 1024 * 1024
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: The maximum size of a small file.
- Introduced in: -

##### small_file_dir

- Default: StarRocksFE.STARROCKS_HOME_DIR + "/small_files"
- Type: String
- Unit: -
- Is mutable: No
- Description: The root directory of small files.
- Introduced in: -

##### enable_auth_check

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Specifies whether to enable the authentication check feature. Valid values: `TRUE` and `FALSE`. `TRUE` specifies to enable this feature, and `FALSE` specifies to disable this feature.
- Introduced in: -

<!--
##### enable_starrocks_external_table_auth_check

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### authorization_enable_column_level_privilege

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### authentication_chain

- Default: {AUTHENTICATION_CHAIN_MECHANISM_NATIVE}
- Type: String[]
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### authentication_ldap_simple_server_host

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: Yes
- Description: The host on which the LDAP server runs.
- Introduced in: -

##### authentication_ldap_simple_server_port

- Default: 389
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The port of the LDAP server.
- Introduced in: -

##### authentication_ldap_simple_bind_base_dn

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: Yes
- Description: The base DN, which is the point from which the LDAP server starts to search for users' authentication information.
- Introduced in: -

##### authentication_ldap_simple_user_search_attr

- Default: uid
- Type: String
- Unit: -
- Is mutable: Yes
- Description: The name of the attribute that identifies users in LDAP objects.
- Introduced in: -

##### authentication_ldap_simple_bind_root_dn

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: Yes
- Description: The administrator DN used to search for users' authentication information.
- Introduced in: -

##### authentication_ldap_simple_bind_root_pwd

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: Yes
- Description: The password of the administrator used to search for users' authentication information.
- Introduced in: -

<!--
##### enable_token_check

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

##### auth_token

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The token that is used for identity authentication within the StarRocks cluster to which the FE belongs. If this parameter is left unspecified, StarRocks generates a random token for the cluster at the time when the leader FE of the cluster is started for the first time.
- Introduced in: -

<!--
##### enable_authentication_kerberos

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### authentication_kerberos_service_principal

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### authentication_kerberos_service_key_tab

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### authorization_enable_admin_user_protection

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### authorization_enable_priv_collection_cache

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### max_partition_number_per_table

- Default: 100000
- Type: Long
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### enable_automatic_bucket

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### default_automatic_bucket_size

- Default: 0
- Type: Long
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### max_agent_tasks_send_per_be

- Default: 10000
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### hive_meta_cache_refresh_min_threads

- Default: 50
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

##### hive_meta_load_concurrency

- Default: 4
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of concurrent threads that are supported for Hive metadata.
- Introduced in: -

##### hive_meta_cache_refresh_interval_s

- Default: 3600 * 2
- Type: Long
- Unit: Seconds
- Is mutable: No
- Description: The time interval at which the cached metadata of Hive external tables is updated.
- Introduced in: -

##### hive_meta_cache_ttl_s

- Default: 3600 * 24
- Type: Long
- Unit: Seconds
- Is mutable: No
- Description: The amount of time after which the cached metadata of Hive external tables expires.
- Introduced in: -

<!--
##### remote_file_cache_ttl_s

- Default: 3600 * 36
- Type: Long
- Unit: Seconds
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### max_hive_partitions_per_rpc

- Default: 5000
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### remote_file_cache_refresh_interval_s

- Default: 60
- Type: Long
- Unit: Seconds
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### remote_file_metadata_load_concurrency

- Default: 32
- Type: Int
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

##### hive_meta_store_timeout_s

- Default: 10
- Type: Long
- Unit: Seconds
- Is mutable: No
- Description: The amount of time after which a connection to a Hive metastore times out.
- Introduced in: -

<!--
##### enable_hms_events_incremental_sync

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### hms_events_polling_interval_ms

- Default: 5000
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### hms_events_batch_size_per_rpc

- Default: 500
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### enable_hms_parallel_process_evens

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### hms_process_events_parallel_num

- Default: 4
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### hive_max_split_size

- Default: 64 * 1024 * 1024
- Type: Long
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### enable_refresh_hive_partitions_statistics

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### enable_iceberg_custom_worker_thread

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### iceberg_worker_num_threads

- Default: Runtime.getRuntime().availableProcessors()
- Type: Long
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### iceberg_table_refresh_threads

- Default: 128
- Type: Int
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### iceberg_table_refresh_expire_sec

- Default: 86400
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### iceberg_metadata_cache_disk_path

- Default: StarRocksFE.STARROCKS_HOME_DIR + "/caches/iceberg"
- Type: String
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### iceberg_metadata_memory_cache_capacity

- Default: 536870912
- Type: Long
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### iceberg_metadata_memory_cache_expiration_seconds

- Default: 86500
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### enable_iceberg_metadata_disk_cache

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### iceberg_metadata_disk_cache_capacity

- Default: 2147483648
- Type: Long
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### iceberg_metadata_disk_cache_expiration_seconds

- Default: 7 * 24 * 60 * 60
- Type: Long
- Unit: Seconds
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### iceberg_metadata_cache_max_entry_size

- Default: 8388608
- Type: Long
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### es_state_sync_interval_second

- Default: 10
- Type: Long
- Unit: Seconds
- Is mutable: No
- Description: The time interval at which the FE obtains Elasticsearch indexes and synchronizes the metadata of StarRocks external tables.
- Introduced in: -

<!--
##### broker_client_timeout_ms

- Default: 120000
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### vectorized_load_enable (Deprecated)

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### enable_pipeline_load (Deprecated)

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### enable_shuffle_load

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### eliminate_shuffle_load_by_replicated_storage

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### enable_vectorized_file_load (Deprecated)

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### enable_routine_load_lag_metrics

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### query_detail_cache_time_nanosecond

- Default: 30000000000
- Type: Long
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### min_routine_load_lag_for_metrics

- Default: 10000
- Type: Long
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### heartbeat_timeout_second

- Default: 5
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### heartbeat_retry_times

- Default: 3
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### enable_display_shadow_partitions

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### enable_dict_optimize_routine_load

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### enable_dict_optimize_stream_load

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### enable_validate_password

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### enable_password_reuse

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### quorum_publish_wait_time_ms

- Default: 5000
- Alias: quorom_publish_wait_time_ms
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: The wait time for quorum publish. You can increase the timeout to avoid unnecessary Clone.
- Introduced in: v3.1
-->

<!--
##### metadata_journal_queue_size

- Default: 1000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### metadata_journal_max_batch_size_mb

- Default: 10
- Type: Int
- Unit: MB
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### metadata_journal_max_batch_cnt

- Default: 100
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### jaeger_grpc_endpoint

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### lake_compaction_selector

- Default: ScoreSelector
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### lake_compaction_sorter

- Default: ScoreSorter
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### lake_compaction_simple_selector_min_versions

- Default: 3
- Type: Long
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### lake_compaction_simple_selector_threshold_versions

- Default: 10
- Type: Long
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### lake_compaction_simple_selector_threshold_seconds

- Default: 300
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### enable_new_publish_mechanism

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### metadata_journal_skip_bad_journal_ids

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### profile_info_reserved_num

- Default: 500
- Type: Int
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### load_profile_info_reserved_num

- Default: 500
- Type: Int
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### profile_info_format

- Default: default
- Type: String
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### ignore_invalid_privilege_authentications

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### ssl_keystore_location

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### ssl_keystore_password

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### ssl_key_password

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### ssl_truststore_location

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### ssl_truststore_password

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### enable_check_db_state

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### binlog_ttl_second

- Default: 60 * 30
- Type: Long
- Unit: Seconds
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### binlog_max_size

- Default: Long.MAX_VALUE
- Type: Long
- Unit:
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### enable_safe_mode

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### safe_mode_checker_interval_sec

- Default: 5
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### auto_increment_cache_size

- Default: 100000
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### enable_experimental_temporary_table

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### max_per_node_grep_log_limit

- Default: 500000
- Type: Long
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### enable_execute_script_on_frontend

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### alter_scheduler_interval_millisecond

- Default: 10000
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### routine_load_scheduler_interval_millisecond

- Default: 10000
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### load_profile_collect_threshold_second

- Default: 0
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### max_upload_task_per_be

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: In each BACKUP operation, the maximum number of upload tasks StarRocks assigned to a BE node. When this item is set to less than or equal to 0, no limit is imposed on the task number.
- Introduced in: v3.1.0

##### max_download_task_per_be

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: In each RESTORE operation, the maximum number of download tasks StarRocks assigned to a BE node. When this item is set to less than or equal to 0, no limit is imposed on the task number.
- Introduced in: v3.1.0

##### enable_colocate_restore

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable Backup and Restore for Colocate Tables. `true` indicates enabling Backup and Restore for Colocate Tables and `false` indicates disabling it.
- Introduced in: v3.2.10, v3.3.3

<!--
##### enable_persistent_index_by_default

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### experimental_enable_fast_schema_evolution_in_shared_data

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### pipe_listener_interval_millis

- Default: 1000
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### pipe_scheduler_interval_millis

- Default: 1000
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### enable_show_external_catalog_privilege

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### primary_key_disk_schedule_time

- Default: 3600
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### access_control

- Default: native
- Type: String
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### ranger_user_ugi

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### catalog_metadata_cache_size

- Default: 500
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### mv_plan_cache_expire_interval_sec

- Default: 24 * 60 * 60
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### mv_plan_cache_max_size

- Default: 1000
- Type: Long
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### mv_query_context_cache_max_size

- Default: 1000
- Type: Long
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### port_connectivity_check_interval_sec

- Default: 60
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### port_connectivity_check_retry_times

- Default: 3
- Type: Long
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### port_connectivity_check_timeout_ms

- Default: 10000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### json_file_size_limit

- Default: 4294967296
- Type: Long
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### allow_system_reserved_names

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to allow users to create columns whose names are initiated with `__op` and `__row`. To enable this feature, set this parameter to `TRUE`. Please note that these name formats are reserved for special purposes in StarRocks and creating such columns may result in undefined behavior. Therefore this feature is disabled by default.
- Introduced in: v3.2.0

<!--
##### use_lock_manager

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### lock_table_num

- Default: 32
- Type: Int
- Unit: -
- Is mutable: No
- Description:
- Introduced in: -
-->

<!--
##### lock_manager_enable_resolve_deadlock

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### lock_manager_enable_loading_using_fine_granularity_lock

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### lock_manager_dead_lock_detection_delay_time_ms

- Default: 3000
- Type: Long
- Unit: Milliseconds
- Is mutable: Yes
- Description:
- Introduced in: -
-->

<!--
##### refresh_dictionary_cache_thread_num

- Default: 2
- Type: Int
- Unit: -
- Is mutable: Yes
- Description:
- Introduced in: -
-->

##### replication_interval_ms

- Default: 100
- Type: Int
- Unit: -
- Is mutable: No
- Description: The minimum time interval at which the replication tasks are scheduled.
- Introduced in: v3.3.5

##### replication_max_parallel_table_count

- Default: 100
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of concurrent data synchronization tasks allowed. StarRocks creates one synchronization task for each table.
- Introduced in: v3.3.5

##### replication_max_parallel_replica_count

- Default: 10240
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of tablet replicas allowed for concurrent synchronization.
- Introduced in: v3.3.5

##### replication_max_parallel_data_size_mb

- Default: 1048576
- Type: Int
- Unit: MB
- Is mutable: Yes
- Description: The maximum size of data allowed for concurrent synchronization.
- Introduced in: v3.3.5

##### replication_transaction_timeout_sec

- Default: 86400
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The timeout duration for synchronization tasks.
- Introduced in: v3.3.5

##### jdbc_meta_default_cache_enable

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: The default value for whether the JDBC Catalog metadata cache is enabled. When set to True, newly created JDBC Catalogs will default to metadata caching enabled.
- Introduced in: -

##### jdbc_meta_default_cache_expire_sec

- Default: 600
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The default expiration time for the JDBC Catalog metadata cache. When `jdbc_meta_default_cache_enable` is set to true, newly created JDBC Catalogs will default to setting the expiration time of the metadata cache.
- Introduced in: -

##### jdbc_connection_pool_size

- Default: 8
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum capacity of the JDBC connection pool for accessing JDBC catalogs.
- Introduced in: -

##### jdbc_minimum_idle_connections

- Default: 1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The minimum number of idle connections in the JDBC connection pool for accessing JDBC catalogs.
- Introduced in: -

##### jdbc_connection_idle_timeout_ms

- Default: 600000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: The maximum amount of time after which a connection for accessing a JDBC catalog times out. Timed-out connections are considered idle.
- Introduced in: -

##### query_detail_explain_level

- Default: COSTS
- Type: String
- Unit: -
- Is mutable: true
- Description: The detail level of query plan returned by the EXPLAIN statement. Valid values: COSTS, NORMAL, VERBOSE.
- Introduced in: v3.2.12, v3.3.5

<!--
##### max_varchar_length

- Default: 1048576
- Type: Int
- Unit:
- Is mutable: Yes
- Description:
- Introduced in: -
-->
