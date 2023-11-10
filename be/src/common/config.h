// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/common/config.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "configbase.h"

namespace starrocks::config {
// The cluster id.
CONF_Int32(cluster_id, "-1");
// The port on which ImpalaInternalService is exported.
CONF_Int32(be_port, "9060");
CONF_Int32(thrift_port, "0");

// The port for brpc.
CONF_Int32(brpc_port, "8060");

// The number of bthreads for brpc, the default value is set to -1, which means the number of bthreads is #cpu-cores.
CONF_Int32(brpc_num_threads, "-1");

// The max number of single connections maintained by the brpc client and each server.
// Theses connections are created during the first few access and will be used thereafter
CONF_Int32(brpc_max_connections_per_server, "1");

// Declare a selection strategy for those servers have many ips.
// Note that there should at most one ip match this list.
// this is a list in semicolon-delimited format, in CIDR notation, e.g. 10.10.10.0/24
// If no ip match this rule, will choose one randomly.
CONF_String(priority_networks, "");

CONF_mBool(enable_auto_adjust_pagecache, "true");
// Memory urget water level, if the memory usage exceeds this level, reduce the size of
// the Pagecache immediately, it should be between (memory_high_level, 100].
CONF_mInt64(memory_urgent_level, "85");
// Memory high water level, if the memory usage exceeds this level, reduce the size of
// the Pagecache slowly, it should be between [1, memory_urgent_level).
CONF_mInt64(memory_high_level, "75");
// Pagecache size adjust period, default 20, it should be between [1, 180].
CONF_mInt64(pagecache_adjust_period, "20");
// Sleep time in seconds between pagecache adjust iterations.
CONF_mInt64(auto_adjust_pagecache_interval_seconds, "10");

// process memory limit specified as number of bytes
// ('<int>[bB]?'), megabytes ('<float>[mM]'), gigabytes ('<float>[gG]'),
// or percentage of the physical memory ('<int>%').
// defaults to bytes if no unit is given"
// must larger than 0. and if larger than physical memory size,
// it will be set to physical memory size.
CONF_String(mem_limit, "90%");

// The port heartbeat service used.
CONF_Int32(heartbeat_service_port, "9050");
// The count of heart beat service.
CONF_Int32(heartbeat_service_thread_count, "1");
// The count of thread to create table.
CONF_Int32(create_tablet_worker_count, "3");
// The count of thread to drop table.
CONF_Int32(drop_tablet_worker_count, "3");
// The count of thread to batch load.
CONF_Int32(push_worker_count_normal_priority, "3");
// The count of thread to high priority batch load.
CONF_Int32(push_worker_count_high_priority, "3");

// The count of thread to publish version per transaction
CONF_mInt32(transaction_publish_version_worker_count, "0");

// The count of thread to apply rowset in primary key table
// 0 means apply worker count is equal to cpu core count
CONF_mInt32(transaction_apply_worker_count, "0");
CONF_mInt32(get_pindex_worker_count, "0");

// The count of thread to clear transaction task.
CONF_Int32(clear_transaction_task_worker_count, "1");
// The count of thread to delete.
CONF_Int32(delete_worker_count_normal_priority, "2");
// The count of thread to high priority delete.
CONF_Int32(delete_worker_count_high_priority, "1");
// The count of thread to alter table.
CONF_mInt32(alter_tablet_worker_count, "3");
// The count of parallel clone task per storage path
CONF_mInt32(parallel_clone_task_per_path, "8");
// The count of thread to clone. Deprecated
CONF_Int32(clone_worker_count, "3");
// The count of thread to clone.
CONF_Int32(storage_medium_migrate_count, "3");
// The count of thread to check consistency.
CONF_Int32(check_consistency_worker_count, "1");
// The count of thread to upload.
CONF_Int32(upload_worker_count, "1");
// The count of thread to download.
CONF_Int32(download_worker_count, "1");
// The count of thread to make snapshot.
CONF_Int32(make_snapshot_worker_count, "5");
// The count of thread to release snapshot.
CONF_Int32(release_snapshot_worker_count, "5");
// The interval time(seconds) for agent report tasks signatrue to FE.
CONF_mInt32(report_task_interval_seconds, "10");
// The interval time(seconds) for agent report disk state to FE.
CONF_mInt32(report_disk_state_interval_seconds, "60");
// The interval time(seconds) for agent report olap table to FE.
CONF_mInt32(report_tablet_interval_seconds, "60");
// The interval time(seconds) for agent report workgroup to FE.
CONF_mInt32(report_workgroup_interval_seconds, "5");
// The interval time (millisecond) for agent report resource usage to FE.
CONF_mInt32(report_resource_usage_interval_ms, "1000");
// The max download speed(KB/s).
CONF_mInt32(max_download_speed_kbps, "50000");
// The download low speed limit(KB/s).
CONF_mInt32(download_low_speed_limit_kbps, "50");
// The download low speed time(seconds).
CONF_mInt32(download_low_speed_time, "300");
// The sleep time for one second.
CONF_Int32(sleep_one_second, "1");
// The sleep time for five seconds.
CONF_Int32(sleep_five_seconds, "5");

// The count of thread to compact
CONF_Int32(compact_threads, "4");
CONF_Int32(compact_thread_pool_queue_size, "100");

// The log dir.
CONF_String(sys_log_dir, "${STARROCKS_HOME}/log");
// The user function dir.
CONF_String(user_function_dir, "${STARROCKS_HOME}/lib/udf");
// The sys log level, INFO, WARNING, ERROR, FATAL.
CONF_String(sys_log_level, "INFO");
// TIME-DAY, TIME-HOUR, SIZE-MB-nnn
CONF_String(sys_log_roll_mode, "SIZE-MB-1024");
// The log roll num.
CONF_Int32(sys_log_roll_num, "10");
// Verbose log.
CONF_Strings(sys_log_verbose_modules, "");
// Verbose log level.
CONF_Int32(sys_log_verbose_level, "10");
// The log buffer level.
CONF_String(log_buffer_level, "");

// Pull load task dir.
CONF_String(pull_load_task_dir, "${STARROCKS_HOME}/var/pull_load");

// The maximum number of bytes to display on the debug webserver's log page.
CONF_Int64(web_log_bytes, "1048576");
// The number of threads available to serve backend execution requests.
CONF_Int32(be_service_threads, "64");
// key=value pair of default query options for StarRocks, separated by ','
CONF_String(default_query_options, "");

// If non-zero, StarRocks will output memory usage every log_mem_usage_interval'th fragment completion.
// CONF_Int32(log_mem_usage_interval, "0");

// Controls the number of threads to run work per core.  It's common to pick 2x
// or 3x the number of cores.  This keeps the cores busy without causing excessive
// thrashing.
CONF_Int32(num_threads_per_core, "3");
// If true, compresses tuple data in Serialize.
CONF_Bool(compress_rowbatches, "true");
// Compress ratio when shuffle row_batches in network, not in storage engine.
// If ratio is less than this value, use uncompressed data instead.
CONF_mDouble(rpc_compress_ratio_threshold, "1.1");
// Serialize and deserialize each returned row batch.
CONF_Bool(serialize_batch, "false");
// Interval between profile reports; in seconds.
CONF_mInt32(status_report_interval, "5");
// Local directory to copy UDF libraries from HDFS into.
CONF_String(local_library_dir, "${UDF_RUNTIME_DIR}");
// Number of olap/external scanner thread pool size.
CONF_mInt32(scanner_thread_pool_thread_num, "48");
// Number of olap/external scanner thread pool size.
CONF_Int32(scanner_thread_pool_queue_size, "102400");
CONF_Int32(udf_thread_pool_size, "1");
// Port on which to run StarRocks test backend.
CONF_Int32(port, "20001");
// Default thrift client connect timeout(in seconds).
CONF_Int32(thrift_connect_timeout_seconds, "3");
// Broker write timeout in seconds.
CONF_Int32(broker_write_timeout_seconds, "30");
// Default thrift client retry interval (in milliseconds).
CONF_mInt64(thrift_client_retry_interval_ms, "100");
// Single read execute fragment row size.
CONF_mInt32(scanner_row_num, "16384");
// Number of max hdfs scanners.
CONF_Int32(max_hdfs_scanner_num, "50");
// Number of max scan keys.
CONF_mInt32(max_scan_key_num, "1024");
// The max number of push down values of a single column.
// if exceed, no conditions will be pushed down for that column.
CONF_mInt32(max_pushdown_conditions_per_column, "1024");
// (Advanced) Maximum size of per-query receive-side buffer.
CONF_mInt32(exchg_node_buffer_size_bytes, "10485760");
// The block_size every block allocate for sorter.
CONF_Int32(sorter_block_size, "8388608");

CONF_mInt64(column_dictionary_key_ratio_threshold, "0");
CONF_mInt64(column_dictionary_key_size_threshold, "0");
// The memory_limitation_per_thread_for_schema_change unit GB.
CONF_mInt32(memory_limitation_per_thread_for_schema_change, "2");
CONF_mDouble(memory_ratio_for_sorting_schema_change, "0.8");

CONF_mInt32(update_cache_expire_sec, "360");
CONF_mInt32(file_descriptor_cache_clean_interval, "3600");
CONF_mInt32(disk_stat_monitor_interval, "5");
CONF_mInt32(profile_report_interval, "30");
CONF_mInt32(unused_rowset_monitor_interval, "30");
CONF_String(storage_root_path, "${STARROCKS_HOME}/storage");
// BE process will exit if the percentage of error disk reach this value.
CONF_mInt32(max_percentage_of_error_disk, "0");
// CONF_Int32(default_num_rows_per_data_block, "1024");
CONF_mInt32(default_num_rows_per_column_file_block, "1024");
CONF_Int32(max_tablet_num_per_shard, "1024");
// pending data policy
CONF_mInt32(pending_data_expire_time_sec, "1800");
// inc_rowset expired interval
CONF_mInt32(inc_rowset_expired_sec, "1800");
// inc_rowset snapshot rs sweep time interval
CONF_mInt32(tablet_rowset_stale_sweep_time_sec, "1800");
// garbage sweep policy
CONF_mInt32(max_garbage_sweep_interval, "3600");
CONF_mInt32(min_garbage_sweep_interval, "180");
CONF_mInt32(snapshot_expire_time_sec, "172800");
CONF_mInt32(trash_file_expire_time_sec, "259200");
//file descriptors cache, by default, cache 16384 descriptors
CONF_Int32(file_descriptor_cache_capacity, "16384");
// minimum file descriptor number
// modify them upon necessity
CONF_Int32(min_file_descriptor_number, "60000");
CONF_Int64(index_stream_cache_capacity, "10737418240");
// CONF_Int64(max_packed_row_block_size, "20971520");

// Cache for storage page size
CONF_mString(storage_page_cache_limit, "20%");
// whether to disable page cache feature in storage
CONF_mBool(disable_storage_page_cache, "false");
// whether to enable the bitmap index memory cache
CONF_mBool(enable_bitmap_index_memory_page_cache, "false");
// whether to enable the zonemap index memory cache
CONF_mBool(enable_zonemap_index_memory_page_cache, "false");
// whether to enable the ordinal index memory cache
CONF_mBool(enable_ordinal_index_memory_page_cache, "false");
// whether to disable column pool
CONF_Bool(disable_column_pool, "false");

CONF_mInt32(base_compaction_check_interval_seconds, "60");
CONF_mInt64(min_base_compaction_num_singleton_deltas, "5");
CONF_mInt64(max_base_compaction_num_singleton_deltas, "100");
// This config is to limit the max concurrency of running base compaction tasks.
// -1 means no limit if enable event_based_compaction_framework, and the max concurrency will be:
CONF_Int32(base_compaction_num_threads_per_disk, "1");
CONF_mDouble(base_cumulative_delta_ratio, "0.3");
CONF_mInt64(base_compaction_interval_seconds_since_last_operation, "86400");

// cumulative compaction policy: max delta file's size unit:B
CONF_mInt32(cumulative_compaction_check_interval_seconds, "1");
CONF_mInt64(min_cumulative_compaction_num_singleton_deltas, "5");
CONF_mInt64(max_cumulative_compaction_num_singleton_deltas, "1000");
// This config is to limit the max concurrency of running cumulative compaction tasks.
// -1 means no limit if enable event_based_compaction_framework, and the max concurrency will be:
CONF_Int32(cumulative_compaction_num_threads_per_disk, "1");
// CONF_Int32(cumulative_compaction_write_mbytes_per_sec, "100");

CONF_mInt32(update_compaction_check_interval_seconds, "60");
CONF_mInt32(update_compaction_num_threads_per_disk, "1");
CONF_Int32(update_compaction_per_tablet_min_interval_seconds, "120"); // 2min
CONF_mInt64(max_update_compaction_num_singleton_deltas, "1000");
CONF_mInt64(update_compaction_size_threshold, "268435456");
CONF_mInt64(update_compaction_result_bytes, "1073741824");

CONF_mInt32(repair_compaction_interval_seconds, "600"); // 10 min
CONF_Int32(manual_compaction_threads, "4");

// if compaction of a tablet failed, this tablet should not be chosen to
// compaction until this interval passes.
CONF_mInt64(min_compaction_failure_interval_sec, "120"); // 2 min

CONF_mInt64(min_cumulative_compaction_failure_interval_sec, "30"); // 30s

// Too many compaction tasks may run out of memory.
// This config is to limit the max concurrency of running compaction tasks.
// -1 means no limit, and the max concurrency will be:
//      C = (cumulative_compaction_num_threads_per_disk + base_compaction_num_threads_per_disk) * dir_num
// set it to larger than C will be set to equal to C.
// This config can be set to 0, which means to forbid any compaction, for some special cases.
CONF_mInt32(max_compaction_concurrency, "-1");

// Threshold to logging compaction trace, in seconds.
CONF_mInt32(compaction_trace_threshold, "60");

// If enabled, will verify compaction/schema-change output rowset correctness
CONF_mBool(enable_rowset_verify, "false");

// Max columns of each compaction group.
// If the number of schema columns is greater than this,
// the columns will be divided into groups for vertical compaction.
CONF_Int64(vertical_compaction_max_columns_per_group, "5");

CONF_Bool(enable_event_based_compaction_framework, "true");

CONF_Bool(enable_size_tiered_compaction_strategy, "true");
CONF_mInt64(size_tiered_min_level_size, "131072");
CONF_mInt64(size_tiered_level_multiple, "5");
CONF_mInt64(size_tiered_level_multiple_dupkey, "10");
CONF_mInt64(size_tiered_level_num, "7");

CONF_Bool(enable_check_string_lengths, "true");
// 5GB
CONF_mInt64(min_cumulative_compaction_size, "5368709120");
// 20GB
CONF_mInt64(min_base_compaction_size, "21474836480");

// Max row source mask memory bytes, default is 200M.
// Should be smaller than compaction_mem_limit.
// When the row source mask buffer exceeds this, it will be persisted to a temporary file on the disk.
CONF_Int64(max_row_source_mask_memory_bytes, "209715200");

// Port to start debug http server in BE
CONF_Int32(be_http_port, "8040");
// Number of http workers in BE
CONF_Int32(be_http_num_workers, "48");
// Period to update rate counters and sampling counters in ms.
CONF_mInt32(periodic_counter_update_period_ms, "500");

// Used for mini Load. mini load data file will be removed after this time.
CONF_Int64(load_data_reserve_hours, "4");
// log error log will be removed after this time
CONF_mInt64(load_error_log_reserve_hours, "48");
CONF_Int32(number_tablet_writer_threads, "16");
CONF_mInt64(max_queueing_memtable_per_tablet, "2");

// delta writer hang after this time, be will exit since storage is in error state
CONF_Int32(be_exit_after_disk_write_hang_second, "60");

// Automatically detect whether a char/varchar column to use dictionary encoding
// If the number of keys in a dictionary is greater than this fraction of the total number of rows
// turn off dictionary dictionary encoding. This only will detect first chunk
// set to 1 means always use dictionary encoding
CONF_Double(dictionary_encoding_ratio, "0.7");
// The minimum chunk size for dictionary encoding speculation
CONF_Int32(dictionary_speculate_min_chunk_size, "10000");

// Whether to use special thread pool for streaming load to avoid deadlock for
// concurrent streaming loads. The maximum number of threads and queue size are
// set INT32_MAX which indicate there is no limit for the thread pool. Note you
// don't need to change these configurations in general.
CONF_mBool(enable_streaming_load_thread_pool, "true");
CONF_Int32(streaming_load_thread_pool_num_min, "0");
CONF_Int32(streaming_load_thread_pool_idle_time_ms, "2000");

// The maximum amount of data that can be processed by a stream load
CONF_mInt64(streaming_load_max_mb, "102400");
// Some data formats, such as JSON, cannot be streamed.
// Therefore, it is necessary to limit the maximum number of
// such data when using stream load to prevent excessive memory consumption.
CONF_mInt64(streaming_load_max_batch_size_mb, "100");
// The alive time of a TabletsChannel.
// If the channel does not receive any data till this time,
// the channel will be removed.
CONF_Int32(streaming_load_rpc_max_alive_time_sec, "1200");
// The timeout of a rpc to open the tablet writer in remote BE.
// short operation time, can set a short timeout
CONF_Int32(tablet_writer_open_rpc_timeout_sec, "60");
// make_snapshot rpc timeout
CONF_Int32(make_snapshot_rpc_timeout_ms, "20000");
// Deprecated, use query_timeout instread
// the timeout of a rpc to process one batch in tablet writer.
// you may need to increase this timeout if using larger 'streaming_load_max_mb',
// or encounter 'tablet writer write failed' error when loading.
// CONF_Int32(tablet_writer_rpc_timeout_sec, "600");
// OlapTableSink sender's send interval, should be less than the real response time of a tablet writer rpc.
CONF_mInt32(olap_table_sink_send_interval_ms, "10");

// Fragment thread pool
CONF_Int32(fragment_pool_thread_num_min, "64");
CONF_Int32(fragment_pool_thread_num_max, "4096");
CONF_Int32(fragment_pool_queue_size, "2048");

// Spill to disk when query
// Writable scratch directories, splitted by ";"
CONF_String(query_scratch_dirs, "${STARROCKS_HOME}");

// For each io buffer size, the maximum number of buffers the IoMgr will hold onto
// With 1024B through 8MB buffers, this is up to ~2GB of buffers.
CONF_Int32(max_free_io_buffers, "128");

CONF_Bool(disable_mem_pools, "false");

// Whether to allocate chunk using mmap. If you enable this, you'd better to
// increase vm.max_map_count's value whose default value is 65530.
// you can do it as root via "sysctl -w vm.max_map_count=262144" or
// "echo 262144 > /proc/sys/vm/max_map_count"
// NOTE: When this is set to true, you must set chunk_reserved_bytes_limit
// to a relative large number or the performace is very very bad.
CONF_Bool(use_mmap_allocate_chunk, "false");

// Chunk Allocator's reserved bytes limit,
// Default value is 2GB, increase this variable can improve performance, but will
// acquire more free memory which can not be used by other modules
CONF_Int64(chunk_reserved_bytes_limit, "2147483648");

// for pprof
CONF_String(pprof_profile_dir, "${STARROCKS_HOME}/log");

// to forward compatibility, will be removed later
CONF_mBool(enable_token_check, "true");

// to open/close system metrics
CONF_Bool(enable_system_metrics, "true");

CONF_mBool(enable_prefetch, "true");

// Number of cores StarRocks will used, this will effect only when it's greater than 0.
// Otherwise, StarRocks will use all cores returned from "/proc/cpuinfo".
CONF_Int32(num_cores, "0");

// When BE start, If there is a broken disk, BE process will exit by default.
// Otherwise, we will ignore the broken disk,
CONF_Bool(ignore_broken_disk, "false");

// Writable scratch directories
CONF_String(scratch_dirs, "/tmp");

// If false and --scratch_dirs contains multiple directories on the same device,
// then only the first writable directory is used
// CONF_Bool(allow_multiple_scratch_dirs_per_device, "false");

// Linux transparent huge page.
CONF_Bool(madvise_huge_pages, "false");

// Whether use mmap to allocate memory.
CONF_Bool(mmap_buffers, "false");

// Sleep time in seconds between memory maintenance iterations
CONF_mInt64(memory_maintenance_sleep_time_s, "10");

// Aligement
CONF_Int32(memory_max_alignment, "16");

// write buffer size before flush
CONF_mInt64(write_buffer_size, "104857600");

// Following 2 configs limit the memory consumption of load process on a Backend.
// eg: memory limit to 80% of mem limit config but up to 100GB(default)
// NOTICE(cmy): set these default values very large because we don't want to
// impact the load performace when user upgrading StarRocks.
// user should set these configs properly if necessary.
CONF_Int32(query_max_memory_limit_percent, "90");
CONF_Int64(load_process_max_memory_limit_bytes, "107374182400"); // 100GB
CONF_Int32(load_process_max_memory_limit_percent, "30");         // 30%
CONF_mBool(enable_new_load_on_memory_limit_exceeded, "true");
CONF_Int64(compaction_max_memory_limit, "-1");
CONF_Int32(compaction_max_memory_limit_percent, "100");
CONF_Int64(compaction_memory_limit_per_worker, "2147483648"); // 2GB
CONF_String(consistency_max_memory_limit, "10G");
CONF_Int32(consistency_max_memory_limit_percent, "20");
CONF_Int32(update_memory_limit_percent, "60");

// Update interval of tablet stat cache.
CONF_mInt32(tablet_stat_cache_update_interval_second, "300");

// Result buffer cancelled time (unit: second).
CONF_mInt32(result_buffer_cancelled_interval_time, "300");

// The increased frequency of priority for remaining tasks in BlockingPriorityQueue.
CONF_mInt32(priority_queue_remaining_tasks_increased_frequency, "512");

// Sync tablet_meta when modifing meta.
CONF_mBool(sync_tablet_meta, "false");

// Default thrift rpc timeout ms.
CONF_mInt32(thrift_rpc_timeout_ms, "5000");

CONF_Bool(thrift_rpc_strict_mode, "true");
// rpc max string body size. 0 means unlimited
CONF_Int32(thrift_rpc_max_body_size, "0");

// txn commit rpc timeout
CONF_mInt32(txn_commit_rpc_timeout_ms, "60000");

// If set to true, metric calculator will run
CONF_Bool(enable_metric_calculator, "true");

// Max consumer num in one data consumer group, for routine load.
CONF_mInt32(max_consumer_num_per_group, "3");

// Max pulsar consumer num in one data consumer group, for routine load.
CONF_mInt32(max_pulsar_consumer_num_per_group, "10");

// kafka request timeout
CONF_Int32(routine_load_kafka_timeout_second, "10");

// pulsar request timeout
CONF_Int32(routine_load_pulsar_timeout_second, "10");

// Is set to true, index loading failure will not cause BE exit,
// and the tablet will be marked as bad, so that FE will try to repair it.
// CONF_Bool(auto_recover_index_loading_failure, "false");

// Max external scan cache batch count, means cache max_memory_cache_batch_count * batch_size row
// default is 20, batch_size's defualt value is 1024 means 20 * 1024 rows will be cached
CONF_mInt32(max_memory_sink_batch_count, "20");

// This configuration is used for the context gc thread schedule period
// note: unit is minute, default is 5min
CONF_mInt32(scan_context_gc_interval_min, "5");

// es scroll keep-alive.
CONF_String(es_scroll_keepalive, "5m");

// HTTP connection timeout for es.
CONF_Int32(es_http_timeout_ms, "5000");

// es index max result window, and this value affects batch size.
// if request batch size exceeds this value, ES will return bad request(400)
// https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules.html
CONF_Int32(es_index_max_result_window, "10000");

// The max client cache number per each host.
// There are variety of client cache in BE, but currently we use the
// same cache size configuration.
// TODO(cmy): use different config to set different client cache if necessary.
CONF_Int32(max_client_cache_size_per_host, "10");

// Dir to save files downloaded by SmallFileMgr
CONF_String(small_file_dir, "${STARROCKS_HOME}/lib/small_file/");
// path gc
CONF_Bool(path_gc_check, "true");
CONF_Int32(path_gc_check_interval_second, "86400");
CONF_mInt32(path_gc_check_step, "1000");
CONF_mInt32(path_gc_check_step_interval_ms, "10");
CONF_mInt32(path_scan_interval_second, "86400");

// The following 2 configs limit the max usage of disk capacity of a data dir.
// If both of these 2 threshold reached, no more data can be writen into that data dir.
// The percent of max used capacity of a data dir
CONF_mInt32(storage_flood_stage_usage_percent, "95"); // 95%
// The min bytes that should be left of a data dir
CONF_mInt64(storage_flood_stage_left_capacity_bytes, "107374182400"); // 100GB
// When choosing storage root path for tablet creation, disks with usage larger than the
// average value by `storage_high_usage_disk_protect_ratio` won't be chosen at first.
CONF_mDouble(storage_high_usage_disk_protect_ratio, "0.1"); // 10%

// Number of thread for flushing memtable per store.
CONF_mInt32(flush_thread_num_per_store, "2");

// Config for tablet meta checkpoint.
CONF_mInt32(tablet_meta_checkpoint_min_new_rowsets_num, "10");
CONF_mInt32(tablet_meta_checkpoint_min_interval_secs, "600");

// Maximum size of a single message body in all protocols.
CONF_Int64(brpc_max_body_size, "2147483648");
// Max unwritten bytes in each socket, if the limit is reached, Socket.Write fails with EOVERCROWDED.
CONF_Int64(brpc_socket_max_unwritten_bytes, "1073741824");

// Max number of txns for every txn_partition_map in txn manager.
// this is a self-protection to avoid too many txns saving in manager.
CONF_mInt64(max_runnings_transactions_per_txn_map, "100");

// The tablet map shard size, the value must be power of two.
// this is an enhancement for better performance to manage tablet.
CONF_Int32(tablet_map_shard_size, "32");

CONF_String(plugin_path, "${STARROCKS_HOME}/plugin");

// txn_map_lock shard size, the value is 2^n, n=0,1,2,3,4
// this is an enhancement for better performance to manage txn.
CONF_Int32(txn_map_shard_size, "128");

// txn_lock shard size, the value is 2^n, n=0,1,2,3,4
// this is an enhancement for better performance to commit and publish txn.
CONF_Int32(txn_shard_size, "1024");

// Whether to continue to start be when load tablet from header failed.
CONF_Bool(ignore_load_tablet_failure, "false");

// Whether to continue to start be when load tablet from header failed.
CONF_Bool(ignore_rowset_stale_unconsistent_delete, "false");

// The chunk size for vector query engine
CONF_Int32(vector_chunk_size, "4096");

// Valid range: [0-1000].
// `0` will disable late materialization.
// `1000` will enable late materialization always.
CONF_Int32(late_materialization_ratio, "10");

// Valid range: [0-1000].
// `0` will disable late materialization select metric type.
// `1000` will enable late materialization always select metric type.
CONF_Int32(metric_late_materialization_ratio, "1000");

// Max batched bytes for each transmit request. (256KB)
CONF_Int64(max_transmit_batched_bytes, "262144");

CONF_Int16(bitmap_max_filter_items, "30");

// The bitmap max filter ratio, valid value range is: [0-1000].
CONF_Int16(bitmap_max_filter_ratio, "1");

CONF_Bool(bitmap_filter_enable_not_equal, "false");

// Only 1 and 2 is valid.
// When storage_format_version is 1, use origin storage format for Date, Datetime and Decimal
// type.
// When storage_format_version is 2, DATE_V2, TIMESTAMP and DECIMAL_V2 will be used as
// storage format.
CONF_mInt16(storage_format_version, "2");

// IMPORTANT NOTE: changing this config to 1 must require all BEs to be upgraded to new version,
// which support this config.
// DO NOT change this config unless you known how.
// 0 for BITSHUFFLE_NULL
// 1 for LZ4_NULL
CONF_mInt16(null_encoding, "0");

// Do pre-aggregate if effect greater than the factor, factor range:[1-100].
CONF_Int16(pre_aggregate_factor, "80");

#ifdef __x86_64__
// Enable genearate minidump for crash.
CONF_Bool(sys_minidump_enable, "false");

// The minidump dir(generated by google_breakpad).
CONF_String(sys_minidump_dir, "${STARROCKS_HOME}");

// The max minidump files number could exist.
CONF_mInt32(sys_minidump_max_files, "16");

// The max minidump file size could exist.
CONF_mInt32(sys_minidump_limit, "20480");

// Interval(seconds) for cleaning old minidumps.
CONF_mInt32(sys_minidump_interval, "600");
#endif
// dump trace info such as query-id and some runtime state
CONF_Bool(dump_trace_info, "true");
// The maximum number of version per tablet. If the
// number of version exceeds this value, new write
// requests will fail.
CONF_mInt16(tablet_max_versions, "1000");

// The maximum number of pending versions allowed for a primary key tablet
CONF_mInt32(tablet_max_pending_versions, "1000");

// NOTE: it will be deleted.
CONF_mBool(enable_bitmap_union_disk_format_with_set, "false");

// The number of scan threads pipeline engine.
CONF_Int64(pipeline_scan_thread_pool_thread_num, "0");
CONF_Double(pipeline_connector_scan_thread_num_per_cpu, "8");
// Queue size of scan thread pool for pipeline engine.
CONF_Int64(pipeline_scan_thread_pool_queue_size, "102400");
// The number of execution threads for pipeline engine.
CONF_Int64(pipeline_exec_thread_pool_thread_num, "0");
// The number of threads for preparing fragment instances in pipeline engine, vCPUs by default.
// *  "n": positive integer, fixed number of threads to n.
// *  "0": default value, means the same as number of cpu cores.
// * "-n": negative integer, means n times of number of cpu cores.
CONF_Int64(pipeline_prepare_thread_pool_thread_num, "0");
CONF_Int64(pipeline_prepare_thread_pool_queue_size, "102400");
// The number of threads for executing sink io task in pipeline engine, vCPUs by default.
CONF_Int64(pipeline_sink_io_thread_pool_thread_num, "0");
CONF_Int64(pipeline_sink_io_thread_pool_queue_size, "102400");
// The buffer size of SinkBuffer.
CONF_Int64(pipeline_sink_buffer_size, "64");
// The degree of parallelism of brpc.
CONF_Int64(pipeline_sink_brpc_dop, "64");
// Used to reject coming fragment instances, when the number of running drivers
// exceeds it*pipeline_exec_thread_pool_thread_num.
CONF_Int64(pipeline_max_num_drivers_per_exec_thread, "10240");
CONF_mBool(pipeline_print_profile, "false");

// The arguments of multilevel feedback pipeline_driver_queue. It prioritizes small queries over larger ones,
// when the value of level_time_slice_base_ns is smaller and queue_ratio_of_adjacent_queue is larger.
CONF_Int64(pipeline_driver_queue_level_time_slice_base_ns, "200000000");
CONF_Double(pipeline_driver_queue_ratio_of_adjacent_queue, "1.2");
// 0 represents PriorityScanTaskQueue (by default), while 1 represents MultiLevelFeedScanTaskQueue.
// - PriorityScanTaskQueue prioritizes scan tasks with lower committed times.
// - MultiLevelFeedScanTaskQueue prioritizes scan tasks with shorter execution time.
//   It is advisable to use MultiLevelFeedScanTaskQueue when scan tasks from large queries may impact those from small queries.
CONF_Int64(pipeline_scan_queue_mode, "0");
// The arguments of MultiLevelFeedScanTaskQueue. It prioritizes small queries over larger ones,
// when the value of level_time_slice_base_ns is smaller and queue_ratio_of_adjacent_queue is larger.
CONF_Int64(pipeline_scan_queue_level_time_slice_base_ns, "100000000");
CONF_Double(pipeline_scan_queue_ratio_of_adjacent_queue, "1.5");

/// For parallel scan on the single tablet.
// These three configs are used to calculate the minimum number of rows picked up from a segment at one time.
// It is `splitted_scan_bytes/scan_row_bytes` and restricted in the range [min_splitted_scan_rows, max_splitted_scan_rows].
CONF_mInt64(tablet_internal_parallel_min_splitted_scan_rows, "16384");
// Default is 16384*64, where 16384 is the chunk size in pipeline.
CONF_mInt64(tablet_internal_parallel_max_splitted_scan_rows, "1048576");
// Default is 512MB.
CONF_mInt64(tablet_internal_parallel_max_splitted_scan_bytes, "536870912");
//
// Only when scan_dop is not less than min_scan_dop, this table can use tablet internal parallel,
// where scan_dop = estimated_scan_rows / splitted_scan_rows.
CONF_mInt64(tablet_internal_parallel_min_scan_dop, "4");

// The bitmap serialize version.
CONF_Int16(bitmap_serialize_version, "1");
// The max hdfs file handle.
CONF_mInt32(max_hdfs_file_handle, "1000");

CONF_Int64(max_segment_file_size, "1073741824");

// Rewrite partial semgent or not.
// if true, partial segment will be rewrite into new segment file first and append other column data
// if false, the data of other column will be append into partial segment file and rebuild segment footer
// we may need the both implementations for perf test for now, so use it to decide which implementations to use
// default: true
CONF_Bool(rewrite_partial_segment, "true");

// Properties to access object storage
CONF_String(object_storage_access_key_id, "");
CONF_String(object_storage_secret_access_key, "");
CONF_String(object_storage_endpoint, "");
CONF_String(object_storage_bucket, "");
// Tencent cos needs to add region information
CONF_String(object_storage_region, "");
CONF_Int64(object_storage_max_connection, "102400");
// Acccess object storage using https.
// this options is applicable only if `object_storage_endpoint` is not specified.
CONF_Bool(object_storage_endpoint_use_https, "false");
// https://github.com/aws/aws-sdk-cpp/issues/587
// https://hadoop.apache.org/docs/current2/hadoop-aws/tools/hadoop-aws/index.html
CONF_Bool(object_storage_endpoint_path_style_access, "false");
// Socket connect timeout for object storage.
// Default is -1, indicate to use the default value in sdk (1000ms)
// Unless you are very far away from your the data center you are talking to, 1000ms is more than sufficient.
CONF_Int64(object_storage_connect_timeout_ms, "-1");
// Request timeout for object storage
// Default is -1, indicate to use the default value in sdk.
// For Curl, it's the low speed time, which contains the time in number milliseconds that transfer speed should be
// below "lowSpeedLimit" for the library to consider it too slow and abort.
// Note that for Curl this config is converted to seconds by rounding down to the nearest whole second except when the
// value is greater than 0 and less than 1000.
// When it's 0, low speed limit check will be disabled.
CONF_Int64(object_storage_request_timeout_ms, "-1");

// orc reader
CONF_Bool(enable_orc_late_materialization, "true");
CONF_Int32(orc_row_index_cache_max_size, "1048576");
CONF_Int32(orc_stripe_cache_max_size, "8388608");
CONF_Bool(enable_orc_libdeflate_decompression, "true");
CONF_Int32(orc_file_cache_max_size, "8388608");
CONF_Int32(orc_natural_read_size, "8388608");
CONF_mBool(orc_coalesce_read_enable, "true");

// parquet reader
CONF_mBool(parquet_coalesce_read_enable, "true");
CONF_Bool(parquet_late_materialization_enable, "true");
CONF_Bool(parquet_late_materialization_v2_enable, "true");

CONF_Int32(io_coalesce_read_max_buffer_size, "8388608");
CONF_Int32(io_coalesce_read_max_distance_size, "1048576");
CONF_mBool(io_coalesce_adaptive_lazy_active, "true");
CONF_Int32(io_tasks_per_scan_operator, "4");
CONF_Int32(connector_io_tasks_per_scan_operator, "16");
CONF_Int32(connector_io_tasks_min_size, "2");
CONF_Int32(connector_io_tasks_adjust_interval_ms, "50");
CONF_Int32(connector_io_tasks_adjust_step, "1");
CONF_Int32(connector_io_tasks_adjust_smooth, "4");
CONF_Int32(connector_io_tasks_slow_io_latency_ms, "50");
CONF_mDouble(scan_use_query_mem_ratio, "0.25");
CONF_Double(connector_scan_use_query_mem_ratio, "0.3");

// hdfs hedged read
CONF_Bool(hdfs_client_enable_hedged_read, "false");
// dfs.client.hedged.read.threadpool.size
CONF_Int32(hdfs_client_hedged_read_threadpool_size, "128");
// dfs.client.hedged.read.threshold.millis
CONF_Int32(hdfs_client_hedged_read_threshold_millis, "2500");
CONF_Int32(hdfs_client_max_cache_size, "8");
CONF_Int32(hdfs_client_io_read_retry, "0");

// Enable output trace logs in aws-sdk-cpp for diagnosis purpose.
// Once logging is enabled in your application, the SDK will generate log files in your current working directory
// following the default naming pattern of aws_sdk_<date>.log.
// The log file generated by the prefix-naming option rolls over once per hour to allow for archiving or deleting log files.
// https://docs.aws.amazon.com/zh_cn/sdk-for-cpp/v1/developer-guide/logging.html
CONF_mBool(aws_sdk_logging_trace_enabled, "false");
CONF_String(aws_sdk_logging_trace_level, "trace");

// Enable RFC-3986 encoding.
// When Querying data on Google Cloud Storage, if the objects key contain special characters like '=', '$', it will fail
// to Authenticate because the request URL does not translate these special characters.
// This is critical for Hive partitioned tables. The object key usually contains '=' like 'dt=20230101'.
// Enabling RFC-3986 encoding will make sure these characters are properly encoded.
CONF_mBool(aws_sdk_enable_compliant_rfc3986_encoding, "false");

// default: 16MB
CONF_mInt64(experimental_s3_max_single_part_size, "16777216");
// default: 16MB
CONF_mInt64(experimental_s3_min_upload_part_size, "16777216");

CONF_Int64(max_load_dop, "16");

CONF_Bool(enable_load_colocate_mv, "true");

CONF_Int64(meta_threshold_to_manual_compact, "10737418240"); // 10G
CONF_Bool(manual_compact_before_data_dir_load, "false");

// size of grf generated by broadcast join below this limit, multiple rf copy will be delivered in passthrough
// style, otherwise, rf will be relayed by other be.
CONF_Int64(deliver_broadcast_rf_passthrough_bytes_limit, "131072");
// in passthrough style, the number of inflight RPCs of parallel deliveries are issued is not exceeds this limit.
CONF_Int64(deliver_broadcast_rf_passthrough_inflight_num, "10");
CONF_Int64(send_rpc_runtime_filter_timeout_ms, "1000");

CONF_Int32(max_batch_publish_latency_ms, "100");

// Config for opentelemetry tracing.
CONF_String(jaeger_endpoint, "");

// Config for query debug trace
CONF_String(query_debug_trace_dir, "${STARROCKS_HOME}/query_debug_trace");

#ifdef USE_STAROS
CONF_Int32(starlet_port, "9070");
CONF_Int32(starlet_cache_thread_num, "64");
// Root dir used for cache if cache enabled.
CONF_String(starlet_cache_dir, "");
// Cache backend check interval (in seconds), for async write sync check and ttl clean, e.t.c.
CONF_Int32(starlet_cache_check_interval, "900");
// Cache backend cache evictor interval (in seconds)
CONF_Int32(starlet_cache_evict_interval, "60");
// Cache will start evict cache files if free space belows this value(percentage)
CONF_Double(starlet_cache_evict_low_water, "0.1");
// Cache will stop evict cache files if free space is above this value(percentage)
CONF_Double(starlet_cache_evict_high_water, "0.2");
// type:Integer. cache directory allocation policy. (0:default, 1:random, 2:round-robin)
CONF_Int32(starlet_cache_dir_allocate_policy, "0");
// Buffer size in starlet fs buffer stream, size <= 0 means not use buffer stream.
// Only support in S3/HDFS currently.
CONF_Int32(starlet_fs_stream_buffer_size_bytes, "131072");
// TODO: support runtime change
CONF_Bool(starlet_use_star_cache, "false");
CONF_Int32(starlet_star_cache_mem_size_percent, "0");
CONF_Int32(starlet_star_cache_disk_size_percent, "60");
CONF_Int64(starlet_star_cache_disk_size_bytes, "0");
CONF_Int32(starlet_star_cache_block_size_bytes, "1048576");
// domain list separated by comma, e.g. '.example.com,.helloworld.com'
CONF_String(starlet_s3_virtual_address_domainlist, "");
// number of caches allowed from s3client factory
CONF_Int32(starlet_s3_client_max_cache_capacity, "8");
// number of instances per cache item
CONF_Int32(starlet_s3_client_num_instances_per_cache, "1");
// whether turn on read prefetch feature
CONF_Bool(starlet_fs_read_prefetch_enable, "false");
// prefetch threadpool size
CONF_Int32(starlet_fs_read_prefetch_threadpool_size, "128");
#endif

CONF_mInt64(lake_metadata_cache_limit, /*2GB=*/"2147483648");
CONF_mBool(lake_print_delete_log, "true");
CONF_mBool(lake_compaction_check_txn_log_first, "false");
// Used to ensure service availability in extreme situations by sacrificing a certain degree of correctness
CONF_mBool(experimental_lake_ignore_lost_segment, "false");
CONF_mInt64(experimental_lake_wait_per_put_ms, "0");
CONF_mInt64(experimental_lake_wait_per_get_ms, "0");
CONF_mInt64(experimental_lake_wait_per_delete_ms, "0");
CONF_mInt64(lake_publish_version_slow_log_ms, "1000");
CONF_mBool(lake_enable_publish_version_trace_log, "false");

CONF_mBool(dependency_librdkafka_debug_enable, "false");

// A comma-separated list of debug contexts to enable.
// Producer debug context: broker, topic, msg
// Consumer debug context: consumer, cgrp, topic, fetch
// Other debug context: generic, metadata, feature, queue, protocol, security, interceptor, plugin
// admin, eos, mock, assigner, conf
CONF_String(dependency_librdkafka_debug, "all");

// max loop count when be waiting its fragments finish
CONF_Int64(loop_count_wait_fragments_finish, "0");

// the maximum number of connections in the connection pool for a single jdbc url
CONF_Int16(jdbc_connection_pool_size, "8");
// the minimum number of idle connections that connection pool tries to maintain.
// if the idle connections dip below this value and the total connections in the pool are less than jdbc_connection_pool_size,
// the connection pool will make a best effort to add additional connections quickly.
CONF_Int16(jdbc_minimum_idle_connections, "1");
// the maximum amount of time that a connection is allowed to sit idle in the pool.
// this setting only applies when jdbc_minimum_idle_connections is less than jdbc_connection_pool_size.
// The minimum allowed value is 10000(10 seconds).
CONF_Int32(jdbc_connection_idle_timeout_ms, "600000");

// spill dirs
CONF_String(spill_local_storage_dir, "${STARROCKS_HOME}/spill");
// when spill occurs, whether enable skip synchronous flush
CONF_mBool(experimental_spill_skip_sync, "true");
// spill Initial number of partitions
CONF_mInt32(spill_init_partition, "16");
// The maximum size of a single log block container file, this is not a hard limit.
// If the file size exceeds this limit, a new file will be created to store the block.
CONF_Int64(spill_max_log_block_container_bytes, "10737418240"); // 10GB
// The maximum size of a single spill directory, for some case the spill directory may
// be the same with storage path. Spill will return with error when used size has exceeded
// the limit.
CONF_mDouble(spill_max_dir_bytes_ratio, "0.8"); // 80%

CONF_Int32(internal_service_query_rpc_thread_num, "-1");

/*
 * When compile with ENABLE_STATUS_FAILED, every use of RETURN_INJECT has probability of 1/cardinality_of_inject
 * to inject error through return random status(except ok).
 */
CONF_Int32(cardinality_of_inject, "10");

/*
 * Config range for inject erros,
 * Specify the source code directory,
 * Split by "," strictly.
 */
CONF_String(directory_of_inject,
            "/src/exec/pipeline/hashjoin,/src/exec/pipeline/scan,/src/exec/pipeline/aggregate,/src/exec/pipeline/"
            "crossjoin,/src/exec/pipeline/sort,/src/exec/pipeline/exchange,/src/exec/pipeline/analysis");

// Used by to_base64
CONF_Int64(max_length_for_to_base64, "200000");
// Used by bitmap functions
CONF_Int64(max_length_for_bitmap_function, "1000000");

CONF_Bool(block_cache_enable, "false");
CONF_Int64(block_cache_disk_size, "0");
CONF_String(block_cache_disk_path, "${STARROCKS_HOME}/block_cache/");
CONF_String(block_cache_meta_path, "${STARROCKS_HOME}/block_cache/");
CONF_Int64(block_cache_block_size, "262144");   // 256K
CONF_Int64(block_cache_mem_size, "2147483648"); // 2GB
CONF_Bool(block_cache_checksum_enable, "false");
// Maximum number of concurrent inserts we allow globally for block cache.
// 0 means unlimited.
CONF_Int64(block_cache_max_concurrent_inserts, "1500000");
// Total memory limit for in-flight parcels.
// Once this is reached, requests will be rejected until the parcel memory usage gets under the limit.
CONF_Int64(block_cache_max_parcel_memory_mb, "256");
CONF_Bool(block_cache_report_stats, "false");
// This essentially turns the LRU into a two-segmented LRU. Setting this to 1 means every new insertion
// will be inserted 1/2 from the end of the LRU, 2 means 1/4 from the end of the LRU, and so on.
// It is only useful for the cachelib engine currently.
CONF_Int64(block_cache_lru_insertion_point, "1");
// Block cache engines, alternatives: cachelib, starcache.
// Set the default value empty to indicate whether it is manully configured by users.
// If not, we need to adjust the default engine based on build switches like "WITH_CACHELIB" and "WITH_STARCACHE".
CONF_String(block_cache_engine, "");
CONF_Bool(block_cache_direct_io_enable, "false");

CONF_mInt64(l0_l1_merge_ratio, "10");
CONF_mInt64(l0_max_file_size, "209715200"); // 200MB
CONF_mInt64(l0_min_mem_usage, "2097152");   // 2MB
CONF_mInt64(l0_max_mem_usage, "104857600"); // 100MB
// if l0_mem_size exceeds this value, l0 need snapshot
CONF_mInt64(l0_snapshot_size, "16777216"); // 16MB
CONF_mInt64(max_tmp_l1_num, "10");
CONF_mBool(enable_parallel_get_and_bf, "true");
// Control if using the minor compaction strategy
CONF_Bool(enable_pindex_minor_compaction, "true");
// if l2 num is larger than this, stop doing async compaction,
// add this config to prevent l2 grow too large.
CONF_mInt64(max_allow_pindex_l2_num, "5");
// control the background compaction threads
CONF_mInt64(pindex_major_compaction_num_threads, "0");
// control the persistent index schedule compaction interval
CONF_mInt64(pindex_major_compaction_schedule_interval_seconds, "15");
// enable use bloom filter for pindex or not
CONF_mBool(enable_pindex_filter, "true");
// use bloom filter in pindex can reduce disk io, but in the following scenarios, we should skip the bloom filter
// 1. The records to be found are in the index, bloom filter is no usage
// 2. The records to be found is very small but bloom filter is very large, read bloom filter may cost a lot of disk io
// So the bloom filter bytes should less than the index data we need to scan in disk, and the default strategy is if bloom
// filter bytes is less or equal than 10% of pindex bytes, we will use bloom filter to filter some records
CONF_mInt32(max_bf_read_bytes_percent, "10");

// Used by query cache, cache entries are evicted when it exceeds its capacity(500MB in default)
CONF_Int64(query_cache_capacity, "536870912");

// Used to limit buffer size of tablet send channel.
CONF_mInt64(send_channel_buffer_limit, "67108864");

// exception_stack_level controls when to print exception's stack
// -1, enable print all exceptions' stack
// 0, disable print exceptions' stack
// 1, print exceptions' stack whose prefix is in the white list(default)
// 2, print exceptions' stack whose prefix is not in the black list
// other value means the default value
CONF_Int32(exception_stack_level, "1");
CONF_String(exception_stack_white_list, "std::");
CONF_String(exception_stack_black_list, "apache::thrift::,ue2::,arangodb::");

// PK table's tabletmeta object size may got very large(lot's of edit versions), so it may not fit into block cache
// that may impact BE load dir time when restart. here we change num_shard_bits to 0 to disable block cache sharding,
// so large tabletmeta object can fit in block cache. After we optimize PK table's tabletmeta object size, we can
// revert this config change.
CONF_String(rocksdb_cf_options_string, "block_based_table_factory={block_cache={capacity=256M;num_shard_bits=0}}");

// limit local exchange buffer's memory size per driver
CONF_Int64(local_exchange_buffer_mem_limit_per_driver, "134217728"); // 128MB
// only used for test. default: 128M
CONF_mInt64(streaming_agg_limited_memory_size, "134217728");
// pipeline streaming aggregate chunk buffer size
CONF_mInt32(streaming_agg_chunk_buffer_size, "1024");
CONF_mInt64(wait_apply_time, "6000"); // 6s

// Max size of a binlog file. The default is 512MB.
CONF_Int64(binlog_file_max_size, "536870912");
// Max size of a binlog page. The default is 1MB.
CONF_Int32(binlog_page_max_size, "1048576");

CONF_mInt64(txn_info_history_size, "20000");
CONF_mInt64(file_write_history_size, "10000");

CONF_mInt32(update_cache_evict_internal_sec, "11");
CONF_mBool(enable_auto_evict_update_cache, "true");

CONF_mInt64(load_tablet_timeout_seconds, "60");

CONF_mBool(enable_pk_value_column_zonemap, "true");

// Used by default mv resource group
CONF_Double(default_mv_resource_group_memory_limit, "0.8");
CONF_Int32(default_mv_resource_group_cpu_limit, "1");

// Max size of key columns size of primary key table, default value is 128 bytes
CONF_mInt32(primary_key_limit_size, "128");

// used for control the max memory cost when batch get pk index in each tablet
CONF_mInt64(primary_key_batch_get_index_memory_limit, "104857600"); // 100MB

// If your sort key cardinality is very high,
// You could enable this config to speed up the point lookup query,
// otherwise, StarRocks will use zone map for one column filter
CONF_mBool(enable_short_key_for_one_column_filter, "false");
CONF_mBool(enable_zone_map_filter, "true");

CONF_mBool(enable_http_stream_load_limit, "false");
CONF_mInt32(finish_publish_version_internal, "100");

CONF_mInt32(get_txn_status_internal_sec, "30");

CONF_mBool(dump_metrics_with_bvar, "true");

CONF_mBool(enable_drop_tablet_if_unfinished_txn, "true");

// 0 means no limit
CONF_Int32(lake_service_max_concurrency, "0");

CONF_mInt64(lake_vacuum_max_batch_delete_size, "10000");

} // namespace starrocks::config
