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
//
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/gensrc/thrift/InternalService.thrift

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

namespace cpp starrocks
namespace java com.starrocks.thrift

include "Status.thrift"
include "Types.thrift"
include "Exprs.thrift"
include "Descriptors.thrift"
include "PlanNodes.thrift"
include "Planner.thrift"
include "DataSinks.thrift"
include "Data.thrift"
include "RuntimeProfile.thrift"
include "WorkGroup.thrift"
include "RuntimeFilter.thrift"

// constants for function version
enum TFunctionVersion {
    RUNTIME_FILTER_SERIALIZE_VERSION_2 = 7,
}

enum TQueryType {
    SELECT,
    LOAD,
    EXTERNAL
}

enum TLoadJobType {
    BROKER,
    SPARK,
    INSERT_QUERY,
    INSERT_VALUES,
    STREAM_LOAD,
    ROUTINE_LOAD,
}

enum TErrorHubType {
    MYSQL,
    BROKER,
    NULL_TYPE
}

struct TMysqlErrorHubInfo {
    1: required string host;
    2: required i32 port;
    3: required string user;
    4: required string passwd;
    5: required string db;
    6: required string table;
}

struct TBrokerErrorHubInfo {
    1: required Types.TNetworkAddress broker_addr;
    2: required string path;
    3: required map<string, string> prop;
}

struct TLoadErrorHubInfo {
    1: required TErrorHubType type = TErrorHubType.NULL_TYPE;
    2: optional TMysqlErrorHubInfo mysql_info;
    3: optional TBrokerErrorHubInfo broker_info;
}

enum TPipelineProfileLevel {
  MERGE = 1;
  DETAIL = 2;
}

enum TSpillMode {
  AUTO,
  FORCE
}

enum TSpillableOperatorType {
  HASH_JOIN = 0;
  AGG = 1;
  AGG_DISTINCT = 2;
  SORT = 3;
  NL_JOIN = 4;
}

enum TTabletInternalParallelMode {
  AUTO,
  FORCE_SPLIT
}

enum TOverflowMode {
  OUTPUT_NULL = 0;
  REPORT_ERROR = 1;
}

// Query options with their respective defaults
struct TQueryOptions {
  2: optional i32 max_errors = 0
  4: optional i32 batch_size = 0
  
  12: optional i64 mem_limit = 2147483648
  13: optional bool abort_on_default_limit_exceeded = 0
  14: optional i32 query_timeout = 3600
  15: optional bool enable_profile = 0

  18: optional TQueryType query_type = TQueryType.SELECT

  // if this is a query option for LOAD, load_mem_limit should be set to limit the mem comsuption
  // of load channel.
  28: optional i64 load_mem_limit = 0;
  // see BE config `starrocks_max_scan_key_num` for details
  // if set, this will overwrite the BE config.
  29: optional i32 max_scan_key_num;
  // see BE config `max_pushdown_conditions_per_column` for details
  // if set, this will overwrite the BE config.
  30: optional i32 max_pushdown_conditions_per_column
  // whether enable spill to disk
  31: optional bool enable_spill = false;

  50: optional Types.TCompressionType transmission_compression_type;

  51: optional i64 runtime_join_filter_pushdown_limit;
  // Timeout in ms to wait until runtime filters are arrived.
  52: optional i32 runtime_filter_wait_timeout_ms = 200;
  // Timeout in ms to send runtime filter across nodes.
  53: optional i32 runtime_filter_send_timeout_ms = 400;
  // For pipeline query engine
  54: optional i32 pipeline_dop;
  // For pipeline query engine
  55: optional TPipelineProfileLevel pipeline_profile_level;
  // For load degree of parallel
  56: optional i32 load_dop;
  57: optional i64 runtime_filter_scan_wait_time_ms;

  58: optional i64 query_mem_limit;

  59: optional bool enable_tablet_internal_parallel;

  60: optional i32 query_delivery_timeout;
  
  61: optional bool enable_query_debug_trace;

  62: optional Types.TCompressionType load_transmission_compression_type;

  63: optional TTabletInternalParallelMode tablet_internal_parallel_mode;

  64: optional TLoadJobType load_job_type

  66: optional bool enable_scan_datacache;

  67: optional bool enable_pipeline_query_statistic = false;

  68: optional i32 transmission_encode_level;
  
  69: optional bool enable_populate_datacache;

  70: optional bool allow_throw_exception = 0;

  71: optional bool hudi_mor_force_jni_reader;

  72: optional i64 rpc_http_min_size;

  // some experimental parameter for spill
  73: optional i32 spill_mem_table_size;
  74: optional i32 spill_mem_table_num;
  75: optional double spill_mem_limit_threshold;
  76: optional i64 spill_operator_min_bytes;
  77: optional i64 spill_operator_max_bytes;
  78: optional i32 spill_encode_level;

  85: optional TSpillMode spill_mode;
  
  86: optional i32 io_tasks_per_scan_operator = 4;
  87: optional i32 connector_io_tasks_per_scan_operator = 16;
  88: optional double runtime_filter_early_return_selectivity = 0.05;


  90: optional i64 log_rejected_record_num = 0;

  91: optional bool use_page_cache;

  92: optional bool enable_connector_adaptive_io_tasks = true;
  93: optional i32 connector_io_tasks_slow_io_latency_ms = 50;
  94: optional double scan_use_query_mem_ratio = 0.25;
  95: optional double connector_scan_use_query_mem_ratio = 0.3;
  // used to identify which operators allow spill, only meaningful when enable_spill=true
  96: optional i64 spillable_operator_mask;

  100: optional i64 group_concat_max_len = 1024;
  101: optional i64 runtime_profile_report_interval = 30;

  102: optional bool enable_collect_table_level_scan_stats;

  104: optional TOverflowMode overflow_mode = TOverflowMode.OUTPUT_NULL;
  105: optional bool use_column_pool = true;

  // Skip the 106:enable_agg_spill_preaggregation

  // Whether use hdfs file metacache
  107: optional bool enable_hdfs_file_metacache;

  108: optional bool enable_file_pagecache;
  109: optional i32 datacache_populate_probability;
}


// A scan range plus the parameters needed to execute that scan.
struct TScanRangeParams {
  1: required PlanNodes.TScanRange scan_range
  2: optional i32 volume_id = -1
}

// Parameters for a single execution instance of a particular TPlanFragment
// TODO: for range partitioning, we also need to specify the range boundaries
struct TPlanFragmentExecParams {
  // a globally unique id assigned to the entire query
  1: required Types.TUniqueId query_id

  // a globally unique id assigned to this particular execution instance of
  // a TPlanFragment
  2: required Types.TUniqueId fragment_instance_id

  // initial scan ranges for each scan node in TPlanFragment.plan_tree
  3: required map<Types.TPlanNodeId, list<TScanRangeParams>> per_node_scan_ranges

  // number of senders for ExchangeNodes contained in TPlanFragment.plan_tree;
  // needed to create a DataStreamRecvr
  4: required map<Types.TPlanNodeId, i32> per_exch_num_senders

  // Output destinations, one per output partition.
  // The partitioning of the output is specified by
  // TPlanFragment.output_sink.output_partition.
  // The number of output partitions is destinations.size().
  5: list<DataSinks.TPlanFragmentDestination> destinations

  // Debug options: perform some action in a particular phase of a particular node
  6: optional Types.TPlanNodeId debug_node_id
  7: optional PlanNodes.TExecNodePhase debug_phase
  8: optional PlanNodes.TDebugAction debug_action

  // Id of this fragment in its role as a sender.
  9: optional i32 sender_id
  10: optional i32 num_senders
  11: optional bool send_query_statistics_with_every_batch
  12: optional bool use_vectorized // whether to use vectorized query engine

  // Global runtime filters
  50: optional RuntimeFilter.TRuntimeFilterParams runtime_filter_params
  51: optional i32 instances_number
  // To enable pass through chunks between sink/exchange if they are in the same process.
  52: optional bool enable_exchange_pass_through

  53: optional map<Types.TPlanNodeId, map<i32, list<TScanRangeParams>>> node_to_per_driver_seq_scan_ranges 

  54: optional bool enable_exchange_perf

  70: optional i32 pipeline_sink_dop
}

// Global query parameters assigned by the coordinator.
struct TQueryGlobals {
  // String containing a timestamp set as the current time.
  // Format is yyyy-MM-dd HH:mm:ss
  1: required string now_string

  // To support timezone in StarRocks. timestamp_ms is the millisecond uinix timestamp for
  // this query to calculate time zone relative function
  2: optional i64 timestamp_ms

  // time_zone is the timezone this query used.
  // If this value is set, BE will ignore now_string
  3: optional string time_zone

  // Added by StarRocks
  // Required by function 'last_query_id'.
  30: optional string last_query_id
}


// Service Protocol Details

enum InternalServiceVersion {
  V1
}

struct TAdaptiveDopParam {
  1: optional i64 max_block_rows_per_driver_seq
  2: optional i64 max_output_amplification_factor
}

// ExecPlanFragment

struct TExecPlanFragmentParams {
  1: required InternalServiceVersion protocol_version

  // required in V1
  2: optional Planner.TPlanFragment fragment

  // required in V1
  3: optional Descriptors.TDescriptorTable desc_tbl

  // required in V1
  4: optional TPlanFragmentExecParams params

  // Initiating coordinator.
  // TODO: determine whether we can get this somehow via the Thrift rpc mechanism.
  // required in V1
  5: optional Types.TNetworkAddress coord

  // backend number assigned by coord to identify backend
  // required in V1
  6: optional i32 backend_num

  // Global query parameters assigned by coordinator.
  // required in V1
  7: optional TQueryGlobals query_globals

  // options for the query
  // required in V1
  8: optional TQueryOptions query_options

  // Whether reportd when the backend fails
  // required in V1
  9: optional bool enable_profile

  // required in V1
  10: optional Types.TResourceInfo resource_info

  // load job related
  11: optional string import_label
  12: optional string db_name
  13: optional i64 load_job_id
  14: optional TLoadErrorHubInfo load_error_hub_info

  50: optional bool is_pipeline
  51: optional i32 pipeline_dop
  52: optional map<Types.TPlanNodeId, i32> per_scan_node_dop

  53: optional WorkGroup.TWorkGroup workgroup
  54: optional bool enable_resource_group
  55: optional i32 func_version
  
  // Sharing data between drivers of same scan operator
  56: optional bool enable_shared_scan

  57: optional bool is_stream_pipeline

  58: optional TAdaptiveDopParam adaptive_dop_param
}

struct TExecPlanFragmentResult {
  // required in V1
  1: optional Status.TStatus status
}

struct TExecBatchPlanFragmentsParams {
  // required in V1
  1: optional TExecPlanFragmentParams common_param
  // required in V1
  2: optional list<TExecPlanFragmentParams> unique_param_per_instance
}

// CancelPlanFragment
struct TCancelPlanFragmentParams {
  1: required InternalServiceVersion protocol_version

  // required in V1
  2: optional Types.TUniqueId fragment_instance_id
}

struct TCancelPlanFragmentResult {
  // required in V1
  1: optional Status.TStatus status
}


// TransmitData
struct TTransmitDataParams {
  1: required InternalServiceVersion protocol_version

  // required in V1
  2: optional Types.TUniqueId dest_fragment_instance_id

  // for debugging purposes; currently ignored
  //3: optional Types.TUniqueId src_fragment_instance_id

  // required in V1
  4: optional Types.TPlanNodeId dest_node_id

  // required in V1
  5: optional Data.TRowBatch row_batch

  // if set to true, indicates that no more row batches will be sent
  // for this dest_node_id
  6: optional bool eos

  7: optional i32 be_number
  8: optional i64 packet_seq

  // Id of this fragment in its role as a sender.
  9: optional i32 sender_id
}

struct TTransmitDataResult {
  // required in V1
  1: optional Status.TStatus status
  2: optional i64 packet_seq
  3: optional Types.TUniqueId dest_fragment_instance_id
  4: optional Types.TPlanNodeId dest_node_id
}

struct TFetchDataParams {
  1: required InternalServiceVersion protocol_version
  // required in V1
  // query id which want to fetch data
  2: required Types.TUniqueId fragment_instance_id
}

struct TFetchDataResult {
    // result batch
    1: required Data.TResultBatch result_batch
    // end of stream flag
    2: required bool eos
    // packet num used check lost of packet
    3: required i32 packet_num
    // Operation result
    4: optional Status.TStatus status
}

struct TCondition {
    1:  required string column_name
    2:  required string condition_op
    3:  required list<string> condition_values
    // whether this condition only used to filter index, not filter chunk row in storage engine
    20: optional bool is_index_filter_only
}

struct TExportStatusResult {
    1: required Status.TStatus status
    2: required Types.TExportState state
    3: optional list<string> files
}

struct TGetFileSchemaRequest {
  1: required PlanNodes.TScanRange scan_range
  2: optional i32 volume_id = -1
}
