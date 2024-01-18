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
//   https://github.com/apache/incubator-doris/blob/master/gensrc/thrift/DataSinks.thrift

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

include "CloudConfiguration.thrift"
include "Exprs.thrift"
include "Types.thrift"
include "Descriptors.thrift"
include "Partitions.thrift"
include "PlanNodes.thrift"

enum TDataSinkType {
    DATA_STREAM_SINK,
    RESULT_SINK,
    DATA_SPLIT_SINK,
    MYSQL_TABLE_SINK,
    EXPORT_SINK,
    OLAP_TABLE_SINK,
    MEMORY_SCRATCH_SINK,
    MULTI_CAST_DATA_STREAM_SINK,
    SCHEMA_TABLE_SINK,
    ICEBERG_TABLE_SINK,
    HIVE_TABLE_SINK,
    TABLE_FUNCTION_TABLE_SINK,
    BLACKHOLE_TABLE_SINK,
    DICTIONARY_CACHE_SINK
}

enum TResultSinkType {
    MYSQL_PROTOCAL,
    FILE,
    STATISTIC,
    VARIABLE,
    HTTP_PROTOCAL
}

enum TResultSinkFormatType {
    JSON,
    OTHERS
}

struct TParquetOptions {
    // parquet row group max size in bytes
    1: optional i64 parquet_max_group_bytes
    2: optional Types.TCompressionType compression_type
    3: optional bool use_dict
}

struct TResultFileSinkOptions {
    1: required string file_path
    2: required PlanNodes.TFileFormatType file_format
    3: optional string column_separator    // only for csv
    4: optional string row_delimiter  // only for csv
    5: optional i64 max_file_size_bytes
    6: optional list<Types.TNetworkAddress> broker_addresses; // only for remote file
    7: optional map<string, string> broker_properties // only for remote file.
    // If use_broker is set, we will write hdfs thourgh broker
    // If use_broker is not set, we will write through libhdfs/S3 directly
    8: optional bool use_broker
    // hdfs_write_buffer_size_kb for writing through lib hdfs directly
    9: optional i32 hdfs_write_buffer_size_kb = 0
    // properties from hdfs-site.xml, core-site.xml and load_properties
    10: optional PlanNodes.THdfsProperties hdfs_properties
    11: optional TParquetOptions parquet_options
    12: optional list<string> file_column_names
}

struct TMemoryScratchSink {

}

// Specification of one output destination of a plan fragment
struct TPlanFragmentDestination {
  // the globally unique fragment instance id
  1: required Types.TUniqueId fragment_instance_id

  // ... which is being executed on this server

  // 'deprecated_server' changed from required to optional in version 3.2
  // can be removed in version 4.0
  2: optional Types.TNetworkAddress deprecated_server
  3: optional Types.TNetworkAddress brpc_server

  4: optional i32 pipeline_driver_sequence
}

// Sink which forwards data to a remote plan fragment,
// according to the given output partition specification
// (ie, the m:1 part of an m:n data stream)
struct TDataStreamSink {
  // destination node id
  1: required Types.TPlanNodeId dest_node_id

  // Specification of how the output of a fragment is partitioned.
  // If the partitioning type is UNPARTITIONED, the output is broadcast
  // to each destination host.
  2: required Partitions.TDataPartition output_partition

  3: optional bool ignore_not_found

  // Only useful in pipeline mode
  // If receiver side is ExchangeMergeSortSourceOperator, then all the
  // packets should be kept in order (according sequence), so the sender
  // side need to maintain a send window in order to avoiding the receiver
  // buffer too many out-of-order packets
  4: optional bool is_merge

  // degree of paralleliasm of destination
  // only used in pipeline engine
  5: optional i32 dest_dop

  // Specify the columns which need to send
  6: optional list<i32> output_columns;
}

struct TMultiCastDataStreamSink {
    1: required list<TDataStreamSink> sinks;
    2: required list< list<TPlanFragmentDestination> > destinations;
}

struct TResultSink {
    1: optional TResultSinkType type;
    2: optional TResultFileSinkOptions file_options;
    3: optional TResultSinkFormatType format;
    4: optional bool is_binary_row;
}

struct TMysqlTableSink {
    1: required string host
    2: required i32 port
    3: required string user
    4: required string passwd
    5: required string db
    6: required string table
}

struct TExportSink {
    1: required Types.TFileType file_type
    2: required string export_path
    3: required string column_separator
    4: required string row_delimiter
    // properties need to access broker.
    5: optional list<Types.TNetworkAddress> broker_addresses
    6: optional map<string, string> properties

    // If use_broker is set, we will write hdfs thourgh broker
    // If use_broker is not set, we will write through libhdfs/S3 directly
    7: optional bool use_broker
    // hdfs_write_buffer_size_kb for writing through lib hdfs directly
    8: optional i32 hdfs_write_buffer_size_kb = 0
    // properties from hdfs-site.xml, core-site.xml and load_properties
    9: optional PlanNodes.THdfsProperties hdfs_properties

    // export file name prefix
    30: optional string file_name_prefix
}

struct TDictionaryCacheSink {
    1: optional list<Types.TNetworkAddress> nodes
    2: optional i64 dictionary_id
    3: optional i64 txn_id
    4: optional Descriptors.TOlapTableSchemaParam schema
    5: optional i64 memory_limit
    6: optional i32 key_size
}

struct TOlapTableSink {
    1: required Types.TUniqueId load_id
    2: required i64 txn_id
    3: required i64 db_id
    4: required i64 table_id
    5: required i32 tuple_id
    6: required i32 num_replicas
    7: required bool need_gen_rollup // Deprecated
    8: optional string db_name
    9: optional string table_name
    10: required Descriptors.TOlapTableSchemaParam schema
    11: required Descriptors.TOlapTablePartitionParam partition
    12: required Descriptors.TOlapTableLocationParam location
    13: required Descriptors.TNodesInfo nodes_info
    14: optional i64 load_channel_timeout_s // the timeout of load channels in second
    15: optional bool is_lake_table
    16: optional string txn_trace_parent
    17: optional Types.TKeysType keys_type
    18: optional Types.TWriteQuorumType write_quorum_type
    19: optional bool enable_replicated_storage
    20: optional string merge_condition
    21: optional bool null_expr_in_auto_increment
    22: optional bool miss_auto_increment_column
    23: optional bool abort_delete // Deprecated
    24: optional i32 auto_increment_slot_id
    25: optional Types.TPartialUpdateMode partial_update_mode
    26: optional string label
    // enable colocated for sync mv 
    27: optional bool enable_colocate_mv_index 
    28: optional i64 automatic_bucket_size
}

struct TSchemaTableSink {
    1: optional string table
    2: optional Descriptors.TNodesInfo nodes_info
}

struct TIcebergTableSink {
    1: optional string location
    2: optional string file_format
    3: optional i64 target_table_id
    4: optional Types.TCompressionType compression_type
    5: optional bool is_static_partition_sink
    6: optional CloudConfiguration.TCloudConfiguration cloud_configuration
}

struct THiveTableSink {
    1: optional string staging_dir
    2: optional string file_format
    3: optional list<string> data_column_names
    4: optional list<string> partition_column_names
    5: optional Types.TCompressionType compression_type
    6: optional bool is_static_partition_sink
    7: optional CloudConfiguration.TCloudConfiguration cloud_configuration
}

struct TTableFunctionTableSink {
    1: optional Descriptors.TTableFunctionTable target_table
    2: optional CloudConfiguration.TCloudConfiguration cloud_configuration
}

struct TDataSink {
  1: required TDataSinkType type
  2: optional TDataStreamSink stream_sink
  3: optional TResultSink result_sink
  5: optional TMysqlTableSink mysql_table_sink
  6: optional TExportSink export_sink
  7: optional TOlapTableSink olap_table_sink
  8: optional TMemoryScratchSink memory_scratch_sink
  9: optional TMultiCastDataStreamSink multi_cast_stream_sink
  10: optional TSchemaTableSink schema_table_sink
  11: optional TIcebergTableSink iceberg_table_sink
  12: optional THiveTableSink hive_table_sink
  13: optional TTableFunctionTableSink table_function_table_sink
  14: optional TDictionaryCacheSink dictionary_cache_sink
}
