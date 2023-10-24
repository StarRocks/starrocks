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
//   https://github.com/apache/incubator-doris/blob/master/gensrc/thrift/Descriptors.thrift

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

include "Types.thrift"
include "Exprs.thrift"

struct TSlotDescriptor {
  1: optional Types.TSlotId id
  2: optional Types.TTupleId parent
  3: optional Types.TTypeDesc slotType
  4: optional i32 columnPos   // Deprecated
  5: optional i32 byteOffset  // Deprecated
  6: optional i32 nullIndicatorByte // Deprecated
  7: optional i32 nullIndicatorBit // Deprecated
  8: optional string colName;
  9: optional i32 slotIdx // Deprecated
  10: optional bool isMaterialized // Deprecated
  11: optional bool isOutputColumn // Deprecated
  12: optional bool isNullable // replace nullIndicatorBit & nullIndicatorByte
  13: optional i32 col_unique_id = -1
}

struct TTupleDescriptor {
  1: optional Types.TTupleId id
  2: optional i32 byteSize // Deprecated
  3: optional i32 numNullBytes // Deprecated
  4: optional Types.TTableId tableId
  5: optional i32 numNullSlots // Deprecated
}

enum THdfsFileFormat {
  TEXT = 0,
  LZO_TEXT = 1,
  RC_FILE = 2,
  SEQUENCE_FILE = 3,
  AVRO = 4,
  PARQUET = 5,
  ORC = 6,

  UNKNOWN = 100
}


// Text file desc
struct TTextFileDesc {
    // property 'field.delim'
    1: optional string  field_delim

    // property 'line.delim'
    2: optional string line_delim

    // property 'collection.delim' 
    3: optional string collection_delim

    // property 'mapkey.delim'
    4: optional string mapkey_delim

    // compression type.
    5: optional Types.TCompressionType compression_type;

    // specifies whether to remove white space from fields
    6: optional bool trim_space

    // enclose character
    7: optional i8 enclose
    
    // escape character
    8: optional i8 escape
}

enum TSchemaTableType {
    SCH_AUTHORS= 0,
    SCH_CHARSETS,
    SCH_COLLATIONS,
    SCH_COLLATION_CHARACTER_SET_APPLICABILITY,
    SCH_COLUMNS,
    SCH_COLUMN_PRIVILEGES,
    SCH_CREATE_TABLE,
    SCH_ENGINES,
    SCH_EVENTS,
    SCH_FILES,
    SCH_GLOBAL_STATUS,
    SCH_GLOBAL_VARIABLES,
    SCH_KEY_COLUMN_USAGE,
    SCH_MATERIALIZED_VIEWS,
    SCH_OPEN_TABLES,
    SCH_PARTITIONS,
    SCH_PLUGINS,
    SCH_PROCESSLIST,
    SCH_PROFILES,
    SCH_REFERENTIAL_CONSTRAINTS,
    SCH_PROCEDURES,
    SCH_SCHEMATA,
    SCH_SCHEMA_PRIVILEGES,
    SCH_SESSION_STATUS,
    SCH_SESSION_VARIABLES,
    SCH_STATISTICS,
    SCH_STATUS,
    SCH_TABLES,
    SCH_TABLES_CONFIG,
    SCH_TABLE_CONSTRAINTS,
    SCH_TABLE_NAMES,
    SCH_TABLE_PRIVILEGES,
    SCH_TRIGGERS,
    SCH_USER_PRIVILEGES,
    SCH_VARIABLES,
    SCH_VIEWS,
    SCH_TASKS,
    SCH_TASK_RUNS,
    SCH_VERBOSE_SESSION_VARIABLES,
    SCH_BE_TABLETS,
    SCH_BE_METRICS,
    SCH_BE_TXNS,
    SCH_BE_CONFIGS,
    SCH_LOADS,
    SCH_LOAD_TRACKING_LOGS,
    SCH_FE_TABLET_SCHEDULES,
    SCH_BE_COMPACTIONS,
    SCH_BE_THREADS,
    SCH_BE_LOGS,
    SCH_BE_BVARS,
    SCH_BE_CLOUD_NATIVE_COMPACTIONS,
    STARROCKS_ROLE_EDGES,
    STARROCKS_GRANT_TO_ROLES,
    STARROCKS_GRANT_TO_USERS,
    SCH_ROUTINE_LOAD_JOBS,
    SCH_STREAM_LOADS,
    SCH_PIPE_FILES,
    SCH_PIPES,
    SCH_FE_METRICS
}

enum THdfsCompression {
  NONE,
  DEFAULT,
  GZIP,
  DEFLATE,
  BZIP2,
  SNAPPY,
  SNAPPY_BLOCKED // Used by sequence and rc files but not stored in the metadata.
}

enum TIndexType {
  BITMAP
}

// Mapping from names defined by Avro to the enum.
// We permit gzip and bzip2 in addition.
const map<string, THdfsCompression> COMPRESSION_MAP = {
  "": THdfsCompression.NONE,
  "none": THdfsCompression.NONE,
  "deflate": THdfsCompression.DEFAULT,
  "gzip": THdfsCompression.GZIP,
  "bzip2": THdfsCompression.BZIP2,
  "snappy": THdfsCompression.SNAPPY
}

struct TColumn {                                                                                      
    1: required string column_name          
    // Deprecated, use |index_len| and |type_desc| instead.                                           
    // TColumnType is too simple to represent complex types, e.g, array.
    2: optional Types.TColumnType column_type      
    3: optional Types.TAggregationType aggregation_type
    4: optional bool is_key                      
    5: optional bool is_allow_null                                                                    
    6: optional string default_value               
    7: optional bool is_bloom_filter_column     
    8: optional Exprs.TExpr define_expr 
    9: optional bool is_auto_increment
    10: optional i32 col_unique_id  = -1
    11: optional bool has_bitmap_index = false
                                                                                                      
    // How many bytes used for short key index encoding.
    // For fixed-length column, this value may be ignored by BE when creating a tablet.
    20: optional i32 index_len                 
    // column type. If this field is set, the |column_type| will be ignored.
    21: optional Types.TTypeDesc type_desc         
}

struct TOlapTableIndexTablets {
    1: required i64 index_id
    2: required list<i64> tablets
}

// its a closed-open range
struct TOlapTablePartition {
    1: required i64 id
    // deprecated, use 'start_keys' and 'end_keys' instead
    2: optional Exprs.TExprNode start_key
    3: optional Exprs.TExprNode end_key

    // how many tablets in one partition
    4: required i32 num_buckets

    5: required list<TOlapTableIndexTablets> indexes

    6: optional list<Exprs.TExprNode> start_keys
    7: optional list<Exprs.TExprNode> end_keys

    8: optional list<list<Exprs.TExprNode>> in_keys
    // for automatic partition
    9: optional bool is_shadow_partition = false
}

struct TOlapTablePartitionParam {
    1: required i64 db_id
    2: required i64 table_id
    3: required i64 version

    // used to split a logical table to multiple paritions
    // deprecated, use 'partition_columns' instead
    4: optional string partition_column

    // used to split a partition to multiple tablets
    5: optional list<string> distributed_columns

    // partitions
    6: required list<TOlapTablePartition> partitions

    7: optional list<string> partition_columns
    8: optional list<Exprs.TExpr> partition_exprs

    9: optional bool enable_automatic_partition
}

struct TOlapTableColumnParam {
    1: required list<TColumn> columns
    2: required list<i32> sort_key_uid
    3: required i32 short_key_column_count
}

struct TOlapTableIndexSchema {
    1: required i64 id
    2: required list<string> columns
    3: required i32 schema_hash
    4: optional TOlapTableColumnParam column_param
}

struct TOlapTableSchemaParam {
    1: required i64 db_id
    2: required i64 table_id
    3: required i64 version

    // Logical columns, contain all column that in logical table
    4: required list<TSlotDescriptor> slot_descs
    5: required TTupleDescriptor tuple_desc
    6: required list<TOlapTableIndexSchema> indexes
}

struct TOlapTableIndex {
  1: optional string index_name
  2: optional list<string> columns
  3: optional TIndexType index_type
  4: optional string comment
}

struct TTabletLocation {
    1: required i64 tablet_id
    2: required list<i64> node_ids
}

struct TOlapTableLocationParam {
    1: required i64 db_id
    2: required i64 table_id
    3: required i64 version
    4: required list<TTabletLocation> tablets
}

struct TNodeInfo {
    1: required i64 id
    2: required i64 option
    3: required string host
    // used to transfer data between nodes
    4: required i32 async_internal_port
}

struct TNodesInfo {
    1: required i64 version
    2: required list<TNodeInfo> nodes
}

struct TOlapTable {
    1: required string tableName
}

struct TMySQLTable {
  1: required string host
  2: required string port
  3: required string user
  4: required string passwd
  5: required string db
  6: required string table
}

struct TEsTable {
}

struct TSchemaTable {
  1: required TSchemaTableType tableType
}

struct TBrokerTable {
}

// Represents an HDFS partition's location in a compressed format.
struct THdfsPartitionLocation {
    // Index of THdfsTable.partition_prefixes.
    // -1 if this location has not been compressed.
    1: optional i32 prefix_index = -1

    // 'suffix' is the rest of the partition location.
    // prefix + suffix = partition path 
    2: optional string suffix
}

struct THdfsPartition {
    1: optional THdfsFileFormat file_format

    // Literal expressions
    2: optional list<Exprs.TExpr> partition_key_exprs

    // Represents an HDFS partition's location in a compressed format.
    3: optional THdfsPartitionLocation location
}

struct THdfsTable {
    // Hdfs table base dir
    1: optional string hdfs_base_dir

    // Schema columns, except partition columns
    2: optional list<TColumn> columns

    // Partition columns
    3: optional list<TColumn> partition_columns

    // Map from partition id to partition metadata.
    4: optional map<i64, THdfsPartition> partitions

    // The prefixes of locations of partitions in this table
    5: optional list<string> partition_prefixes
}

struct TFileTable {
    // File table base dir
    1: optional string location

    // Schema columns
    2: optional list<TColumn> columns
}

struct TTableFunctionTable {
    // Table Function Table's file dir 
    1: optional string path 

    // Schema columns
    2: optional list<TColumn> columns

    // File format
    3: optional string file_format;

    // Compression type
    4: optional Types.TCompressionType compression_type

    // Partition column ids, set if partition_by used in table function
    5: optional list<i32> partition_column_ids

    // Write single file
    6: optional bool write_single_file
}

struct TIcebergSchema {
    1: optional list<TIcebergSchemaField> fields
}

struct TIcebergSchemaField {
    // Refer to field id in iceberg schema
    1: optional i32 field_id

    // Refer to field name
    2: optional string name

    // You can fill other field properties here if you needed
    // .......

    // Children fields for struct, map and list(array)
    100: optional list<TIcebergSchemaField> children
}

struct TPartitionMap {
    1: optional map<i64, THdfsPartition> partitions
}

struct TCompressedPartitionMap {
    1: optional i32 original_len
    2: optional i32 compressed_len
    3: optional string compressed_serialized_partitions
}

struct TIcebergTable {
    // table location
    1: optional string location

    // Schema columns, except partition columns
    2: optional list<TColumn> columns

    // Iceberg schema, used to support schema evolution
    3: optional TIcebergSchema iceberg_schema

    // partition column names
    4: optional list<string> partition_column_names

    // partition map may be very big, serialize costs too much, just use serialized byte[]
    5: optional TCompressedPartitionMap compressed_partitions

    // if serialize partition info throws exception, then use unserialized partitions
    6: optional map<i64, THdfsPartition> partitions
}

struct THudiTable {
    // table location
    1: optional string location

    // Schema columns, except partition columns
    2: optional list<TColumn> columns

    // Partition columns
    3: optional list<TColumn> partition_columns

    // Map from partition id to partition metadata.
    4: optional map<i64, THdfsPartition> partitions

    // The prefixes of locations of partitions in this table
    5: optional list<string> partition_prefixes

    // hudi table instant time
    6: optional string instant_time

    // hudi table hive_column_names
    7: optional string hive_column_names

    // hudi table hive_column_types
    8: optional string hive_column_types

    // hudi table input_format
    9: optional string input_format

    // hudi table serde_lib
    10: optional string serde_lib
}

struct TPaimonTable {
    // paimon table catalog type
    1: optional string catalog_type

    // paimon table metastore URI
    2: optional string metastore_uri

    // paimon table warehouse path
    3: optional string warehouse_path
}

struct TDeltaLakeTable {
    // table location
    1: optional string location

    // Schema columns, except partition columns
    2: optional list<TColumn> columns

    // Partition columns
    3: optional list<TColumn> partition_columns

    // Map from partition id to partition metadata.
    4: optional map<i64, THdfsPartition> partitions

    // The prefixes of locations of partitions in this table
    5: optional list<string> partition_prefixes
}

struct TJDBCTable {
    1: optional string jdbc_driver_name
    2: optional string jdbc_driver_url
    3: optional string jdbc_driver_checksum
    4: optional string jdbc_driver_class
    5: optional string jdbc_url
    6: optional string jdbc_table
    7: optional string jdbc_user
    8: optional string jdbc_passwd
}

// "Union" of all table types.
struct TTableDescriptor {
  1: required Types.TTableId id
  2: required Types.TTableType tableType
  3: required i32 numCols
  4: required i32 numClusteringCols

  // Unqualified name of table
  7: required string tableName;

  // Name of the database that the table belongs to
  8: required string dbName;
  10: optional TMySQLTable mysqlTable
  11: optional TOlapTable olapTable
  12: optional TSchemaTable schemaTable
  14: optional TBrokerTable BrokerTable
  15: optional TEsTable esTable
  16: optional TJDBCTable jdbcTable

  // Hdfs Table schema
  30: optional THdfsTable hdfsTable

  // Iceberg Table schema
  31: optional TIcebergTable icebergTable

  // Hudi Table schema
  32: optional THudiTable hudiTable

  // Delta Lake schema
  33: optional TDeltaLakeTable deltaLakeTable

  // File Table
  34: optional TFileTable fileTable

  // Table Function Table
  35: optional TTableFunctionTable tableFunctionTable

  // Paimon Table schema
  36: optional TPaimonTable paimonTable
}

struct TDescriptorTable {
  1: optional list<TSlotDescriptor> slotDescriptors;
  2: required list<TTupleDescriptor> tupleDescriptors;

  // all table descriptors referenced by tupleDescriptors
  3: optional list<TTableDescriptor> tableDescriptors;
  4: optional bool is_cached;
}

// Describe route info of a Olap Table
struct TOlapTableRouteInfo {
  1: optional TOlapTableSchemaParam schema
  2: optional TOlapTablePartitionParam partition
  3: optional TOlapTableLocationParam location
  5: optional i32 num_replicas
  6: optional string db_name
  7: optional string table_name
  8: optional TNodesInfo nodes_info
  9: optional Types.TKeysType keys_type
}

enum TIMTType {
  OLAP_TABLE,
  ROWSTORE_TABLE, // Not implemented
}

struct TIMTDescriptor {
  1: optional TIMTType imt_type
  2: optional TOlapTableRouteInfo olap_table
  3: optional bool need_maintain // Not implemented

  // For maintained IMT, some extra information are necessary
  11: optional Types.TUniqueId load_id
  12: optional i64 txn_id
}
