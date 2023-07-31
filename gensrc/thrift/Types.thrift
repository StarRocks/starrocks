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
//   https://github.com/apache/incubator-doris/blob/master/gensrc/thrift/Types.thrift

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


typedef i64 TTimestamp
typedef i32 TPlanNodeId
typedef i32 TTupleId
typedef i32 TSlotId
typedef i64 TTableId
typedef i64 TTabletId
typedef i64 TVersion
typedef i64 TVersionHash
typedef i32 TSchemaHash
typedef i32 TPort
typedef i64 TCount
typedef i64 TSize
typedef i32 TClusterId
typedef i64 TEpoch

// add for real time load, partitionid is not defined previously, define it here
typedef i64 TTransactionId
typedef i64 TPartitionId

enum TStorageType {
    ROW,
    COLUMN,
}

enum TStorageMedium {
    HDD,
    SSD
}

enum TVarType {
    SESSION,
    GLOBAL,
    VERBOSE
}

enum TPrimitiveType {
  INVALID_TYPE,
  NULL_TYPE,
  BOOLEAN,
  TINYINT,
  SMALLINT,
  INT,
  BIGINT,
  FLOAT,
  DOUBLE,
  DATE,
  DATETIME,
  BINARY,
  DECIMAL,
  // CHAR(n). Currently only supported in UDAs
  CHAR,
  LARGEINT,
  VARCHAR,
  HLL,
  DECIMALV2,
  TIME,
  OBJECT,
  PERCENTILE,
  DECIMAL32,
  DECIMAL64,
  DECIMAL128,
  JSON,
  FUNCTION,
  VARBINARY
}

enum TTypeNodeType {
    SCALAR,
    ARRAY,
    MAP,
    STRUCT
}

struct TScalarType {
    1: required TPrimitiveType type

    // Only set if type == CHAR or type == VARCHAR
    2: optional i32 len

    // Only set for DECIMAL
    3: optional i32 precision
    4: optional i32 scale
}

// Represents a field in a STRUCT type.
// TODO: Model column stats for struct fields.
struct TStructField {
    1: optional string name
    2: optional string comment
}

struct TTypeNode {
    1: required TTypeNodeType type

    // only set for scalar types
    2: optional TScalarType scalar_type

    // only used for structs; has struct_fields.size() corresponding child types
    3: optional list<TStructField> struct_fields

    // only used for structs; for output use
    4: optional bool is_named
}

// A flattened representation of a tree of column types obtained by depth-first
// traversal. Complex types such as map, array and struct have child types corresponding
// to the map key/value, array item type, and struct fields, respectively.
// For scalar types the list contains only a single node.
// Note: We cannot rename this to TType because it conflicts with Thrift's internal TType
// and the generated Python thrift files will not work.
// Note: TTypeDesc in impala is TColumnType, but we already use TColumnType, so we name this
// to TTypeDesc. In future, we merge these two to one
struct TTypeDesc {
    1: list<TTypeNode> types
}

enum TAggregationType {
    SUM,
    MAX,
    MIN,
    REPLACE,
    HLL_UNION,
    NONE,
    BITMAP_UNION,
    REPLACE_IF_NOT_NULL,
    PERCENTILE_UNION
}

enum TPushType {
    LOAD,
    DELETE,
    LOAD_DELETE,
    // for spark load push request
    LOAD_V2,
    CANCEL_DELETE
}

enum TTaskType {
    CREATE,
    DROP,
    PUSH, // Deprecated
    CLONE,
    STORAGE_MEDIUM_MIGRATE,
    ROLLUP, // Deprecated
    SCHEMA_CHANGE,  // Deprecated
    CANCEL_DELETE,  // Deprecated
    MAKE_SNAPSHOT,
    RELEASE_SNAPSHOT,
    CHECK_CONSISTENCY,
    UPLOAD,
    DOWNLOAD,
    CLEAR_REMOTE_FILE,
    MOVE,
    REALTIME_PUSH,
    PUBLISH_VERSION,
    CLEAR_ALTER_TASK,
    CLEAR_TRANSACTION_TASK,
    RECOVER_TABLET, // deprecated
    STREAM_LOAD,
    UPDATE_TABLET_META_INFO,
    // this type of task will replace both ROLLUP and SCHEMA_CHANGE
    ALTER,
    INSTALL_PLUGIN,
    UNINSTALL_PLUGIN,
    // this use for calculate enum count
    DROP_AUTO_INCREMENT_MAP,
    COMPACTION,
    NUM_TASK_TYPE
}

enum TStmtType {
  QUERY,
  DDL,  // Data definition, e.g. CREATE TABLE (includes read-only functions e.g. SHOW)
  DML,  // Data modification e.g. INSERT
  EXPLAIN   // EXPLAIN
}

// level of verboseness for "explain" output
// TODO: should this go somewhere else?
enum TExplainLevel {
  NORMAL,
  VERBOSE,
  COSTS
}

struct TColumnType {
  1: required TPrimitiveType type
  // Only set if type == CHAR_ARRAY
  2: optional i32 len
  3: optional i32 index_len
  4: optional i32 precision
  5: optional i32 scale
}

// A TNetworkAddress is the standard host, port representation of a
// network address. The hostname field must be resolvable to an IPv4
// address.
struct TNetworkAddress {
  1: required string hostname
  2: required i32 port
}

// Wire format for UniqueId
struct TUniqueId {
  1: required i64 hi
  2: required i64 lo
}

enum QueryState {
  CREATED,
  INITIALIZED,
  COMPILED,
  RUNNING,
  FINISHED,
  EXCEPTION
}

enum TFunctionType {
  SCALAR,
  AGGREGATE,
}

enum TFunctionBinaryType {
  // StarRocks builtin. We can either run this interpreted or via codegen
  // depending on the query option.
  BUILTIN,

  // Hive UDFs, loaded from *.jar
  HIVE,

  // Native-interface, precompiled UDFs loaded from *.so
  NATIVE,

  // Native-interface, precompiled to IR; loaded from *.ll
  IR,

  // StarRocks customized UDF in jar.
  SRJAR
}

// Represents a fully qualified function name.
struct TFunctionName {
  // Name of the function's parent database. Not set if in global
  // namespace (e.g. builtins)
  1: optional string db_name

  // Name of the function
  2: required string function_name
}

struct TScalarFunction {
    // Symbol for the function
    1: required string symbol
    2: optional string prepare_fn_symbol
    3: optional string close_fn_symbol
}

struct TAggregateFunction {
  1: required TTypeDesc intermediate_type
  2: optional string update_fn_symbol
  3: optional string init_fn_symbol
  4: optional string serialize_fn_symbol
  5: optional string merge_fn_symbol
  6: optional string finalize_fn_symbol
  8: optional string get_value_fn_symbol
  9: optional string remove_fn_symbol
  10: optional bool is_analytic_only_fn = false
  11: optional string symbol
  // used for agg_func(a order by b, c) like array_agg, group_concat
  12: optional list<bool> is_asc_order
  // Indicates, for each expr, if nulls should be listed first or last. This is
  // independent of is_asc_order.
  13: optional list<bool> nulls_first
}

struct TTableFunction {
  1: required list<TTypeDesc> ret_types
  2: optional string symbol
}

// Represents a function in the Catalog.
struct TFunction {
  // Fully qualified function name.
  1: required TFunctionName name

  // Type of the udf. e.g. hive, native, ir
  2: required TFunctionBinaryType binary_type

  // The types of the arguments to the function
  3: required list<TTypeDesc> arg_types

  // Return type for the function.
  4: required TTypeDesc ret_type

  // If true, this function takes var args.
  5: required bool has_var_args

  // Optional comment to attach to the function
  6: optional string comment

  7: optional string signature

  // HDFS path for the function binary. This binary must exist at the time the
  // function is created.
  8: optional string hdfs_location

  // One of these should be set.
  9: optional TScalarFunction scalar_fn
  10: optional TAggregateFunction aggregate_fn

  11: optional i64 id
  12: optional string checksum

  // Builtin Function id, used to mark the function in the vectorization engine,
  // and it's different with `id` because `id` is use for serialized and cache
  // UDF function.
  30: optional i64 fid
  31: optional TTableFunction table_fn
  32: optional bool could_apply_dict_optimize

  // Ignore nulls
  33: optional bool ignore_nulls
}

enum TLoadJobState {
    PENDING,
    ETL,
    LOADING,
    FINISHED,
    CANCELLED
}

enum TEtlState {
    RUNNING,
    FINISHED,
    CANCELLED,
    UNKNOWN
}

enum TTableType {
    MYSQL_TABLE,
    OLAP_TABLE,
    SCHEMA_TABLE,
    KUDU_TABLE, // Deprecated
    BROKER_TABLE,
    ES_TABLE,
    HDFS_TABLE,
    ICEBERG_TABLE,
    HUDI_TABLE,
    JDBC_TABLE,
    PAIMON_TABLE,
    VIEW = 20,
    MATERIALIZED_VIEW,
    FILE_TABLE,
    DELTALAKE_TABLE,
    TABLE_FUNCTION_TABLE
}

enum TKeysType {
    PRIMARY_KEYS,
    DUP_KEYS,
    UNIQUE_KEYS,
    AGG_KEYS
}

enum TPriority {
    NORMAL,
    HIGH
}

struct TBackend {
    1: required string host
    2: required TPort be_port
    3: required TPort http_port
}

struct TResourceInfo {
    1: required string user
    2: required string group
}

enum TExportState {
    RUNNING,
    FINISHED,
    CANCELLED,
    UNKNOWN
}

enum TFileType {
    FILE_LOCAL,
    FILE_BROKER,
    FILE_STREAM,    // file content is streaming in the buffer
}

struct TTabletCommitInfo {
    1: required i64 tabletId
    2: required i64 backendId
    3: optional list<string> invalid_dict_cache_columns
    4: optional list<string> valid_dict_cache_columns
    5: optional list<i64> valid_dict_collected_versions
}

struct TTabletFailInfo {
    1: optional i64 tabletId
    2: optional i64 backendId
}

enum TLoadType {
    MANUAL_LOAD,
    ROUTINE_LOAD,
    MINI_LOAD
}

enum TLoadSourceType {
    RAW,
    KAFKA,
    PULSAR
}

enum TOpType {
    UPSERT,
    DELETE,
}

struct TUserRoles {
    1: optional list<i64> role_id_list
}

// represent a user identity
struct TUserIdentity {
    1: optional string username
    2: optional string host
    3: optional bool is_domain
    4: optional bool is_ephemeral
    5: optional TUserRoles current_role_ids
}

const i32 TSNAPSHOT_REQ_VERSION1 = 3; // corresponding to alpha rowset
const i32 TSNAPSHOT_REQ_VERSION2 = 4; // corresponding to beta rowset
// the snapshot request should always set prefer snapshot version to TPREFER_SNAPSHOT_REQ_VERSION
const i32 TPREFER_SNAPSHOT_REQ_VERSION = TSNAPSHOT_REQ_VERSION2;

enum TCompressionType {
    UNKNOWN_COMPRESSION = 0,
    DEFAULT_COMPRESSION = 1,
    NO_COMPRESSION = 2,
    SNAPPY = 3,
    LZ4 = 4,
    LZ4_FRAME = 5;
    ZLIB = 6;
    ZSTD = 7;
    GZIP = 8;
    DEFLATE = 9;
    BZIP2 = 10;
    LZO = 11; // Deprecated
    BROTLI = 12;
}

enum TWriteQuorumType {
    ONE = 0;
    MAJORITY = 1;
    ALL = 2;
}

enum StreamSourceType {
    BINLOG,
    KAFKA, // NOT IMPLEMENTED
}

struct TBinlogOffset {
    1: optional TTabletId tablet_id
    2: optional TVersion version
    3: optional i64 lsn
}

enum TPartialUpdateMode {
    UNKNOWN_MODE = 0;
    ROW_MODE = 1;
    COLUMN_MODE = 2;
    AUTO_MODE = 3;
}

enum TRunMode {
    SHARED_NOTHING = 0;
    SHARED_DATA = 1;
    HYBRID = 2;
}

struct TIcebergColumnStats {
    1: optional map<i32, i64> column_sizes
    2: optional map<i32, i64> value_counts
    3: optional map<i32, i64> null_value_counts
    4: optional map<i32, i64> nan_value_counts
    5: optional map<i32, binary> lower_bounds;
    6: optional map<i32, binary> upper_bounds;
}

struct TIcebergDataFile {
    1: optional string path
    2: optional string format
    3: optional i64 record_count
    4: optional i64 file_size_in_bytes
    5: optional string partition_path;
    6: optional list<i64> split_offsets;
    7: optional TIcebergColumnStats column_stats;
}

struct TSinkCommitInfo {
    1: optional TIcebergDataFile iceberg_data_file
    // ... for other tables sink commit info

    100: optional bool is_overwrite;
}
