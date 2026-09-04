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

include "CloudConfiguration.thrift"

typedef i64 TTimestamp
typedef i32 TPlanNodeId
typedef i32 TTupleId
typedef i32 TSlotId
typedef i64 TTableId
typedef i64 TDatabaseId
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
    COLUMN_WITH_ROW
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
  VARBINARY,
  DECIMAL256,
  INT256,
  VARIANT
}

enum TTypeNodeType {
    SCALAR,
    ARRAY,
    MAP,
    STRUCT
}

// Logical semantics for a native geo value. The WKB payload does not encode
// this distinction, so it must be transported with the type descriptor.
enum TGeoLogicalType {
    UNKNOWN = 0,
    GEOGRAPHY = 1,
    GEOMETRY = 2
}

enum TGeoCoordinateSystem {
    UNKNOWN = 0,
    SPHERICAL = 1,
    CARTESIAN = 2
}

enum TGeoEdgeAlgorithm {
    UNKNOWN = 0,
    GEODESIC = 1,
    LINEAR = 2
}

enum TGeoEncoding {
    UNKNOWN = 0,
    WKB = 1
}

enum TGeoDimension {
    UNKNOWN = 0,
    XY = 1,
    XYZ = 2,
    XYM = 3,
    XYZM = 4,
    MIXED = 5
}

enum TGeoValidationState {
    UNKNOWN = 0,
    UNVALIDATED = 1,
    STRUCTURALLY_VALIDATED = 2,
    SEMANTICALLY_VALIDATED = 3
}

struct TGeoTypeDesc {
    1: optional TGeoLogicalType logical_type
    2: optional TGeoCoordinateSystem coordinate_system
    3: optional TGeoEdgeAlgorithm edge_algorithm
    4: optional string crs
    5: optional i32 srid
}

struct TGeoStorageDesc {
    1: optional TGeoEncoding encoding
    2: optional TGeoDimension dimension
    3: optional TGeoValidationState validation_state
}

struct TScalarType {
    1: required TPrimitiveType type

    // Only set if type == CHAR or type == VARCHAR
    2: optional i32 len

    // Only set for DECIMAL
    3: optional i32 precision
    4: optional i32 scale

    // Only meaningful for DATETIME read from lake formats that distinguish
    // timestamp-without-time-zone (NTZ) from timestamp-with-local-time-zone. Rides along
    // as metadata and does NOT affect type identity. Default (false) means the value is a
    // UTC instant that must be shifted into the session timezone (Hive/Iceberg/Paimon LTZ);
    // Paimon TIMESTAMP sets it to true so the reader keeps the wall clock unshifted.
    5: optional bool datetime_is_ntz

    // Only meaningful for native GEOGRAPHY/GEOMETRY values. These descriptors
    // deliberately separate logical semantics from the canonical WKB payload.
    6: optional TGeoTypeDesc geo_type_desc
    7: optional TGeoStorageDesc geo_storage_desc
}

// Represents a field in a STRUCT type.
// TODO: Model column stats for struct fields.
struct TStructField {
    1: optional string name
    2: optional string comment
    3: optional i32 id
    // physical_name is used to store the physical name of the field in the storage layer.
    // for example, the physical name of a struct field in a parquet file.
    // used in delta lake column mapping name mode
    4: optional string physical_name
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
    PERCENTILE_UNION,
    AGG_STATE_UNION
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
    REMOTE_SNAPSHOT,
    REPLICATE_SNAPSHOT,
    UPDATE_SCHEMA,
    COMPACTION_CONTROL,
    EXTERNAL_CLUSTER_SNAPSHOT,
    // Placeholder for external cluster snapshot feature.
    TABLET_RESTORE,
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
  SRJAR,
  
  // 
  PYTHON
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
  14: optional bool is_distinct = false
}

struct TTableFunction {
  1: required list<TTypeDesc> ret_types
  2: optional string symbol
  // Table function left join
  3: optional bool is_left_join
}

struct TAggStateDesc {
    1: optional string agg_func_name
    2: optional list<TTypeDesc> arg_types
    3: optional TTypeDesc ret_type
    4: optional bool result_nullable
    5: optional i32 func_version
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

  7: optional string signature // Deprecated

  // HDFS path for the function binary. This binary must exist at the time the
  // function is created.
  8: optional string hdfs_location

  // One of these should be set.
  9: optional TScalarFunction scalar_fn
  10: optional TAggregateFunction aggregate_fn

  11: optional i64 id
  12: optional string checksum
  13: optional TAggStateDesc agg_state_desc

  // Builtin Function id, used to mark the function in the vectorization engine,
  // and it's different with `id` because `id` is use for serialized and cache
  // UDF function.
  30: optional i64 fid
  31: optional TTableFunction table_fn
  32: optional bool could_apply_dict_optimize

  // Ignore nulls
  33: optional bool ignore_nulls
  34: optional bool isolated
  35: optional string input_type
  36: optional string content
  37: optional CloudConfiguration.TCloudConfiguration cloud_configuration
  // For Python UDFs: user-provided Arrow Flight worker service URL. When set, the BE connects
  // to this external worker instead of spawning a local one (see CREATE FUNCTION "service_url").
  38: optional string service_url
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

// NOTE: enum values are assigned explicitly on purpose.
// Under implicit numbering, inserting a member anywhere but the end silently
// shifts the value of every member after it, which breaks the wire format
// between mixed-version processes. Explicit values make such an insertion a
// no-op for existing members.
// Rules for this enum:
//   - append new members with the next free value; never renumber or reuse one;
//   - values >= 300 are reserved for extension fields and must not be used here.
enum TTableType {
    MYSQL_TABLE = 0,
    OLAP_TABLE = 1,
    SCHEMA_TABLE = 2,
    KUDU_TABLE = 3, // Deprecated
    BROKER_TABLE = 4,
    ES_TABLE = 5,
    HDFS_TABLE = 6,
    ICEBERG_TABLE = 7,
    HUDI_TABLE = 8,
    JDBC_TABLE = 9,
    PAIMON_TABLE = 10,
    VIEW = 20,
    MATERIALIZED_VIEW = 21,
    FILE_TABLE = 22,
    DELTALAKE_TABLE = 23,
    TABLE_FUNCTION_TABLE = 24,
    ODPS_TABLE = 25,
    LOGICAL_ICEBERG_METADATA_TABLE = 26,
    ICEBERG_REFS_TABLE = 27,
    ICEBERG_HISTORY_TABLE = 28,
    ICEBERG_METADATA_LOG_ENTRIES_TABLE = 29,
    ICEBERG_SNAPSHOTS_TABLE = 30,
    ICEBERG_MANIFESTS_TABLE = 31,
    ICEBERG_FILES_TABLE = 32,
    ICEBERG_PARTITIONS_TABLE = 33,
    BENCHMARK_TABLE = 34,
    ICEBERG_PROPERTIES_TABLE = 35,
    LANCE_TABLE = 36,
    FLUSS_TABLE = 37
}

enum TKeysType {
    PRIMARY_KEYS,
    DUP_KEYS,
    UNIQUE_KEYS,
    AGG_KEYS
}

enum TPrimaryKeyEncodingType {
    PK_ENCODING_TYPE_NONE = 0,
    PK_ENCODING_TYPE_V1 = 1,
    PK_ENCODING_TYPE_V2 = 2
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
    AUTO = 13;
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
    COLUMN_UPSERT_MODE = 2;
    AUTO_MODE = 3;
    COLUMN_UPDATE_MODE = 4;
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

enum TIcebergFileContent {
    DATA,
    POSITION_DELETES,
    EQUALITY_DELETES,
}

// Extension point for TIcebergDataFile. DO NOT MODIFY: do not add fields here,
// and do not rename, renumber or remove it. The field numbers inside are
// allocated separately, so anything added here collides with them, and
// renaming or removing it breaks whatever fills it in. New TIcebergDataFile
// fields belong on TIcebergDataFile itself, whose remaining numbers are free.
struct TIcebergDataFileExt {
}

struct TIcebergDataFile {
    1: optional string path
    2: optional string format
    3: optional i64 record_count
    4: optional i64 file_size_in_bytes
    5: optional string partition_path;
    6: optional list<i64> split_offsets;
    7: optional TIcebergColumnStats column_stats;
    8: optional string partition_null_fingerprint;
    9: optional TIcebergFileContent file_content;
    10: optional string referenced_data_file;
    11: optional TIcebergDataFileExt ext;
}

struct THiveFileInfo {
    1: optional string file_name
    2: optional string partition_path
    4: optional i64 record_count
    5: optional i64 file_size_in_bytes
}

// Extension point for TSinkCommitInfo. DO NOT MODIFY: do not add fields here,
// and do not rename, renumber or remove it. The field numbers inside are
// allocated separately, so anything added here collides with them, and
// renaming or removing it breaks whatever fills it in. New TSinkCommitInfo
// fields belong on TSinkCommitInfo itself, whose remaining numbers are free.
struct TSinkCommitInfoExt {
}

struct TSinkCommitInfo {
    1: optional TIcebergDataFile iceberg_data_file
    2: optional THiveFileInfo hive_file_info
    // ... for other tables sink commit info

    100: optional bool is_overwrite;
    101: optional string staging_dir
    102: optional bool is_rewrite;
    103: optional TSinkCommitInfoExt ext;
}

struct TSnapshotInfo {
    1: optional TBackend backend
    2: optional string snapshot_path
    3: optional bool incremental_snapshot
}

// Placeholder for external cluster snapshot feature.
struct TClusterSnapshotPartitionSpec {
    1: optional i64 db_id
    2: optional i64 table_id
    3: optional i64 partition_id
    4: optional i64 physical_partition_id
}

enum TTxnType {
    TXN_NORMAL = 0,
    TXN_REPLICATION = 1
}

enum TNodeType {
    Backend = 0,
    Compute = 1
}

struct TParquetOptions {
    // parquet row group max size in bytes
    1: optional i64 parquet_max_group_bytes
    2: optional TCompressionType compression_type
    3: optional bool use_dict
    // for files table function
    4: optional string version
}

enum TVariantType {
    NORMAL_VALUE = 0,
    NULL_VALUE = 1,
    MINIMUM = 2,
    MAXIMUM = 3,
}

struct TVariant {
    1: optional TTypeDesc type
    2: optional string value
    3: optional TVariantType variant_type
}

struct TTuple {
    1: optional list<TVariant> values
}

struct TTabletRange {
    1: optional TTuple lower_bound
    2: optional TTuple upper_bound
    3: optional bool lower_bound_included
    4: optional bool upper_bound_included
}
