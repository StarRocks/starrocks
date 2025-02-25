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
//   https://github.com/apache/incubator-doris/blob/master/gensrc/thrift/FrontendService.thrift

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
include "Partitions.thrift"
include "InternalService.thrift"
include "PlanNodes.thrift"
include "Planner.thrift"
include "Descriptors.thrift"
include "Data.thrift"
include "Exprs.thrift"
include "RuntimeProfile.thrift"
include "MasterService.thrift"
include "AgentService.thrift"
include "ResourceUsage.thrift"
include "MVMaintenance.thrift"
include "DataCache.thrift"

// These are supporting structs for JniFrontend.java, which serves as the glue
// between our C++ execution environment and the Java frontend.

struct TSetSessionParams {
    1: required string user
}

struct TAuthenticateParams {
    1: required string user
    2: required string passwd
    3: optional string host
    4: optional string db_name
    5: optional list<string> table_names;
}

struct TColumnDesc {
  1: required string columnName
  2: required Types.TPrimitiveType columnType
  3: optional i32 columnLength
  4: optional i32 columnPrecision
  5: optional i32 columnScale
  6: optional bool allowNull
  20: optional string columnKey
  21: optional bool key
  22: optional string aggregationType
  23: optional string dbName
  24: optional string tableName
  25: optional string columnDefault
  // Let FE control the type, which makes it easier to modify and display complex types
  26: optional string columnTypeStr
  27: optional string dataType
  28: optional string generatedColumnExprStr
}

// A column definition; used by CREATE TABLE and DESCRIBE <table> statements. A column
// definition has a different meaning (and additional fields) from a column descriptor,
// so this is a separate struct from TColumnDesc.
struct TColumnDef {
  1: required TColumnDesc columnDesc
  2: optional string comment
}

// Arguments to DescribeTable, which returns a list of column descriptors for a
// given table
struct TDescribeTableParams {
  1: optional string db
  2: required string table_name
  3: optional string user   // deprecated
  4: optional string user_ip    // deprecated
  5: optional Types.TUserIdentity current_user_ident // to replace the user and user ip
  6: optional i64 limit

  // If not set, match default_catalog
  7: optional string catalog_name
}

// Results of a call to describeTable()
struct TDescribeTableResult {
  1: required list<TColumnDef> columns
}

struct TShowVariableRequest {
    1: required i64 threadId
    2: required Types.TVarType varType
}

struct TVerboseVariableRecord {
    1: optional string variable_name
    2: optional string value
    3: optional string default_value
    4: optional bool is_changed
}

// Results of a call to describeTable()
struct TShowVariableResult {
    1: required map<string, string> variables
    2: optional list<TVerboseVariableRecord> verbose_variables
}

// Valid table file formats
enum TFileFormat {
  PARQUETFILE,
  RCFILE,
  SEQUENCEFILE,
  TEXTFILE,
}

// set type
enum TSetType {
  OPT_DEFAULT,
  OPT_GLOBAL,
  OPT_SESSION,
}

// The row format specifies how to interpret the fields (columns) and lines (rows) in a
// data file when creating a new table.
struct TTableRowFormat {
  // Optional terminator string used to delimit fields (columns) in the table
  1: optional string field_terminator

  // Optional terminator string used to delimit lines (rows) in a table
  2: optional string line_terminator

  // Optional string used to specify a special escape character sequence
  3: optional string escaped_by
}


// Represents a single item in a partition spec (column name + value)
struct TPartitionKeyValue {
  // Partition column name
  1: required string name,

  // Partition value
  2: required string value
}

// Per-client session state
struct TSessionState {
  // The default database, changed by USE <database> queries.
  1: required string database

  // The user who this session belongs to.
  2: required string user

  // The user who this session belongs to.
  3: required i64 connection_id
}

struct TClientRequest {
  // select stmt to be executed
  1: required string stmt

  // query options
  2: required InternalService.TQueryOptions queryOptions

  // session state
  3: required TSessionState sessionState;
}


// Parameters for SHOW DATABASES commands
struct TExplainParams {
  // Optional pattern to match database names. If not set, all databases are returned.
  1: required string explain
}

struct TSetVar{
    1: required TSetType type
    2: required string variable
    3: required Exprs.TExpr value
}
// Parameters for Set commands
struct TSetParams {
  // Optional pattern to match database names. If not set, all databases are returned.
  1: required list<TSetVar> set_vars
}

struct TKillParams {
  // Optional pattern to match database names. If not set, all databases are returned.
  1: required bool is_kill_connection
  2: required i64 connection_id
}

struct TCommonDdlParams {
  //1: required Ddl.TCommonDdlType ddl_type
  //2: optional Ddl.TCreateDbParams create_db_params
  //3: optional Ddl.TCreateTableParams create_table_params
  //4: optional Ddl.TLoadParams load_params
}

// Parameters for the USE db command
struct TUseDbParams {
  1: required string db
}

struct TResultSetMetadata {
  1: required list<TColumnDesc> columnDescs
}

// Result of call to JniFrontend.CreateQueryRequest()
struct TQueryExecRequest {
  // global descriptor tbl for all fragments
  1: optional Descriptors.TDescriptorTable desc_tbl

  // fragments[i] may consume the output of fragments[j > i];
  // fragments[0] is the root fragment and also the coordinator fragment, if
  // it is unpartitioned.
  2: required list<Planner.TPlanFragment> fragments

  // Specifies the destination fragment of the output of each fragment.
  // parent_fragment_idx.size() == fragments.size() - 1 and
  // fragments[i] sends its output to fragments[dest_fragment_idx[i-1]]
  3: optional list<i32> dest_fragment_idx

  // A map from scan node ids to a list of scan range locations.
  // The node ids refer to scan nodes in fragments[].plan_tree
  4: optional map<Types.TPlanNodeId, list<Planner.TScanRangeLocations>>
      per_node_scan_ranges

  // Metadata of the query result set (only for select)
  5: optional TResultSetMetadata result_set_metadata

  7: required InternalService.TQueryGlobals query_globals

  // The statement type governs when the coordinator can judge a query to be finished.
  // DML queries are complete after Wait(), SELECTs may not be.
  9: required Types.TStmtType stmt_type

  // The statement type governs when the coordinator can judge a query to be finished.
  // DML queries are complete after Wait(), SELECTs may not be.
  10: optional bool is_block_query;
}

enum TDdlType {
  USE,
  DESCRIBE,
  SET,
  EXPLAIN,
  KILL,
  COMMON
}

struct TDdlExecRequest {
  1: required TDdlType ddl_type

  // Parameters for USE commands
  2: optional TUseDbParams use_db_params;

  // Parameters for DESCRIBE table commands
  3: optional TDescribeTableParams describe_table_params

  10: optional TExplainParams explain_params

  11: optional TSetParams set_params
  12: optional TKillParams kill_params
  //13: optional Ddl.TMasterDdlRequest common_params
}

// Results of an EXPLAIN
struct TExplainResult {
    // each line in the explain plan occupies an entry in the list
    1: required list<Data.TResultRow> results
}

// Result of call to createExecRequest()
struct TExecRequest {
  1: required Types.TStmtType stmt_type;

  2: optional string sql_stmt;

  // Globally unique id for this request. Assigned by the planner.
  3: required Types.TUniqueId request_id

  // Copied from the corresponding TClientRequest
  4: required InternalService.TQueryOptions query_options;

  // TQueryExecRequest for the backend
  // Set iff stmt_type is QUERY or DML
  5: optional TQueryExecRequest query_exec_request

  // Set iff stmt_type is DDL
  6: optional TDdlExecRequest ddl_exec_request

  // Metadata of the query result set (not set for DML)
  7: optional TResultSetMetadata result_set_metadata

  // Result of EXPLAIN. Set iff stmt_type is EXPLAIN
  8: optional TExplainResult explain_result
}

// Arguments to getDbNames, which returns a list of dbs that match an optional
// pattern
struct TGetDbsParams {
  // If not set, match every database
  1: optional string pattern
  2: optional string user   // deprecated
  3: optional string user_ip    // deprecated
  4: optional Types.TUserIdentity current_user_ident // to replace the user and user ip

  // If not set, match default_catalog
  5: optional string catalog_name
}

// getDbNames returns a list of database names
struct TGetDbsResult {
  1: list<string> dbs
}

// Arguments to getTableNames, which returns a list of tables that match an
// optional pattern.
struct TGetTablesParams {
  // If not set, match tables in all DBs
  1: optional string db

  // If not set, match every table
  2: optional string pattern
  3: optional string user   // deprecated
  4: optional string user_ip    // deprecated
  5: optional Types.TUserIdentity current_user_ident // to replace the user and user ip
  20: optional Types.TTableType type // getting a certain type of tables
  21: optional i64 limit

  // If not set, match default_catalog
  22: optional string catalog_name
  23: optional string table_name
}

struct TTableStatus {
    1: required string name
    2: required string type
    3: required string comment
    4: optional string engine
    5: optional i64 last_check_time
    6: optional i64 create_time
    20: optional string ddl_sql
}

struct TListTableStatusResult {
    1: required list<TTableStatus> tables
}

struct TMaterializedViewStatus {
    1: optional string id
    2: optional string database_name
    3: optional string name
    4: optional string refresh_type
    5: optional string is_active
    6: optional string last_refresh_start_time
    7: optional string last_refresh_finished_time
    8: optional string last_refresh_duration
    9: optional string last_refresh_state
    10: optional string last_refresh_error_code
    11: optional string last_refresh_error_message
    12: optional string text
    13: optional string rows

    14: optional string partition_type
    15: optional string last_refresh_force_refresh
    16: optional string last_refresh_start_partition
    17: optional string last_refresh_end_partition
    18: optional string last_refresh_base_refresh_partitions
    19: optional string last_refresh_mv_refresh_partitions

    20: optional i64 last_check_time
    21: optional i64 create_time
    22: optional string ddl_sql

    23: optional string task_id
    24: optional string task_name
    25: optional string inactive_reason

    26: optional string extra_message
    27: optional string query_rewrite_status

    28: optional string creator
}

struct TListPipesParams {
    1: optional Types.TUserIdentity user_ident
}

struct TListPipesInfo {
    // pipe entity
    1: optional i64 pipe_id
    2: optional string pipe_name
    3: optional string properties

    // schema info
    10: optional string database_name
    11: optional string table_name

    // pipe status and statistics
    20: optional string state

    // pipe statistics
    30: optional i64 loaded_files
    31: optional i64 loaded_rows
    32: optional i64 loaded_bytes
    33: optional string load_status
    34: optional string last_error
    35: optional i64 created_time
}

struct TListPipesResult {
    1: optional list<TListPipesInfo> pipes;
}

struct TListPipeFilesParams {
    1: optional Types.TUserIdentity user_ident
}

struct TListPipeFilesInfo {
    // pipe entity
    1: optional i64 pipe_id
    2: optional string pipe_name
    3: optional string database_name


    // file entity
    10: optional string file_name
    11: optional string file_version
    12: optional string state
    13: optional i64 file_size
    14: optional i64 file_rows
    15: optional string last_modified

    // load status
    20: optional string staged_time
    21: optional string start_load
    22: optional string finish_load

    // error information
    30: optional string first_error_msg
    31: optional i64 error_count
    32: optional i64 error_line
}

struct TListPipeFilesResult {
    1: optional list<TListPipeFilesInfo> pipe_files
}

struct TListMaterializedViewStatusResult {
    1: optional list<TMaterializedViewStatus> materialized_views
}

// Arguments to showTasks/ShowTaskRuns
struct TGetTasksParams {
    1: optional string db
    2: optional Types.TUserIdentity current_user_ident
    // task's name
    3: optional string task_name
    // task run's query_id
    4: optional string query_id
    // task's state
    5: optional string state
}

struct TTaskInfo {
    1: optional string task_name
    2: optional i64 create_time
    3: optional string schedule
    4: optional string database
    5: optional string definition
    6: optional i64 expire_time
    7: optional string properties
    8: optional string catalog
    9: optional string creator
}

struct TGetTaskInfoResult {
    1: required list<TTaskInfo> tasks
}

struct TTaskRunInfo {
    1: optional string query_id
    2: optional string task_name
    3: optional i64 create_time
    4: optional i64 finish_time
    5: optional string state
    6: optional string database
    7: optional string definition
    8: optional i64 expire_time
    9: optional i32 error_code
    10: optional string error_message
    11: optional string progress

    12: optional string extra_message
    13: optional string properties

    14: optional string catalog
}

struct TGetTaskRunInfoResult {
    1: optional list<TTaskRunInfo> task_runs
}

struct TGetLoadsParams {
    1: optional string db
    2: optional i64 job_id
    3: optional i64 txn_id
    4: optional string label
    5: optional string load_type
}

struct TTrackingLoadInfo {
    1: optional i64 job_id
    2: optional string label
    3: optional string db
    4: optional list<string> urls
    5: optional string load_type
}

struct TGetTrackingLoadsResult {
    1: optional list<TTrackingLoadInfo> trackingLoads;
}

struct TLoadInfo {
    1: optional i64 job_id
    2: optional string label
    3: optional string state
    4: optional string progress
    5: optional string type
    6: optional string priority
    7: optional string etl_info
    8: optional string task_info
    9: optional string create_time
    10: optional string etl_start_time
    11: optional string etl_finish_time
    12: optional string load_start_time
    13: optional string load_finish_time
    14: optional string url
    15: optional string job_details
    16: optional string error_msg
    17: optional string db
    18: optional i64 txn_id
    19: optional string tracking_sql
    20: optional i64 num_scan_rows
    21: optional i64 num_filtered_rows
    22: optional i64 num_unselected_rows
    23: optional i64 num_sink_rows
    24: optional string rejected_record_path
    25: optional string load_id
    26: optional string profile_id
    27: optional string table
    28: optional string user
    29: optional string load_commit_time
    30: optional string warehouse
    31: optional string runtime_details
    32: optional string properties
    33: optional i64 num_scan_bytes
}

struct TGetLoadsResult {
    1: optional list<TLoadInfo> loads
}

struct TRoutineLoadJobInfo {
    1: optional i64 id
    2: optional string name
    3: optional string create_time
    4: optional string pause_time
    5: optional string end_time
    6: optional string db_name
    7: optional string table_name
    8: optional string state
    9: optional string data_source_type
    10: optional i64 current_task_num
    11: optional string job_properties
    12: optional string data_source_properties
    13: optional string custom_properties
    14: optional string statistic
    15: optional string progress
    16: optional string reasons_of_state_changed
    17: optional string error_log_urls
    18: optional string tracking_sql
    19: optional string other_msg
}

struct TGetRoutineLoadJobsResult {
    1: optional list<TRoutineLoadJobInfo> loads
}

struct TStreamLoadInfo {
    1: string label,
    2: i64 id,
    3: string load_id,
    4: i64 txn_id,
    5: string db_name,
    6: string table_name,
    7: string state,
    8: string error_msg,
    9: string tracking_url,
    10: i64 channel_num,
    11: i64 prepared_channel_num,
    12: i64 num_rows_normal,
    13: i64 num_rows_ab_normal,
    14: i64 num_rows_unselected,
    15: i64 num_load_bytes,
    16: i64 timeout_second,
    17: string create_time_ms,
    18: string before_load_time_ms,
    19: string start_loading_time_ms,
    20: string start_preparing_time_ms,
    21: string finish_preparing_time_ms,
    22: string end_time_ms,
    23: string channel_state,
    24: string type
    25: string tracking_sql,

}

struct TGetStreamLoadsResult {
    1: optional list<TStreamLoadInfo> loads
}

// getTableNames returns a list of unqualified table names
struct TGetTablesResult {
  1: list<string> tables
}

struct TBatchReportExecStatusResult {
  // required in V1
  1: optional list<Status.TStatus> status_list
}

struct TReportExecStatusResult {
  // required in V1
  1: optional Status.TStatus status
}

struct TReportAuditStatisticsResult {
  // required in V1
  1: optional Status.TStatus status
}

// Service Protocol Details
enum FrontendServiceVersion {
  V1
}

// The results of an INSERT query, sent to the coordinator as part of
// TReportExecStatusParams
struct TReportExecStatusParams {
  1: required FrontendServiceVersion protocol_version

  // required in V1
  2: optional Types.TUniqueId query_id

  // passed into ExecPlanFragment() as TExecPlanFragmentParams.backend_num
  // required in V1
  3: optional i32 backend_num

  // required in V1
  4: optional Types.TUniqueId fragment_instance_id

  // Status of fragment execution; any error status means it's done.
  // required in V1
  5: optional Status.TStatus status

  // If true, fragment finished executing.
  // required in V1
  6: optional bool done

  // cumulative profile
  // required in V1
  7: optional RuntimeProfile.TRuntimeProfileTree profile

  // New errors that have not been reported to the coordinator
  // optional in V1
  9: optional list<string> error_log // Decrepated

  // URL of files need to load
  // optional
  10: optional list<string> delta_urls
  11: optional map<string, string> load_counters
  12: optional string tracking_url

  // export files
  13: optional list<string> export_files

  14: optional list<Types.TTabletCommitInfo> commitInfos

  15: optional i64 loaded_rows

  16: optional i64 backend_id

  17: optional i64 sink_load_bytes

  18: optional i64 source_load_rows

  19: optional i64 source_load_bytes

  20: optional InternalService.TLoadJobType load_type

  21: optional list<Types.TTabletFailInfo> failInfos

  22: optional i64 filtered_rows

  23: optional i64 unselected_rows

  24: optional i64 source_scan_bytes

  25: optional list<Types.TSinkCommitInfo> sink_commit_infos

  27: optional string rejected_record_path

  28: optional RuntimeProfile.TRuntimeProfileTree load_channel_profile;

  29: optional DataCache.TLoadDataCacheMetrics load_datacache_metrics
}

struct TBatchReportExecStatusParams {
  1: required list<TReportExecStatusParams> params_list
}

struct TReportFragmentFinishParams {
    1: optional Types.TUniqueId query_id
    2: optional Types.TUniqueId fragment_instance_id
    4: optional i32 backend_num
}

struct TReportFragmentFinishResponse {
    1: optional Status.TStatus status
}

struct TAuditStatisticsItem {
    1: optional i64 scan_rows
    2: optional i64 scan_bytes
    3: optional i64 table_id
}

struct TAuditStatistics {
    3: optional i64 scan_rows
    4: optional i64 scan_bytes
    5: optional i64 returned_rows
    6: optional i64 cpu_cost_ns
    7: optional i64 mem_cost_bytes
    8: optional i64 spill_bytes
    9: optional list<TAuditStatisticsItem> stats_items
}

struct TReportAuditStatisticsParams {
    1: optional Types.TUniqueId query_id
    2: optional Types.TUniqueId fragment_instance_id
    3: optional TAuditStatistics audit_statistics
}

struct TFeResult {
    1: required FrontendServiceVersion protocolVersion
    2: required Status.TStatus status
}

struct TUpdateMiniEtlTaskStatusRequest {
    1: required FrontendServiceVersion protocolVersion
    2: required Types.TUniqueId etlTaskId
    3: required AgentService.TMiniLoadEtlStatusResult etlTaskStatus
}

struct TMasterOpRequest {
    1: required string user
    2: required string db
    3: required string sql
    4: optional Types.TResourceInfo resourceInfo
    5: optional string cluster
    6: optional i64 execMemLimit // deprecated, move into query_options
    7: optional i32 queryTimeout // deprecated, move into query_options
    8: optional string user_ip
    9: optional string time_zone
    10: optional i64 stmt_id
    11: optional i64 sqlMode
    12: optional i64 loadMemLimit // deprecated, move into query_options
    13: optional bool enableStrictMode
    // this can replace the "user" field
    14: optional Types.TUserIdentity current_user_ident
    15: optional i32 stmtIdx  // the idx of the sql in multi statements
    16: optional InternalService.TQueryOptions query_options
    17: optional string catalog

    // Following is added by StarRocks
    // TODO(zc): Should forward all session variables and connection context
    30: optional Types.TUniqueId queryId
    31: optional bool isLastStmt
    32: optional string modified_variables_sql
    33: optional Types.TUserRoles user_roles
    34: optional i32 forward_times
    35: optional string session_id
    36: optional i32 connectionId

    101: optional i64 warehouse_id    // begin from 101, in case of conflict with other's change
}

struct TColumnDefinition {
    1: required string columnName;
    2: required Types.TColumnType columnType;
    3: optional Types.TAggregationType aggType;
    4: optional string defaultValue;
}

struct TShowResultSetMetaData {
    1: required list<TColumnDefinition> columns;
}

struct TShowResultSet {
    1: required TShowResultSetMetaData metaData;
    2: required list<list<string>> resultRows;
}

struct TMasterOpResult {
    1: required i64 maxJournalId;
    2: required binary packet;
    // for show statement
    3: optional TShowResultSet resultSet;
    4: optional string state;
    // for query statement
    5: optional list<binary> channelBufferList;

    6: optional string resource_group_name;
    7: optional TAuditStatistics audit_statistics;
    8: optional string errorMsg;
}

struct TIsMethodSupportedRequest {
    1: optional string function_name
}

struct TMiniLoadBeginResult {
    1: required Status.TStatus status
    2: optional i64 txn_id
}

struct TUpdateExportTaskStatusRequest {
    1: required FrontendServiceVersion protocolVersion
    2: required Types.TUniqueId taskId
    3: required InternalService.TExportStatusResult taskStatus
}

struct TLoadTxnBeginRequest {
    1: optional string cluster
    2: required string user
    3: required string passwd
    4: required string db
    5: required string tbl
    6: optional string user_ip
    7: required string label
    8: optional i64 timestamp   // deprecated, use request_id instead
    9: optional i64 auth_code
    // The real value of timeout should be i32. i64 ensures the compatibility of interface.
    10: optional i64 timeout
    11: optional Types.TUniqueId request_id

    // begin from 101, in case of conflict with other's change
    101: optional string warehouse  // deprecated, use backend_id implicitly convey information about the warehouse
    102: optional i64 backend_id
}

struct TLoadTxnBeginResult {
    1: required Status.TStatus status
    2: optional i64 txnId
    3: optional string job_status // if label already used, set status of existing job
    4: optional i64 timeout
}

// StreamLoad request, used to load a streaming to engine
struct TStreamLoadPutRequest {
    1: optional string cluster
    2: required string user
    3: required string passwd
    4: required string db
    5: required string tbl
    6: optional string user_ip

    // and use this to assgin to OlapTableSink
    7: required Types.TUniqueId loadId
    8: required i64 txnId

    9: required Types.TFileType fileType
    10: required PlanNodes.TFileFormatType formatType

    // only valid when file_type is FILE_LOCAL
    11: optional string path

    // describe how table's column map to field in source file
    // slot descriptor stands for field of source file
    12: optional string columns
    // filters that applied on data
    13: optional string where
    // only valid when file type is CSV
    14: optional string columnSeparator

    15: optional string partitions
    16: optional i64 auth_code
    17: optional bool negative
    18: optional i32 timeout
    19: optional bool strictMode
    20: optional string timezone
    21: optional i64 loadMemLimit
    22: optional bool isTempPartition
    23: optional bool strip_outer_array
    24: optional string jsonpaths
    25: optional i64 thrift_rpc_timeout_ms
    26: optional string json_root
    27: optional bool partial_update
    28: optional string transmission_compression_type
    29: optional i32 load_dop
    30: optional bool enable_replicated_storage
    31: optional string merge_condition
    32: optional i64 log_rejected_record_num
    // only valid when file type is CSV
    50: optional string rowDelimiter
    // only valid when file type is CSV
    51: optional i64 skipHeader
    // only valid when file type is CSV
    52: optional bool trimSpace
    // only valid when file type is CSV
    53: optional byte enclose
    // only valid when file type is CSV
    54: optional byte escape
    55: optional Types.TPartialUpdateMode partial_update_mode
    56: optional string payload_compression_type

    // begin from 101, in case of conflict with other's change
    101: optional string warehouse  // deprecated, use backend_id implicitly convey information about the warehouse
    102: optional i64 backend_id
}

struct TStreamLoadPutResult {
    1: required Status.TStatus status
    // valid when status is OK
    2: optional InternalService.TExecPlanFragmentParams params
}

struct TMergeCommitRequest {
    1: optional string db
    2: optional string tbl
    3: optional string user
    4: optional string passwd
    5: optional string user_ip
    6: optional i64 backend_id
    7: optional string backend_host;
    8: optional map<string, string> params;
}

struct TMergeCommitResult {
    1: optional Status.TStatus status;
    // only valid for success
    2: optional string label;
}

struct TKafkaRLTaskProgress {
    1: required map<i32,i64> partitionCmtOffset
    2: optional map<i32,i64> partitionCmtOffsetTimestamp
}

struct TPulsarRLTaskProgress {
    1: required map<string,i64> partitionBacklogNum
}

struct TRLTaskTxnCommitAttachment {
    1: required Types.TLoadSourceType loadSourceType
    2: required Types.TUniqueId id
    3: required i64 jobId
    4: optional i64 loadedRows
    5: optional i64 filteredRows
    6: optional i64 unselectedRows
    7: optional i64 receivedBytes
    8: optional i64 loadedBytes
    9: optional i64 loadCostMs
    10: optional TKafkaRLTaskProgress kafkaRLTaskProgress
    11: optional string errorLogUrl
    12: optional TPulsarRLTaskProgress pulsarRLTaskProgress
}

struct TMiniLoadTxnCommitAttachment {
    1: required i64 loadedRows
    2: required i64 filteredRows
    3: optional string errorLogUrl
}

struct TManualLoadTxnCommitAttachment {
    1: optional i64 loadedRows
    2: optional i64 filteredRows
    3: optional string errorLogUrl
    4: optional i64 receivedBytes
    5: optional i64 loadedBytes
    6: optional i64 unselectedRows
    7: optional i64 planTime
    8: optional i64 receiveDataTime
    9: optional i64 beginTxnTime
}

struct TTxnCommitAttachment {
    1: required Types.TLoadType loadType
    2: optional TRLTaskTxnCommitAttachment rlTaskTxnCommitAttachment
    3: optional TMiniLoadTxnCommitAttachment mlTxnCommitAttachment
    10: optional TManualLoadTxnCommitAttachment manualLoadTxnCommitAttachment
}

struct TLoadTxnCommitRequest {
    1: optional string cluster
    2: required string user
    3: required string passwd
    4: required string db
    5: required string tbl
    6: optional string user_ip
    7: required i64 txnId
    8: required bool sync
    9: optional list<Types.TTabletCommitInfo> commitInfos
    10: optional i64 auth_code
    11: optional TTxnCommitAttachment txnCommitAttachment
    12: optional i64 thrift_rpc_timeout_ms
    13: optional list<Types.TTabletFailInfo> failInfos
}

struct TLoadTxnCommitResult {
    1: required Status.TStatus status
    // If the error code is SR_EAGAIN, the BE will retry
    // the commit after waiting for retry_interval_ms
    2: optional i64 retry_interval_ms
}

struct TLoadTxnRollbackRequest {
    1: optional string cluster
    2: required string user
    3: required string passwd
    4: required string db
    5: required string tbl
    6: optional string user_ip
    7: required i64 txnId
    8: optional string reason
    9: optional i64 auth_code
    10: optional TTxnCommitAttachment txnCommitAttachment
    11: optional list<Types.TTabletFailInfo> failInfos
    12: optional list<Types.TTabletCommitInfo> commitInfos
}

struct TGetLoadTxnStatusResult {
    1: required Status.TTransactionStatus status
    2: optional string reason;
}

struct TGetLoadTxnStatusRequest {
    1: optional string cluster
    2: required string user
    3: required string passwd
    4: required string db
    5: required string tbl
    6: optional string user_ip
    7: optional i64 auth_code
    8: required i64 txnId
}

struct TLoadTxnRollbackResult {
    1: required Status.TStatus status
}

struct TSnapshotLoaderReportRequest {
    1: required i64 job_id
    2: required i64 task_id
    3: required Types.TTaskType task_type
    4: optional i32 finished_num
    5: optional i32 total_num
}

// Arguments to getUserPrivs, which returns a list of user privileges.
struct TGetUserPrivsParams {
  1: optional Types.TUserIdentity current_user_ident // to replace the user and user ip
}

struct TUserPrivDesc {
  1: optional string user_ident_str
  2: optional string priv
  3: optional bool is_grantable
}

struct TGetUserPrivsResult {
  1: optional list<TUserPrivDesc> user_privs
}

// Arguments to getDBPrivs, which returns a list of database privileges.
struct TGetDBPrivsParams {
  1: optional Types.TUserIdentity current_user_ident // to replace the user and user ip
}

struct TDBPrivDesc {
  1: optional string user_ident_str
  2: optional string priv
  3: optional string db_name
  4: optional bool is_grantable
}

struct TGetDBPrivsResult {
  1: optional list<TDBPrivDesc> db_privs
}

// Arguments to getTablePrivs, which returns a list of table privileges.
struct TGetTablePrivsParams {
  1: optional Types.TUserIdentity current_user_ident // to replace the user and user ip
}

struct TTablePrivDesc {
  1: optional string user_ident_str
  2: optional string priv
  3: optional string db_name
  4: optional string table_name
  5: optional bool is_grantable
}

struct TGetTablePrivsResult {
  1: optional list<TTablePrivDesc> table_privs
}

struct TRefreshTableRequest {
  1: optional string db_name
  2: optional string table_name
  3: optional list<string> partitions
  4: optional string catalog_name
}

struct TRefreshTableResponse {
    1: required Status.TStatus status
}

struct TGetTableMetaRequest {
    1: optional string db_name
    2: optional string table_name
    3: optional TAuthenticateParams auth_info
}

struct TBackendMeta {
    1: optional i64 backend_id
    2: optional string host
    3: optional i32 be_port
    4: optional i32 rpc_port
    5: optional i32 http_port
    6: optional bool alive
    7: optional i32  state
}

struct TReplicaMeta {
    1: optional i64 replica_id
    2: optional i64 backend_id
    3: optional i32 schema_hash
    4: optional i64 version
    5: optional i64 version_hash // Deprecated
    6: optional i64 data_size
    7: optional i64 row_count
    8: optional string state
    9: optional i64 last_failed_version
    10: optional i64 last_failed_version_hash // Deprecated
    11: optional i64 last_failed_time
    12: optional i64 last_success_version
    13: optional i64 last_success_version_hash // Deprecated
    14: optional i64 version_count
    15: optional i64 path_hash
    16: optional bool bad
}

struct TTabletMeta {
    1: optional i64 tablet_id
    2: optional i64 db_id
    3: optional i64 table_id
    4: optional i64 partition_id
    5: optional i64 index_id
    6: optional Types.TStorageMedium storage_medium
    7: optional i32 old_schema_hash
    8: optional i32 new_schema_hash
    9: optional i64 checked_version
    10: optional i64 checked_version_hash // Deprecated
    11: optional bool consistent
    12: optional list<TReplicaMeta> replicas
}

struct TIndexInfo {
    1: optional string index_name
    2: optional list<string> columns
    3: optional string index_type
    4: optional string comment
}

struct TColumnMeta {
  1: optional string columnName
  2: optional Types.TTypeDesc columnType
  3: optional i32 columnLength
  4: optional i32 columnPrecision
  5: optional i32 columnScale
  6: optional string columnKey
  7: optional bool key
  8: optional string aggregationType
  9: optional string comment
  10: optional bool allowNull
  11: optional string defaultValue
}

struct TSchemaMeta {
    1: optional list<TColumnMeta> columns
    2: optional i32 schema_version
    3: optional i32 schema_hash
    4: optional i16 short_key_col_count
    5: optional Types.TStorageType storage_type
    6: optional string keys_type
}

enum TIndexState {
    NORMAL,
    SHADOW,
}

struct TIndexMeta {
    1: optional i64 index_id
    2: optional i64 partition_id
    3: optional TIndexState index_state
    4: optional i64 row_count
    5: optional i64 rollup_index_id
    6: optional i64 rollup_finished_version
    7: optional TSchemaMeta schema_meta
    8: optional list<TTabletMeta> tablets
}

struct TDataProperty {
    1: optional Types.TStorageMedium storage_medium
    2: optional i64 cold_time
}

struct TBasePartitionDesc {
    1: optional map<i64, i16> replica_num_map
    2: optional map<i64, bool> in_memory_map
    3: optional map<i64, TDataProperty> data_property
}

struct TSinglePartitionDesc {
    1: optional TBasePartitionDesc base_desc
}

// one single partition range
struct TRange {
    1: optional i64 partition_id
    2: optional TBasePartitionDesc base_desc
    3: optional binary start_key
    4: optional binary end_key
    5: optional bool is_temp
}

struct TRangePartitionDesc {
    // partition keys
    1: optional list<TColumnMeta> columns
    // partition ranges
    2: optional map<i64, TRange> ranges
}

struct TPartitionInfo {
    1: optional Partitions.TPartitionType type
    2: optional TSinglePartitionDesc single_partition_desc
    3: optional TRangePartitionDesc  range_partition_desc
}

struct TPartitionMeta {
    1: optional i64 partition_id
    2: optional string partition_name
    3: optional string state
    4: optional i64 commit_version_hash // Deprecated
    5: optional i64 visible_version
    6: optional i64 visible_version_hash // Deprecated
    7: optional i64 visible_time
    8: optional i64 next_version
    9: optional i64 next_version_hash // Deprecated
    10: optional bool is_temp
}

struct THashDistributionInfo {
    1: optional i32 bucket_num
    2: optional list<string> distribution_columns
}

struct TRandomDistributionInfo {
    1: optional i32 bucket_num
}

struct TDistributionDesc {
    1: optional string distribution_type
    2: optional THashDistributionInfo hash_distribution
    3: optional TRandomDistributionInfo random_distribution
}

struct TTableMeta {
    1: optional i64 table_id
    2: optional string table_name
    3: optional i64 db_id
    4: optional string db_name
    5: optional i32 cluster_id
    6: optional string state
    7: optional double bloomfilter_fpp
    8: optional i64 base_index_id
    9: optional string key_type
    10: optional TDistributionDesc distribution_desc
    11: optional map<string, string> properties
    12: optional list<TIndexMeta> indexes
    13: optional TPartitionInfo partition_info
    14: optional list<TPartitionMeta> partitions
    15: optional list<TIndexInfo> index_infos
    16: optional string colocate_group
    17: optional list<string> bloomfilter_columns
    18: optional string table_type
}

struct TGetTableMetaResponse {
    1: optional Status.TStatus status
    2: optional TTableMeta table_meta
    3: optional list<TBackendMeta> backends
}

struct TBeginRemoteTxnRequest {
    1: optional i64 db_id
    2: optional list<i64> table_ids
    3: optional string label
    4: optional i32 source_type
    5: optional i64 timeout_second
    6: optional TAuthenticateParams auth_info
}

struct TBeginRemoteTxnResponse {
    1: optional Status.TStatus status
    2: optional string txn_label
    3: optional i64 txn_id
}

struct TCommitRemoteTxnRequest {
    1: optional i64 txn_id
    2: optional i64 db_id
    3: optional TAuthenticateParams auth_info
    4: optional i32 commit_timeout_ms
    5: optional list<Types.TTabletCommitInfo> commit_infos
    6: optional TTxnCommitAttachment commit_attachment
    7: optional list<Types.TTabletFailInfo> fail_infos
}

struct TCommitRemoteTxnResponse {
    1: optional Status.TStatus status
}

struct TAbortRemoteTxnRequest {
    1: optional i64 txn_id
    2: optional i64 db_id
    3: optional string error_msg
    4: optional TAuthenticateParams auth_info
    5: optional list<Types.TTabletCommitInfo> commit_infos
    6: optional TTxnCommitAttachment commit_attachment
    7: optional list<Types.TTabletFailInfo> fail_infos
}

struct TAbortRemoteTxnResponse {
    1: optional Status.TStatus status
}

struct TSetConfigRequest {
    1: optional list<string> keys
    2: optional list<string> values
    3: optional bool is_persistent
    4: optional string user_identity
}

struct TSetConfigResponse {
    1: required Status.TStatus status
}

struct TCreatePartitionRequest {
    1: optional i64 txn_id
    2: optional i64 db_id
    3: optional i64 table_id
    // for each partition column's partition values
    4: optional list<list<string>> partition_values
    5: optional bool is_temp
}

struct TCreatePartitionResult {
    1: optional Status.TStatus status
    2: optional list<Descriptors.TOlapTablePartition> partitions
    3: optional list<Descriptors.TTabletLocation> tablets
    4: optional list<Descriptors.TNodeInfo> nodes
}

struct TImmutablePartitionRequest {
    1: optional i64 txn_id
    2: optional i64 db_id
    3: optional i64 table_id
    4: optional list<i64> partition_ids

    // begin from 101, in case of conflict with other's change
    101: optional i64 backend_id
}

struct TImmutablePartitionResult {
    1: optional Status.TStatus status
    2: optional list<Descriptors.TOlapTablePartition> partitions
    3: optional list<Descriptors.TTabletLocation> tablets
    4: optional list<Descriptors.TNodeInfo> nodes
}

struct TAuthInfo {
    // If not set, match every database
    1: optional string pattern
    2: optional string user   // deprecated
    3: optional string user_ip    // deprecated
    4: optional Types.TUserIdentity current_user_ident // to replace the user and user ip

    // If not set, match default_catalog
    5: optional string catalog_name
}

struct TGetTablesConfigRequest {
    1: optional TAuthInfo auth_info
}

struct TTableConfigInfo {
    1: optional string table_schema
    2: optional string table_name
    3: optional string table_engine
    4: optional string table_model
    5: optional string primary_key
    6: optional string partition_key
    7: optional string distribute_key
    8: optional string distribute_type
    9: optional i32 distribute_bucket
    10: optional string sort_key
    11: optional string properties
    12: optional i64 table_id
}

struct TGetTablesConfigResponse {
    1: optional list<TTableConfigInfo> tables_config_infos
}

struct TGetPartitionsMetaRequest {
    1: optional TAuthInfo auth_info
    // get partitions where table id >= start_table_id_offset
    2: optional i64 start_table_id_offset;
}

struct TPartitionMetaInfo {
    1: optional string db_name
    2: optional string table_name
    3: optional string partition_name
    4: optional i64 partition_id
    5: optional i64 compact_version
    6: optional i64 visible_version
    7: optional i64 visible_version_time
    8: optional i64 next_version
    9: optional string partition_key
    10: optional string partition_value
    11: optional string distribution_key
    12: optional i32 buckets
    13: optional i32 replication_num
    14: optional string storage_medium
    15: optional i64 cooldown_time
    16: optional i64 last_consistency_check_time
    17: optional bool is_in_memory
    18: optional bool is_temp
    19: optional string data_size
    20: optional i64 row_count
    21: optional bool enable_datacache
    22: optional double avg_cs
    23: optional double p50_cs
    24: optional double max_cs
    25: optional string storage_path
    26: optional i64 data_version
    27: optional i64 version_epoch
    28: optional Types.TTxnType version_txn_type = Types.TTxnType.TXN_NORMAL
}

struct TGetPartitionsMetaResponse {
    1: optional list<TPartitionMetaInfo> partitions_meta_infos
    // max table id in partitions_meta_infos + 1, if set to 0, it means reaches end
    2: optional i64 next_table_id_offset;
}

struct TGetTablesInfoRequest {
    1: optional TAuthInfo auth_info
    2: optional string table_name;
}

struct TTableInfo {
    1: optional string table_catalog
    2: optional string table_schema
    3: optional string table_name
    4: optional string table_type
    5: optional string engine
    6: optional i64 version
    7: optional string row_format
    8: optional i64 table_rows
    9: optional i64 avg_row_length
    10: optional i64 data_length
    11: optional i64 max_data_length
    12: optional i64 index_length
    13: optional i64 data_free
    14: optional i64 auto_increment
    15: optional i64 create_time
    16: optional i64 update_time
    17: optional i64 check_time
    18: optional string table_collation
    19: optional i64 checksum
    20: optional string create_options
    21: optional string table_comment
    22: optional string session_id
    23: optional i64 table_id
}

struct TGetTablesInfoResponse {
    1: optional list<TTableInfo> tables_infos
}

struct TGetTemporaryTablesInfoRequest {
    1: optional TAuthInfo auth_info
    // only for no predicate and limit parameter is set
    2: optional i64 limit
}

struct TGetTemporaryTablesInfoResponse {
    1: optional list<TTableInfo> tables_infos
}

struct TTabletSchedule {
    1: optional i64 table_id
    2: optional i64 partition_id
    3: optional i64 tablet_id
    4: optional string type
    5: optional string priority
    6: optional string state
    7: optional string tablet_status
    8: optional double create_time
    9: optional double schedule_time
    10: optional double finish_time
    11: optional i64 clone_src
    12: optional i64 clone_dest
    13: optional i64 clone_bytes
    14: optional double clone_duration
    15: optional string error_msg
}

struct TGetTabletScheduleRequest {
    1: optional i64 table_id
    2: optional i64 partition_id
    3: optional i64 tablet_id
    4: optional string type
    5: optional string state
    6: optional i64 limit
    7: optional Types.TUserIdentity current_user_ident
}

struct TGetTabletScheduleResponse {
    1: optional list<TTabletSchedule> tablet_schedules
}

struct TUpdateResourceUsageRequest {
    1: optional i64 backend_id 
    2: optional ResourceUsage.TResourceUsage resource_usage
}

struct TUpdateResourceUsageResponse {
    1: optional Status.TStatus status
}

struct TGetQueryStatisticsRequest {
}

struct TQueryStatisticsInfo {
    1: optional i64 queryStartTime
    2: optional string feIp
    3: optional string queryId
    4: optional string connId
    5: optional string db
    6: optional string user
    7: optional i64 cpuCostNs
    8: optional i64 scanBytes
    9: optional i64 scanRows
    10: optional i64 memUsageBytes
    11: optional i64 spillBytes
    12: optional i64 execTime
    13: optional string wareHouseName
    14: optional string customQueryId
    15: optional string resourceGroupName
}

struct TGetQueryStatisticsResponse {
    1: optional Status.TStatus status
    2: optional list<TQueryStatisticsInfo> queryStatistics_infos;
}

struct TResourceLogicalSlot {
    1: optional Types.TUniqueId slot_id
    2: optional string request_fe_name
    3: optional i64 group_id
    4: optional i32 num_slots
    5: optional i64 expired_pending_time_ms
    6: optional i64 expired_allocated_time_ms
    7: optional i64 fe_start_time_ms

    100: optional i32 num_fragments
    101: optional i32 pipeline_dop
}

struct TRequireSlotRequest {
    1: optional TResourceLogicalSlot slot
}

struct TRequireSlotResponse {

}

struct TFinishSlotRequirementRequest {
    1: optional Status.TStatus status
    2: optional Types.TUniqueId slot_id

    100: optional i32 pipeline_dop
}

struct TFinishSlotRequirementResponse {
    1: optional Status.TStatus status
}

struct TReleaseSlotRequest {
    1: optional Types.TUniqueId slot_id
}

struct TReleaseSlotResponse {
    1: optional Status.TStatus status
}

struct TGetWarehousesRequest {
}

struct TWarehouseInfo {
    1: optional string warehouse
    2: optional i64 id
    3: optional i64 num_unfinished_query_jobs
    4: optional i64 num_unfinished_load_jobs
    5: optional i64 num_unfinished_backup_jobs
    6: optional i64 num_unfinished_restore_jobs
    7: optional i64 last_finished_job_timestamp_ms
}

struct TGetWarehousesResponse {
    1: optional Status.TStatus status
    2: optional list<TWarehouseInfo> warehouse_infos;
}

struct TAllocateAutoIncrementIdParam {
    1: optional i64 table_id
    2: optional i64 rows
}

struct TAllocateAutoIncrementIdResult {
    1: optional i64 auto_increment_id
    2: optional i64 allocated_rows
    3: optional Status.TStatus status
}

struct TGetRoleEdgesRequest {

}

struct TGetRoleEdgesItem {
    1: optional string from_role
    2: optional string to_role
    3: optional string to_user
}
struct TGetRoleEdgesResponse {
    1: optional list<TGetRoleEdgesItem> role_edges
}

struct TObjectDependencyItem {
    1: optional i64 object_id
    2: optional string object_name
    3: optional string database
    4: optional string catalog
    5: optional string object_type
    
    11: optional i64 ref_object_id
    12: optional string ref_object_name
    13: optional string ref_database
    14: optional string ref_catalog
    15: optional string ref_object_type
}

struct TObjectDependencyReq {
    1: optional TAuthInfo auth_info
}

struct TObjectDependencyRes {
    1: optional list<TObjectDependencyItem> items
}

struct TFeLocksItem {
    1: optional string lock_type
    2: optional string lock_object
    3: optional string lock_mode
    4: optional i64 start_time
    5: optional i64 hold_time_ms
    
    11: optional string thread_info
    12: optional bool granted
    14: optional string waiter_list
}

struct TFeLocksReq {
    1: optional TAuthInfo auth_info
}

struct TFeLocksRes {
    1: optional list<TFeLocksItem> items
}

struct TFeMemoryItem {
    1: optional string module_name
    2: optional string class_name
    3: optional i64 current_consumption
    4: optional i64 peak_consumption
    5: optional string counter_info
}

struct TFeMemoryReq {
    1: optional TAuthInfo auth_info
}

struct TFeMemoryRes {
    1: optional list<TFeMemoryItem> items
}

struct TColumnStatsUsageReq {
    1: optional TAuthInfo auth_info
    2: optional string table_catalog
    3: optional string table_database
    4: optional string table_name
}

struct TColumnStatsUsage {
    1: optional string table_catalog
    2: optional string table_database
    3: optional string table_name
    4: optional string column_name
    5: optional string usage
    6: optional i64 last_used
    7: optional i64 created
}

struct TColumnStatsUsageRes {
    1: optional list<TColumnStatsUsage> items;
}

struct TAnalyzeStatusReq {
    1: optional TAuthInfo auth_info
    2: optional string table_catalog
    3: optional string table_database
    4: optional string table_name
}

struct TAnalyzeStatusItem {
    1: optional string id
    2: optional string catalog_name
    3: optional string database_name
    4: optional string table_name
    5: optional string columns
    6: optional string type
    7: optional string schedule
    8: optional string status
    9: optional string start_time
    10: optional string end_time
    11: optional string properties
    12: optional string reason
}

struct TAnalyzeStatusRes {
    1: optional list<TAnalyzeStatusItem> items
}

enum TGrantsToType {
    ROLE,
    USER,
}

struct TGetGrantsToRolesOrUserRequest {
    1: optional TGrantsToType type;
}

struct TGetGrantsToRolesOrUserItem {
    1: optional string grantee
    2: optional string object_catalog
    3: optional string object_database
    4: optional string object_name
    5: optional string object_type
    6: optional string privilege_type
    7: optional bool is_grantable
}

struct TGetGrantsToRolesOrUserResponse {
    1: optional list<TGetGrantsToRolesOrUserItem> grants_to
}

struct TGetProfileRequest {
    1: optional list<string> query_id
}

struct TGetProfileResponse {
    1: optional Status.TStatus status
    2: optional list<string> query_result
}

struct TGetDictQueryParamRequest {
    1: optional string db_name
    2: optional string table_name
}

struct TGetDictQueryParamResponse {
  1: required Descriptors.TOlapTableSchemaParam schema
  2: required Descriptors.TOlapTablePartitionParam partition
  3: required Descriptors.TOlapTableLocationParam location
  4: required Descriptors.TNodesInfo nodes_info
}

struct TReplicaReplicationInfo {
    1: optional Types.TBackend src_backend
}

struct TTabletReplicationInfo {
    1: optional i64 tablet_id
    2: optional i64 src_tablet_id
    3: optional list<TReplicaReplicationInfo> replica_replication_infos
}

struct TIndexReplicationInfo {
    1: optional i64 index_id
    2: optional i32 src_schema_hash
    3: optional map<i64, TTabletReplicationInfo> tablet_replication_infos
}

struct TPartitionReplicationInfo {
    1: optional i64 partition_id
    2: optional i64 src_version
    3: optional map<i64, TIndexReplicationInfo> index_replication_infos
    4: optional i64 src_version_epoch
}

struct TTableReplicationRequest {
    1: optional string username
    2: optional string password
    3: optional i64 database_id
    4: optional i64 table_id
    5: optional string src_token
    6: optional Types.TTableType src_table_type
    7: optional i64 src_table_data_size
    8: optional map<i64, TPartitionReplicationInfo> partition_replication_infos
    9: optional string job_id
}

struct TTableReplicationResponse {
    1: optional Status.TStatus status
}

struct TReportLakeCompactionRequest {
    1: optional i64 txn_id
}

struct TReportLakeCompactionResponse {
    1: optional bool valid
}

struct TListSessionsOptions {
    1: optional bool temporary_table_only;
}

struct TListSessionsRequest {
    1: optional TListSessionsOptions options;
}

struct TSessionInfo {
    1: optional string session_id;
}

struct TListSessionsResponse {
    1: optional Status.TStatus status;
    2: optional list<TSessionInfo> sessions;
}

struct TGetKeysRequest {
}

struct TGetKeysResponse {
    1: optional list<binary> key_metas;
}

struct TStartCheckpointRequest {
    1: optional i64 epoch;
    2: optional i64 journal_id;
    3: optional bool is_global_state_mgr;
}

struct TStartCheckpointResponse {
    1: optional Status.TStatus status;
}

struct TFinishCheckpointRequest {
    1: optional i64 journal_id;
    2: optional string node_name;
    3: optional bool is_success;
    4: optional string message;
    5: optional bool is_global_state_mgr;
}

struct TFinishCheckpointResponse {
    1: optional Status.TStatus status;
}

// Batch fetching partition meta info by a list of tablet ids
// FIXME: add auth info to the request, so the API will be secured
struct TPartitionMetaRequest {
    1: optional list<i64> tablet_ids;
}

struct TPartitionMetaResponse {
    1: optional Status.TStatus status;
    // (tablet_id -> array index in partition_metas)
    // In case of partial failure, the failed tablet_id will not be in the map
    2: optional map<i64,i32> tablet_id_partition_meta_index;
    // all the partition meta info found for the tablets
    3: optional list<TPartitionMeta> partition_metas;
}

struct TClusterSnapshotsItem {
    1: optional string snapshot_name;
    2: optional string snapshot_type;
    3: optional i64 created_time;
    4: optional i64 fe_jouranl_id;
    5: optional i64 starmgr_jouranl_id;
    6: optional string properties;
    7: optional string storage_volume;
    8: optional string storage_path;
}

struct TClusterSnapshotsRequest {
}

struct TClusterSnapshotsResponse {
    1: optional list<TClusterSnapshotsItem> items;
}

struct TClusterSnapshotJobsItem {
    1: optional string snapshot_name;
    2: optional i64 job_id;
    3: optional i64 created_time;
    4: optional i64 finished_time;
    5: optional string state;
    6: optional string detail_info;
    7: optional string error_message;
}

struct TClusterSnapshotJobsRequest {
}

struct TClusterSnapshotJobsResponse {
    1: optional list<TClusterSnapshotJobsItem> items;
}

service FrontendService {
    TGetDbsResult getDbNames(1:TGetDbsParams params)
    TGetTablesResult getTableNames(1:TGetTablesParams params)
  
    TGetTablesInfoResponse getTablesInfo(1: TGetTablesInfoRequest request)

    TGetTablesConfigResponse getTablesConfig(1: TGetTablesConfigRequest request)

    TGetUserPrivsResult getUserPrivs(1:TGetUserPrivsParams params)
    TGetDBPrivsResult getDBPrivs(1:TGetDBPrivsParams params)
    TGetTablePrivsResult getTablePrivs(1:TGetTablePrivsParams params)

    TGetKeysResponse getKeys(1:TGetKeysRequest params);

    TGetLoadsResult getLoads(1:TGetLoadsParams params)
    TGetTrackingLoadsResult getTrackingLoads(1:TGetLoadsParams params)
    TGetRoutineLoadJobsResult getRoutineLoadJobs(1:TGetLoadsParams params)
    TGetStreamLoadsResult getStreamLoads(1:TGetLoadsParams params)

    TGetProfileResponse getQueryProfile(1:TGetProfileRequest request)

    TDescribeTableResult describeTable(1:TDescribeTableParams params)
    TShowVariableResult showVariables(1:TShowVariableRequest params)
    TReportExecStatusResult reportExecStatus(1:TReportExecStatusParams params)
    TReportAuditStatisticsResult reportAuditStatistics(1:TReportAuditStatisticsParams params)
    TBatchReportExecStatusResult batchReportExecStatus(1:TBatchReportExecStatusParams params)

    MasterService.TMasterResult finishTask(1:MasterService.TFinishTaskRequest request)
    MasterService.TMasterResult report(1:MasterService.TReportRequest request)
    
    // Deprecated
    MasterService.TFetchResourceResult fetchResource()

    //NOTE: Do not add numbers to the parameters, otherwise it will cause compatibility problems
    TFeResult isMethodSupported(TIsMethodSupportedRequest request)

    //NOTE: Do not add numbers to the parameters, otherwise it will cause compatibility problems
    TMasterOpResult forward(TMasterOpRequest params)

    TListTableStatusResult listTableStatus(1:TGetTablesParams params)
    TListMaterializedViewStatusResult listMaterializedViewStatus(1:TGetTablesParams params)
    TListPipesResult listPipes(1: TListPipesParams params)
    TListPipeFilesResult listPipeFiles(1: TListPipeFilesParams params)

    TGetTaskInfoResult getTasks(1:TGetTasksParams params)
    TGetTaskRunInfoResult getTaskRuns(1:TGetTasksParams params)

    TFeResult updateExportTaskStatus(1:TUpdateExportTaskStatusRequest request)

    TLoadTxnBeginResult loadTxnBegin(1: TLoadTxnBeginRequest request)
    TLoadTxnCommitResult loadTxnCommit(1: TLoadTxnCommitRequest request)
    TLoadTxnRollbackResult loadTxnRollback(1: TLoadTxnRollbackRequest request)
    TLoadTxnCommitResult loadTxnPrepare(1: TLoadTxnCommitRequest request)

    TStreamLoadPutResult streamLoadPut(1: TStreamLoadPutRequest request)

    TMergeCommitResult requestMergeCommit(1: TMergeCommitRequest request)

    Status.TStatus snapshotLoaderReport(1: TSnapshotLoaderReportRequest request)

    TRefreshTableResponse refreshTable(1:TRefreshTableRequest request)

    TGetTableMetaResponse getTableMeta(1: TGetTableMetaRequest request)

    TBeginRemoteTxnResponse  beginRemoteTxn(1: TBeginRemoteTxnRequest request)
    TCommitRemoteTxnResponse commitRemoteTxn(1: TCommitRemoteTxnRequest request)
    TAbortRemoteTxnResponse  abortRemoteTxn(1: TAbortRemoteTxnRequest request)

    TSetConfigResponse setConfig(1: TSetConfigRequest request)
    TCreatePartitionResult createPartition(1: TCreatePartitionRequest request)
    TImmutablePartitionResult updateImmutablePartition(1: TImmutablePartitionRequest request)

    TUpdateResourceUsageResponse updateResourceUsage(1: TUpdateResourceUsageRequest request)

    TGetWarehousesResponse getWarehouses(1: TGetWarehousesRequest request)

    TGetQueryStatisticsResponse getQueryStatistics(1: TGetQueryStatisticsRequest request)

    // For Materialized View
    MVMaintenance.TMVReportEpochResponse mvReport(1: MVMaintenance.TMVMaintenanceTasks request)

    TAllocateAutoIncrementIdResult allocAutoIncrementId (1:TAllocateAutoIncrementIdParam params)

    TGetTabletScheduleResponse getTabletSchedule(1: TGetTabletScheduleRequest request)

    TGetRoleEdgesResponse getRoleEdges(1: TGetRoleEdgesRequest request)
    TGetGrantsToRolesOrUserResponse getGrantsTo(1: TGetGrantsToRolesOrUserRequest request)

    // sys.object_dependencies
    TObjectDependencyRes listObjectDependencies(1: TObjectDependencyReq request)

    // sys.fe_locks
    TFeLocksRes listFeLocks(1: TFeLocksReq request)

    // sys.fe_memory_usage
    TFeMemoryRes listFeMemoryUsage(1: TFeMemoryReq request)

    // information_schema.column_stats_uage
    TColumnStatsUsageRes getColumnStatsUsage(1: TColumnStatsUsageReq request)
    // information_schema.analyze_status
    TAnalyzeStatusRes getAnalyzeStatus(1: TAnalyzeStatusReq request)

    TRequireSlotResponse requireSlotAsync(1: TRequireSlotRequest request)
    TFinishSlotRequirementResponse finishSlotRequirement(1: TFinishSlotRequirementRequest request)
    TReleaseSlotResponse releaseSlot(1: TReleaseSlotRequest request)

    TGetLoadTxnStatusResult getLoadTxnStatus(1: TGetLoadTxnStatusRequest request)

    TGetDictQueryParamResponse getDictQueryParam(1: TGetDictQueryParamRequest request)

    TTableReplicationResponse startTableReplication(1: TTableReplicationRequest request)

    TGetPartitionsMetaResponse getPartitionsMeta(1: TGetPartitionsMetaRequest request)

    TReportLakeCompactionResponse reportLakeCompaction(1: TReportLakeCompactionRequest request)

    TListSessionsResponse listSessions(1: TListSessionsRequest request)
    TGetTemporaryTablesInfoResponse getTemporaryTablesInfo(1: TGetTemporaryTablesInfoRequest request)

    TReportFragmentFinishResponse reportFragmentFinish(TReportFragmentFinishParams request)

    TStartCheckpointResponse startCheckpoint(1: TStartCheckpointRequest request)

    TFinishCheckpointResponse finishCheckpoint(1: TFinishCheckpointRequest request)

    TPartitionMetaResponse getPartitionMeta(TPartitionMetaRequest request)

    TClusterSnapshotsResponse getClusterSnapshotsInfo(1: TClusterSnapshotsRequest request)
    TClusterSnapshotJobsResponse getClusterSnapshotJobsInfo(1: TClusterSnapshotJobsRequest request)
}

