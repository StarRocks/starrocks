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

#pragma once

#include "column/column.h"
#include "column/type_traits.h"
#include "common/status.h"
#include "exec/schema_scanner.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/FrontendService_types.h"
#include "runtime/datetime_value.h"
#include "types/logical_type.h"

namespace starrocks {

class FrontendServiceClient;
template <class T>
class ClientConnection;

// this class is a helper for getting schema info from FE
class SchemaHelper {
public:
    static Status get_db_names(const SchemaScannerState& state, const TGetDbsParams& request, TGetDbsResult* result);

    static Status get_table_names(const SchemaScannerState& state, const TGetTablesParams& table_params,
                                  TGetTablesResult* table_result);

    static Status list_table_status(const SchemaScannerState& state, const TGetTablesParams& table_params,
                                    TListTableStatusResult* table_result);

    static Status list_materialized_view_status(const SchemaScannerState& state, const TGetTablesParams& request,
                                                TListMaterializedViewStatusResult* result);

    static Status list_pipes(const SchemaScannerState& state, const TListPipesParams& req, TListPipesResult* res);
    static Status list_pipe_files(const SchemaScannerState& state, const TListPipeFilesParams& req,
                                  TListPipeFilesResult* res);

    static Status list_object_dependencies(const SchemaScannerState& state, const TObjectDependencyReq& req,
                                           TObjectDependencyRes* res);
    static Status list_fe_locks(const SchemaScannerState& state, const TFeLocksReq& req, TFeLocksRes* res);

    static Status list_fe_memory_usage(const SchemaScannerState& state, const TFeMemoryReq& req, TFeMemoryRes* res);

    static Status get_tables_info(const SchemaScannerState& state, const TGetTablesInfoRequest& request,
                                  TGetTablesInfoResponse* response);

    static Status get_temporary_tables_info(const SchemaScannerState& state,
                                            const TGetTemporaryTablesInfoRequest& request,
                                            TGetTemporaryTablesInfoResponse* response);

    static Status describe_table(const SchemaScannerState& state, const TDescribeTableParams& desc_params,
                                 TDescribeTableResult* desc_result);

    static Status show_variables(const SchemaScannerState& state, const TShowVariableRequest& var_params,
                                 TShowVariableResult* var_result);

    static std::string extract_db_name(const std::string& full_name);

    static Status get_user_privs(const SchemaScannerState& state, const TGetUserPrivsParams& var_params,
                                 TGetUserPrivsResult* var_result);

    static Status get_db_privs(const SchemaScannerState& state, const TGetDBPrivsParams& var_params,
                               TGetDBPrivsResult* var_result);

    static Status get_table_privs(const SchemaScannerState& state, const TGetTablePrivsParams& var_params,
                                  TGetTablePrivsResult* var_result);

    static Status get_tasks(const SchemaScannerState& state, const TGetTasksParams& var_params,
                            TGetTaskInfoResult* var_result);

    static Status get_loads(const SchemaScannerState& state, const TGetLoadsParams& var_params,
                            TGetLoadsResult* var_result);

    static Status get_tracking_loads(const SchemaScannerState& state, const TGetLoadsParams& var_params,
                                     TGetTrackingLoadsResult* var_result);

    static Status get_routine_load_jobs(const SchemaScannerState& state, const TGetLoadsParams& var_params,
                                        TGetRoutineLoadJobsResult* var_result);

    static Status get_stream_loads(const SchemaScannerState& state, const TGetLoadsParams& var_params,
                                   TGetStreamLoadsResult* var_result);

    static Status get_task_runs(const SchemaScannerState& state, const TGetTasksParams& var_params,
                                TGetTaskRunInfoResult* var_result);

    static Status get_tables_config(const SchemaScannerState& state, const TGetTablesConfigRequest& var_params,
                                    TGetTablesConfigResponse* var_result);

    static Status get_tablet_schedules(const SchemaScannerState& state, const TGetTabletScheduleRequest& request,
                                       TGetTabletScheduleResponse* response);

    static Status get_role_edges(const SchemaScannerState& state, const TGetRoleEdgesRequest& request,
                                 TGetRoleEdgesResponse* response);

    static Status get_grants_to(const SchemaScannerState& state, const TGetGrantsToRolesOrUserRequest& request,
                                TGetGrantsToRolesOrUserResponse* response);

    static Status get_partitions_meta(const SchemaScannerState& state, const TGetPartitionsMetaRequest& var_params,
                                      TGetPartitionsMetaResponse* var_result);

    static Status get_column_stats_usage(const SchemaScannerState& state, const TColumnStatsUsageReq& var_params,
                                         TColumnStatsUsageRes* var_result);

    static Status get_analyze_status(const SchemaScannerState& state, const TAnalyzeStatusReq& var_params,
                                     TAnalyzeStatusRes* var_result);

    static Status get_cluster_snapshots_info(const SchemaScannerState& state, const TClusterSnapshotsRequest& req,
                                             TClusterSnapshotsResponse* res);

    static Status get_cluster_snapshot_jobs_info(const SchemaScannerState& state,
                                                 const TClusterSnapshotJobsRequest& req,
                                                 TClusterSnapshotJobsResponse* res);

    static Status get_applicable_roles(const SchemaScannerState& state, const TGetApplicableRolesRequest& request,
                                       TGetApplicableRolesResponse* response);

    static Status get_keywords(const SchemaScannerState& state, const TGetKeywordsRequest& request,
                               TGetKeywordsResponse* response);

private:
    static Status _call_rpc(const SchemaScannerState& state,
                            std::function<void(ClientConnection<FrontendServiceClient>&)> callback);
};

template <LogicalType SlotType>
void fill_data_column_with_slot(Column* data_column, void* slot) {
    using ColumnType = typename RunTimeTypeTraits<SlotType>::ColumnType;
    using ValueType = typename RunTimeTypeTraits<SlotType>::CppType;

    ColumnType* result = down_cast<ColumnType*>(data_column);
    if constexpr (IsDate<ValueType>) {
        auto* date_time_value = (DateTimeValue*)slot;
        DateValue date_value =
                DateValue::create(date_time_value->year(), date_time_value->month(), date_time_value->day());
        result->append(date_value);
    } else if constexpr (IsTimestamp<ValueType>) {
        auto* date_time_value = (DateTimeValue*)slot;
        TimestampValue timestamp_value =
                TimestampValue::create(date_time_value->year(), date_time_value->month(), date_time_value->day(),
                                       date_time_value->hour(), date_time_value->minute(), date_time_value->second());
        result->append(timestamp_value);
    } else {
        result->append(*(ValueType*)slot);
    }
}

template <LogicalType SlotType>
void fill_column_with_slot(Column* result, void* slot) {
    if (result->is_nullable()) {
        auto* nullable_column = down_cast<NullableColumn*>(result);
        NullData& null_data = nullable_column->null_column_data();
        Column* data_column = nullable_column->data_column().get();
        null_data.push_back(0);
        fill_data_column_with_slot<SlotType>(data_column, slot);
    } else {
        fill_data_column_with_slot<SlotType>(result, slot);
    }
}

void fill_data_column_with_null(Column* data_column);

} // namespace starrocks
