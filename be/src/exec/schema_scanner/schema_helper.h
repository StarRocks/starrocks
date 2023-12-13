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
#include "gen_cpp/FrontendService_types.h"
#include "runtime/datetime_value.h"
#include "types/logical_type.h"

namespace starrocks {

// this class is a helper for getting schema info from FE
class SchemaHelper {
public:
    static Status get_db_names(const std::string& ip, const int32_t port, const TGetDbsParams& request,
                               TGetDbsResult* result, const int timeout_ms);

    static Status get_table_names(const std::string& ip, const int32_t port, const TGetTablesParams& table_params,
                                  TGetTablesResult* table_result, const int timeout_ms);

    static Status list_table_status(const std::string& ip, const int32_t port, const TGetTablesParams& table_params,
                                    TListTableStatusResult* table_result, const int timeout_ms);

    static Status list_materialized_view_status(const std::string& ip, const int32_t port,
                                                const TGetTablesParams& request,
                                                TListMaterializedViewStatusResult* result);

    static Status list_pipes(const std::string& ip, int32_t port, const TListPipesParams& req, TListPipesResult* res);
    static Status list_pipe_files(const std::string& ip, int32_t port, const TListPipeFilesParams& req,
                                  TListPipeFilesResult* res);

    static Status list_object_dependencies(const std::string& ip, int32_t port, const TObjectDependencyReq& req,
                                           TObjectDependencyRes* res);
    static Status list_fe_locks(const std::string& ip, int32_t port, const TFeLocksReq& req, TFeLocksRes* res);

    static Status get_tables_info(const std::string& ip, const int32_t port, const TGetTablesInfoRequest& request,
                                  TGetTablesInfoResponse* response, const int timeout_ms);

    static Status describe_table(const std::string& ip, const int32_t port, const TDescribeTableParams& desc_params,
                                 TDescribeTableResult* desc_result, const int timeout_ms);

    static Status show_variables(const std::string& ip, const int32_t port, const TShowVariableRequest& var_params,
                                 TShowVariableResult* var_result);

    static std::string extract_db_name(const std::string& full_name);

    static Status get_user_privs(const std::string& ip, const int32_t port, const TGetUserPrivsParams& var_params,
                                 TGetUserPrivsResult* var_result);

    static Status get_db_privs(const std::string& ip, const int32_t port, const TGetDBPrivsParams& var_params,
                               TGetDBPrivsResult* var_result);

    static Status get_table_privs(const std::string& ip, const int32_t port, const TGetTablePrivsParams& var_params,
                                  TGetTablePrivsResult* var_result);

    static Status get_tasks(const std::string& ip, const int32_t port, const TGetTasksParams& var_params,
                            TGetTaskInfoResult* var_result);

    static Status get_loads(const std::string& ip, const int32_t port, const TGetLoadsParams& var_params,
                            TGetLoadsResult* var_result, int timeout_ms);

    static Status get_tracking_loads(const std::string& ip, const int32_t port, const TGetLoadsParams& var_params,
                                     TGetTrackingLoadsResult* var_result, int timeout_ms);

    static Status get_routine_load_jobs(const std::string& ip, const int32_t port, const TGetLoadsParams& var_params,
                                        TGetRoutineLoadJobsResult* var_result, int timeout_ms);

    static Status get_stream_loads(const std::string& ip, const int32_t port, const TGetLoadsParams& var_params,
                                   TGetStreamLoadsResult* var_result, int timeout_ms);

    static Status get_task_runs(const std::string& ip, const int32_t port, const TGetTasksParams& var_params,
                                TGetTaskRunInfoResult* var_result);

    static Status get_tables_config(const std::string& ip, const int32_t port,
                                    const TGetTablesConfigRequest& var_params, TGetTablesConfigResponse* var_result);

    static Status get_tablet_schedules(const std::string& ip, const int32_t port,
                                       const TGetTabletScheduleRequest& request, TGetTabletScheduleResponse* response);

    static Status get_role_edges(const std::string& ip, const int32_t port, const TGetRoleEdgesRequest& request,
                                 TGetRoleEdgesResponse* response);

    static Status get_grants_to(const std::string& ip, const int32_t port,
                                const TGetGrantsToRolesOrUserRequest& request,
                                TGetGrantsToRolesOrUserResponse* response, int timeout_ms);
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
