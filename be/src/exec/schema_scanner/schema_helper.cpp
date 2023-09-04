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

#include "exec/schema_scanner/schema_helper.h"

#include <sstream>

#include "gen_cpp/FrontendService.h"
#include "gen_cpp/FrontendService_types.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/network_util.h"
#include "util/runtime_profile.h"
#include "util/thrift_rpc_helper.h"

namespace starrocks {

Status SchemaHelper::get_db_names(const std::string& ip, const int32_t port, const TGetDbsParams& request,
                                  TGetDbsResult* result) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(
            ip, port, [&request, &result](FrontendServiceConnection& client) { client->getDbNames(*result, request); });
}

Status SchemaHelper::get_table_names(const std::string& ip, const int32_t port, const TGetTablesParams& request,
                                     TGetTablesResult* result) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(
            ip, port,
            [&request, &result](FrontendServiceConnection& client) { client->getTableNames(*result, request); });
}

Status SchemaHelper::list_table_status(const std::string& ip, const int32_t port, const TGetTablesParams& request,
                                       TListTableStatusResult* result) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(
            ip, port,
            [&request, &result](FrontendServiceConnection& client) { client->listTableStatus(*result, request); });
}

Status SchemaHelper::list_materialized_view_status(const std::string& ip, const int32_t port,
                                                   const TGetTablesParams& request,
                                                   TListMaterializedViewStatusResult* result) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(ip, port,
                                                       [&request, &result](FrontendServiceConnection& client) {
                                                           client->listMaterializedViewStatus(*result, request);
                                                       });
}

Status SchemaHelper::list_pipes(const std::string& ip, int32_t port, const TListPipesParams& req,
                                TListPipesResult* res) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(
            ip, port, [&req, &res](FrontendServiceConnection& client) { client->listPipes(*res, req); });
}

Status SchemaHelper::list_pipe_files(const std::string& ip, int32_t port, const TListPipeFilesParams& req,
                                     TListPipeFilesResult* res) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(
            ip, port, [&req, &res](FrontendServiceConnection& client) { client->listPipeFiles(*res, req); });
}

Status SchemaHelper::get_tables_info(const std::string& ip, const int32_t port, const TGetTablesInfoRequest& request,
                                     TGetTablesInfoResponse* response) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(
            ip, port,
            [&request, &response](FrontendServiceConnection& client) { client->getTablesInfo(*response, request); });
}

Status SchemaHelper::describe_table(const std::string& ip, const int32_t port, const TDescribeTableParams& request,
                                    TDescribeTableResult* result) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(
            ip, port,
            [&request, &result](FrontendServiceConnection& client) { client->describeTable(*result, request); });
}

Status SchemaHelper::show_variables(const std::string& ip, const int32_t port, const TShowVariableRequest& request,
                                    TShowVariableResult* result) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(
            ip, port,
            [&request, &result](FrontendServiceConnection& client) { client->showVariables(*result, request); });
}

std::string SchemaHelper::extract_db_name(const std::string& full_name) {
    auto found = full_name.find(':');
    if (found == std::string::npos) {
        return full_name;
    }
    found++;
    return std::string(full_name.c_str() + found, full_name.size() - found);
}

Status SchemaHelper::get_user_privs(const std::string& ip, const int32_t port, const TGetUserPrivsParams& request,
                                    TGetUserPrivsResult* result) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(
            ip, port,
            [&request, &result](FrontendServiceConnection& client) { client->getUserPrivs(*result, request); });
}

Status SchemaHelper::get_db_privs(const std::string& ip, const int32_t port, const TGetDBPrivsParams& request,
                                  TGetDBPrivsResult* result) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(
            ip, port, [&request, &result](FrontendServiceConnection& client) { client->getDBPrivs(*result, request); });
}

Status SchemaHelper::get_table_privs(const std::string& ip, const int32_t port, const TGetTablePrivsParams& request,
                                     TGetTablePrivsResult* result) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(
            ip, port,
            [&request, &result](FrontendServiceConnection& client) { client->getTablePrivs(*result, request); });
}

Status SchemaHelper::get_tables_config(const std::string& ip, const int32_t port,
                                       const TGetTablesConfigRequest& var_params,
                                       TGetTablesConfigResponse* var_result) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(ip, port,
                                                       [&var_params, &var_result](FrontendServiceConnection& client) {
                                                           client->getTablesConfig(*var_result, var_params);
                                                       });
}

Status SchemaHelper::get_tasks(const std::string& ip, const int32_t port, const TGetTasksParams& var_params,
                               TGetTaskInfoResult* var_result) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(ip, port,
                                                       [&var_params, &var_result](FrontendServiceConnection& client) {
                                                           client->getTasks(*var_result, var_params);
                                                       });
}

Status SchemaHelper::get_task_runs(const std::string& ip, const int32_t port, const TGetTasksParams& var_params,
                                   TGetTaskRunInfoResult* var_result) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(ip, port,
                                                       [&var_params, &var_result](FrontendServiceConnection& client) {
                                                           client->getTaskRuns(*var_result, var_params);
                                                       });
}

Status SchemaHelper::get_loads(const std::string& ip, const int32_t port, const TGetLoadsParams& var_params,
                               TGetLoadsResult* var_result, int timeout_ms) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(
            ip, port,
            [&var_params, &var_result](FrontendServiceConnection& client) {
                client->getLoads(*var_result, var_params);
            },
            timeout_ms);
}

Status SchemaHelper::get_tracking_loads(const std::string& ip, const int32_t port, const TGetLoadsParams& var_params,
                                        TGetTrackingLoadsResult* var_result, int timeout_ms) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(
            ip, port,
            [&var_params, &var_result](FrontendServiceConnection& client) {
                client->getTrackingLoads(*var_result, var_params);
            },
            timeout_ms);
}

Status SchemaHelper::get_routine_load_jobs(const std::string& ip, const int32_t port, const TGetLoadsParams& var_params,
                                           TGetRoutineLoadJobsResult* var_result, int timeout_ms) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(
            ip, port,
            [&var_params, &var_result](FrontendServiceConnection& client) {
                client->getRoutineLoadJobs(*var_result, var_params);
            },
            timeout_ms);
}

Status SchemaHelper::get_stream_loads(const std::string& ip, const int32_t port, const TGetLoadsParams& var_params,
                                      TGetStreamLoadsResult* var_result, int timeout_ms) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(
            ip, port,
            [&var_params, &var_result](FrontendServiceConnection& client) {
                client->getStreamLoads(*var_result, var_params);
            },
            timeout_ms);
}

Status SchemaHelper::get_tablet_schedules(const std::string& ip, const int32_t port,
                                          const TGetTabletScheduleRequest& request,
                                          TGetTabletScheduleResponse* response) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(ip, port,
                                                       [&request, &response](FrontendServiceConnection& client) {
                                                           client->getTabletSchedule(*response, request);
                                                       });
}

Status SchemaHelper::get_role_edges(const std::string& ip, const int32_t port, const TGetRoleEdgesRequest& request,
                                    TGetRoleEdgesResponse* response) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(
            ip, port,
            [&request, &response](FrontendServiceConnection& client) { client->getRoleEdges(*response, request); });
}

Status SchemaHelper::get_grants_to(const std::string& ip, const int32_t port,
                                   const TGetGrantsToRolesOrUserRequest& request,
                                   TGetGrantsToRolesOrUserResponse* response, int timeout_ms) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(
            ip, port,
            [&request, &response](FrontendServiceConnection& client) { client->getGrantsTo(*response, request); },
            timeout_ms);
}

void fill_data_column_with_null(Column* data_column) {
    auto* nullable_column = down_cast<NullableColumn*>(data_column);
    nullable_column->append_nulls(1);
}

} // namespace starrocks
