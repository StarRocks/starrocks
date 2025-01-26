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
#include <utility>

#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/network_util.h"
#include "util/runtime_profile.h"
#include "util/thrift_rpc_helper.h"

namespace starrocks {

Status SchemaHelper::_call_rpc(const SchemaScannerState& state,
                               std::function<void(ClientConnection<FrontendServiceClient>&)> callback) {
    DCHECK(state.param);
    SCOPED_TIMER((state.param)->_rpc_timer);
    return ThriftRpcHelper::rpc<FrontendServiceClient>(state.ip, state.port, std::move(callback), state.timeout_ms);
}

Status SchemaHelper::get_db_names(const SchemaScannerState& state, const TGetDbsParams& request,
                                  TGetDbsResult* result) {
    return _call_rpc(state,
                     [&request, &result](FrontendServiceConnection& client) { client->getDbNames(*result, request); });
}

Status SchemaHelper::get_table_names(const SchemaScannerState& state, const TGetTablesParams& request,
                                     TGetTablesResult* result) {
    return _call_rpc(
            state, [&request, &result](FrontendServiceConnection& client) { client->getTableNames(*result, request); });
}

Status SchemaHelper::list_table_status(const SchemaScannerState& state, const TGetTablesParams& request,
                                       TListTableStatusResult* result) {
    return _call_rpc(state, [&request, &result](FrontendServiceConnection& client) {
        client->listTableStatus(*result, request);
    });
}

Status SchemaHelper::list_materialized_view_status(const SchemaScannerState& state, const TGetTablesParams& request,
                                                   TListMaterializedViewStatusResult* result) {
    return _call_rpc(state, [&request, &result](FrontendServiceConnection& client) {
        client->listMaterializedViewStatus(*result, request);
    });
}

Status SchemaHelper::list_pipes(const SchemaScannerState& state, const TListPipesParams& req, TListPipesResult* res) {
    return _call_rpc(state, [&req, &res](FrontendServiceConnection& client) { client->listPipes(*res, req); });
}

Status SchemaHelper::list_pipe_files(const SchemaScannerState& state, const TListPipeFilesParams& req,
                                     TListPipeFilesResult* res) {
    return _call_rpc(state, [&req, &res](FrontendServiceConnection& client) { client->listPipeFiles(*res, req); });
}

Status SchemaHelper::list_object_dependencies(const SchemaScannerState& state, const TObjectDependencyReq& req,
                                              TObjectDependencyRes* res) {
    return _call_rpc(state,
                     [&req, &res](FrontendServiceConnection& client) { client->listObjectDependencies(*res, req); });
}

Status SchemaHelper::list_fe_locks(const SchemaScannerState& state, const TFeLocksReq& req, TFeLocksRes* res) {
    return _call_rpc(state, [&req, &res](FrontendServiceConnection& client) { client->listFeLocks(*res, req); });
}

Status SchemaHelper::list_fe_memory_usage(const SchemaScannerState& state, const TFeMemoryReq& req, TFeMemoryRes* res) {
    return _call_rpc(state, [&req, &res](FrontendServiceConnection& client) { client->listFeMemoryUsage(*res, req); });
}

Status SchemaHelper::get_tables_info(const SchemaScannerState& state, const TGetTablesInfoRequest& request,
                                     TGetTablesInfoResponse* response) {
    return _call_rpc(state, [&request, &response](FrontendServiceConnection& client) {
        client->getTablesInfo(*response, request);
    });
}

Status SchemaHelper::get_temporary_tables_info(const SchemaScannerState& state,
                                               const TGetTemporaryTablesInfoRequest& request,
                                               TGetTemporaryTablesInfoResponse* response) {
    return _call_rpc(state, [&request, &response](FrontendServiceConnection& client) {
        client->getTemporaryTablesInfo(*response, request);
    });
}

Status SchemaHelper::describe_table(const SchemaScannerState& state, const TDescribeTableParams& request,
                                    TDescribeTableResult* result) {
    return _call_rpc(
            state, [&request, &result](FrontendServiceConnection& client) { client->describeTable(*result, request); });
}

Status SchemaHelper::show_variables(const SchemaScannerState& state, const TShowVariableRequest& request,
                                    TShowVariableResult* result) {
    return _call_rpc(
            state, [&request, &result](FrontendServiceConnection& client) { client->showVariables(*result, request); });
}

std::string SchemaHelper::extract_db_name(const std::string& full_name) {
    auto found = full_name.find(':');
    if (found == std::string::npos) {
        return full_name;
    }
    found++;
    return std::string(full_name.c_str() + found, full_name.size() - found);
}

Status SchemaHelper::get_user_privs(const SchemaScannerState& state, const TGetUserPrivsParams& request,
                                    TGetUserPrivsResult* result) {
    return _call_rpc(
            state, [&request, &result](FrontendServiceConnection& client) { client->getUserPrivs(*result, request); });
}

Status SchemaHelper::get_db_privs(const SchemaScannerState& state, const TGetDBPrivsParams& request,
                                  TGetDBPrivsResult* result) {
    return _call_rpc(state,
                     [&request, &result](FrontendServiceConnection& client) { client->getDBPrivs(*result, request); });
}

Status SchemaHelper::get_table_privs(const SchemaScannerState& state, const TGetTablePrivsParams& request,
                                     TGetTablePrivsResult* result) {
    return _call_rpc(
            state, [&request, &result](FrontendServiceConnection& client) { client->getTablePrivs(*result, request); });
}

Status SchemaHelper::get_tables_config(const SchemaScannerState& state, const TGetTablesConfigRequest& var_params,
                                       TGetTablesConfigResponse* var_result) {
    return _call_rpc(state, [&var_params, &var_result](FrontendServiceConnection& client) {
        client->getTablesConfig(*var_result, var_params);
    });
}

Status SchemaHelper::get_tasks(const SchemaScannerState& state, const TGetTasksParams& var_params,
                               TGetTaskInfoResult* var_result) {
    return _call_rpc(state, [&var_params, &var_result](FrontendServiceConnection& client) {
        client->getTasks(*var_result, var_params);
    });
}

Status SchemaHelper::get_task_runs(const SchemaScannerState& state, const TGetTasksParams& var_params,
                                   TGetTaskRunInfoResult* var_result) {
    return _call_rpc(state, [&var_params, &var_result](FrontendServiceConnection& client) {
        client->getTaskRuns(*var_result, var_params);
    });
}

Status SchemaHelper::get_loads(const SchemaScannerState& state, const TGetLoadsParams& var_params,
                               TGetLoadsResult* var_result) {
    return _call_rpc(state, [&var_params, &var_result](FrontendServiceConnection& client) {
        client->getLoads(*var_result, var_params);
    });
}

Status SchemaHelper::get_tracking_loads(const SchemaScannerState& state, const TGetLoadsParams& var_params,
                                        TGetTrackingLoadsResult* var_result) {
    return _call_rpc(state, [&var_params, &var_result](FrontendServiceConnection& client) {
        client->getTrackingLoads(*var_result, var_params);
    });
}

Status SchemaHelper::get_routine_load_jobs(const SchemaScannerState& state, const TGetLoadsParams& var_params,
                                           TGetRoutineLoadJobsResult* var_result) {
    return _call_rpc(state, [&var_params, &var_result](FrontendServiceConnection& client) {
        client->getRoutineLoadJobs(*var_result, var_params);
    });
}

Status SchemaHelper::get_stream_loads(const SchemaScannerState& state, const TGetLoadsParams& var_params,
                                      TGetStreamLoadsResult* var_result) {
    return _call_rpc(state, [&var_params, &var_result](FrontendServiceConnection& client) {
        client->getStreamLoads(*var_result, var_params);
    });
}

Status SchemaHelper::get_tablet_schedules(const SchemaScannerState& state, const TGetTabletScheduleRequest& request,
                                          TGetTabletScheduleResponse* response) {
    return _call_rpc(state, [&request, &response](FrontendServiceConnection& client) {
        client->getTabletSchedule(*response, request);
    });
}

Status SchemaHelper::get_role_edges(const SchemaScannerState& state, const TGetRoleEdgesRequest& request,
                                    TGetRoleEdgesResponse* response) {
    return _call_rpc(state, [&request, &response](FrontendServiceConnection& client) {
        client->getRoleEdges(*response, request);
    });
}

Status SchemaHelper::get_grants_to(const SchemaScannerState& state, const TGetGrantsToRolesOrUserRequest& request,
                                   TGetGrantsToRolesOrUserResponse* response) {
    return _call_rpc(state, [&request, &response](FrontendServiceConnection& client) {
        client->getGrantsTo(*response, request);
    });
}

Status SchemaHelper::get_partitions_meta(const SchemaScannerState& state, const TGetPartitionsMetaRequest& var_params,
                                         TGetPartitionsMetaResponse* var_result) {
    return _call_rpc(state, [&var_params, &var_result](FrontendServiceConnection& client) {
        client->getPartitionsMeta(*var_result, var_params);
    });
}

Status SchemaHelper::get_cluster_snapshots_info(const SchemaScannerState& state, const TClusterSnapshotsRequest& req,
                                                TClusterSnapshotsResponse* res) {
    return _call_rpc(state,
                     [&req, &res](FrontendServiceConnection& client) { client->getClusterSnapshotsInfo(*res, req); });
}

Status SchemaHelper::get_cluster_snapshot_jobs_info(const SchemaScannerState& state,
                                                    const TClusterSnapshotJobsRequest& req,
                                                    TClusterSnapshotJobsResponse* res) {
    return _call_rpc(
            state, [&req, &res](FrontendServiceConnection& client) { client->getClusterSnapshotJobsInfo(*res, req); });
}

void fill_data_column_with_null(Column* data_column) {
    auto* nullable_column = down_cast<NullableColumn*>(data_column);
    nullable_column->append_nulls(1);
}

} // namespace starrocks
