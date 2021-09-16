// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/schema_scanner/schema_helper.h"

#include <boost/thread/thread.hpp>
#include <sstream>

#include "exec/text_converter.hpp"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/FrontendService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "runtime/tuple_row.h"
#include "util/debug_util.h"
#include "util/network_util.h"
#include "util/runtime_profile.h"
#include "util/thrift_rpc_helper.h"
#include "util/thrift_util.h"

namespace starrocks::vectorized {

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

Status SchemaHelper::describe_table(const std::string& ip, const int32_t port, const TDescribeTableParams& request,
                                    TDescribeTableResult* result) {
    return ThriftRpcHelper::rpc<FrontendServiceClient>(
            ip, port,
            [&request, &result](FrontendServiceConnection& client) { client->describeTable(*result, request); });
}

Status SchemaHelper::show_varialbes(const std::string& ip, const int32_t port, const TShowVariableRequest& request,
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

void fill_data_column_with_null(vectorized::Column* data_column) {
    vectorized::NullableColumn* nullable_column = down_cast<vectorized::NullableColumn*>(data_column);
    nullable_column->append_nulls(1);
}

} // namespace starrocks::vectorized
