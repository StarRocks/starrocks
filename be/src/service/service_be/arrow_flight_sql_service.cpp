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

#include "arrow_flight_sql_service.h"

#include <arrow/array/builder_binary.h>
#include <arrow/flight/server.h>
#include <arrow/flight/types.h>
#include <exec/pipeline/query_context.h>
#include <util/arrow/utils.h>

#include "common/status.h"
#include "exec/arrow_flight_batch_reader.h"
#include "exprs/base64.h"
#include "service/backend_options.h"
#include "util/uid_util.h"

namespace starrocks {

Status ArrowFlightSqlServer::start(int port) {
    if (port <= 0) {
        LOG(INFO) << "[ARROW] Arrow Flight SQL Server is disabled. You can modify `arrow_flight_port` in `be.conf` to "
                     "a positive value to enable it.";
        return Status::OK();
    }

    _running = true;

    arrow::flight::Location bind_location;
    RETURN_STATUS_IF_ERROR(arrow::flight::Location::ForGrpcTcp(BackendOptions::get_service_bind_address(), port)
                                   .Value(&bind_location));
    arrow::flight::FlightServerOptions flight_options(bind_location);
    RETURN_STATUS_IF_ERROR(Init(flight_options));

    return Status::OK();
}

void ArrowFlightSqlServer::stop() {
    if (!_running) {
        return;
    }
    _running = false;
    if (const auto status = Shutdown(); !status.ok()) {
        LOG(INFO) << "[ARROW] Failed to stop Arrow Flight SQL Server [error=" << status << "]";
    }
}

arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> ArrowFlightSqlServer::GetFlightInfoSchemas(
        const arrow::flight::ServerCallContext& context, const arrow::flight::sql::GetDbSchemas& command,
        const arrow::flight::FlightDescriptor& descriptor) {
    return arrow::Status::NotImplemented("GetFlightInfoSchemas Result");
}

arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> ArrowFlightSqlServer::DoGetStatement(
        const arrow::flight::ServerCallContext& context, const arrow::flight::sql::StatementQueryTicket& command) {
    ARROW_ASSIGN_OR_RAISE(auto pair, decode_ticket(command.statement_handle));

    const std::string query_id = pair.first;
    const std::string result_fragment_id = pair.second;
    TUniqueId queryid;
    if (!parse_id(query_id, &queryid)) {
        return arrow::Status::Invalid("Invalid query ID format:", query_id);
    }
    TUniqueId resultfragmentid;
    if (!parse_id(result_fragment_id, &resultfragmentid)) {
        return arrow::Status::Invalid("Invalid fragment ID format:", result_fragment_id);
    }

    std::shared_ptr<ArrowFlightBatchReader> reader = std::make_shared<ArrowFlightBatchReader>(resultfragmentid);
    return std::make_unique<arrow::flight::RecordBatchStream>(reader);
}

arrow::Result<std::pair<std::string, std::string>> ArrowFlightSqlServer::decode_ticket(const std::string& ticket) {
    auto divider = ticket.find(':');
    if (divider == std::string::npos) {
        return arrow::Status::Invalid("Malformed ticket");
    }

    std::string query_id = ticket.substr(0, divider);
    std::string result_fragment_id = ticket.substr(divider + 1);

    return std::make_pair(std::move(query_id), std::move(result_fragment_id));
}

} // namespace starrocks
