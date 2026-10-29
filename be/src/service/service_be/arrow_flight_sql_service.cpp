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

#include <fstream>
#include <sstream>

#include <arrow/array/builder_binary.h>
#include <arrow/flight/server.h>
#include <arrow/flight/types.h>
#include <exec/pipeline/query_context.h>
#include <util/arrow/utils.h>

#include "common/config.h"
#include "common/status.h"
#include "exec/arrow_flight_batch_reader.h"
#include "exprs/base64.h"
#include "service/backend_options.h"
#include "util/uid_util.h"

namespace starrocks {

static Status read_pem_file(const std::string& path, std::string& content) {
    if (path.empty()) {
        return Status::InvalidArgument(
            "PEM file path is empty.");
    }

    std::ifstream input(path, std::ios::in | std::ios::binary);
    if (!input.is_open()) {
        return Status::InvalidArgument(
            "Failed to open PEM file '" + path +
            "'. Verify that the file exists and is readable.");
    }

    std::ostringstream ss;
    ss << input.rdbuf();
    content = ss.str();

    if (content.empty()) {
        return Status::InvalidArgument(
            "PEM file '" + path + "' is empty.");
    }

    return Status::OK();
}

Status ArrowFlightSqlServer::start(int port) {
    if (port <= 0) {
        LOG(INFO) << "[ARROW] Arrow Flight SQL Server is disabled. You can modify `arrow_flight_port` in `be.conf` to "
                     "a positive value to enable it.";
        return Status::OK();
    }

    const bool tls_enabled = config::arrow_flight_ssl_enable;

    arrow::flight::Location bind_location;
    if (tls_enabled) {
        RETURN_STATUS_IF_ERROR(arrow::flight::Location::ForGrpcTls(BackendOptions::get_service_bind_address(), port)
                                       .Value(&bind_location));
    } else {
        RETURN_STATUS_IF_ERROR(arrow::flight::Location::ForGrpcTcp(BackendOptions::get_service_bind_address(), port)
                                       .Value(&bind_location));
    }

    arrow::flight::FlightServerOptions flight_options(bind_location);

    if (tls_enabled) {
        if (config::arrow_flight_ssl_cert_file.empty() || config::arrow_flight_ssl_key_file.empty()) {
            return Status::InvalidArgument(
                    "arrow_flight_ssl_enable=true requires both arrow_flight_ssl_cert_file and arrow_flight_ssl_key_file");
        }

        std::string pem_cert;
        std::string pem_key;
        RETURN_IF_ERROR(read_pem_file(config::arrow_flight_ssl_cert_file, pem_cert));
        RETURN_IF_ERROR(read_pem_file(config::arrow_flight_ssl_key_file, pem_key));

        arrow::flight::CertKeyPair cert_key_pair;
        cert_key_pair.pem_cert = std::move(pem_cert);
        cert_key_pair.pem_key = std::move(pem_key);
        flight_options.tls_certificates.emplace_back(std::move(cert_key_pair));

        flight_options.verify_client = config::arrow_flight_ssl_require_client_auth;
        if (flight_options.verify_client) {
            if (config::arrow_flight_ssl_ca_cert_file.empty()) {
                return Status::InvalidArgument(
                        "arrow_flight_ssl_require_client_auth=true requires arrow_flight_ssl_ca_cert_file");
            }
            RETURN_IF_ERROR(read_pem_file(config::arrow_flight_ssl_ca_cert_file, flight_options.root_certificates));
        }
    }

    // Not authenticated in BE flight server.
    // After the authentication between the ADBC Client and the FE flight server is completed,
    // the FE flight server will put the query id in the Ticket and send it back to the Client.
    // When the Client uses the Ticket to fetch data from the BE flight server, the BE flight
    // server will verify the query id, this step is equivalent to authentication.
    _bearer_middleware = std::make_shared<NoOpBearerAuthServerMiddlewareFactory>();
    flight_options.auth_handler = std::make_shared<arrow::flight::NoOpAuthHandler>();
    flight_options.middleware.emplace_back("bearer-auth-server", _bearer_middleware);

    LOG(INFO) << "[ARROW] Arrow Flight SQL server transport=" << (tls_enabled ? "TLS" : "PLAINTEXT")
              << " [mTLS=" << (flight_options.verify_client ? "enabled" : "disabled") << "]";

    
    RETURN_STATUS_IF_ERROR(Init(flight_options));
    _running = true;

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

    auto reader = std::make_shared<ArrowFlightBatchReader>(ExecEnv::GetInstance()->result_mgr(), resultfragmentid);
    ARROW_RETURN_NOT_OK(reader->init());
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
