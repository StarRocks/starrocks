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

#include "arrow_flight_auth_server_middleware.h"

#include "arrow_flight_call_header_utils.h"

namespace starrocks {

void NoOpHeaderAuthServerMiddleware::SendingHeaders(arrow::flight::AddCallHeaders* outgoing_headers) {
    outgoing_headers->AddHeader(kAuthHeader, std::string(kBearerPrefix) + kBearerDefaultToken);
}

arrow::Status NoOpHeaderAuthServerMiddlewareFactory::StartCall(
        const arrow::flight::CallInfo& info, const arrow::flight::ServerCallContext& context,
        std::shared_ptr<arrow::flight::ServerMiddleware>* middleware) {
    std::string username, password;
    ParseBasicHeader(context.incoming_headers(), username, password);
    *middleware = std::make_shared<NoOpHeaderAuthServerMiddleware>();
    return arrow::Status::OK();
}

void NoOpBearerAuthServerMiddleware::SendingHeaders(arrow::flight::AddCallHeaders* outgoing_headers) {}

// This factory deliberately does not gate on the bearer header: the BE Flight server is not
// the authentication boundary. The FE authenticates the ADBC client and only then mints the
// per-query ticket (query id + result fragment id) that the client presents here; verifying
// that ticket in DoGetStatement() is what actually authenticates the call. See the comment in
// ArrowFlightSqlServer::start() (arrow_flight_sql_service.cpp) for the full flow. Do not turn
// this into a real bearer-token check: callers are never told to send kBearerDefaultToken, so
// doing so would reject every legitimate request.
arrow::Status NoOpBearerAuthServerMiddlewareFactory::StartCall(
        const arrow::flight::CallInfo& info, const arrow::flight::ServerCallContext& context,
        std::shared_ptr<arrow::flight::ServerMiddleware>* middleware) {
    *middleware = std::make_shared<NoOpBearerAuthServerMiddleware>();
    return arrow::Status::OK();
}

} // namespace starrocks
