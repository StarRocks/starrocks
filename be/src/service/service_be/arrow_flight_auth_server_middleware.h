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

#include <arrow/status.h>

#include "arrow/flight/server.h"
#include "arrow/flight/server_middleware.h"
#include "arrow/flight/types.h"

namespace starrocks {

// Just return default bearer token.
class NoOpHeaderAuthServerMiddleware : public arrow::flight::ServerMiddleware {
public:
    void SendingHeaders(arrow::flight::AddCallHeaders* outgoing_headers) override;

    void CallCompleted(const arrow::Status& status) override {}

    [[nodiscard]] std::string name() const override { return "NoOpHeaderAuthServerMiddleware"; }
};

// Factory for base64 header authentication.
// No actual authentication.
class NoOpHeaderAuthServerMiddlewareFactory : public arrow::flight::ServerMiddlewareFactory {
public:
    NoOpHeaderAuthServerMiddlewareFactory() = default;

    arrow::Status StartCall(const arrow::flight::CallInfo& info, const arrow::flight::ServerCallContext& context,
                            std::shared_ptr<arrow::flight::ServerMiddleware>* middleware) override;
};

// A server middleware for the bearer header on the BE Flight endpoint.
// No actual authentication: the real credential is the per-query ticket verified
// in ArrowFlightSqlServer::DoGetStatement(), not this header. See the cpp file.
class NoOpBearerAuthServerMiddleware : public arrow::flight::ServerMiddleware {
public:
    NoOpBearerAuthServerMiddleware() = default;

    void SendingHeaders(arrow::flight::AddCallHeaders* outgoing_headers) override;

    void CallCompleted(const arrow::Status& status) override {}

    [[nodiscard]] std::string name() const override { return "NoOpBearerAuthServerMiddleware"; }
};

// Factory for base64 header authentication.
// No actual authentication.
class NoOpBearerAuthServerMiddlewareFactory : public arrow::flight::ServerMiddlewareFactory {
public:
    NoOpBearerAuthServerMiddlewareFactory() = default;

    arrow::Status StartCall(const arrow::flight::CallInfo& info, const arrow::flight::ServerCallContext& context,
                            std::shared_ptr<arrow::flight::ServerMiddleware>* middleware) override;
};

} // namespace starrocks
