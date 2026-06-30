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

#include "service/service_be/http_auth_response.h"

#include <fmt/format.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "agent/master_info.h"
#include "common/config.h"
#include "common/logging.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/FrontendService_types.h"
#include "http/http_request.h"
#include "http/utils.h"
#include "runtime/client_cache.h"
#include "util/thrift_rpc_helper.h"

namespace starrocks {

std::string build_auth_failure_body(std::string_view message) {
    rapidjson::Document doc;
    doc.SetObject();
    auto& alloc = doc.GetAllocator();
    doc.AddMember("status", rapidjson::Value("FAILED", alloc), alloc);
    doc.AddMember("code", rapidjson::Value("1", alloc), alloc);
    doc.AddMember("message", rapidjson::Value(message.data(), message.size(), alloc), alloc);
    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
    doc.Accept(writer);
    return std::string(buf.GetString(), buf.GetSize());
}

EvHttpServer::AuthVerifyFailure make_unauthorized(std::string_view message) {
    return EvHttpServer::AuthVerifyFailure{
            .http_status = HttpStatus::UNAUTHORIZED,
            .www_authenticate = "Basic realm=\"\"",
            .body = build_auth_failure_body(message),
    };
}

EvHttpServer::AuthVerifyFailure make_server_error(HttpStatus http_status, std::string_view message) {
    return EvHttpServer::AuthVerifyFailure{
            .http_status = http_status,
            .www_authenticate = {}, // no Basic challenge — re-auth won't help
            .body = build_auth_failure_body(message),
    };
}

std::optional<EvHttpServer::AuthVerifyFailure> auth_failure_from_fe_status(const Status& fe_status) {
    // FE's checkAuth distinguishes credential failure (NOT_AUTHORIZED) from
    // internal problems (INTERNAL_ERROR — e.g. an unknown TPrivilegeRequirement
    // enum value from a newer BE talking to an older FE). The two have different
    // remedies, so map them to different HTTP status classes. The 401 body comes
    // from FE (already scrubbed to "Access denied for user@host" there); the 500
    // body is generic so we don't echo internal error strings to the client.
    if (fe_status.is_not_authorized()) {
        return make_unauthorized(fe_status.message());
    }
    if (!fe_status.ok()) {
        LOG(WARNING) << "checkAuth FE returned non-OK status: " << fe_status;
        return make_server_error(HttpStatus::INTERNAL_SERVER_ERROR, "auth verification failed");
    }
    return std::nullopt;
}

TPrivilegeRequirement::type to_thrift_privilege(HttpHandler::RequiredPrivilege priv) {
    switch (priv) {
    case HttpHandler::RequiredPrivilege::NONE:
        return TPrivilegeRequirement::NONE;
    case HttpHandler::RequiredPrivilege::OPERATE:
        return TPrivilegeRequirement::OPERATE;
    case HttpHandler::RequiredPrivilege::NODE:
        return TPrivilegeRequirement::NODE;
    }
    LOG(ERROR) << "to_thrift_privilege: unknown RequiredPrivilege value " << static_cast<int>(priv)
               << ", failing closed as OPERATE";
    return TPrivilegeRequirement::OPERATE;
}

std::optional<EvHttpServer::AuthVerifyFailure> verify_http_basic_auth(
        HttpRequest* req, HttpHandler::RequiredPrivilege required_privilege) {
    if (!config::enable_http_auth) {
        return std::nullopt;
    }

    AuthInfo auth;
    if (!parse_basic_auth(*req, &auth)) {
        return make_unauthorized("missing Basic authorization");
    }

    TAuthenticateParams params;
    params.__set_user(auth.user);
    params.__set_passwd(auth.passwd);
    params.__set_host(auth.user_ip);
    if (required_privilege != HttpHandler::RequiredPrivilege::NONE) {
        params.__set_required_privilege(to_thrift_privilege(required_privilege));
    }

    TNetworkAddress master = get_master_address();
    if (master.hostname.empty() || master.port == 0) {
        LOG(WARNING) << "checkAuth: FE master address is not yet known on this BE";
        return make_server_error(HttpStatus::SERVICE_UNAVAILABLE,
                                 "auth verification failed: FE master address unknown");
    }
    TFeResult result;
    Status rpc_st = ThriftRpcHelper::rpc<FrontendServiceClient>(
            master.hostname, master.port,
            [&result, &params](FrontendServiceConnection& client) { client->checkAuth(result, params); },
            config::thrift_rpc_timeout_ms, /*retry_times=*/1);
    if (!rpc_st.ok()) {
        LOG(WARNING) << "checkAuth RPC to FE failed: " << rpc_st;
        return make_server_error(HttpStatus::SERVICE_UNAVAILABLE, "auth verification failed");
    }
    return auth_failure_from_fe_status(Status(result.status));
}

} // namespace starrocks
