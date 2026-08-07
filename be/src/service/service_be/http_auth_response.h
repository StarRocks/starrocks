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

#include <optional>
#include <string>
#include <string_view>

#include "common/status.h"
#include "gen_cpp/FrontendService_types.h"
#include "http/ev_http_server.h"
#include "http/http_handler.h"
#include "http/http_status.h"

namespace starrocks {

class HttpRequest;

// Helpers for shaping `AuthVerifyFailure` responses returned by the BE-side
// AuthVerifier. Extracted from http_service.cpp so the FE-Status-to-HTTP-status
// mapping can be unit-tested without spinning up a real server / FE.

// Renders the JSON body sent on auth failure: mirrors FE's RestBaseResult
// (FAILED case) shape: {status, code, message}.
std::string build_auth_failure_body(std::string_view message);

// 401 + WWW-Authenticate. Use for genuine credential / authorization failures
// where the client can fix the issue by re-authenticating.
EvHttpServer::AuthVerifyFailure make_unauthorized(std::string_view message);

// 5xx, no WWW-Authenticate. Use for server-side problems (FE unreachable, FE
// returned an internal error). The client's credentials may be valid; sending
// 401 would mislead them into retrying the same creds and burning retries on a
// server-side fault.
EvHttpServer::AuthVerifyFailure make_server_error(HttpStatus http_status, std::string_view message);

// Maps the FE-side `checkAuth` Status to an AuthVerifyFailure (or nullopt on
// success). NOT_AUTHORIZED is the only status that maps to 401; any other
// non-OK status is treated as an internal server problem (e.g. unknown
// TPrivilegeRequirement enum value from a newer BE talking to an older FE)
// and surfaced as 500. Pure function — safe to test directly.
std::optional<EvHttpServer::AuthVerifyFailure> auth_failure_from_fe_status(const Status& fe_status);

// Maps the BE-side `RequiredPrivilege` enum to its thrift twin. Listing every
// case (no `default`) makes the compiler flag missing entries when the enum
// grows; the unreachable fallback returns OPERATE so an unmapped value still
// rejects non-operator callers without unnecessarily locking out legitimate
// db_admin holders.
TPrivilegeRequirement::type to_thrift_privilege(HttpHandler::RequiredPrivilege priv);

// The full BE auth-verifier hook. Returns `nullopt` when authentication +
// authorization both succeed (or when `enable_http_auth=false`), otherwise an
// `AuthVerifyFailure` the EvHttpServer wraps into the HTTP response. Order of
// checks: flag gate → Basic-Auth header → FE master address known → FE
// checkAuth RPC. Anything before the RPC is unit-testable; the RPC itself is
// covered by the end-to-end smoke test.
std::optional<EvHttpServer::AuthVerifyFailure> verify_http_basic_auth(
        HttpRequest* req, HttpHandler::RequiredPrivilege required_privilege);

} // namespace starrocks
