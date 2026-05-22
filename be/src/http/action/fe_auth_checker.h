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

#include <string>

#include "common/status.h"

namespace starrocks {

class HttpRequest;

// Verifies HTTP Basic Auth credentials against the FE for BE-side admin HTTP
// endpoints. The credentials are forwarded to the FE leader via the
// `FrontendService.checkAuth` RPC, which checks the password and that the user
// has the requested system-level privilege on the SYSTEM object.
class FeAuthChecker {
public:
    // `required_system_privilege` must match a PrivilegeType enum name on the
    // FE side (e.g. "NODE", "OPERATE", "ADMIN").
    //
    // Returns Status::OK() if the credentials are valid and the user has the
    // requested privilege. Returns Status::NotAuthorized on bad credentials or
    // insufficient privilege (caller should respond with HTTP 401 + Basic
    // challenge). Returns Status::InternalError if the FE master address is
    // unknown or the RPC fails (caller should respond with HTTP 500).
    static Status check(HttpRequest* req, const std::string& required_system_privilege, int32_t rpc_timeout_ms = 10000);
};

} // namespace starrocks
