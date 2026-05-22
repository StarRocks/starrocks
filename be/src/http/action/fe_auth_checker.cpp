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

#include "http/action/fe_auth_checker.h"

#include <string>

#include "common/system/master_info.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/FrontendService_types.h"
#include "http/http_auth.h"
#include "http/http_request.h"
#include "runtime/thrift_rpc_helper.h"

namespace starrocks {

Status FeAuthChecker::check(HttpRequest* req, const std::string& required_system_privilege, int32_t rpc_timeout_ms) {
    std::string user;
    std::string passwd;
    if (!parse_basic_auth(*req, &user, &passwd)) {
        return Status::NotAuthorized("missing or malformed HTTP Basic Authorization header");
    }

    TNetworkAddress master_addr = get_master_address();
    if (master_addr.hostname.empty() || master_addr.port == 0) {
        return Status::InternalError("FE master address is not yet known on this BE");
    }

    TCheckAuthRequest auth_req;
    auth_req.__set_user(user);
    auth_req.__set_passwd(passwd);
    if (req->remote_host() != nullptr) {
        auth_req.__set_host(req->remote_host());
    }
    auth_req.__set_required_system_privilege(required_system_privilege);

    TCheckAuthResponse auth_resp;
    Status rpc_status = ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&auth_req, &auth_resp](FrontendServiceConnection& client) { client->checkAuth(auth_resp, auth_req); },
            rpc_timeout_ms);
    if (!rpc_status.ok()) {
        return Status::InternalError("FE checkAuth RPC failed: " + std::string(rpc_status.message()));
    }

    if (!auth_resp.__isset.status) {
        return Status::InternalError("FE checkAuth returned no status");
    }
    const TStatus& fe_status = auth_resp.status;
    if (fe_status.status_code == TStatusCode::OK) {
        return Status::OK();
    }
    std::string err_msg = fe_status.error_msgs.empty() ? "access denied" : fe_status.error_msgs.front();
    if (fe_status.status_code == TStatusCode::NOT_AUTHORIZED) {
        return Status::NotAuthorized(err_msg);
    }
    return Status::InternalError(err_msg);
}

} // namespace starrocks
