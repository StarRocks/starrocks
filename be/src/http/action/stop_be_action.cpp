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

#include "http/action/stop_be_action.h"

#include <sstream>
#include <string>

#include "base/utility/defer_op.h"
#include "common/config_http_fwd.h"
#include "common/process_exit.h"
#include "common/status.h"
#include "common/system/master_info.h"
#include "platform/http/http_channel.h"
#include "platform/http/http_request.h"
#include "platform/http/http_status.h"

#ifdef USE_STAROS
#include "compute_env/staros/staros_worker_runtime.h"
#endif

namespace starrocks {

namespace {
// stop_be shuts the process down for any caller that can reach this HTTP endpoint.
// config::enable_stop_be_action is only a feature toggle (default true), not a credential,
// so gate it behind the same cluster-internal shared token already used for other
// admin/internal BE HTTP endpoints (see DownloadAction::check_token).
Status check_internal_token(HttpRequest* req) {
    const std::string& token_str = req->param("token");
    if (token_str.empty()) {
        return Status::InternalError("token is not specified.");
    }
    if (token_str != get_master_token()) {
        return Status::InternalError("invalid token.");
    }
    return Status::OK();
}
} // namespace

std::string StopBeAction::construct_response_message(const std::string& msg) {
    std::stringstream ss;
    ss << "{";
    ss << "\"status\": "
       << "\"" << msg << "\"";
    ss << "}";

    return ss.str();
}

void StopBeAction::handle(HttpRequest* req) {
    LOG(INFO) << "Accept one stop_be request " << req->debug_string();

    if (!config::enable_stop_be_action) {
        LOG(WARNING) << "Reject stop_be request because config::enable_stop_be_action is false";
        HttpChannel::send_reply(req, HttpStatus::FORBIDDEN,
                                construct_response_message("stop_be action is disabled by config"));
        return;
    }

    if (config::enable_token_check) {
        Status token_st = check_internal_token(req);
        if (!token_st.ok()) {
            LOG(WARNING) << "Rejected stop_be request: " << token_st;
            HttpChannel::send_reply(req, HttpStatus::UNAUTHORIZED,
                                    construct_response_message(std::string(token_st.message())));
            return;
        }
    }

    DeferOp defer([&]() {
#ifdef USE_STAROS
        set_starlet_in_shutdown();
#endif
        set_process_quick_exit();
    });

    std::string response_msg = construct_response_message("OK");
    if (process_exit_in_progress()) {
        response_msg = construct_response_message("Be is shutting down");
    }

    HttpChannel::send_reply(req, HttpStatus::OK, response_msg);
}

} // end namespace starrocks
