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

<<<<<<< HEAD
=======
#include "base/utility/defer_op.h"
#include "common/config_http_fwd.h"
>>>>>>> 82ccf15b17 ([BugFix] Add enable_stop_be_action config to control /api/_stop_be (#73499))
#include "common/process_exit.h"
#include "http/action/fe_auth_checker.h"
#include "http/http_channel.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "util/defer_op.h"

#ifdef USE_STAROS
#include "service/staros_worker.h"
#endif

namespace starrocks {

namespace {
// Privilege required on SYSTEM to invoke /api/_stop_be. Must match a
// PrivilegeType enum name on the FE side. NODE is what ALTER SYSTEM uses.
constexpr const char* kRequiredSystemPrivilege = "NODE";
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

    if (config::enable_stop_be_action_fe_auth) {
        if (Status auth_status = FeAuthChecker::check(req, kRequiredSystemPrivilege); !auth_status.ok()) {
            LOG(WARNING) << "Reject stop_be request: " << auth_status;
            if (auth_status.is_not_authorized()) {
                HttpChannel::send_basic_challenge(req, "stop_be");
            } else {
                HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR,
                                        construct_response_message(std::string(auth_status.message())));
            }
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
