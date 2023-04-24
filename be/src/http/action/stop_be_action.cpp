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

#include "http/http_channel.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "util/defer_op.h"

namespace starrocks {
extern std::atomic<bool> k_starrocks_exit_quick;

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

    DeferOp defer([&]() {
        if (!k_starrocks_exit_quick.load(std::memory_order_acquire)) {
            k_starrocks_exit_quick.store(true);
        }
    });

    std::string response_msg = construct_response_message("OK");
    if (k_starrocks_exit_quick.load(std::memory_order_acquire)) {
        response_msg = construct_response_message("Be is shutting down");
    }

    HttpChannel::send_reply(req, HttpStatus::OK, response_msg);
}

} // end namespace starrocks