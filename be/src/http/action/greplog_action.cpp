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

#include "http/action/greplog_action.h"

#include "common/greplog.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"

namespace starrocks {

const int64_t GREP_LOG_LIMIT = 1000000;

Status get_int64_param(HttpRequest* req, const std::string& name, int64_t* value) {
    const std::string& str_value = req->param(name);
    if (!str_value.empty()) {
        try {
            *value = std::stoll(str_value);
        } catch (const std::exception& e) {
            return Status::InternalError("Invalid param " + name + ": " + str_value);
        }
    }
    return Status::OK();
}

void GrepLogAction::handle(HttpRequest* req) {
    if (req->method() != HttpMethod::GET) {
        HttpChannel::send_reply(req, HttpStatus::METHOD_NOT_ALLOWED, "Method Not Allowed");
        return;
    }
    int64_t start_ts = 0;
    int64_t end_ts = 0;
    if (!get_int64_param(req, "start_ts", &start_ts).ok()) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "Invalid param start_ts");
        return;
    }
    if (!get_int64_param(req, "end_ts", &end_ts).ok()) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "Invalid param end_ts");
        return;
    }
    std::string pattern = req->param("pattern");
    std::string level = req->param("level");
    if (level.empty()) {
        level = "I";
    }
    int64_t limit = GREP_LOG_LIMIT;
    if (!get_int64_param(req, "limit", &limit).ok()) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "Invalid param limit");
        return;
    }
    if (limit <= 0 || limit > GREP_LOG_LIMIT) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "Invalid param limit");
        return;
    }

    auto ret = grep_log_as_string(start_ts, end_ts, std::toupper(level[0]), pattern, limit);

    HttpChannel::send_reply(req, HttpStatus::OK, ret);
}

} // namespace starrocks