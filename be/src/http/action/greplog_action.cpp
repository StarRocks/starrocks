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
<<<<<<< HEAD
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
=======
#include "http/utils.h"
#include "platform/http/http_channel.h"
#include "platform/http/http_headers.h"
#include "platform/http/http_request.h"
#include "platform/http/http_status.h"
>>>>>>> a6cb26f ([BugFix] Do not let a malformed numeric HTTP parameter kill the BE (#79022))

namespace starrocks {

const int64_t GREP_LOG_LIMIT = 1000000;

void GrepLogAction::handle(HttpRequest* req) {
    if (req->method() != HttpMethod::GET) {
        HttpChannel::send_reply(req, HttpStatus::METHOD_NOT_ALLOWED, "Method Not Allowed");
        return;
    }
    int64_t start_ts = 0;
    if (!req->param("start_ts").empty() && !parse_int64_param("start_ts", req->param("start_ts"), &start_ts).ok()) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "Invalid param start_ts");
        return;
    }
    int64_t end_ts = 0;
    if (!req->param("end_ts").empty() && !parse_int64_param("end_ts", req->param("end_ts"), &end_ts).ok()) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "Invalid param end_ts");
        return;
    }
    std::string pattern = req->param("pattern");
    std::string level = req->param("level");
    if (level.empty()) {
        level = "I";
    }
    int64_t limit = GREP_LOG_LIMIT;
    if (!req->param("limit").empty() &&
        !parse_int64_param("limit", req->param("limit"), &limit, 1, GREP_LOG_LIMIT).ok()) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "Invalid param limit");
        return;
    }

    auto ret = grep_log_as_string(start_ts, end_ts, level, pattern, limit);

    HttpChannel::send_reply(req, HttpStatus::OK, ret);
}

} // namespace starrocks