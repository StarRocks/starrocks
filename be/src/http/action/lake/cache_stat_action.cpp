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

#include "cache_stat_action.h"

#include <common/config.h>
#include <fslib/star_cache_handler.h>

#include <string>
#include <utility>
#include <vector>

#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"

namespace starrocks::lake {

const static std::string ACTION_KEY = "action";
const static std::string ACTION_STAT = "stat";
const static std::string ACTION_SNAPSHOT = "snapshot";

/**
 *  Get cache statistic. GET /cache/{stat}|{snapshot}, result is a map indicates the size of each tablet in cache.
 *   1. If called after cn restart or {snapshot}, return the current snapshot of the cache.eg.
 *       {
 *          "isSnapshot": "true",
 *          "detail": "1:1024,2:2048"
 *       }
 *   2. Otherwise, return the changes(increase or decrease) since last api called. eg.
*       {
 *          "isSnapshot": "false",
 *          "detail": "1:1024,2:-1024"
 *       }
 */
void CacheStatAction::handle(HttpRequest* req) {
    if (!config::starlet_enable_cache_stat) {
        LOG(WARNING) << "config enable_cache_stat is disabled!";
        HttpChannel::send_reply(req, "");
        return;
    }
    bool is_snapshot = false;
    std::vector<std::pair<uint64_t, int64_t>> result;
    butil::Timer timer;
    timer.start();
    if (req->param(ACTION_KEY) == ACTION_STAT) {
        is_snapshot = staros::starlet::fslib::star_cache_get_stat(false, &result);
    } else if (req->param(ACTION_KEY) == ACTION_SNAPSHOT) {
        is_snapshot = staros::starlet::fslib::star_cache_get_stat(true, &result);
    } else {
        LOG(WARNING) << "unknown action: " << req->param(ACTION_KEY);
    }
    timer.stop();
    std::stringstream ss;
    ss << "{";
    ss << R"("isSnapshot": )" << (is_snapshot ? "true" : "false") << R"(,)";
    ss << R"("detail": ")";
    uint64_t cnt = 0;
    for (auto& p : result) {
        ss << p.first << ":" << p.second << ",";
        cnt++;
    }
    ss << R"("})";
    LOG(INFO) << "get_cache_stat cost " << timer.m_elapsed() << " ms, isSnapshot=" << is_snapshot << ", count=" << cnt;
    req->add_output_header(HttpHeaders::CONTENT_TYPE, "text/plain; version=0.0.4");
    HttpChannel::send_reply(req, ss.str());
}

} // namespace starrocks::lake
