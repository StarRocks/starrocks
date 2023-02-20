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

#include "http/action/query_cache_action.h"

#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include <string>

#include "common/logging.h"
#include "gutil/strings/substitute.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"

namespace starrocks {

const static std::string HEADER_JSON = "application/json";
const static std::string ACTION_KEY = "action";
const static std::string ACTION_STAT = "stat";
const static std::string ACTION_INVALIDATE_ALL = "invalidate_all";

void QueryCacheAction::handle(HttpRequest* req) {
    VLOG_ROW << req->debug_string();
    const auto& action = req->param(ACTION_KEY);
    if (req->method() == HttpMethod::GET) {
        if (action == ACTION_STAT) {
            _handle_stat(req);
        } else {
            _handle_error(req, strings::Substitute("Not support GET method: '$0'", req->uri()));
        }
    } else if (req->method() == HttpMethod::PUT) {
        if (action == ACTION_INVALIDATE_ALL) {
            _handle_invalidate_all(req);
        } else {
            _handle_error(req, strings::Substitute("Not support PUT method: '$0'", req->uri()));
        }
    } else {
        _handle_error(req,
                      strings::Substitute("Not support $0 method: '$1'", to_method_desc(req->method()), req->uri()));
    }
}
void QueryCacheAction::_handle(HttpRequest* req, const std::function<void(rapidjson::Document&)>& func) {
    rapidjson::Document root;
    root.SetObject();
    func(root);
    rapidjson::StringBuffer strbuf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    HttpChannel::send_reply(req, HttpStatus::OK, strbuf.GetString());
}
void QueryCacheAction::_handle_stat(HttpRequest* req) {
    auto cache_mgr = _exec_env->cache_mgr();
    _handle(req, [=](rapidjson::Document& root) {
        auto& allocator = root.GetAllocator();
        if (cache_mgr == nullptr) {
            root.AddMember("error", rapidjson::StringRef("Cache Manager is nullptr"), allocator);
            return;
        }
        auto capacity = cache_mgr->capacity();
        auto usage = cache_mgr->memory_usage();
        auto usage_ratio = capacity == 0 ? 0.0 : double(usage) / double(capacity);
        auto lookup_count = cache_mgr->lookup_count();
        auto hit_count = cache_mgr->hit_count();
        auto hit_ratio = lookup_count == 0 ? 0.0 : double(hit_count) / double(lookup_count);

        root.AddMember("capacity", rapidjson::Value(capacity), allocator);
        root.AddMember("usage", rapidjson::Value(usage), allocator);
        root.AddMember("usage_ratio", rapidjson::Value(usage_ratio), allocator);
        root.AddMember("lookup_count", rapidjson::Value(lookup_count), allocator);
        root.AddMember("hit_count", rapidjson::Value(hit_count), allocator);
        root.AddMember("hit_ratio", rapidjson::Value(hit_ratio), allocator);
    });
}

void QueryCacheAction::_handle_invalidate_all(HttpRequest* req) {
    auto cache_mgr = _exec_env->cache_mgr();
    _handle(req, [&](rapidjson::Document& root) {
        auto& allocator = root.GetAllocator();
        if (cache_mgr == nullptr) {
            root.AddMember("error", rapidjson::StringRef("Cache Manager is nullptr"), allocator);
            return;
        }
        cache_mgr->invalidate_all();
        root.AddMember("status", rapidjson::StringRef("OK"), allocator);
    });
}

void QueryCacheAction::_handle_error(HttpRequest* req, const std::string& err_msg) {
    _handle(req, [err_msg](rapidjson::Document& root) {
        auto& allocator = root.GetAllocator();
        root.AddMember("error", rapidjson::Value(err_msg.c_str(), err_msg.size()), allocator);
    });
}

} // namespace starrocks
