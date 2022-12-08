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

#include "http/action/runtime_filter_cache_action.h"

#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>

#include <string>

#include "common/logging.h"
#include "gutil/strings/substitute.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "runtime/runtime_filter_cache.h"

namespace starrocks {

const static std::string HEADER_JSON = "application/json";
const static std::string ACTION_KEY = "action";
const static std::string ACTION_STAT = "stat";
const static std::string ACTION_TRACE = "trace";
const static std::string ACTION_ENABLE_TRACE = "enable_trace";
const static std::string ACTION_DISABLE_TRACE = "disable_trace";

void RuntimeFilterCacheAction::handle(HttpRequest* req) {
    LOG(INFO) << req->debug_string();
    const auto& action = req->param(ACTION_KEY);
    if (req->method() == HttpMethod::GET) {
        if (action == ACTION_STAT) {
            _handle_stat(req);
        } else if (action == ACTION_TRACE) {
            _handle_trace(req);
        } else {
            _handle_error(req, strings::Substitute("Not support GET method: '$0'", req->uri()));
        }
    } else if (req->method() == HttpMethod::PUT) {
        if (action == ACTION_ENABLE_TRACE) {
            _handle_trace_switch(req, true);
        } else if (action == ACTION_DISABLE_TRACE) {
            _handle_trace_switch(req, false);
        } else {
            _handle_error(req, strings::Substitute("Not support PUT method: '$0'", req->uri()));
        }
    } else {
        _handle_error(req,
                      strings::Substitute("Not support $0 method: '$1'", to_method_desc(req->method()), req->uri()));
    }
}
void RuntimeFilterCacheAction::_handle(HttpRequest* req, const std::function<void(rapidjson::Document&)>& func) {
    rapidjson::Document root;
    root.SetObject();
    func(root);
    rapidjson::StringBuffer strbuf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    HttpChannel::send_reply(req, HttpStatus::OK, strbuf.GetString());
}
void RuntimeFilterCacheAction::_handle_stat(HttpRequest* req) {
    size_t cache_times = _exec_env->runtime_filter_cache()->cache_times();
    size_t use_times = _exec_env->runtime_filter_cache()->use_times();
    const std::string& enable_trace = _exec_env->runtime_filter_cache()->enable_trace() ? "true" : "false";
    _handle(req, [=](rapidjson::Document& root) {
        auto& allocator = root.GetAllocator();
        root.AddMember("cache_times", rapidjson::Value(cache_times), allocator);
        root.AddMember("use_times", rapidjson::Value(use_times), allocator);
        root.AddMember("enable_trace", rapidjson::Value(enable_trace.c_str(), enable_trace.size()), allocator);
    });
}

void RuntimeFilterCacheAction::_handle_trace(HttpRequest* req) {
    auto events = _exec_env->runtime_filter_cache()->get_events();
    _handle(req, [&](rapidjson::Document& root) {
        auto& allocator = root.GetAllocator();
        rapidjson::Document traces_obj;
        traces_obj.SetArray();
        for (auto& event : events) {
            rapidjson::Document query_obj;
            query_obj.SetObject();
            query_obj.AddMember("query_id", rapidjson::Value(event.first.c_str(), event.first.size()), allocator);
            rapidjson::Document query_events;
            query_events.SetArray();
            for (auto& s : event.second) {
                query_events.PushBack(rapidjson::Value(s.c_str(), s.size()), allocator);
            }
            query_obj.AddMember("events", query_events, allocator);
            traces_obj.PushBack(query_obj, allocator);
        }
        root.AddMember("traces", traces_obj, allocator);
    });
}

void RuntimeFilterCacheAction::_handle_trace_switch(HttpRequest* req, bool on) {
    _exec_env->runtime_filter_cache()->set_enable_trace(on);
    _handle_stat(req);
}

void RuntimeFilterCacheAction::_handle_error(HttpRequest* req, const std::string& err_msg) {
    _handle(req, [err_msg](rapidjson::Document& root) {
        auto& allocator = root.GetAllocator();
        root.AddMember("error", rapidjson::Value(err_msg.c_str(), err_msg.size()), allocator);
    });
}

} // namespace starrocks
