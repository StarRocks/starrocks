// This file is licensed under the Elastic License 2.0. Copyright 2021-present StarRocks Limited.

#include "http/action/runtime_filter_cache_action.h"

#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>

#include <string>

#include "common/logging.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"

namespace starrocks {

const static std::string HEADER_JSON = "application/json";

void RuntimeFilterCacheAction::handle(HttpRequest* req) {
    LOG(INFO) << req->debug_string();
    size_t cache_times = _exec_env->runtime_filter_cache()->cache_times();
    size_t use_times = _exec_env->runtime_filter_cache()->use_times();
    auto events = _exec_env->runtime_filter_cache()->get_events();

    rapidjson::Document root;
    root.SetObject();
    auto& allocator = root.GetAllocator();
    root.AddMember("cache_times", rapidjson::Value(cache_times), allocator);
    root.AddMember("use_times", rapidjson::Value(use_times), allocator);

    rapidjson::Document trace_obj;
    trace_obj.SetArray();
    for (auto it = events.begin(); auto it = events.end(); ++it) {
        rapidjson::Document query_obj;
        query_obj.SetObject();
        query_obj.AddMember("query_id", rapidjson::Value(it->first.c_str(), it->first.size()), allocator);

        rapidjson::Document query_events;
        query_events.SetArray();
        for (auto& s : it->second) {
            query_events.PushBack(rapidjson::Value(s.c_str(), s.size()), allocator);
        }
        query_obj.AddMember("events", query_events, allocator);
        trace_obj.PushBack(query_obj, allocator);
    }
    root.AddMember("trace", trace_obj, allocator);

    rapidjson::StringBuffer strbuf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    HttpChannel::send_reply(req, HttpStatus::OK, strbuf.GetString());
}

} // namespace starrocks
