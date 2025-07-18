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

#include "http/action/jit_cache_action.h"

#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include <string>

#include "common/logging.h"
#include "exprs/jit/jit_engine.h"
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

void JITCacheAction::handle(HttpRequest* req) {
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
void JITCacheAction::_handle(HttpRequest* req, const std::function<void(rapidjson::Document&)>& func) {
    rapidjson::Document root;
    root.SetObject();
    func(root);
    rapidjson::StringBuffer strbuf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    HttpChannel::send_reply(req, HttpStatus::OK, strbuf.GetString());
}
void JITCacheAction::_handle_stat(HttpRequest* req) {
    auto* jit_engine = JITEngine::get_instance();
    _handle(req, [=](rapidjson::Document& root) {
        auto& allocator = root.GetAllocator();

        if (jit_engine == nullptr) {
            root.AddMember("error", rapidjson::StringRef("JITEngine is nullptr"), allocator);
            return;
        }
        auto* object_cache = jit_engine->get_object_cache();
        auto* callable_cache = jit_engine->get_callable_cache();
        rapidjson::Value object_cache_obj(rapidjson::kObjectType);
        if (object_cache == nullptr) {
            object_cache_obj.AddMember("error", rapidjson::StringRef("JITObjectCache is nullptr"), allocator);
        } else {
            object_cache_obj.AddMember("capacity", rapidjson::Value(object_cache->get_capacity()), allocator);
            object_cache_obj.AddMember("usage", rapidjson::Value(object_cache->get_memory_usage()), allocator);
            auto usage_ratio = double(object_cache->get_memory_usage()) /
                               double(object_cache->get_capacity() == 0 ? 1 : object_cache->get_capacity());
            object_cache_obj.AddMember("usage_ratio", rapidjson::Value(usage_ratio), allocator);
            object_cache_obj.AddMember("lookup_count", rapidjson::Value(object_cache->get_lookup_count()), allocator);
            object_cache_obj.AddMember("hit_count", rapidjson::Value(object_cache->get_hit_count()), allocator);
            auto hit_ratio = double(object_cache->get_hit_count()) /
                             double(object_cache->get_lookup_count() == 0 ? 1 : object_cache->get_lookup_count());
            object_cache_obj.AddMember("hit_ratio", rapidjson::Value(hit_ratio), allocator);
        }
        root.AddMember("object_cache", object_cache_obj, allocator);

        rapidjson::Value callable_cache_obj(rapidjson::kObjectType);
        if (callable_cache == nullptr) {
            callable_cache_obj.AddMember("error", rapidjson::StringRef("JITCallableCache is nullptr"), allocator);
        } else {
            callable_cache_obj.AddMember("capacity", rapidjson::Value(callable_cache->get_capacity()), allocator);
            callable_cache_obj.AddMember("usage", rapidjson::Value(callable_cache->get_memory_usage()), allocator);
            auto usage_ratio = double(callable_cache->get_memory_usage()) /
                               double(callable_cache->get_capacity() == 0 ? 1 : callable_cache->get_capacity());
            callable_cache_obj.AddMember("usage_ratio", rapidjson::Value(usage_ratio), allocator);
            callable_cache_obj.AddMember("lookup_count", rapidjson::Value(callable_cache->get_lookup_count()),
                                         allocator);
            callable_cache_obj.AddMember("hit_count", rapidjson::Value(callable_cache->get_hit_count()), allocator);
            auto hit_ratio = double(callable_cache->get_hit_count()) /
                             double(callable_cache->get_lookup_count() == 0 ? 1 : callable_cache->get_lookup_count());
            callable_cache_obj.AddMember("hit_ratio", rapidjson::Value(hit_ratio), allocator);
        }
        root.AddMember("callable_cache", callable_cache_obj, allocator);
    });
}

void JITCacheAction::_handle_invalidate_all(HttpRequest* req) {
    auto* jit_engine = JITEngine::get_instance();
    _handle(req, [&](rapidjson::Document& root) {
        auto& allocator = root.GetAllocator();
        if (jit_engine == nullptr) {
            root.AddMember("error", rapidjson::StringRef("JITEngine is nullptr"), allocator);
            return;
        }
        auto* object_cache = jit_engine->get_object_cache();
        auto* callable_cache = jit_engine->get_callable_cache();
        auto invalidate = [](Cache* cache) {
            auto old_capacity = cache->get_capacity();
            // set capacity of cache to zero, the cache shall prune all cache entries.
            cache->set_capacity(0);
            cache->set_capacity(old_capacity);
        };
        if (object_cache != nullptr) {
            invalidate(object_cache);
        }
        if (callable_cache != nullptr) {
            invalidate(callable_cache);
        }
        root.AddMember("status", rapidjson::StringRef("OK"), allocator);
    });
}

void JITCacheAction::_handle_error(HttpRequest* req, const std::string& err_msg) {
    _handle(req, [err_msg](rapidjson::Document& root) {
        auto& allocator = root.GetAllocator();
        root.AddMember("error", rapidjson::Value(err_msg.c_str(), err_msg.size()), allocator);
    });
}

} // namespace starrocks
