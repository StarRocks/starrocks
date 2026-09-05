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

#include "http/action/tantivy_cache_action.h"

#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include "common/config.h"
#include "gutil/strings/substitute.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "runtime/exec_env.h"
#include "storage/index/inverted/tantivy/random_access_bridge.h"
#include "storage/index/inverted/tantivy/tantivy_cache.h"

namespace starrocks {
namespace {

void send_json(HttpRequest* req, rapidjson::Document* root) {
    rapidjson::StringBuffer buffer;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
    root->Accept(writer);
    req->add_output_header(HttpHeaders::CONTENT_TYPE, "application/json");
    HttpChannel::send_reply(req, HttpStatus::OK, buffer.GetString());
}

template <typename T>
void add_uint64(rapidjson::Value* object, const char* name, T value, rapidjson::Document::AllocatorType& allocator) {
    object->AddMember(rapidjson::StringRef(name), rapidjson::Value().SetUint64(static_cast<uint64_t>(value)),
                      allocator);
}

} // namespace

void TantivyCacheAction::handle(HttpRequest* req) {
    const auto& action = req->param("action");
    if (req->method() == HttpMethod::GET && action == "status") {
        _handle_status(req);
    } else if (req->method() == HttpMethod::PUT && action == "prune") {
        _handle_prune(req);
    } else {
        _send_error(req, strings::Substitute("unsupported Tantivy cache request: '$0'", req->uri()));
    }
}

void TantivyCacheAction::_handle_status(HttpRequest* req) {
    auto* manager = _exec_env->tantivy_cache_manager();
    if (manager == nullptr) {
        _send_error(req, "Tantivy cache manager is not initialized");
        return;
    }

    const auto reader_stats = manager->reader_cache()->stats();
    const auto query_stats = manager->query_cache()->stats();
    const auto buffer_pool_stats = manager->read_buffer_pool()->stats();
    rapidjson::Document root;
    root.SetObject();
    auto& allocator = root.GetAllocator();
    root.AddMember("reader_cache_enabled", config::enable_tantivy_reader_cache, allocator);
    root.AddMember("query_cache_enabled", config::enable_tantivy_query_cache, allocator);

    rapidjson::Value reader(rapidjson::kObjectType);
    add_uint64(&reader, "capacity_bytes", manager->reader_cache()->capacity(), allocator);
    add_uint64(&reader, "effective_usage_bytes", manager->reader_cache()->memory_usage(), allocator);
    add_uint64(&reader, "estimated_resident_bytes", reader_stats.estimated_resident_bytes, allocator);
    add_uint64(&reader, "resident_directory_entries", reader_stats.resident_directory_entries, allocator);
    add_uint64(&reader, "resident_directory_bytes", reader_stats.resident_directory_bytes, allocator);
    add_uint64(&reader, "entries", reader_stats.entries, allocator);
    add_uint64(&reader, "lookup", reader_stats.lookup, allocator);
    add_uint64(&reader, "hit", reader_stats.hit, allocator);
    add_uint64(&reader, "miss", reader_stats.miss, allocator);
    add_uint64(&reader, "bypass", reader_stats.bypass, allocator);
    add_uint64(&reader, "insert", reader_stats.insert, allocator);
    add_uint64(&reader, "oversize_reject", reader_stats.oversize_reject, allocator);
    add_uint64(&reader, "build", reader_stats.build, allocator);
    add_uint64(&reader, "build_error", reader_stats.build_error, allocator);
    add_uint64(&reader, "duplicate_build_prevented", reader_stats.duplicate_build_prevented, allocator);
    add_uint64(&reader, "singleflight_waiters", reader_stats.singleflight_waiters, allocator);
    add_uint64(&reader, "singleflight_wait_ns", reader_stats.singleflight_wait_ns, allocator);
    root.AddMember("reader", reader, allocator);

    rapidjson::Value buffer_pool(rapidjson::kObjectType);
    add_uint64(&buffer_pool, "acquire", buffer_pool_stats.acquire, allocator);
    add_uint64(&buffer_pool, "hit", buffer_pool_stats.hit, allocator);
    add_uint64(&buffer_pool, "miss", buffer_pool_stats.miss, allocator);
    add_uint64(&buffer_pool, "release", buffer_pool_stats.release, allocator);
    add_uint64(&buffer_pool, "capacity_bytes", buffer_pool_stats.capacity_bytes, allocator);
    add_uint64(&buffer_pool, "max_buffer_bytes", buffer_pool_stats.max_buffer_bytes, allocator);
    add_uint64(&buffer_pool, "cached_bytes", buffer_pool_stats.cached_bytes, allocator);
    add_uint64(&buffer_pool, "in_use_bytes", buffer_pool_stats.in_use_bytes, allocator);
    root.AddMember("read_buffer_pool", buffer_pool, allocator);

    rapidjson::Value query(rapidjson::kObjectType);
    add_uint64(&query, "capacity_bytes", manager->query_cache()->capacity(), allocator);
    add_uint64(&query, "effective_usage_bytes", manager->query_cache()->memory_usage(), allocator);
    add_uint64(&query, "estimated_resident_bytes", query_stats.estimated_resident_bytes, allocator);
    add_uint64(&query, "entries", query_stats.entries, allocator);
    add_uint64(&query, "lookup", query_stats.lookup, allocator);
    add_uint64(&query, "hit", query_stats.hit, allocator);
    add_uint64(&query, "miss", query_stats.miss, allocator);
    add_uint64(&query, "bypass", query_stats.bypass, allocator);
    add_uint64(&query, "insert", query_stats.insert, allocator);
    add_uint64(&query, "oversize_reject", query_stats.oversize_reject, allocator);
    add_uint64(&query, "key_too_large", query_stats.key_too_large, allocator);
    add_uint64(&query, "admission_record", query_stats.ghost_record, allocator);
    add_uint64(&query, "admission_accept", query_stats.ghost_admit, allocator);
    root.AddMember("query", query, allocator);
    send_json(req, &root);
}

void TantivyCacheAction::_handle_prune(HttpRequest* req) {
    auto* manager = _exec_env->tantivy_cache_manager();
    if (manager == nullptr) {
        _send_error(req, "Tantivy cache manager is not initialized");
        return;
    }

    const auto& type = req->param("type");
    if (type == "query") {
        manager->query_cache()->prune();
    } else if (type == "reader") {
        manager->reader_cache()->prune();
        manager->read_buffer_pool()->prune();
    } else if (type == "all") {
        manager->query_cache()->prune();
        manager->reader_cache()->prune();
        manager->read_buffer_pool()->prune();
    } else {
        _send_error(req, "type must be query, reader, or all");
        return;
    }

    rapidjson::Document root;
    root.SetObject();
    auto& allocator = root.GetAllocator();
    root.AddMember("status", rapidjson::StringRef("OK"), allocator);
    root.AddMember("type", rapidjson::Value(type.c_str(), type.size(), allocator), allocator);
    send_json(req, &root);
}

void TantivyCacheAction::_send_error(HttpRequest* req, const std::string& message) {
    rapidjson::Document root;
    root.SetObject();
    auto& allocator = root.GetAllocator();
    root.AddMember("error", rapidjson::Value(message.c_str(), message.size(), allocator), allocator);
    send_json(req, &root);
}

} // namespace starrocks
