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

#include "http/action/datacache_action.h"

#include <fmt/format.h>
#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include <string>
#include <sstream>
#include <chrono>

#include "cache/block_cache/block_cache.h"
#include "cache/block_cache/block_cache_hit_rate_counter.hpp"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"

namespace starrocks {

const static std::string HEADER_JSON = "application/json";
const static std::string ACTION_KEY = "action";
const static std::string ACTION_STAT = "stat";
const static std::string ACTION_APP_STAT = "app_stat";
const static std::string ACTION_PROMETHEUS = "prometheus";

std::string cache_status_str(const DataCacheStatus& status) {
    std::string str_status;
    switch (status) {
    case DataCacheStatus::NORMAL:
        str_status = "NORMAL";
        break;
    case DataCacheStatus::UPDATING:
        str_status = "UPDATING";
        break;
    case DataCacheStatus::ABNORMAL:
        str_status = "ABNORMAL";
        break;
    case DataCacheStatus::LOADING:
        str_status = "LOADING";
        break;
    }
    return str_status;
}

bool DataCacheAction::_check_request(HttpRequest* req) {
    if (req->method() != HttpMethod::GET) {
        HttpChannel::send_reply(req, HttpStatus::METHOD_NOT_ALLOWED, "Method Not Allowed");
        return false;
    }
    if (req->param(ACTION_KEY) != ACTION_STAT && 
        req->param(ACTION_KEY) != ACTION_APP_STAT && 
        req->param(ACTION_KEY) != ACTION_PROMETHEUS) {
        HttpChannel::send_reply(req, HttpStatus::NOT_FOUND, "Not Found");
        return false;
    }
    return true;
}

void DataCacheAction::handle(HttpRequest* req) {
    VLOG_ROW << req->debug_string();
    if (!_check_request(req)) {
        return;
    }
    if (!_block_cache || !_block_cache->is_initialized()) {
        _handle_error(req, strings::Substitute("Cache system is not ready"));
    } else if (_block_cache->engine_type() != DataCacheEngineType::STARCACHE) {
        _handle_error(req, strings::Substitute("No more metrics for current cache engine type"));
    } else if (req->param(ACTION_KEY) == ACTION_STAT) {
        _handle_stat(req);
    } else if (req->param(ACTION_KEY) == ACTION_APP_STAT) {
        _handle_app_stat(req);
    } else if (req->param(ACTION_KEY) == ACTION_PROMETHEUS) {
        _handle_prometheus(req);
    } else {
        _handle_error(req, strings::Substitute("Unknown action type"));
    }
}

void DataCacheAction::_handle(HttpRequest* req, const std::function<void(rapidjson::Document&)>& func) {
    rapidjson::Document root;
    root.SetObject();
    func(root);
    rapidjson::StringBuffer strbuf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    HttpChannel::send_reply(req, HttpStatus::OK, strbuf.GetString());
}

void DataCacheAction::_handle_stat(HttpRequest* req) {
    _handle(req, [=](rapidjson::Document& root) {
#ifdef WITH_STARCACHE
        auto& allocator = root.GetAllocator();
        auto&& metrics = _block_cache->cache_metrics(2);
        std::string status = cache_status_str(metrics.status);

        rapidjson::Value status_value;
        status_value.SetString(status.c_str(), status.length(), allocator);
        root.AddMember("status", status_value, allocator);
        root.AddMember("mem_quota_bytes", rapidjson::Value(metrics.mem_quota_bytes), allocator);
        root.AddMember("mem_used_bytes", rapidjson::Value(metrics.mem_used_bytes), allocator);
        root.AddMember("disk_quota_bytes", rapidjson::Value(metrics.disk_quota_bytes), allocator);
        root.AddMember("disk_used_bytes", rapidjson::Value(metrics.disk_used_bytes), allocator);

        auto mem_used_rate = 0.0;
        if (metrics.mem_quota_bytes > 0) {
            mem_used_rate =
                    std::round(double(metrics.mem_used_bytes) / double(metrics.mem_quota_bytes) * 100.0) / 100.0;
        }
        auto disk_used_rate = 0.0;
        if (metrics.disk_quota_bytes > 0) {
            disk_used_rate =
                    std::round(double(metrics.disk_used_bytes) / double(metrics.disk_quota_bytes) * 100.0) / 100.0;
        }
        root.AddMember("mem_used_rate", rapidjson::Value(mem_used_rate), allocator);
        root.AddMember("disk_used_rate", rapidjson::Value(disk_used_rate), allocator);

        std::string disk_spaces;
        for (size_t i = 0; i < metrics.disk_dir_spaces.size(); ++i) {
            std::string space =
                    fmt::format("{}:{}", metrics.disk_dir_spaces[i].path, metrics.disk_dir_spaces[i].quota_bytes);
            if (i != metrics.disk_dir_spaces.size() - 1) {
                space.append(";");
            }
            disk_spaces += space;
        }

        rapidjson::Value disk_spaces_value;
        disk_spaces_value.SetString(disk_spaces.c_str(), disk_spaces.length(), allocator);
        root.AddMember("disk_spaces", disk_spaces_value, allocator);
        root.AddMember("meta_used_bytes", rapidjson::Value(metrics.meta_used_bytes), allocator);

        root.AddMember("hit_count", rapidjson::Value(metrics.detail_l1->hit_count), allocator);
        root.AddMember("miss_count", rapidjson::Value(metrics.detail_l1->miss_count), allocator);

        size_t total_reads = metrics.detail_l1->hit_count + metrics.detail_l1->miss_count;
        auto hit_rate =
                total_reads == 0
                        ? 0.0
                        : std::round(double(metrics.detail_l1->hit_count) / double(total_reads) * 100.0) / 100.0;
        root.AddMember("hit_rate", rapidjson::Value(hit_rate), allocator);

        root.AddMember("hit_bytes", rapidjson::Value(metrics.detail_l1->hit_bytes), allocator);
        root.AddMember("miss_bytes", rapidjson::Value(metrics.detail_l1->miss_bytes), allocator);

        root.AddMember("hit_count_last_minute", rapidjson::Value(metrics.detail_l2->hit_count_last_minite), allocator);
        root.AddMember("miss_count_last_minute", rapidjson::Value(metrics.detail_l2->miss_count_last_minite),
                       allocator);
        root.AddMember("hit_bytes_last_minute", rapidjson::Value(metrics.detail_l2->hit_bytes_last_minite), allocator);
        root.AddMember("miss_bytes_last_minute", rapidjson::Value(metrics.detail_l2->miss_bytes_last_minite),
                       allocator);

        root.AddMember("read_mem_bytes", rapidjson::Value(metrics.detail_l2->read_mem_bytes), allocator);
        root.AddMember("read_disk_bytes", rapidjson::Value(metrics.detail_l2->read_disk_bytes), allocator);

        root.AddMember("write_bytes", rapidjson::Value(metrics.detail_l2->write_bytes), allocator);
        root.AddMember("write_success_count", rapidjson::Value(metrics.detail_l2->write_success_count), allocator);
        root.AddMember("write_fail_count", rapidjson::Value(metrics.detail_l2->write_fail_count), allocator);

        root.AddMember("remove_bytes", rapidjson::Value(metrics.detail_l2->remove_bytes), allocator);
        root.AddMember("remove_success_count", rapidjson::Value(metrics.detail_l2->remove_success_count), allocator);
        root.AddMember("remove_fail_count", rapidjson::Value(metrics.detail_l2->remove_fail_count), allocator);

        root.AddMember("current_reading_count", rapidjson::Value(metrics.detail_l2->current_reading_count), allocator);
        root.AddMember("current_writing_count", rapidjson::Value(metrics.detail_l2->current_writing_count), allocator);
        root.AddMember("current_removing_count", rapidjson::Value(metrics.detail_l2->current_removing_count),
                       allocator);
#endif
    });
}

void DataCacheAction::_handle_app_stat(HttpRequest* req) {
    _handle(req, [=](rapidjson::Document& root) {
#ifdef WITH_STARCACHE
        auto& allocator = root.GetAllocator();
        BlockCacheHitRateCounter* hit_rate_counter = BlockCacheHitRateCounter::instance();
        root.AddMember("hit_bytes", rapidjson::Value(hit_rate_counter->get_hit_bytes()), allocator);
        root.AddMember("miss_bytes", rapidjson::Value(hit_rate_counter->get_miss_bytes()), allocator);
        root.AddMember("hit_rate", rapidjson::Value(hit_rate_counter->hit_rate()), allocator);
        root.AddMember("hit_bytes_last_minute", rapidjson::Value(hit_rate_counter->get_hit_bytes_last_minute()),
                       allocator);
        root.AddMember("miss_bytes_last_minute", rapidjson::Value(hit_rate_counter->get_miss_bytes_last_minute()),
                       allocator);
        root.AddMember("hit_rate_last_minute", rapidjson::Value(hit_rate_counter->hit_rate_last_minute()), allocator);
#endif
    });
}

void DataCacheAction::_handle_prometheus(HttpRequest* req) {
    if (!_block_cache || !_block_cache->is_initialized()) {
        _handle_error(req, "Cache system is not ready");
        return;
    }

    std::stringstream ss;
    auto&& metrics = _block_cache->cache_metrics(2);
    BlockCacheHitRateCounter* hit_rate_counter = BlockCacheHitRateCounter::instance();

    // Cache status
    ss << "# HELP starrocks_cache_status Cache status\n"
       << "# TYPE starrocks_cache_status gauge\n"
       << "starrocks_cache_status{status=\"" << cache_status_str(metrics.status) << "\"} 1\n";

    // Memory metrics
    ss << "# HELP starrocks_cache_memory_bytes Memory usage in bytes\n"
       << "# TYPE starrocks_cache_memory_bytes gauge\n"
       << "starrocks_cache_memory_bytes{type=\"quota\"} " << metrics.mem_quota_bytes << "\n"
       << "starrocks_cache_memory_bytes{type=\"used\"} " << metrics.mem_used_bytes << "\n";

    // Memory usage rate
    double mem_used_rate = 0.0;
    if (metrics.mem_quota_bytes > 0) {
        mem_used_rate = std::round(double(metrics.mem_used_bytes) / double(metrics.mem_quota_bytes) * 100.0) / 100.0;
    }
    ss << "# HELP starrocks_cache_memory_usage_rate Memory usage rate\n"
       << "# TYPE starrocks_cache_memory_usage_rate gauge\n"
       << "starrocks_cache_memory_usage_rate " << mem_used_rate << "\n";

    // Disk metrics
    ss << "# HELP starrocks_cache_disk_bytes Disk usage in bytes\n"
       << "# TYPE starrocks_cache_disk_bytes gauge\n"
       << "starrocks_cache_disk_bytes{type=\"quota\"} " << metrics.disk_quota_bytes << "\n"
       << "starrocks_cache_disk_bytes{type=\"used\"} " << metrics.disk_used_bytes << "\n";

    // Disk usage rate
    double disk_used_rate = 0.0;
    if (metrics.disk_quota_bytes > 0) {
        disk_used_rate = std::round(double(metrics.disk_used_bytes) / double(metrics.disk_quota_bytes) * 100.0) / 100.0;
    }
    ss << "# HELP starrocks_cache_disk_usage_rate Disk usage rate\n"
       << "# TYPE starrocks_cache_disk_usage_rate gauge\n"
       << "starrocks_cache_disk_usage_rate " << disk_used_rate << "\n";

    // Disk spaces
    for (size_t i = 0; i < metrics.disk_dir_spaces.size(); ++i) {
        ss << "# HELP starrocks_cache_disk_space Disk space configuration\n"
           << "# TYPE starrocks_cache_disk_space gauge\n"
           << "starrocks_cache_disk_space{path=\"" << metrics.disk_dir_spaces[i].path 
           << "\",quota_bytes=\"" << metrics.disk_dir_spaces[i].quota_bytes << "\"} 1\n";
    }

    // Meta metrics
    ss << "# HELP starrocks_cache_meta_bytes Meta data usage in bytes\n"
       << "# TYPE starrocks_cache_meta_bytes gauge\n"
       << "starrocks_cache_meta_bytes " << metrics.meta_used_bytes << "\n";

    // Hit/miss operations
    ss << "# HELP starrocks_cache_operations_total Total cache operations\n"
       << "# TYPE starrocks_cache_operations_total counter\n"
       << "starrocks_cache_operations_total{type=\"hit\"} " << metrics.detail_l1->hit_count << "\n"
       << "starrocks_cache_operations_total{type=\"miss\"} " << metrics.detail_l1->miss_count << "\n";

    // Hit/miss bytes
    ss << "# HELP starrocks_cache_bytes_total Total cache bytes\n"
       << "# TYPE starrocks_cache_bytes_total counter\n"
       << "starrocks_cache_bytes_total{type=\"hit\"} " << metrics.detail_l1->hit_bytes << "\n"
       << "starrocks_cache_bytes_total{type=\"miss\"} " << metrics.detail_l1->miss_bytes << "\n";

    // Hit rate
    ss << "# HELP starrocks_cache_hit_rate Cache hit rate\n"
       << "# TYPE starrocks_cache_hit_rate gauge\n"
       << "starrocks_cache_hit_rate " << hit_rate_counter->hit_rate() << "\n";

    // Last minute metrics
    ss << "# HELP starrocks_cache_hit_rate_last_minute Cache hit rate in last minute\n"
       << "# TYPE starrocks_cache_hit_rate_last_minute gauge\n"
       << "starrocks_cache_hit_rate_last_minute " << hit_rate_counter->hit_rate_last_minute() << "\n";

    // Buffer metrics
    ss << "# HELP starrocks_cache_buffer_items Buffer items count\n"
       << "# TYPE starrocks_cache_buffer_items gauge\n"
       << "starrocks_cache_buffer_items " << metrics.detail_l2->buffer_item_count << "\n";

    ss << "# HELP starrocks_cache_buffer_bytes Buffer items bytes\n"
       << "# TYPE starrocks_cache_buffer_bytes gauge\n"
       << "starrocks_cache_buffer_bytes " << metrics.detail_l2->buffer_item_bytes << "\n";

    // Read metrics
    ss << "# HELP starrocks_cache_read_bytes_total Total read bytes\n"
       << "# TYPE starrocks_cache_read_bytes_total counter\n"
       << "starrocks_cache_read_bytes_total{type=\"memory\"} " << metrics.detail_l2->read_mem_bytes << "\n"
       << "starrocks_cache_read_bytes_total{type=\"disk\"} " << metrics.detail_l2->read_disk_bytes << "\n";

    // Write metrics
    ss << "# HELP starrocks_cache_write_operations_total Total write operations\n"
       << "# TYPE starrocks_cache_write_operations_total counter\n"
       << "starrocks_cache_write_operations_total{type=\"success\"} " << metrics.detail_l2->write_success_count << "\n"
       << "starrocks_cache_write_operations_total{type=\"fail\"} " << metrics.detail_l2->write_fail_count << "\n";

    ss << "# HELP starrocks_cache_write_bytes_total Total write bytes\n"
       << "# TYPE starrocks_cache_write_bytes_total counter\n"
       << "starrocks_cache_write_bytes_total " << metrics.detail_l2->write_bytes << "\n";

    // Remove metrics
    ss << "# HELP starrocks_cache_remove_operations_total Total remove operations\n"
       << "# TYPE starrocks_cache_remove_operations_total counter\n"
       << "starrocks_cache_remove_operations_total{type=\"success\"} " << metrics.detail_l2->remove_success_count << "\n"
       << "starrocks_cache_remove_operations_total{type=\"fail\"} " << metrics.detail_l2->remove_fail_count << "\n";

    ss << "# HELP starrocks_cache_remove_bytes_total Total remove bytes\n"
       << "# TYPE starrocks_cache_remove_bytes_total counter\n"
       << "starrocks_cache_remove_bytes_total " << metrics.detail_l2->remove_bytes << "\n";

    // Current operation counts
    ss << "# HELP starrocks_cache_current_operations Current operations count\n"
       << "# TYPE starrocks_cache_current_operations gauge\n"
       << "starrocks_cache_current_operations{type=\"reading\"} " << metrics.detail_l2->current_reading_count << "\n"
       << "starrocks_cache_current_operations{type=\"writing\"} " << metrics.detail_l2->current_writing_count << "\n"
       << "starrocks_cache_current_operations{type=\"removing\"} " << metrics.detail_l2->current_removing_count << "\n";

    req->add_output_header(HttpHeaders::CONTENT_TYPE, "text/plain; version=0.0.4");
    HttpChannel::send_reply(req, ss.str());
}

void DataCacheAction::_handle_error(HttpRequest* req, const std::string& err_msg) {
    _handle(req, [err_msg](rapidjson::Document& root) {
        auto& allocator = root.GetAllocator();
        root.AddMember("error", rapidjson::Value(err_msg.c_str(), err_msg.size()), allocator);
    });
}

} // namespace starrocks
