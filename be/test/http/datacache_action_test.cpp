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

#include <event2/http.h>
#include <event2/http_struct.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include "cache/block_cache/block_cache.h"
#include "cache/block_cache/block_cache_hit_rate_counter.hpp"
#include "gen_cpp/HeartbeatService_types.h"
#include "http/http_channel.h"
#include "http/http_request.h"
#include "runtime/exec_env.h"
#include "util/brpc_stub_cache.h"

class mg_connection;

namespace starrocks {

extern void (*s_injected_send_reply)(HttpRequest*, HttpStatus, std::string_view);

namespace {
static std::string k_response_str;
static void inject_send_reply(HttpRequest* request, HttpStatus status, std::string_view content) {
    k_response_str = content;
}
} // namespace

Status init_datacache_instance(const std::string& engine, BlockCache* cache) {
    if (cache->is_initialized()) {
        return Status::OK();
    }
    CacheOptions options;
    options.mem_space_size = 20 * 1024 * 1024;
    options.block_size = 256 * 1024;
    options.max_concurrent_inserts = 100000;
    options.enable_checksum = false;
    options.engine = engine;
    return cache->init(options);
}

class DataCacheActionTest : public testing::Test {
public:
    DataCacheActionTest() = default;
    ~DataCacheActionTest() override = default;
    static void SetUpTestSuite() { s_injected_send_reply = inject_send_reply; }
    static void TearDownTestSuite() { s_injected_send_reply = nullptr; }

    void SetUp() override {
        k_response_str = "";
        _env._brpc_stub_cache = new BrpcStubCache();
        _evhttp_req = evhttp_request_new(nullptr, nullptr);
    }
    void TearDown() override {
        delete _env._brpc_stub_cache;
        _env._brpc_stub_cache = nullptr;

        if (_evhttp_req != nullptr) {
            evhttp_request_free(_evhttp_req);
        }
    }

private:
    ExecEnv _env;
    evhttp_request* _evhttp_req = nullptr;
};

TEST_F(DataCacheActionTest, stat_success) {
    auto cache = BlockCache::instance();
    ASSERT_TRUE(init_datacache_instance("starcache", cache).ok());
    _env._block_cache = cache;

    DataCacheAction action(&_env);

    HttpRequest request(_evhttp_req);
    request._method = HttpMethod::GET;
    request._params.emplace("action", "stat");
    request.set_handler(&action);
    action.on_header(&request);
    action.handle(&request);

    rapidjson::Document doc;
    doc.Parse(k_response_str.c_str());
    ASSERT_STREQ("NORMAL", doc["status"].GetString());

    _env._block_cache = nullptr;
}

TEST_F(DataCacheActionTest, app_stat_success) {
    BlockCacheHitRateCounter* counter = BlockCacheHitRateCounter::instance();
    counter->reset();
    auto cache = BlockCache::instance();
    ASSERT_TRUE(init_datacache_instance("starcache", cache).ok());
    _env._block_cache = cache;

    DataCacheAction action(&_env);

    {
        HttpRequest request(_evhttp_req);
        request._method = HttpMethod::GET;
        request._params.emplace("action", "app_stat");
        request.set_handler(&action);
        action.on_header(&request);
        action.handle(&request);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        EXPECT_EQ(0, doc["hit_bytes"].GetInt64());
        EXPECT_EQ(0, doc["miss_bytes"].GetInt64());
        EXPECT_EQ(0, doc["hit_rate"].GetDouble());
        EXPECT_EQ(0, doc["hit_bytes_last_minute"].GetInt64());
        EXPECT_EQ(0, doc["miss_bytes_last_minute"].GetInt64());
        EXPECT_EQ(0, doc["hit_rate_last_minute"].GetDouble());
    }

    counter->update(3, 10);

    {
        HttpRequest request(_evhttp_req);
        request._method = HttpMethod::GET;
        request._params.emplace("action", "app_stat");
        request.set_handler(&action);
        action.on_header(&request);
        action.handle(&request);

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());
        EXPECT_EQ(3, doc["hit_bytes"].GetInt64());
        EXPECT_EQ(10, doc["miss_bytes"].GetInt64());
        EXPECT_EQ(0.23, doc["hit_rate"].GetDouble());
    }
    _env._block_cache = nullptr;
}

TEST_F(DataCacheActionTest, stat_with_uninitialized_cache) {
    DataCacheAction action(&_env);

    HttpRequest request(_evhttp_req);
    request._method = HttpMethod::GET;
    request._params.emplace("action", "stat");
    request.set_handler(&action);
    action.on_header(&request);
    action.handle(&request);

    rapidjson::Document doc;
    doc.Parse(k_response_str.c_str());
    ASSERT_STREQ("Cache system is not ready", doc["error"].GetString());
}

} // namespace starrocks
