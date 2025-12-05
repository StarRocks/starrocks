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

#include "cache/data_cache_hit_rate_counter.hpp"
#include "cache/disk_cache/starcache_engine.h"
#include "cache/disk_cache/test_cache_utils.h"
#include "http/http_channel.h"
#include "http/http_request.h"
#include "util/brpc_stub_cache.h"

namespace starrocks {

extern void (*s_injected_send_reply)(HttpRequest*, HttpStatus, std::string_view);

namespace {
static std::string k_response_str;
static void inject_send_reply(HttpRequest* request, HttpStatus status, std::string_view content) {
    k_response_str = content;
}
} // namespace

class DataCacheActionTest : public testing::Test {
public:
    DataCacheActionTest() = default;
    ~DataCacheActionTest() override = default;
    static void SetUpTestSuite() { s_injected_send_reply = inject_send_reply; }
    static void TearDownTestSuite() { s_injected_send_reply = nullptr; }

    void SetUp() override {
        k_response_str = "";
        _evhttp_req = evhttp_request_new(nullptr, nullptr);

        auto options = TestCacheUtils::create_simple_options(256 * KB, 20 * MB);
        _cache = std::make_shared<StarCacheEngine>();
        ASSERT_OK(reinterpret_cast<StarCacheEngine*>(_cache.get())->init(options));
        _action = std::make_shared<DataCacheAction>(_cache.get(), nullptr);
    }
    void TearDown() override {
        if (_evhttp_req != nullptr) {
            evhttp_request_free(_evhttp_req);
        }
    }

    std::shared_ptr<HttpRequest> create_request(const std::string& action_name);

protected:
    evhttp_request* _evhttp_req = nullptr;
    std::shared_ptr<LocalDiskCacheEngine> _cache;
    std::shared_ptr<DataCacheAction> _action;
};

std::shared_ptr<HttpRequest> DataCacheActionTest::create_request(const std::string& action_name) {
    auto request = std::make_shared<HttpRequest>(_evhttp_req);
    request->set_method(HttpMethod::GET);
    request->add_param("action", action_name);
    request->set_handler(_action.get());
    return request;
}

TEST_F(DataCacheActionTest, stat_success) {
    auto request = create_request("stat");

    _action->on_header(request.get());
    _action->handle(request.get());

    rapidjson::Document doc;
    doc.Parse(k_response_str.c_str());
    ASSERT_STREQ("NORMAL", doc["block_cache_status"].GetString());
}

TEST_F(DataCacheActionTest, app_stat_success) {
    DataCacheHitRateCounter* counter = DataCacheHitRateCounter::instance();
    counter->reset();

    {
        auto request = create_request("app_stat");
        _action->on_header(request.get());
        _action->handle(request.get());

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());

        EXPECT_EQ(0, doc["block_cache_hit_bytes"].GetInt64());
        EXPECT_EQ(0, doc["block_cache_miss_bytes"].GetInt64());
        EXPECT_EQ(0, doc["block_cache_hit_rate"].GetDouble());

        EXPECT_EQ(0, doc["block_cache_hit_bytes_last_minute"].GetInt64());
        EXPECT_EQ(0, doc["block_cache_miss_bytes_last_minute"].GetInt64());
        EXPECT_EQ(0, doc["block_cache_hit_rate_last_minute"].GetDouble());

        EXPECT_EQ(0, doc["page_cache_hit_count"].GetInt64());
        EXPECT_EQ(0, doc["page_cache_miss_count"].GetInt64());
        EXPECT_EQ(0, doc["page_cache_hit_rate"].GetDouble());

        EXPECT_EQ(0, doc["page_cache_hit_count_last_minute"].GetInt64());
        EXPECT_EQ(0, doc["page_cache_miss_count_last_minute"].GetInt64());
        EXPECT_EQ(0, doc["page_cache_hit_rate_last_minute"].GetDouble());
    }

    counter->update_block_cache_stat(3, 10);
    counter->update_page_cache_stat(6, 10);

    {
        auto request = create_request("app_stat");
        _action->on_header(request.get());
        _action->handle(request.get());

        rapidjson::Document doc;
        doc.Parse(k_response_str.c_str());

        EXPECT_EQ(3, doc["block_cache_hit_bytes"].GetInt64());
        EXPECT_EQ(10, doc["block_cache_miss_bytes"].GetInt64());
        EXPECT_EQ(0.23, doc["block_cache_hit_rate"].GetDouble());

        EXPECT_EQ(6, doc["page_cache_hit_count"].GetInt64());
        EXPECT_EQ(4, doc["page_cache_miss_count"].GetInt64());
        EXPECT_EQ(0.6, doc["page_cache_hit_rate"].GetDouble());
    }
}

TEST_F(DataCacheActionTest, stat_with_uninitialized_cache) {
    auto cache = std::make_shared<StarCacheEngine>();
    DataCacheAction action(cache.get(), nullptr);

    HttpRequest request(_evhttp_req);
    request.set_method(HttpMethod::GET);
    request.add_param("action", "stat");
    request.set_handler(&action);
    action.on_header(&request);
    action.handle(&request);

    rapidjson::Document doc;
    doc.Parse(k_response_str.c_str());
    ASSERT_STREQ("Cache system is not ready", doc["error"].GetString());
}

} // namespace starrocks
