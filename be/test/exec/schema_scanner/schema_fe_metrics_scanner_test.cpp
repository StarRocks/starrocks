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

#include "exec/schema_scanner/schema_fe_metrics_scanner.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "http/ev_http_server.h"
#include "http/http_channel.h"
#include "http/http_client.h"
#include "http/http_handler.h"
#include "http/http_request.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class SchemaFeMetricsScannerGetHandler : public HttpHandler {
public:
    SchemaFeMetricsScannerGetHandler() = default;
    void handle(HttpRequest* req) override {
        std::string resp = R"([{"tags":{"metric":"jvm_young_gc","type":"count"},"unit":"nounit","value:741}])";
        HttpChannel::send_reply(req, resp);
    }
};

class SchemaFeMetricsScannerTest : public testing::Test {
public:
    SchemaFeMetricsScannerTest() {}
    ~SchemaFeMetricsScannerTest() override = default;

    void SetUp() override {
        _server = std::make_unique<EvHttpServer>(0);
        _server->register_handler(GET, "/metrics", &_handler);
        ASSERT_OK(_server->start());
        _port = _server->get_real_port();
        ASSERT_NE(0, _port);
        _hostname = "http://127.0.0.1:" + std::to_string(_port);
    }

    void TearDown() override {
        _server->stop();
        _server->join();
    }

protected:
    SchemaFeMetricsScannerGetHandler _handler;
    std::unique_ptr<EvHttpServer> _server = nullptr;
    int _port = 0;
    std::string _hostname = "";
    ObjectPool _pool;
};

TEST_F(SchemaFeMetricsScannerTest, test_partial_json) {
    RuntimeState state;
    TFrontend frontend;
    frontend.__set_ip("127.0.0.1");
    frontend.__set_id("1");
    frontend.__set_http_port(_port);

    SchemaScannerParam param;
    param.frontends.push_back(std::move(frontend));

    SchemaFeMetricsScanner scanner;
    ASSERT_OK(scanner.init(&param, &_pool));
    auto st = scanner.start(&state);
    ASSERT_ERROR(st);
    ASSERT_TRUE(st.message().find("A string is opened") != std::string::npos);
}
} // namespace starrocks
