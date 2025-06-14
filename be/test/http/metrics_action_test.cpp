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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/http/metrics_action_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "http/action/metrics_action.h"

#include <gtest/gtest.h>

#include "http/http_channel.h"
#include "http/http_request.h"
#include "http/http_response.h"
#include "util/metrics.h"
#ifdef USE_STAROS
#include "metrics/metrics.h"
#endif

namespace starrocks {

#ifdef USE_STAROS
extern bool sDisableStarOSMetrics;
#endif

// Mock part
const char* s_expect_response = nullptr;

void mock_send_reply(const std::string& content) {
    ASSERT_STREQ(s_expect_response, content.c_str());
}

class MetricsActionTest : public testing::Test {
public:
    MetricsActionTest() = default;
    ~MetricsActionTest() override = default;
    void SetUp() override {
        config::dump_metrics_with_bvar = false;
        _evhttp_req = evhttp_request_new(nullptr, nullptr);
#ifdef USE_STAROS
        // disable staros metrics output to avoid confusing the test result.
        sDisableStarOSMetrics = true;
#endif
    }

    void TearDown() override {
#ifdef USE_STAROS
        sDisableStarOSMetrics = false;
#endif
        if (_evhttp_req != nullptr) {
            evhttp_request_free(_evhttp_req);
        }
    }

private:
    evhttp_request* _evhttp_req = nullptr;
};

TEST_F(MetricsActionTest, prometheus_output) {
    MetricRegistry registry("test");
    IntGauge cpu_idle(MetricUnit::PERCENT);
    cpu_idle.set_value(50);
    registry.register_metric("cpu_idle", &cpu_idle);
    IntCounter put_requests_total(MetricUnit::NOUNIT);
    put_requests_total.increment(2345);
    registry.register_metric("requests_total", MetricLabels().add("type", "put").add("path", "/sports"),
                             &put_requests_total);
    s_expect_response =
            "# TYPE test_cpu_idle gauge\n"
            "test_cpu_idle 50\n"
            "# TYPE test_requests_total counter\n"
            "test_requests_total{path=\"/sports\",type=\"put\"} 2345\n";
    HttpRequest request(_evhttp_req);
    MetricsAction action(&registry, &mock_send_reply);
    action.handle(&request);
}

TEST_F(MetricsActionTest, prometheus_no_prefix) {
    MetricRegistry registry("");
    IntGauge cpu_idle(MetricUnit::PERCENT);
    cpu_idle.set_value(50);
    registry.register_metric("cpu_idle", &cpu_idle);
    s_expect_response =
            "# TYPE cpu_idle gauge\n"
            "cpu_idle 50\n";
    HttpRequest request(_evhttp_req);
    MetricsAction action(&registry, &mock_send_reply);
    action.handle(&request);
}

TEST_F(MetricsActionTest, prometheus_no_name) {
    MetricRegistry registry("test");
    IntGauge cpu_idle(MetricUnit::PERCENT);
    cpu_idle.set_value(50);
    registry.register_metric("", &cpu_idle);
    s_expect_response = "";
    HttpRequest request(_evhttp_req);
    MetricsAction action(&registry, &mock_send_reply);
    action.handle(&request);
}

} // namespace starrocks
