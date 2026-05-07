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

#include "base/metrics.h"
#include "common/config_metrics_fwd.h"
#include "common/metrics/process_metrics_registry.h"
#include "http/http_channel.h"
#include "http/http_request.h"
#include "http/http_response.h"
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
        _old_dump_metrics_with_bvar = config::dump_metrics_with_bvar;
        _old_enable_collect_table_metrics = config::enable_collect_table_metrics;
        _old_enable_table_metrics = config::enable_table_metrics;
        _old_max_table_metrics_num = config::max_table_metrics_num;
        config::dump_metrics_with_bvar = false;
        config::enable_collect_table_metrics = false;
        config::enable_table_metrics = false;
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
        config::dump_metrics_with_bvar = _old_dump_metrics_with_bvar;
        config::enable_collect_table_metrics = _old_enable_collect_table_metrics;
        config::enable_table_metrics = _old_enable_table_metrics;
        config::max_table_metrics_num = _old_max_table_metrics_num;
        if (_evhttp_req != nullptr) {
            evhttp_request_free(_evhttp_req);
        }
    }

protected:
    void enable_table_metrics(ProcessMetricsRegistry* registry) {
        config::enable_collect_table_metrics = true;
        config::enable_table_metrics = true;
        config::max_table_metrics_num = 100;

        registry->table_metrics_mgr()->register_table(42);
        registry->table_metrics(42)->scan_read_rows.increment(11);
    }

protected:
    bool _old_dump_metrics_with_bvar = false;
    bool _old_enable_collect_table_metrics = false;
    bool _old_enable_table_metrics = false;
    int64_t _old_max_table_metrics_num = 0;
    evhttp_request* _evhttp_req = nullptr;
};

TEST_F(MetricsActionTest, prometheus_output) {
    ProcessMetricsRegistry registry("test");
    IntGauge cpu_idle(MetricUnit::PERCENT);
    cpu_idle.set_value(50);
    registry.root_registry()->register_metric("cpu_idle", &cpu_idle);
    IntCounter put_requests_total(MetricUnit::NOUNIT);
    put_requests_total.increment(2345);
    registry.root_registry()->register_metric(
            "requests_total", MetricLabels().add("type", "put").add("path", "/sports"), &put_requests_total);
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
    ProcessMetricsRegistry registry("");
    IntGauge cpu_idle(MetricUnit::PERCENT);
    cpu_idle.set_value(50);
    registry.root_registry()->register_metric("cpu_idle", &cpu_idle);
    s_expect_response =
            "# TYPE cpu_idle gauge\n"
            "cpu_idle 50\n";
    HttpRequest request(_evhttp_req);
    MetricsAction action(&registry, &mock_send_reply);
    action.handle(&request);
}

TEST_F(MetricsActionTest, prometheus_no_name) {
    ProcessMetricsRegistry registry("test");
    IntGauge cpu_idle(MetricUnit::PERCENT);
    cpu_idle.set_value(50);
    registry.root_registry()->register_metric("", &cpu_idle);
    s_expect_response = "";
    HttpRequest request(_evhttp_req);
    MetricsAction action(&registry, &mock_send_reply);
    action.handle(&request);
}

TEST_F(MetricsActionTest, prometheus_output_with_table_metrics) {
    ProcessMetricsRegistry registry("test");
    enable_table_metrics(&registry);
    s_expect_response =
            "# TYPE starrocks_be_table_load_bytes counter\n"
            "starrocks_be_table_load_bytes{table_id=\"42\"} 0\n"
            "# TYPE starrocks_be_table_load_rows counter\n"
            "starrocks_be_table_load_rows{table_id=\"42\"} 0\n"
            "# TYPE starrocks_be_table_scan_read_bytes counter\n"
            "starrocks_be_table_scan_read_bytes{table_id=\"42\"} 0\n"
            "# TYPE starrocks_be_table_scan_read_rows counter\n"
            "starrocks_be_table_scan_read_rows{table_id=\"42\"} 11\n";
    HttpRequest request(_evhttp_req);
    MetricsAction action(&registry, &mock_send_reply);
    action.handle(&request);
}

TEST_F(MetricsActionTest, json_output_with_table_metrics) {
    ProcessMetricsRegistry registry("test");
    IntCounter put_requests_total(MetricUnit::NOUNIT);
    put_requests_total.increment(2345);
    registry.root_registry()->register_metric("requests_total", MetricLabels().add("type", "put"), &put_requests_total);
    enable_table_metrics(&registry);
    s_expect_response =
            "[{\"tags\":{\"metric\":\"requests_total\",\"type\":\"put\"},\"unit\":\"nounit\",\"value\":2345},"
            "{\"tags\":{\"metric\":\"table_load_bytes\",\"table_id\":\"42\"},\"unit\":\"bytes\",\"value\":0},"
            "{\"tags\":{\"metric\":\"table_load_rows\",\"table_id\":\"42\"},\"unit\":\"bytes\",\"value\":0},"
            "{\"tags\":{\"metric\":\"table_scan_read_bytes\",\"table_id\":\"42\"},\"unit\":\"bytes\",\"value\":0},"
            "{\"tags\":{\"metric\":\"table_scan_read_rows\",\"table_id\":\"42\"},\"unit\":\"rows\",\"value\":11}]";
    HttpRequest request(_evhttp_req);
    request.add_param("type", "json");
    MetricsAction action(&registry, &mock_send_reply);
    action.handle(&request);
}

TEST_F(MetricsActionTest, core_output_does_not_collect_table_metrics) {
    ProcessMetricsRegistry registry("starrocks_be");
    IntGauge process_thread_num(MetricUnit::NOUNIT);
    process_thread_num.set_value(5);
    registry.root_registry()->register_metric("process_thread_num", &process_thread_num);
    enable_table_metrics(&registry);
    s_expect_response = "starrocks_be_process_thread_num LONG 5\n";
    HttpRequest request(_evhttp_req);
    request.add_param("type", "core");
    MetricsAction action(&registry, &mock_send_reply);
    action.handle(&request);
}

} // namespace starrocks
