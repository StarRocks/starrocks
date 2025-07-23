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

#include <gtest/gtest.h>

#include "common/config.h"
#include "util/table_metrics.h"

namespace starrocks {

class TableMetricsMgrTest : public testing::Test {};

TEST_F(TableMetricsMgrTest, register_unregister) {
    config::enable_table_metrics = true;
    auto mgr = std::make_shared<TableMetricsManager>();
    mgr->register_table(1);
    mgr->register_table(2);
    ASSERT_EQ(mgr->get_table_metrics(1)->ref_count, 1);
    ASSERT_EQ(mgr->get_table_metrics(1)->installed, true);
    ASSERT_EQ(mgr->get_table_metrics(2)->ref_count, 1);
    ASSERT_EQ(mgr->get_table_metrics(2)->installed, true);
    mgr->register_table(1);
    ASSERT_EQ(mgr->get_table_metrics(1)->ref_count, 2);

    mgr->unregister_table(2);
    ASSERT_EQ(mgr->get_table_metrics(2)->ref_count, 0);
    mgr->unregister_table(1);
    ASSERT_EQ(mgr->get_table_metrics(1)->ref_count, 1);

    // after cleanup
    mgr->cleanup();
    // this is block hole metrics
    ASSERT_EQ(mgr->get_table_metrics(2)->installed, false);
    ASSERT_EQ(mgr->metric_registry()->get_metric("table_scan_read_bytes", MetricLabels().add("table_id", "2")),
              nullptr);
    ASSERT_NE(mgr->metric_registry()->get_metric("table_scan_read_bytes", MetricLabels().add("table_id", "1")),
              nullptr);
}
// register, un register

TEST_F(TableMetricsMgrTest, test_max_table_metrics_num) {
    config::enable_table_metrics = true;
    config::max_table_metrics_num = 2;
    auto mgr = std::make_shared<TableMetricsManager>();
    mgr->register_table(1);
    mgr->register_table(2);
    mgr->register_table(3);
    ASSERT_EQ(mgr->get_table_metrics(1)->ref_count, 1);
    ASSERT_EQ(mgr->get_table_metrics(1)->installed, true);
    ASSERT_EQ(mgr->get_table_metrics(2)->ref_count, 1);
    ASSERT_EQ(mgr->get_table_metrics(2)->installed, true);
    ASSERT_EQ(mgr->get_table_metrics(3)->ref_count, 1);
    ASSERT_EQ(mgr->get_table_metrics(3)->installed, false);

    mgr->unregister_table(1);
    ASSERT_NE(mgr->metric_registry()->get_metric("table_scan_read_bytes", MetricLabels().add("table_id", "1")),
              nullptr);
    ASSERT_NE(mgr->metric_registry()->get_metric("table_scan_read_bytes", MetricLabels().add("table_id", "2")),
              nullptr);
    ASSERT_EQ(mgr->metric_registry()->get_metric("table_scan_read_bytes", MetricLabels().add("table_id", "3")),
              nullptr);

    // after cleanup
    mgr->cleanup();
    ASSERT_EQ(mgr->metric_registry()->get_metric("table_scan_read_bytes", MetricLabels().add("table_id", "1")),
              nullptr);
    ASSERT_NE(mgr->metric_registry()->get_metric("table_scan_read_bytes", MetricLabels().add("table_id", "2")),
              nullptr);

    mgr->register_table(4);
    ASSERT_EQ(mgr->get_table_metrics(4)->ref_count, 1);
    ASSERT_EQ(mgr->get_table_metrics(4)->installed, true);
    ASSERT_NE(mgr->metric_registry()->get_metric("table_scan_read_bytes", MetricLabels().add("table_id", "4")),
              nullptr);
}
} // namespace starrocks