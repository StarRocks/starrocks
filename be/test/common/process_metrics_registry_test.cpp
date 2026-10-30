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

#include "common/metrics/process_metrics_registry.h"

#include <gtest/gtest.h>

#include <sstream>

#include "base/utility/defer_op.h"
#include "common/config_metrics_fwd.h"

namespace starrocks {

class ProcessMetricsRegistryTestVisitor : public MetricsVisitor {
public:
    void visit(const std::string& prefix, const std::string& name, MetricCollector* collector) override {
        for (auto& it : collector->metrics()) {
            _ss << prefix << "_" << name;
            if (!it.first.empty()) {
                _ss << "{" << it.first.to_string() << "}";
            }
            _ss << " " << it.second->to_string() << "\n";
        }
    }

    std::string to_string() const { return _ss.str(); }

private:
    std::stringstream _ss;
};

TEST(ProcessMetricsRegistryTest, CollectsRootRegistryMetrics) {
    ProcessMetricsRegistry registry("test_be");
    IntCounter counter(MetricUnit::REQUESTS);
    counter.increment(7);

    ASSERT_TRUE(registry.root_registry()->register_metric("requests_total", &counter));

    ProcessMetricsRegistryTestVisitor visitor;
    registry.collect_root(&visitor);

    ASSERT_EQ("test_be_requests_total 7\n", visitor.to_string());
}

TEST(ProcessMetricsRegistryTest, OwnsTableMetricsRegistry) {
    const bool old_enable_table_metrics = config::enable_table_metrics;
    const int64_t old_max_table_metrics_num = config::max_table_metrics_num;
    DeferOp restore_config([&] {
        config::enable_table_metrics = old_enable_table_metrics;
        config::max_table_metrics_num = old_max_table_metrics_num;
    });
    config::enable_table_metrics = true;
    config::max_table_metrics_num = 100;

    ProcessMetricsRegistry registry("test_be");
    registry.table_metrics_mgr()->register_table(42);
    auto table_metrics = registry.table_metrics(42);
    table_metrics->scan_read_rows.increment(11);

    ProcessMetricsRegistryTestVisitor visitor;
    registry.collect_table(&visitor);

    ASSERT_NE(std::string::npos, visitor.to_string().find("starrocks_be_table_scan_read_rows{table_id=42} 11\n"));
}

} // namespace starrocks
