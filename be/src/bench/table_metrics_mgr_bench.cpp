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

#include <benchmark/benchmark.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>
#include "util/table_metrics.h"

namespace starrocks {

class TableMetricsBench : public ::benchmark::Fixture {
public:
    TableMetricsBench() {
        metrics = std::make_unique<MetricRegistry>("benchmark");
        manager = std::make_unique<TableMetricsManager>(metrics.get());
        for (uint64_t i = 0; i < kMaxTableId; ++i) {
            table_ids[i] = i;
        }
    }

    void SetUp(const benchmark::State& state) override {
    }

    void TearDown(const benchmark::State&) override {
    }

    void bench_register_table(uint64_t start_id) {
        for (uint64_t i = 0;i < kMaxTableId; i++) {
            uint64_t table_id = table_ids[(i + start_id) % kMaxTableId];
            manager->register_table(table_id);
        }
    }

protected:
    static constexpr uint64_t kMaxTableId = 100000;
    std::array<uint64_t, kMaxTableId> table_ids;
    std::unique_ptr<MetricRegistry> metrics;
    std::unique_ptr<TableMetricsManager> manager;
};

BENCHMARK_DEFINE_F(TableMetricsBench, BM_RegisterTable)(benchmark::State& state) {
    const int thread_id = state.thread_index;
    
    for (auto _ : state) {
        uint64_t start_id = thread_id;
        for (uint64_t i = 0;i < kMaxTableId; ++i) {
            manager->register_table(table_ids[(i + start_id) % kMaxTableId]);
        }
    }
}

BENCHMARK_REGISTER_F(TableMetricsBench, BM_RegisterTable)
    ->ThreadRange(1, 32)
    ->Unit(benchmark::kMillisecond);

BENCHMARK_MAIN();

}