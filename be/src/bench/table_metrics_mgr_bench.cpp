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

#include <cstdint>
#include <memory>

#include "common/config.h"
#include "util/metrics.h"
#include "util/table_metrics.h"

namespace starrocks {

class TableMetricsBench : public ::benchmark::Fixture {
public:
    TableMetricsBench() {
        config::max_table_metrics_num = INT64_MAX;
        config::enable_table_metrics = true;

        for (uint64_t i = 0; i < kMaxTableId; ++i) {
            table_ids[i] = i;
        }
    }

    void SetUp(const benchmark::State& state) override {}

    void TearDown(const benchmark::State& state) override {}

protected:
    static constexpr uint64_t kMaxTableId = 10000;
    std::array<uint64_t, kMaxTableId> table_ids;
    std::unique_ptr<TableMetricsManager> manager;
};

BENCHMARK_DEFINE_F(TableMetricsBench, BM_RegisterTable)(benchmark::State& state) {
    const int thread_id = state.thread_index;
    if (thread_id == 0) {
        manager = std::make_unique<TableMetricsManager>();
    }
    for (auto _ : state) {
        uint64_t start_id = thread_id;
        for (uint64_t i = 0; i < kMaxTableId; ++i) {
            manager->register_table(table_ids[(i + start_id) % kMaxTableId]);
        }
    }
}

BENCHMARK_DEFINE_F(TableMetricsBench, BM_UnregisterTable)(benchmark::State& state) {
    const int thread_id = state.thread_index;
    if (thread_id == 0) {
        manager = std::make_unique<TableMetricsManager>();
        for (uint64_t i = 0; i < kMaxTableId; ++i) {
            manager->register_table(table_ids[i]);
        }
    }

    for (auto _ : state) {
        uint64_t start_id = thread_id;
        for (uint64_t i = 0; i < kMaxTableId; ++i) {
            manager->unregister_table(table_ids[(i + start_id) % kMaxTableId]);
        }
    }
}

BENCHMARK_DEFINE_F(TableMetricsBench, BM_GetTableMetrics)(benchmark::State& state) {
    const int thread_id = state.thread_index;
    if (thread_id == 0) {
        manager = std::make_unique<TableMetricsManager>();
        for (uint64_t i = 0; i < kMaxTableId; ++i) {
            manager->register_table(table_ids[i]);
        }
    }

    for (auto _ : state) {
        uint64_t start_id = thread_id;
        for (uint64_t i = 0; i < kMaxTableId; ++i) {
            auto metrics = manager->get_table_metrics(table_ids[(i + start_id) % kMaxTableId]);
            benchmark::DoNotOptimize(metrics);
        }
    }
}

BENCHMARK_DEFINE_F(TableMetricsBench, BM_Cleanup)(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
        manager = std::make_unique<TableMetricsManager>();
        for (uint64_t i = 0; i < kMaxTableId; ++i) {
            manager->register_table(table_ids[i]);
        }
        for (uint64_t i = 0; i < kMaxTableId; i++) {
            manager->unregister_table(table_ids[i]);
        }
        state.ResumeTiming();

        manager->cleanup(true);
    }
}

BENCHMARK_REGISTER_F(TableMetricsBench, BM_RegisterTable)
        ->ThreadRange(1, 32)
        ->Iterations(1)
        ->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(TableMetricsBench, BM_UnregisterTable)->ThreadRange(1, 32)->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(TableMetricsBench, BM_GetTableMetrics)->ThreadRange(1, 32)->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(TableMetricsBench, BM_Cleanup)->Unit(benchmark::kMillisecond);

BENCHMARK_MAIN();

} // namespace starrocks
