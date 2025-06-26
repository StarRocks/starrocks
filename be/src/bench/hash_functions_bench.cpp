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
#include <testutil/assert.h>

#include <chrono>
#include <memory>
#include <random>
#include <vector>

#include "bench/bench_util.h"
#include "exprs/hash_functions.h"
#include "exprs/time_functions.h"
#include "runtime/runtime_state.h"
#include "testutil/function_utils.h"

namespace starrocks {

class HashFunctionsBench {
public:
    void SetUp();
    void TearDown() {}

    HashFunctionsBench(size_t num_column, size_t num_rows) : _num_column(num_column), _num_rows(num_rows) {}

    void do_bench(benchmark::State& state, size_t num_column, bool test_default_hash);

private:
    size_t _num_column = 0;
    size_t _num_rows = 0;
    Columns _columns{};
};

void HashFunctionsBench::SetUp() {
    for (int i = 0; i < _num_column; i++) {
        auto columnPtr = BenchUtil::create_random_string_column(_num_rows, 32);
        _columns.push_back(std::move(columnPtr));
    }
}

void HashFunctionsBench::do_bench(benchmark::State& state, size_t num_rows, bool test_default_hash) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    if (test_default_hash) {
        ColumnPtr result = HashFunctions::murmur_hash3_32(ctx.get(), _columns).value();
        auto column = ColumnHelper::cast_to<TYPE_INT>(result);
    } else {
        ColumnPtr result = HashFunctions::xx_hash3_64(ctx.get(), _columns).value();
        auto column = ColumnHelper::cast_to<TYPE_BIGINT>(result);
    }
}

static void BM_HashFunctions_Eval_Arg(benchmark::internal::Benchmark* b) {
    b->Args({10, true});
    b->Args({10, false});
    b->Args({100, true});
    b->Args({100, false});
    b->Args({10000, true});
    b->Args({10000, false});
    b->Args({1000000, true});
    b->Args({1000000, false});
    b->Iterations(10000);
}

static void BM_HashFunctions_Eval(benchmark::State& state) {
    size_t num_rows = state.range(0);
    bool test_default_hash = state.range(1);

    HashFunctionsBench hashFunctionsBench(1, num_rows);
    hashFunctionsBench.SetUp();

    for (auto _ : state) {
        hashFunctionsBench.do_bench(state, num_rows, test_default_hash);
    }
}

BENCHMARK(BM_HashFunctions_Eval)->Apply(BM_HashFunctions_Eval_Arg);

class HourFromUnixtimeBench {
public:
    HourFromUnixtimeBench(int N = 4096)
            : N(N), min_ts(946684800), max_ts(1893456000), rng(12345), dist(min_ts, max_ts) {
        // Generate N random unix timestamps
        timestamps.reserve(N);
        for (int i = 0; i < N; ++i) {
            timestamps.push_back(dist(rng));
        }
        // Construct Column
        col = Int64Column::create();
        for (int i = 0; i < N; ++i) {
            col->append(timestamps[i]);
        }
        columns.emplace_back(col);
        // Construct context
        globals.__set_now_string("2020-01-01 00:00:00");
        globals.__set_timestamp_ms(1577836800000);
        globals.__set_time_zone("UTC");
        state = std::make_unique<starrocks::RuntimeState>(globals);
        utils = std::make_unique<starrocks::FunctionUtils>(state.get());
    }

    void bench_hour_from_unixtime(benchmark::State& state_bench) {
        for (auto _ : state_bench) {
            auto result = starrocks::TimeFunctions::hour_from_unixtime(utils->get_fn_ctx(), columns).value();
            benchmark::DoNotOptimize(result);
        }
    }

    void bench_from_unixtime_extract_hour(benchmark::State& state_bench) {
        for (auto _ : state_bench) {
            auto dt_col_ptr = starrocks::TimeFunctions::from_unix_to_datetime_64(utils->get_fn_ctx(), columns).value();
            auto dt_col = starrocks::ColumnHelper::cast_to<starrocks::TYPE_DATETIME>(dt_col_ptr);
            int64_t sum = 0;
            for (int i = 0; i < N; ++i) {
                int year, month, day, hour, minute, second, usec;
                dt_col->get_data()[i].to_timestamp(&year, &month, &day, &hour, &minute, &second, &usec);
                sum += hour;
            }
            benchmark::DoNotOptimize(sum);
        }
    }

private:
    int N;
    int64_t min_ts, max_ts;
    std::mt19937_64 rng;
    std::uniform_int_distribution<int64_t> dist;
    std::vector<int64_t> timestamps;
    starrocks::Int64Column::Ptr col;
    starrocks::Columns columns;
    starrocks::TQueryGlobals globals;
    std::unique_ptr<starrocks::RuntimeState> state;
    std::unique_ptr<starrocks::FunctionUtils> utils;
};

static void BM_HourFromUnixtime(benchmark::State& state) {
    static HourFromUnixtimeBench suite;
    suite.bench_hour_from_unixtime(state);
}

static void BM_FromUnixtime_HourExtract(benchmark::State& state) {
    static HourFromUnixtimeBench suite;
    suite.bench_from_unixtime_extract_hour(state);
}

// Run on (104 X 3200 MHz CPU s)
// CPU Caches:
//   L1 Data 32 KiB (x52)
//   L1 Instruction 32 KiB (x52)
//   L2 Unified 1024 KiB (x52)
//   L3 Unified 36608 KiB (x2)
// Load Average: 36.03, 17.40, 21.87
// ----------------------------------------------------------------------
// Benchmark                            Time             CPU   Iterations
// ----------------------------------------------------------------------
// BM_HourFromUnixtime             147895 ns       147841 ns         4147
// BM_FromUnixtime_HourExtract     656650 ns       651969 ns         1445
BENCHMARK(BM_HourFromUnixtime);
BENCHMARK(BM_FromUnixtime_HourExtract);

} // namespace starrocks

BENCHMARK_MAIN();