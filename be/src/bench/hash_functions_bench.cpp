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
#include "util/hash.h"
#include "util/phmap/phmap.h"

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

const static size_t kCRC32HashSeed = 0x811C9DC5;
// Custom hash functor using crc_hash_64
struct CrcHash64String {
    size_t operator()(const std::string& s) const {
        return starrocks::crc_hash_64(s.data(), static_cast<int32_t>(s.size()), kCRC32HashSeed);
    }
};

struct CrcHash64StringUnmixed {
    size_t operator()(const std::string& s) const {
        return starrocks::crc_hash_64_unmixed(s.data(), static_cast<int32_t>(s.size()), kCRC32HashSeed);
    }
};

// Generate a dataset to simulate HashSet fingerprint collisions, which can degrade HashSet performance
static std::vector<std::string> gen_32bytes_string(size_t num_elems) {
    std::vector<std::string> data;
    data.reserve(num_elems);
    for (size_t i = 0; i < num_elems; ++i) {
        char buf[33];
        snprintf(buf, sizeof(buf), "%032zu", i);
        // construct lowest 7bit conflicts
        size_t hash = starrocks::crc_hash_64_unmixed(buf, sizeof(buf), kCRC32HashSeed);
        while ((hash & 0xF) != 0) {
            buf[32]++;
            hash = starrocks::crc_hash_64_unmixed(buf, sizeof(buf), kCRC32HashSeed);
        }
        data.emplace_back(buf);
    }
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(data.begin(), data.end(), g);

    return data;
}

template <class HashFunction>
static void BM_PhmapFlatHashSet_CrcHash64_Insert_Impl(benchmark::State& state) {
    size_t num_elems = state.range(0);
    auto data = gen_32bytes_string(num_elems);
    for (auto _ : state) {
        phmap::flat_hash_set<std::string, HashFunction> set;
        for (const auto& s : data) {
            set.insert(s);
        }
        benchmark::DoNotOptimize(set);
    }
}

static void BM_PhmapFlatHashSet_CrcHash64_Insert(benchmark::State& state) {
    BM_PhmapFlatHashSet_CrcHash64_Insert_Impl<CrcHash64String>(state);
}

static void BM_PhmapFlatHashSet_CrcHash64_Insert_Unmixed(benchmark::State& state) {
    BM_PhmapFlatHashSet_CrcHash64_Insert_Impl<CrcHash64StringUnmixed>(state);
}

template <class HashFunction>
static void BM_PhmapFlatHashSet_CrcHash64_Lookup_Impl(benchmark::State& state) {
    size_t num_elems = state.range(0);
    auto data = gen_32bytes_string(num_elems);
    // Half success, half failure
    auto lookup_data = gen_32bytes_string(num_elems * 2);

    phmap::flat_hash_set<std::string, HashFunction> set;
    for (const auto& s : data) {
        set.insert(s);
    }

    for (auto _ : state) {
        size_t found = 0;
        for (const auto& s : lookup_data) {
            found += set.count(s);
        }
        benchmark::DoNotOptimize(found);
    }
}

static void BM_PhmapFlatHashSet_CrcHash64_Lookup(benchmark::State& state) {
    BM_PhmapFlatHashSet_CrcHash64_Lookup_Impl<CrcHash64String>(state);
}

static void BM_PhmapFlatHashSet_CrcHash64_Lookup_Unmixed(benchmark::State& state) {
    BM_PhmapFlatHashSet_CrcHash64_Lookup_Impl<CrcHash64StringUnmixed>(state);
}

// 2025-06-19T15:48:27+08:00
// Running ./hash_functions_bench
// Run on (32 X 2500 MHz CPU s)
// CPU Caches:
//   L1 Data 32 KiB (x16)
//   L1 Instruction 32 KiB (x16)
//   L2 Unified 1024 KiB (x16)
//   L3 Unified 36608 KiB (x1)
// Load Average: 0.12, 0.09, 0.25
// -----------------------------------------------------------------------------------------------
// Benchmark                                                     Time             CPU   Iterations
// -----------------------------------------------------------------------------------------------
// BM_PhmapFlatHashSet_CrcHash64_Insert/10000              1098692 ns      1098642 ns          642
// BM_PhmapFlatHashSet_CrcHash64_Insert/100000            13848007 ns     13846984 ns           48
// BM_PhmapFlatHashSet_CrcHash64_Insert/1000000          552114016 ns    552078649 ns            1
// BM_PhmapFlatHashSet_CrcHash64_Insert_Unmixed/10000      1290259 ns      1290163 ns          541
// BM_PhmapFlatHashSet_CrcHash64_Insert_Unmixed/100000    18784369 ns     18783587 ns           38
// BM_PhmapFlatHashSet_CrcHash64_Insert_Unmixed/1000000  626093501 ns    626056004 ns            1
// BM_PhmapFlatHashSet_CrcHash64_Lookup/10000               605826 ns       605798 ns         1161
// BM_PhmapFlatHashSet_CrcHash64_Lookup/100000            11105350 ns     11104843 ns           63
// BM_PhmapFlatHashSet_CrcHash64_Lookup/1000000          254053820 ns    254043024 ns            3
// BM_PhmapFlatHashSet_CrcHash64_Lookup_Unmixed/10000       709504 ns       709473 ns          987
// BM_PhmapFlatHashSet_CrcHash64_Lookup_Unmixed/100000    10992399 ns     10991650 ns           63
// BM_PhmapFlatHashSet_CrcHash64_Lookup_Unmixed/1000000  254696960 ns    254686158 ns            3
BENCHMARK(BM_PhmapFlatHashSet_CrcHash64_Insert)->Arg(10 * 1000)->Arg(100 * 1000)->Arg(1000 * 1000);
BENCHMARK(BM_PhmapFlatHashSet_CrcHash64_Insert_Unmixed)->Arg(10 * 1000)->Arg(100 * 1000)->Arg(1000 * 1000);
BENCHMARK(BM_PhmapFlatHashSet_CrcHash64_Lookup)->Arg(10 * 1000)->Arg(100 * 1000)->Arg(1000 * 1000);
BENCHMARK(BM_PhmapFlatHashSet_CrcHash64_Lookup_Unmixed)->Arg(10 * 1000)->Arg(100 * 1000)->Arg(1000 * 1000);

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
            // Use const_cast for read-only benchmark access
            auto* mutable_dt_col = const_cast<starrocks::TimestampColumn*>(dt_col.get());
            for (int i = 0; i < N; ++i) {
                int year, month, day, hour, minute, second, usec;
                mutable_dt_col->get_data()[i].to_timestamp(&year, &month, &day, &hour, &minute, &second, &usec);
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
    starrocks::Int64Column::MutablePtr col;
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