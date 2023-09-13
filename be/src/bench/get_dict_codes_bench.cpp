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

#include <map>
#include <memory>
#include <random>
#include <vector>

#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "simd/batch_run_counter.h"
#include "simd/simd.h"
#include "util/slice.h"

namespace starrocks {
namespace parquet {

static const int kDictSize = 4000;
static const int kDictLength = 5;
static std::string kAlphaNumber =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

struct SliceHasher {
    uint32_t operator()(const Slice& s) const { return HashUtil::hash(s.data, s.size, 397); }
};

static void BM_GetDictCodesWithMap(benchmark::State& state) {
    std::vector<Slice> dict_values;
    for (int i = 0; i < kDictSize; i++) {
        dict_values.emplace_back(Slice(kAlphaNumber.data() + i, kDictLength));
    }
    std::unordered_map<Slice, int32_t, SliceHasher> dict_code_by_value;
    dict_code_by_value.reserve(kDictSize);
    for (int i = 0; i < kDictSize; i++) {
        dict_code_by_value[dict_values[i]] = i;
    }

    auto null_score = state.range(0);
    Filter filter(kDictSize + 1, 1);

    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<int> dist(0, 999);

    ColumnPtr column = ColumnHelper::create_column(TypeDescriptor{TYPE_VARCHAR}, true);
    column->append_strings_overflow(dict_values, kDictLength);
    column->append_default();
    for (int i = 0; i < kDictSize + 1; i++) {
        int random_number = dist(rng);
        if (random_number < null_score) {
            filter[i] = 0;
        }
    }

    for (auto _ : state) {
        state.PauseTiming();
        std::vector<int32_t> dict_codes;
        state.ResumeTiming();
        column->filter(filter);
        if (column->size() == 0) {
            continue;
        }
        const std::vector<uint8_t>& null_data = down_cast<NullableColumn*>(column.get())->immutable_null_column_data();
        bool has_null = column->has_null();
        bool all_null = false;

        if (has_null) {
            size_t count = SIMD::count_nonzero(null_data);
            all_null = (count == null_data.size());
        }
        if (all_null) {
            // do nothing
            continue;
        }

        auto* dict_nullable_column = down_cast<NullableColumn*>(column.get());
        auto* dict_value_binary_column = down_cast<BinaryColumn*>(dict_nullable_column->data_column().get());
        std::vector<Slice> dict_values_filtered = dict_value_binary_column->get_data();
        if (!has_null) {
            dict_codes.reserve(dict_values_filtered.size());
            for (size_t i = 0; i < dict_values_filtered.size(); i++) {
                dict_codes.emplace_back(dict_code_by_value[dict_values_filtered[i]]);
            }
        }
    }
}

template <int batch_size>
static void do_GetDictCodesWithFilterBatch(benchmark::State& state) {
    auto null_score = state.range(0);
    Filter filter(kDictSize, 1);

    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<int> dist(0, 99);

    for (int i = 0; i < kDictSize + 1; i++) {
        int random_number = dist(rng);
        if (random_number < null_score) {
            filter[i] = 0;
        }
    }

    for (auto _ : state) {
        state.PauseTiming();
        std::vector<int32_t> dict_codes;
        state.ResumeTiming();
        {
            BatchRunCounter<batch_size> batch_run(filter.data(), 0, filter.size() - 1);
            BatchCount batch = batch_run.next_batch();
            int index = 0;
            while (batch.length > 0) {
                if (batch.AllSet()) {
                    for (int32_t i = 0; i < batch.length; i++) {
                        dict_codes.emplace_back(index + i);
                    }
                } else if (batch.NoneSet()) {
                    // do nothing
                } else {
                    for (int32_t i = 0; i < batch.length; i++) {
                        if (filter[index + i]) {
                            dict_codes.emplace_back(index + i);
                        }
                    }
                }
                index += batch.length;
                batch = batch_run.next_batch();
            }
        }
    }
}

static void BM_GetDictCodesWithFilterBatch8(benchmark::State& state) {
    do_GetDictCodesWithFilterBatch<8>(state);
}

static void BM_GetDictCodesWithFilterBatch16(benchmark::State& state) {
    do_GetDictCodesWithFilterBatch<16>(state);
}

static void BM_GetDictCodesWithFilterBatch32(benchmark::State& state) {
    do_GetDictCodesWithFilterBatch<32>(state);
}

BENCHMARK(BM_GetDictCodesWithMap)->DenseRange(0, 100, 20)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_GetDictCodesWithMap)->DenseRange(200, 800, 200)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_GetDictCodesWithMap)->DenseRange(900, 1000, 20)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_GetDictCodesWithFilterBatch8)->DenseRange(0, 100, 20)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_GetDictCodesWithFilterBatch8)->DenseRange(200, 800, 200)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_GetDictCodesWithFilterBatch8)->DenseRange(900, 1000, 20)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_GetDictCodesWithFilterBatch16)->DenseRange(0, 100, 20)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_GetDictCodesWithFilterBatch16)->DenseRange(200, 800, 200)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_GetDictCodesWithFilterBatch16)->DenseRange(900, 1000, 20)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_GetDictCodesWithFilterBatch32)->DenseRange(0, 100, 20)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_GetDictCodesWithFilterBatch32)->DenseRange(200, 800, 200)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_GetDictCodesWithFilterBatch32)->DenseRange(900, 1000, 20)->Unit(benchmark::kMillisecond);

/*
result: num after '/' is filtered num each thousand
--------------------------------------------------------------------------------
Benchmark                                      Time             CPU   Iterations
--------------------------------------------------------------------------------
BM_GetDictCodesWithMap/0                   0.016 ms        0.016 ms        43205
BM_GetDictCodesWithMap/20                  0.022 ms        0.022 ms        31911
BM_GetDictCodesWithMap/40                  0.026 ms        0.026 ms        27811
BM_GetDictCodesWithMap/60                  0.027 ms        0.027 ms        25514
BM_GetDictCodesWithMap/80                  0.027 ms        0.027 ms        25459
BM_GetDictCodesWithMap/100                 0.027 ms        0.027 ms        25176
BM_GetDictCodesWithMap/200                 0.079 ms        0.079 ms        10000
BM_GetDictCodesWithMap/400                 0.060 ms        0.060 ms        11538
BM_GetDictCodesWithMap/600                 0.039 ms        0.039 ms        17202
BM_GetDictCodesWithMap/800                 0.021 ms        0.021 ms        32877
BM_GetDictCodesWithMap/900                 0.010 ms        0.010 ms        63951
BM_GetDictCodesWithMap/920                 0.009 ms        0.009 ms       100000
BM_GetDictCodesWithMap/940                 0.006 ms        0.007 ms       101940
BM_GetDictCodesWithMap/960                 0.005 ms        0.005 ms       133784
BM_GetDictCodesWithMap/980                 0.003 ms        0.003 ms       262643
BM_GetDictCodesWithMap/1000                0.001 ms        0.001 ms       915738
BM_GetDictCodesWithFilterBatch8/0          0.007 ms        0.007 ms        96666
BM_GetDictCodesWithFilterBatch8/20         0.007 ms        0.007 ms        96365
BM_GetDictCodesWithFilterBatch8/40         0.013 ms        0.013 ms        51321
BM_GetDictCodesWithFilterBatch8/60         0.014 ms        0.014 ms        50447
BM_GetDictCodesWithFilterBatch8/80         0.005 ms        0.005 ms       138567
BM_GetDictCodesWithFilterBatch8/100        0.002 ms        0.002 ms       431874
BM_GetDictCodesWithFilterBatch8/200        0.002 ms        0.002 ms       430421
BM_GetDictCodesWithFilterBatch8/400        0.002 ms        0.002 ms       431767
BM_GetDictCodesWithFilterBatch8/600        0.002 ms        0.002 ms       432447
BM_GetDictCodesWithFilterBatch8/800        0.002 ms        0.002 ms       431432
BM_GetDictCodesWithFilterBatch8/900        0.002 ms        0.002 ms       430866
BM_GetDictCodesWithFilterBatch8/920        0.002 ms        0.002 ms       430960
BM_GetDictCodesWithFilterBatch8/940        0.002 ms        0.002 ms       431136
BM_GetDictCodesWithFilterBatch8/960        0.002 ms        0.002 ms       430117
BM_GetDictCodesWithFilterBatch8/980        0.002 ms        0.002 ms       431726
BM_GetDictCodesWithFilterBatch8/1000       0.002 ms        0.002 ms       426819
BM_GetDictCodesWithFilterBatch16/0         0.007 ms        0.007 ms        95404
BM_GetDictCodesWithFilterBatch16/20        0.008 ms        0.008 ms        92134
BM_GetDictCodesWithFilterBatch16/40        0.011 ms        0.011 ms        60803
BM_GetDictCodesWithFilterBatch16/60        0.013 ms        0.013 ms        54988
BM_GetDictCodesWithFilterBatch16/80        0.008 ms        0.008 ms        92929
BM_GetDictCodesWithFilterBatch16/100       0.001 ms        0.001 ms       650863
BM_GetDictCodesWithFilterBatch16/200       0.001 ms        0.001 ms       651083
BM_GetDictCodesWithFilterBatch16/400       0.001 ms        0.001 ms       642681
BM_GetDictCodesWithFilterBatch16/600       0.001 ms        0.001 ms       645533
BM_GetDictCodesWithFilterBatch16/800       0.001 ms        0.001 ms       646922
BM_GetDictCodesWithFilterBatch16/900       0.001 ms        0.001 ms       653366
BM_GetDictCodesWithFilterBatch16/920       0.001 ms        0.001 ms       652897
BM_GetDictCodesWithFilterBatch16/940       0.001 ms        0.001 ms       651243
BM_GetDictCodesWithFilterBatch16/960       0.001 ms        0.001 ms       652812
BM_GetDictCodesWithFilterBatch16/980       0.001 ms        0.001 ms       651356
BM_GetDictCodesWithFilterBatch16/1000      0.001 ms        0.001 ms       649397
BM_GetDictCodesWithFilterBatch32/0         0.007 ms        0.007 ms        99050
BM_GetDictCodesWithFilterBatch32/20        0.007 ms        0.007 ms       103533
BM_GetDictCodesWithFilterBatch32/40        0.010 ms        0.010 ms        76477
BM_GetDictCodesWithFilterBatch32/60        0.010 ms        0.010 ms        69480
BM_GetDictCodesWithFilterBatch32/80        0.004 ms        0.004 ms       163877
BM_GetDictCodesWithFilterBatch32/100       0.001 ms        0.001 ms       801300
BM_GetDictCodesWithFilterBatch32/200       0.001 ms        0.001 ms       801533
BM_GetDictCodesWithFilterBatch32/400       0.001 ms        0.001 ms       801402
BM_GetDictCodesWithFilterBatch32/600       0.001 ms        0.001 ms       806368
BM_GetDictCodesWithFilterBatch32/800       0.001 ms        0.001 ms       805272
BM_GetDictCodesWithFilterBatch32/900       0.001 ms        0.001 ms       807373
BM_GetDictCodesWithFilterBatch32/920       0.001 ms        0.001 ms       802017
BM_GetDictCodesWithFilterBatch32/940       0.001 ms        0.001 ms       799113
BM_GetDictCodesWithFilterBatch32/960       0.001 ms        0.001 ms       779697
BM_GetDictCodesWithFilterBatch32/980       0.001 ms        0.001 ms       796140
BM_GetDictCodesWithFilterBatch32/1000      0.001 ms        0.001 ms       788616
*/
} // namespace parquet
} // namespace starrocks

BENCHMARK_MAIN();
